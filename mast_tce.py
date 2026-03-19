from __future__ import annotations

import csv
import hashlib
import io
import re
from dataclasses import dataclass
from urllib.parse import urljoin

from .config import settings
from .http_client import get_text


class MastTceError(RuntimeError):
    pass


@dataclass(frozen=True)
class ParsedTce:
    sector: int
    tic_id: str
    tce_id: str
    mes: float
    depth_pct: float
    duration_hr: float
    period_days: float | None
    cdpp_ppm: float | None
    raw: dict[str, str]


def _get_str(row: dict[str, str], aliases: list[str]) -> str | None:
    for key in aliases:
        value = row.get(key)
        if value is None:
            continue
        value = str(value).strip()
        if value:
            return value
    return None


def _get_float(row: dict[str, str], aliases: list[str]) -> float | None:
    for key in aliases:
        value = row.get(key)
        if value in (None, ""):
            continue
        try:
            return float(str(value).strip())
        except ValueError:
            continue
    return None


def _normalize_depth_pct(row: dict[str, str]) -> float | None:
    if row.get("depth_pct") not in (None, ""):
        val = _get_float(row, ["depth_pct"])
        return None if val is None else max(0.0, val)
    val = _get_float(row, ["tce_depth", "depth", "transit_depth"])
    if val is None:
        return None
    if val > 100.0:
        return max(0.0, val / 1e4)
    if 0.0 < val < 1.0:
        return max(0.0, val * 100.0)
    return max(0.0, val)


def _normalize_duration_hr(row: dict[str, str]) -> float | None:
    val = _get_float(row, ["duration_hr", "tce_duration_hr", "duration_hours"])
    if val is not None:
        return max(0.0, val)
    days = _get_float(row, ["duration_days", "tce_duration_days"])
    if days is not None:
        return max(0.0, days * 24.0)
    val = _get_float(row, ["tce_duration", "duration", "transit_duration"])
    if val is None:
        return None
    return max(0.0, val)


def _sector_url(sector: int) -> str:
    return settings.mast_tce_bulk_url_template.format(sector=sector, sector02=f"{sector:02d}", sector03=f"{sector:03d}")


def _stable_tce_variant_suffix(
    tic_id: str,
    period_days: float | None,
    mes: float,
    depth_pct: float,
    duration_hr: float,
) -> str:
    fingerprint = "|".join(
        [
            str(tic_id),
            "" if period_days is None else f"{float(period_days):.8f}",
            f"{float(mes):.8f}",
            f"{float(depth_pct):.8f}",
            f"{float(duration_hr):.8f}",
        ]
    )
    return hashlib.sha1(fingerprint.encode("utf-8")).hexdigest()[:8]


def _discover_sector_url_from_bulk_index(sector: int) -> str:
    html = get_text(settings.mast_tce_bulk_index_url)
    # Current TCE bulk filenames usually look like:
    # tess2023xx-s0080-s0080_dvr-tcestats.csv
    sector_tag = f"s{sector:04d}"
    patterns = [
        re.compile(r'href="([^"]*%s[^"]*tcestats\.csv)"' % re.escape(sector_tag), re.IGNORECASE),
        re.compile(r'href="([^"]*%s[^"]*tce[^"]*\.csv)"' % re.escape(sector_tag), re.IGNORECASE),
    ]
    for pattern in patterns:
        match = pattern.search(html)
        if match:
            return urljoin(settings.mast_tce_bulk_index_url, match.group(1))
    raise MastTceError(
        f"Unable to discover TCE bulk CSV for sector {sector} from {settings.mast_tce_bulk_index_url}"
    )


def download_sector_tce_csv(sector: int) -> str:
    primary_url = _sector_url(sector)
    try:
        return get_text(primary_url)
    except Exception as primary_exc:  # noqa: BLE001
        try:
            discovered_url = _discover_sector_url_from_bulk_index(sector)
            return get_text(discovered_url)
        except Exception as fallback_exc:  # noqa: BLE001
            raise MastTceError(
                f"Failed to download TCE CSV sector={sector}. "
                f"Legacy URL error: {primary_exc}. Fallback discovery error: {fallback_exc}"
            ) from fallback_exc


def parse_tce_csv(csv_text: str, sector: int) -> list[ParsedTce]:
    lines = csv_text.splitlines()
    header_idx = 0
    for idx, line in enumerate(lines):
        if not line.strip():
            continue
        if line.lstrip().startswith("#"):
            continue
        header_idx = idx
        break
    reader = csv.DictReader(io.StringIO("\n".join(lines[header_idx:])))
    if not reader.fieldnames:
        raise MastTceError("CSV has no header")
    out: list[ParsedTce] = []
    seen_row_fingerprints: set[tuple[str, str, str, str, str]] = set()
    seen_tce_ids: set[str] = set()
    for raw_row in reader:
        row = {str(k).strip(): ("" if v is None else str(v).strip()) for k, v in raw_row.items()}
        tic_id = _get_str(row, ["tic_id", "ticid", "tic", "TICID"])
        mes = _get_float(row, ["MES", "mes", "tce_mes", "tce_ws_maxmes", "tce_model_snr", "tce_ws_mesmedian"])
        depth_pct = _normalize_depth_pct(row)
        duration_hr = _normalize_duration_hr(row)
        if not tic_id or mes is None or depth_pct is None or duration_hr is None:
            continue
        period_days = _get_float(row, ["period_days", "tce_period", "period"])
        cdpp_ppm = _get_float(row, ["cdpp_ppm", "cdpp", "tce_cdpp", "robcdpp"])
        row_fingerprint = (
            str(tic_id),
            "" if period_days is None else f"{float(period_days):.8f}",
            f"{float(mes):.8f}",
            f"{float(depth_pct):.8f}",
            f"{float(duration_hr):.8f}",
        )
        if row_fingerprint in seen_row_fingerprints:
            continue
        seen_row_fingerprints.add(row_fingerprint)

        base_tce_id = _get_str(row, ["tce_id", "tce", "event", "planetNumber"]) or f"{tic_id}-1"
        tce_id = str(base_tce_id)
        if tce_id in seen_tce_ids:
            tce_id = f"{base_tce_id}~{_stable_tce_variant_suffix(tic_id, period_days, float(mes), float(depth_pct), float(duration_hr))}"
        seen_tce_ids.add(tce_id)
        out.append(
            ParsedTce(
                sector=sector,
                tic_id=tic_id,
                tce_id=str(tce_id),
                mes=float(mes),
                depth_pct=float(depth_pct),
                duration_hr=float(duration_hr),
                period_days=None if period_days is None else float(period_days),
                cdpp_ppm=None if cdpp_ppm is None else float(cdpp_ppm),
                raw=row,
            )
        )
    return out
