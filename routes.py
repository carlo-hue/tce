from __future__ import annotations

import csv
import io
import json
import logging
import math
import re
import xml.etree.ElementTree as ET
from pathlib import PurePosixPath
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from flask import Blueprint, Response, current_app, jsonify, redirect, render_template, request, url_for

from .models import SQLiteCache
from .config import settings
from .exoarchive import ExoArchiveClient
from .http_client import get_text_with_timeout
from .mast_dv import MastDVClient
from .mast_tce import MastTceError, download_sector_tce_csv, parse_tce_csv
from .ranking import RANKING_VERSION, RankingInput, compute_base_v1

logger = logging.getLogger(__name__)

_cache = SQLiteCache(settings.cache_path)
_exo = ExoArchiveClient(_cache)
_dv = MastDVClient(_cache)

INITIAL_QUERY_KEYS = ("tic_id", "toi", "target", "gaia_id", "ra", "dec")


def _sort_items(items: list[dict], sort_by: str) -> list[dict]:
    if sort_by == "mes":
        return sorted(items, key=lambda x: (-float(x["mes"]), -float(x["score"]), float(x["depth_pct"])))
    if sort_by == "depth_pct":
        return sorted(items, key=lambda x: (float(x["depth_pct"]), -float(x["score"]), -float(x["mes"])))
    if sort_by == "period":
        return sorted(items, key=lambda x: ((float(x["period_days"]) if x["period_days"] is not None else float("inf")), -float(x["score"])))
    return sorted(items, key=lambda x: (-float(x["score"]), -float(x["mes"]), float(x["depth_pct"])))


def _optional_float(name: str) -> float | None:
    raw = request.args.get(name)
    if raw is None or str(raw).strip() == "":
        return None
    return float(raw)


def _bool_arg(name: str, default: bool = False) -> bool:
    raw = request.args.get(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _error(message: str, code: int = 400):
    return jsonify({"error": message}), code


def _initial_query_context() -> dict[str, str]:
    context: dict[str, str] = {}
    for key in INITIAL_QUERY_KEYS:
        value = str(request.args.get(key, "")).strip()
        if value:
            context[key] = value
    return context


def _optional_url(endpoint: str) -> str:
    if endpoint not in current_app.view_functions:
        return "#"
    try:
        return url_for(endpoint)
    except Exception:
        return "#"


def create_blueprint() -> Blueprint:
    blueprint = Blueprint(
        "tess_tce",
        __name__,
        url_prefix="/tess-tce",
        template_folder="templates",
        static_folder="static",
    )

    @blueprint.get("/")
    def index():
        initial_query = _initial_query_context()
        return render_template(
            "tess_tce/index.html",
            api_base=url_for("tess_tce.index").rstrip("/") + "/api",
            catalog_query_url=_optional_url("catalog.api_query_catalogs"),
            initial_query=initial_query,
            module_links={
                "admin": _optional_url("admin.list_projects"),
                "variable_stars": _optional_url("variable_stars.index"),
                "exoplanets": _optional_url("exoplanets.index"),
                "field_star_map": _optional_url("field_star_map.index"),
                "galassie_nane": _optional_url("galassie_nane.index"),
                "tess_tce": url_for("tess_tce.index"),
            },
        )

    @blueprint.get("/api/health")
    def api_health():
        return jsonify({"status": "ok", "module": "tess_tce"})

    @blueprint.get("/api/tce")
    def api_tce():
        try:
            sector = int(request.args.get("sector", "").strip())
        except Exception:
            return _error("sector (int) e' obbligatorio", 422)

        raw_limit = request.args.get("limit")
        limit: int | None
        if raw_limit is None or str(raw_limit).strip() == "":
            limit = None
        else:
            try:
                limit = int(str(raw_limit).strip())
            except Exception:
                return _error("limit deve essere int", 422)
            limit = max(1, min(limit, 5000))

        include_dv = _bool_arg("include_dv", False)
        include_status = _bool_arg("include_status", False)
        use_cdpp = _bool_arg("use_cdpp", False)
        sort_by = request.args.get("sort_by", "score")
        try:
            min_mes = float(request.args.get("min_mes", "7.1"))
            max_depth_pct = _optional_float("max_depth_pct")
            min_duration_hr = _optional_float("min_duration_hr")
            max_duration_hr = _optional_float("max_duration_hr")
        except ValueError:
            return _error("Parametri numerici non validi", 422)

        if max_duration_hr is not None and min_duration_hr is not None and max_duration_hr < min_duration_hr:
            return _error("max_duration_hr must be >= min_duration_hr", 422)

        try:
            csv_text = download_sector_tce_csv(sector)
            parsed = parse_tce_csv(csv_text, sector)
        except MastTceError as exc:
            logger.warning("TCE CSV download/parse failed sector=%s: %s", sector, exc)
            return jsonify({"error": str(exc)}), 502
        except Exception as exc:  # noqa: BLE001
            logger.exception("Unexpected TCE error sector=%s", sector)
            return jsonify({"error": f"Unexpected error: {exc}"}), 500

        items: list[dict] = []
        for row in parsed:
            if max_depth_pct is not None and row.depth_pct > max_depth_pct:
                continue
            if min_duration_hr is not None and row.duration_hr < min_duration_hr:
                continue
            if max_duration_hr is not None and row.duration_hr > max_duration_hr:
                continue
            try:
                ranking = compute_base_v1(
                    RankingInput(mes=row.mes, depth_pct=row.depth_pct, duration_hr=row.duration_hr, cdpp_ppm=row.cdpp_ppm),
                    min_mes=min_mes,
                    use_cdpp=use_cdpp,
                )
            except ValueError as exc:
                return _error(str(exc), 422)
            items.append(
                {
                    "rank": None,
                    "sector": row.sector,
                    "tic_id": row.tic_id,
                    "tce_id": row.tce_id,
                    "period_days": row.period_days,
                    "duration_hr": row.duration_hr,
                    "depth_pct": row.depth_pct,
                    "mes": row.mes,
                    "cdpp_ppm": row.cdpp_ppm,
                    "score": ranking["score"],
                    "ranking_version": ranking["ranking_version"],
                    "ranking_components": {
                        "mes_norm": ranking["mes_norm"],
                        "depth_score": ranking["depth_score"],
                        "duration_score": ranking["duration_score"],
                        "snr_norm": ranking["snr_norm"],
                    },
                    "status": "NOT_REQUESTED",
                    "status_reason": "Status TAP non ancora richiesto (fetch on-demand disponibile)",
                    "status_available_unknown": True,
                    "dv_products": None,
                    "dv_status": "NOT_REQUESTED",
                    "dv_error": None,
                    "dv_error_type": None,
                    "dv_available_unknown": True,
                }
            )

        sorted_items = _sort_items(items, sort_by)
        selected = sorted_items if limit is None else sorted_items[:limit]
        notes: list[str] = []

        if include_status and selected:
            status_rows = _exo.resolve_status_batch([item["tic_id"] for item in selected], max_workers=5)
            by_tic_status = {tic: status for tic, status in status_rows}
            for item in selected:
                status = by_tic_status.get(item["tic_id"]) or {"status": "UNKNOWN", "reason": "Missing batch status result"}
                item["status"] = status.get("status", "UNKNOWN")
                item["status_reason"] = status.get("reason", "")
                item["status_available_unknown"] = False
            notes.append("include_status=true esegue TAP status enrichment (piu' lento).")

        if include_dv and selected:
            batch = _dv.get_dv_products_batch([item["tic_id"] for item in selected])
            by_tic = {
                tic: (products, dv_status, dv_error, dv_error_type)
                for tic, products, _cached, dv_status, dv_error, dv_error_type in batch
            }
            for item in selected:
                products, dv_status, dv_error, dv_error_type = by_tic.get(
                    item["tic_id"],
                    ([], "UNAVAILABLE", "DV batch result missing", "missing_batch_result"),
                )
                item["dv_products"] = products
                item["dv_status"] = dv_status
                item["dv_error"] = dv_error
                item["dv_error_type"] = dv_error_type
                item["dv_available_unknown"] = False
            notes.append("include_dv=true esegue enrichment DV (lento). Flusso consigliato: 2-step.")

        for idx, item in enumerate(selected, start=1):
            item["rank"] = idx

        return jsonify(
            {
                "sector": sector,
                "count": len(selected),
                "limit": limit,
                "ranking_version": RANKING_VERSION,
                "include_dv": include_dv,
                "params": {
                    "use_cdpp": use_cdpp,
                    "include_status": include_status,
                    "min_mes": min_mes,
                    "max_depth_pct": max_depth_pct,
                    "min_duration_hr": min_duration_hr,
                    "max_duration_hr": max_duration_hr,
                    "sort_by": sort_by,
                },
                "items": selected,
                "notes": notes,
            }
        )

    @blueprint.post("/api/dv-products/batch")
    def api_dv_batch():
        payload = request.get_json(silent=True) or {}
        raw_tics = payload.get("tics", [])
        if not isinstance(raw_tics, list):
            return _error("body.tics must be a list", 422)
        seen: set[str] = set()
        tics: list[str] = []
        for raw in raw_tics:
            tic = str(raw).strip()
            if not tic or tic in seen:
                continue
            seen.add(tic)
            tics.append(tic)
        if not tics:
            return jsonify({"results": []})
        rows = _dv.get_dv_products_batch(tics)
        return jsonify(
            {
                "results": [
                    {
                        "tic_id": tic,
                        "products": products,
                        "cached": cached,
                        "dv_status": dv_status,
                        "dv_error": dv_error,
                        "dv_error_type": dv_error_type,
                    }
                    for tic, products, cached, dv_status, dv_error, dv_error_type in rows
                ]
            }
        )

    @blueprint.post("/api/status/batch")
    def api_status_batch():
        payload = request.get_json(silent=True) or {}
        raw_tics = payload.get("tics", [])
        if not isinstance(raw_tics, list):
            return _error("body.tics must be a list", 422)
        seen: set[str] = set()
        tics: list[str] = []
        for raw in raw_tics:
            tic = str(raw).strip()
            if not tic or tic in seen:
                continue
            seen.add(tic)
            tics.append(tic)
        if not tics:
            return jsonify({"results": []})
        rows = _exo.resolve_status_batch(tics, max_workers=5)
        return jsonify({"results": [{"tic_id": tic, **status} for tic, status in rows]})

    @blueprint.get("/api/tce/<tic_id>/status")
    def api_tic_status(tic_id: str):
        try:
            status = _exo.resolve_status(tic_id)
        except Exception as exc:  # noqa: BLE001
            status = {"status": "UNKNOWN", "reason": f"Status lookup failed: {exc}"}
        return jsonify({"tic_id": tic_id, **status})

    @blueprint.get("/api/tce/<tic_id>/dv-products")
    def api_tic_dv_products(tic_id: str):
        try:
            products, cached, dv_status, dv_error, dv_error_type = _dv.get_dv_products(tic_id)
        except Exception as exc:  # noqa: BLE001
            logger.warning("DV fetch failed tic=%s: %s", tic_id, exc)
            return jsonify(
                {
                    "tic_id": tic_id,
                    "products": [],
                    "cached": False,
                    "dv_status": "RETRY",
                    "dv_error": f"timeout/network error: {exc}",
                    "dv_error_type": "timeout_or_network",
                }
            )
        return jsonify(
            {
                "tic_id": tic_id,
                "products": products,
                "cached": cached,
                "dv_status": dv_status,
                "dv_error": dv_error,
                "dv_error_type": dv_error_type,
            }
        )

    @blueprint.post("/api/cache/clear")
    def api_cache_clear():
        payload = request.get_json(silent=True) or {}
        namespaces = payload.get("namespaces") or ["mast_dv_products", "exo_status", "gaia_lookup", "vsx_lookup", "tic_catalog"]
        if not isinstance(namespaces, list) or not all(isinstance(ns, str) and ns.strip() for ns in namespaces):
            return _error("body.namespaces must be a non-empty list of strings", 422)
        summary: dict[str, int] = {}
        total_deleted = 0
        for namespace in namespaces:
            deleted = _cache.clear(namespace.strip())
            summary[namespace.strip()] = deleted
            total_deleted += deleted
        return jsonify(
            {
                "ok": True,
                "cleared_namespaces": summary,
                "total_deleted": total_deleted,
                "cache_path": settings.cache_path,
            }
        )

    @blueprint.get("/api/xml-view")
    def api_xml_view():
        url = request.args.get("url", "").strip()
        if not url:
            return _error("url query param required", 422)
        _host_or_none, validated = _validate_allowed_external_url(url)
        if _host_or_none is None:
            return validated
        try:
            req = Request(url, headers={"User-Agent": "AGATA-TESS-TCE/1.0"})
            with urlopen(req, timeout=settings.http_timeout_seconds) as resp:
                content_type = resp.headers.get("Content-Type", "")
                payload = resp.read().decode("utf-8", errors="replace")
        except Exception as exc:  # noqa: BLE001
            logger.warning("XML view fetch failed: %s", exc)
            return jsonify({"error": f"Failed to fetch XML: {exc}"}), 502
        return jsonify({"url": url, "content_type": content_type, "xml_text": payload})

    @blueprint.get("/api/gaia-lookup")
    def api_gaia_lookup():
        tic_id = request.args.get("tic_id", "").strip()
        radius_raw = request.args.get("radius_arcsec", "5").strip()
        if tic_id:
            if not tic_id.isdigit():
                return _error("tic_id must be numeric", 422)
            try:
                radius_arcsec = float(radius_raw or "5")
            except ValueError:
                return _error("radius_arcsec must be numeric", 422)
            radius_arcsec = max(0.5, min(radius_arcsec, 30.0))
            return jsonify(_gaia_lookup_cached_by_tic(tic_id, radius_arcsec))
        return _error("tic_id query param required", 422)

    @blueprint.get("/api/vsx-lookup")
    def api_vsx_lookup():
        tic_id = request.args.get("tic_id", "").strip()
        radius_raw = request.args.get("radius_arcsec", "5").strip()
        if tic_id:
            if not tic_id.isdigit():
                return _error("tic_id must be numeric", 422)
            try:
                radius_arcsec = float(radius_raw or "5")
            except ValueError:
                return _error("radius_arcsec must be numeric", 422)
            radius_arcsec = max(0.5, min(radius_arcsec, 30.0))
            return jsonify(_vsx_lookup_cached_by_tic(tic_id, radius_arcsec))
        return _error("tic_id query param required", 422)

    @blueprint.get("/api/download")
    def api_download_redirect():
        url = request.args.get("url", "").strip()
        if not url:
            return _error("url query param required", 422)
        _host_or_none, validated = _validate_allowed_external_url(url)
        if _host_or_none is None:
            return validated
        disposition = str(request.args.get("disposition", "")).strip().lower()
        if disposition == "inline":
            try:
                req = Request(url, headers={"User-Agent": "AGATA-TESS-TCE/1.0"})
                with urlopen(req, timeout=settings.http_timeout_seconds) as resp:
                    payload = resp.read()
                    content_type = resp.headers.get("Content-Type", "application/octet-stream")
            except Exception as exc:  # noqa: BLE001
                logger.warning("Inline download fetch failed: %s", exc)
                return jsonify({"error": f"Failed to fetch inline document: {exc}"}), 502
            parsed = urlparse(url)
            filename = str(request.args.get("filename", "")).strip() or PurePosixPath(parsed.path or "").name or "document"
            if filename.lower().endswith(".pdf"):
                content_type = "application/pdf"
            elif filename.lower().endswith(".xml"):
                content_type = "application/xml; charset=utf-8"
            response = Response(payload, content_type=content_type)
            response.headers["Content-Disposition"] = f'inline; filename="{filename}"'
            response.headers["X-Content-Type-Options"] = "nosniff"
            return response
        return redirect(url, code=307)

    @blueprint.get("/api/README")
    def api_readme() -> Response:
        return Response(
            "AGATA TESS TCE module API: /api/tce, /api/dv-products/batch, /api/tce/<tic>/dv-products, /api/download",
            mimetype="text/plain",
        )

    return blueprint


def _validate_allowed_external_url(url: str) -> tuple[str, str] | tuple[None, tuple[Response, int]]:
    parsed = urlparse(url)
    if parsed.scheme.lower() != "https":
        return None, _error("Only https URLs are allowed", 400)
    host = (parsed.hostname or "").lower()
    if host not in set(settings.allowed_download_hosts):
        return None, _error(f"Host not allowed: {host or 'unknown'}", 400)
    return host, url


def _tap_json_rows(url: str, query: str, timeout: float) -> list[dict]:
    raw = get_text_with_timeout(
        url,
        params={"REQUEST": "doQuery", "LANG": "ADQL", "FORMAT": "json", "QUERY": query},
        timeout=timeout,
    )
    if raw.lstrip().startswith("<"):
        raise RuntimeError(_summarize_tap_error(raw))
    payload = json.loads(raw or "{}")
    cols = [c.get("name") for c in (payload.get("metadata") or [])]
    rows = payload.get("data") or []
    return [dict(zip(cols, row)) for row in rows if isinstance(row, list)]


def _tap_csv_rows(url: str, query: str, timeout: float) -> list[dict[str, str]]:
    raw = get_text_with_timeout(
        url,
        params={"REQUEST": "doQuery", "LANG": "ADQL", "FORMAT": "csv", "QUERY": query},
        timeout=timeout,
    )
    if raw.lstrip().startswith("<"):
        raise RuntimeError(_summarize_tap_error(raw))
    text = raw.strip()
    if not text:
        return []
    reader = csv.DictReader(io.StringIO(text))
    return [{str(k): ("" if v is None else str(v).strip()) for k, v in row.items()} for row in reader]


def _summarize_tap_error(raw: str) -> str:
    text = str(raw or "").strip()
    if not text:
        return "TAP error: empty response"
    if not text.startswith("<"):
        compact = re.sub(r"\s+", " ", text)
        return compact[:220]
    try:
        root = ET.fromstring(text)
    except ET.ParseError:
        compact = re.sub(r"\s+", " ", text)
        return compact[:220]
    infos = []
    for elem in root.iter():
        tag = elem.tag.split("}")[-1].lower()
        if tag == "info":
            name = str(elem.attrib.get("name", "")).strip()
            value = str(elem.attrib.get("value", "")).strip()
            body = " ".join(str(elem.text or "").split())
            part = body or value or name
            if part:
                infos.append(part)
    if infos:
        return f"TAP error: {' | '.join(infos[:2])}"[:220]
    compact = re.sub(r"\s+", " ", text)
    return compact[:220]


def _row_float(row: dict, *keys: str) -> float | None:
    for key in keys:
        value = row.get(key)
        if value in (None, ""):
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


def _angular_sep_arcsec(ra1_deg: float, dec1_deg: float, ra2_deg: float, dec2_deg: float) -> float:
    dra = math.radians(ra1_deg - ra2_deg) * math.cos(math.radians((dec1_deg + dec2_deg) / 2.0))
    ddec = math.radians(dec1_deg - dec2_deg)
    return math.degrees(math.sqrt(dra * dra + ddec * ddec)) * 3600.0


def _tic_catalog_row_cached(tic_id: str) -> dict | None:
    tic_id = str(tic_id).strip()
    cached = _cache.get("tic_catalog", tic_id)
    if isinstance(cached, dict):
        return cached
    q_viz = (
        'SELECT TOP 1 TIC, GAIA, RAJ2000, DEJ2000, Tmag '
        f'FROM "IV/38/tic" WHERE TIC = {int(tic_id)}'
    )
    rows = _tap_json_rows(
        "https://tapvizier.cds.unistra.fr/TAPVizieR/tap/sync",
        q_viz,
        min(settings.http_timeout_seconds, 8),
    )
    if not rows:
        return None
    row = rows[0]
    result = {
        "tic_id": tic_id,
        "gaia_dr2_id": str(row.get("GAIA") or "").strip() or None,
        "tic_ra_deg": _row_float(row, "RAJ2000"),
        "tic_dec_deg": _row_float(row, "DEJ2000"),
        "tic_tmag": _row_float(row, "Tmag"),
    }
    _cache.set("tic_catalog", tic_id, result, ttl_seconds=86400 * 7)
    return result


def _vsx_query_by_position(tic_ra_deg: float, tic_dec_deg: float, radius_arcsec: float) -> dict | None:
    radius_deg = radius_arcsec / 3600.0
    timeout = min(settings.http_timeout_seconds, 8)
    coordinate_variants = [
        ("RAJ2000", "DEJ2000"),
        ("_RAJ2000", "_DEJ2000"),
        ('"RAJ2000"', '"DEJ2000"'),
        ('"RA_ICRS"', '"DE_ICRS"'),
    ]
    for ra_col, dec_col in coordinate_variants:
        query = (
            "SELECT TOP 1 OID, Name, Type, Period, "
            f"{ra_col} AS src_ra_deg, {dec_col} AS src_dec_deg "
            'FROM "B/vsx/vsx" '
            "WHERE 1 = CONTAINS("
            f"POINT('ICRS', {ra_col}, {dec_col}), "
            f"CIRCLE('ICRS', {tic_ra_deg}, {tic_dec_deg}, {radius_deg})"
            ")"
        )
        try:
            rows = _tap_json_rows("https://tapvizier.cds.unistra.fr/TAPVizieR/tap/sync", query, timeout)
        except Exception:
            continue
        if not rows:
            continue
        row = rows[0]
        src_ra = _row_float(row, "src_ra_deg")
        src_dec = _row_float(row, "src_dec_deg")
        sep_arcsec = (
            _angular_sep_arcsec(tic_ra_deg, tic_dec_deg, src_ra, src_dec)
            if src_ra is not None and src_dec is not None
            else None
        )
        return {
            "oid": str(row.get("OID") or "").strip() or None,
            "name": str(row.get("Name") or "").strip() or None,
            "vsx_type": str(row.get("Type") or "").strip() or None,
            "period_days": _row_float(row, "Period"),
            "sep_arcsec": sep_arcsec,
        }
    return None


def _gaia_varisum_by_source_id(source_id: str) -> dict | None:
    safe_source_id = "".join(ch for ch in str(source_id or "").strip() if ch.isdigit())
    if not safe_source_id:
        return None
    variability_flag_columns = ("VCR", "VRRLyr", "VCep", "VPN", "VST", "VLPV", "VEB", "VRM", "VMSO", "VAGN", "Vmicro", "VCC")
    rows = _tap_json_rows(
        "https://tapvizier.cds.unistra.fr/TAPVizieR/tap/sync",
        (
            f'SELECT TOP 1 Source, {", ".join(variability_flag_columns)} '
            f'FROM "I/358/varisum" WHERE Source = {safe_source_id}'
        ),
        min(settings.http_timeout_seconds, 8),
    )
    if not rows:
        return None
    row = rows[0]
    variability_flags = [name for name in variability_flag_columns if str(row.get(name) or "0").strip() not in {"", "0"}]
    return {
        "source_id": str(row.get("Source") or safe_source_id).strip() or safe_source_id,
        "gaia_variability_type": ", ".join(variability_flags) if variability_flags else None,
        "gaia_variability_catalog": "Gaia DR3 varisum",
    }


def _vsx_lookup_cached_by_tic(tic_id: str, radius_arcsec: float = 5.0) -> dict:
    tic_id = str(tic_id).strip()
    radius_arcsec = max(0.5, min(float(radius_arcsec), 30.0))
    key = f"{tic_id}|r={radius_arcsec:.3f}"
    cached = _cache.get("vsx_lookup", key)
    if isinstance(cached, dict):
        return cached
    try:
        tic_row = _tic_catalog_row_cached(tic_id)
        if not tic_row:
            result = {
                "status": "NOT_FOUND",
                "reason": "TIC non trovato in VizieR IV/38/tic",
                "tic_id": tic_id,
                "search_radius_arcsec": radius_arcsec,
            }
            _cache.set("vsx_lookup", key, result, ttl_seconds=86400 * 7)
            return result
        tic_ra_deg = _row_float(tic_row, "tic_ra_deg")
        tic_dec_deg = _row_float(tic_row, "tic_dec_deg")
        if tic_ra_deg is None or tic_dec_deg is None:
            result = {
                "status": "NOT_FOUND",
                "reason": "Coordinate TIC non disponibili per il lookup VSX",
                "tic_id": tic_id,
                "tic_ra_deg": tic_row.get("tic_ra_deg"),
                "tic_dec_deg": tic_row.get("tic_dec_deg"),
                "search_radius_arcsec": radius_arcsec,
            }
            _cache.set("vsx_lookup", key, result, ttl_seconds=86400 * 7)
            return result
        vsx_row = _vsx_query_by_position(tic_ra_deg, tic_dec_deg, radius_arcsec)
        result = {
            "status": "OK",
            "tic_id": tic_id,
            "tic_ra_deg": tic_ra_deg,
            "tic_dec_deg": tic_dec_deg,
            "search_radius_arcsec": radius_arcsec,
            "vsx_variable_known": bool(vsx_row),
            "vsx_catalog": "AAVSO VSX",
            "vsx_type": vsx_row.get("vsx_type") if vsx_row else None,
            "vsx_period_days": vsx_row.get("period_days") if vsx_row else None,
            "vsx_name": vsx_row.get("name") if vsx_row else None,
            "vsx_oid": vsx_row.get("oid") if vsx_row else None,
            "sep_arcsec": vsx_row.get("sep_arcsec") if vsx_row else None,
        }
        _cache.set("vsx_lookup", key, result, ttl_seconds=86400 * 7)
        return result
    except Exception as exc:  # noqa: BLE001
        result = {
            "status": "ERROR",
            "reason": f"VSX lookup failed: {exc}",
            "tic_id": tic_id,
            "search_radius_arcsec": radius_arcsec,
        }
        _cache.set("vsx_lookup", key, result, ttl_seconds=3600)
        return result


def _gaia_source_details(source_id: str, release: str = "DR3") -> dict:
    source_table = "gaiadr3.gaia_source" if str(release).upper() == "DR3" else "gaiadr2.gaia_source"
    rows = _tap_csv_rows(
        "https://gea.esac.esa.int/tap-server/tap/sync",
        (
            "SELECT TOP 1 source_id, ra, dec, phot_g_mean_mag, bp_rp, parallax, radial_velocity "
            f"FROM {source_table} WHERE source_id = {source_id}"
        ),
        min(settings.http_timeout_seconds, 8),
    )
    return rows[0] if rows else {}


def _gaia_lookup_cached_by_tic(tic_id: str, radius_arcsec: float = 5.0) -> dict:
    tic_id = str(tic_id).strip()
    radius_arcsec = max(0.5, min(float(radius_arcsec), 30.0))
    key = f"{tic_id}|r={radius_arcsec:.3f}"
    cached = _cache.get("gaia_lookup", key)
    if isinstance(cached, dict):
        return cached
    try:
        # 1) VizieR TIC catalog (IV/38/tic) -> GAIA (DR2 identifier)
        q_viz = (
            'SELECT TOP 1 TIC, GAIA, RAJ2000, DEJ2000, Tmag '
            f'FROM "IV/38/tic" WHERE TIC = {int(tic_id)}'
        )
        viz_rows = _tap_json_rows(
            "https://tapvizier.cds.unistra.fr/TAPVizieR/tap/sync",
            q_viz,
            min(settings.http_timeout_seconds, 8),
        )
        if not viz_rows:
            result = {
                "status": "NOT_FOUND",
                "reason": "TIC non trovato in VizieR IV/38/tic",
                "tic_id": tic_id,
                "search_radius_arcsec": radius_arcsec,
            }
            _cache.set("gaia_lookup", key, result, ttl_seconds=86400 * 7)
            return result
        viz_row = viz_rows[0]
        gaia_dr2_id = str(viz_row.get("GAIA") or "").strip()
        tic_ra_deg = _row_float(viz_row, "RAJ2000")
        tic_dec_deg = _row_float(viz_row, "DEJ2000")
        tic_mag = _row_float(viz_row, "Tmag")
        if not gaia_dr2_id:
            result = {
                "status": "NOT_FOUND",
                "reason": "Campo GAIA non valorizzato per questo TIC in VizieR IV/38/tic",
                "tic_id": tic_id,
                "tic_ra_deg": tic_ra_deg,
                "tic_dec_deg": tic_dec_deg,
                "tic_tmag": tic_mag,
                "search_radius_arcsec": radius_arcsec,
            }
            _cache.set("gaia_lookup", key, result, ttl_seconds=86400 * 7)
            return result

        # 2) Map DR2 -> DR3 when available
        gaia_dr3_id = None
        try:
            q_map = f"SELECT TOP 1 dr3_source_id FROM gaiadr3.dr2_neighbourhood WHERE dr2_source_id = {gaia_dr2_id}"
            map_rows = _tap_csv_rows(
                "https://gea.esac.esa.int/tap-server/tap/sync",
                q_map,
                min(settings.http_timeout_seconds, 8),
            )
            if map_rows and map_rows[0].get("dr3_source_id"):
                gaia_dr3_id = str(map_rows[0].get("dr3_source_id")).strip()
        except Exception:
            gaia_dr3_id = None

        # 3) Gaia source details (prefer DR3 if mapped)
        gaia_details = {}
        effective_source_id = gaia_dr3_id or gaia_dr2_id
        effective_release = "DR3" if gaia_dr3_id else "DR2"
        if effective_source_id:
            try:
                gaia_details = _gaia_source_details(effective_source_id, "DR3" if gaia_dr3_id else "DR2")
            except Exception:
                gaia_details = {}

        selected_source_id = str(gaia_details.get("source_id") or effective_source_id or "").strip() or None
        gaia_source_ra = _row_float(gaia_details, "ra")
        gaia_source_dec = _row_float(gaia_details, "dec")
        selected_gaia_mag = _row_float(gaia_details, "phot_g_mean_mag")
        var_info = {
            "gaia_variable_known": False,
            "gaia_variability_type": None,
            "gaia_variability_period_days": None,
            "gaia_variability_mag_g": selected_gaia_mag,
            "gaia_variability_mag_bp": None,
            "gaia_variability_mag_rp": None,
            "gaia_variability_catalog": None,
        }
        if selected_source_id:
            variable_row = _gaia_varisum_by_source_id(selected_source_id)
            if variable_row:
                selected_source_id = variable_row.get("source_id") or selected_source_id
                selected_gaia_mag = variable_row.get("gaia_variability_mag_g") or selected_gaia_mag
                var_info = {
                    "gaia_variable_known": True,
                    "gaia_variability_type": variable_row.get("gaia_variability_type"),
                    "gaia_variability_period_days": None,
                    "gaia_variability_mag_g": None,
                    "gaia_variability_mag_bp": None,
                    "gaia_variability_mag_rp": None,
                    "gaia_variability_catalog": variable_row.get("gaia_variability_catalog"),
                }

        sep_arcsec = None
        if tic_ra_deg is not None and tic_dec_deg is not None and gaia_source_ra is not None and gaia_source_dec is not None:
            sep_arcsec = _angular_sep_arcsec(tic_ra_deg, tic_dec_deg, gaia_source_ra, gaia_source_dec)

        result = {
            "status": "OK",
            "tic_id": tic_id,
            "source_id": selected_source_id,
            "gaia_release": effective_release,
            "gaia_source_id_vizier_dr2": (gaia_dr2_id or None),
            "gaia_source_id_dr3": (gaia_dr3_id or None),
            "tic_ra_deg": tic_ra_deg,
            "tic_dec_deg": tic_dec_deg,
            "tic_tmag": tic_mag,
            "gaia_source_ra_deg": gaia_source_ra,
            "gaia_source_dec_deg": gaia_source_dec,
            "sep_arcsec": sep_arcsec,
            "search_radius_arcsec": radius_arcsec,
            "phot_g_mean_mag": selected_gaia_mag,
            "bp_rp": gaia_details.get("bp_rp"),
            "parallax": gaia_details.get("parallax"),
            "radial_velocity": gaia_details.get("radial_velocity"),
            **var_info,
        }
        _cache.set("gaia_lookup", key, result, ttl_seconds=86400 * 7)
        return result
    except Exception as exc:  # noqa: BLE001
        result = {
            "status": "ERROR",
            "reason": f"Gaia lookup failed: {exc}",
            "tic_id": tic_id,
            "search_radius_arcsec": radius_arcsec,
        }
        _cache.set("gaia_lookup", key, result, ttl_seconds=3600)
        return result
