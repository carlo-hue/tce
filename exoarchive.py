from __future__ import annotations

import csv
import io
from concurrent.futures import ThreadPoolExecutor, as_completed

from .config import settings
from .http_client import get_text_with_timeout
from .models import SQLiteCache


class ExoArchiveClient:
    def __init__(self, cache: SQLiteCache) -> None:
        self.cache = cache

    def resolve_status(self, tic_id: str) -> dict[str, str]:
        cached = self.cache.get("exo_status", tic_id)
        if cached is not None:
            return cached
        try:
            if self._has_confirmed_planet(tic_id):
                result = {"status": "CONFIRMED_PLANET", "reason": "Matched table ps by TIC"}
            else:
                toi = self._lookup_toi(tic_id)
                result = toi or {"status": "TCE_ONLY", "reason": "No match in ps or toi"}
        except Exception as exc:  # noqa: BLE001
            result = {"status": "UNKNOWN", "reason": f"Lookup failed: {exc}"}
        self.cache.set("exo_status", tic_id, result, ttl_seconds=86400)
        return result

    def _tap_csv(self, query: str) -> list[dict[str, str]]:
        text = get_text_with_timeout(
            settings.exoarchive_tap_url,
            params={"query": query, "format": "csv"},
            timeout=settings.exoarchive_tap_timeout_seconds,
        ).strip()
        if not text:
            return []
        reader = csv.DictReader(io.StringIO(text))
        return [{str(k): ("" if v is None else str(v)) for k, v in row.items()} for row in reader]

    def resolve_status_batch(self, tic_ids: list[str], max_workers: int = 5) -> list[tuple[str, dict[str, str]]]:
        if not tic_ids:
            return []
        results: dict[str, tuple[str, dict[str, str]]] = {}
        workers = min(max(1, max_workers), len(tic_ids))
        with ThreadPoolExecutor(max_workers=workers) as ex:
            future_map = {ex.submit(self.resolve_status, tic): tic for tic in tic_ids}
            for future in as_completed(future_map):
                tic = future_map[future]
                try:
                    status = future.result()
                except Exception as exc:  # noqa: BLE001
                    status = {"status": "UNKNOWN", "reason": f"Batch status lookup failed: {exc}"}
                results[tic] = (tic, status)
        return [results[tic] for tic in tic_ids]

    def _has_confirmed_planet(self, tic_id: str) -> bool:
        safe = "".join(ch for ch in str(tic_id) if ch.isdigit())
        rows = self._tap_csv(f"select top 1 pl_name,tic_id from ps where tic_id = '{safe}'")
        return bool(rows)

    def _lookup_toi(self, tic_id: str) -> dict[str, str] | None:
        safe = "".join(ch for ch in str(tic_id) if ch.isdigit())
        rows = self._tap_csv(
            "select top 5 tid, tfopwg_disp "
            f"from toi where cast(tid as varchar(64)) = '{safe}'"
        )
        if not rows:
            return None
        dispositions = " ".join(((r.get("tfopwg_disp", "") or "")).upper() for r in rows)
        if "FP" in dispositions:
            return {"status": "FP", "reason": "TOI disposition flagged FP"}
        return {"status": "TOI", "reason": "Matched TOI table by TIC"}
