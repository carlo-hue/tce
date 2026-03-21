from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any
from urllib.parse import quote

from .config import settings
from .http_client import HttpError
from .http_client import post_form_json
from .models import SQLiteCache

DV_ALLOWED_SUBGROUPS = ("DVR", "DVS", "DVM", "DVT")
DV_TTL_READY_SECONDS = 7 * 86400
DV_TTL_UNAVAILABLE_SECONDS = 6 * 3600
DV_TTL_RETRY_SECONDS = 30 * 60


def parse_mast_invoke_rows(body: object) -> list[dict[str, Any]]:
    """Pure parser for MAST /api/v0/invoke responses."""
    if isinstance(body, list):
        return [row for row in body if isinstance(row, dict)]
    if isinstance(body, dict):
        data = body.get("data")
        if isinstance(data, list):
            return [row for row in data if isinstance(row, dict)]
    return []


def classify_dv_lookup(
    products: list[dict[str, Any]],
    error_message: str | None = None,
) -> tuple[str, str | None, str | None]:
    """
    Pure classifier for DV outcome.

    Returns:
    - ('READY', None, None): DV products found
    - ('UNAVAILABLE', msg, type): valid response but unavailable / not found / parsing problem
    - ('RETRY', msg, type): timeout/network/server-side transient issue
    """
    if products:
        return "READY", None, None
    if not error_message:
        return "UNAVAILABLE", "Nessun DV MAST trovato (DVR/DVS/DVM/DVT assenti)", "no_dv_found"
    msg = str(error_message)
    low = msg.lower()
    unavailable_tokens = ("not found", "no data", "invalid web service call", "missing value for parameter")
    if any(tok in low for tok in unavailable_tokens):
        return "UNAVAILABLE", msg, "mast_no_data"
    transient_tokens = ("timeout", "timed out", "temporar", "connection reset", "name resolution", "unreachable")
    if any(tok in low for tok in transient_tokens):
        return "RETRY", msg, "timeout_or_network"
    if "500 " in low or "502 " in low or "503 " in low or "504 " in low:
        return "RETRY", msg, "http_server_error"
    return "UNAVAILABLE", msg, "unexpected_mast_response"


def ttl_for_dv_status(dv_status: str) -> int:
    status = str(dv_status or "").upper()
    if status == "READY":
        return DV_TTL_READY_SECONDS
    if status == "RETRY":
        return DV_TTL_RETRY_SECONDS
    return DV_TTL_UNAVAILABLE_SECONDS


class MastDVClient:
    def __init__(self, cache: SQLiteCache) -> None:
        self.cache = cache

    def get_dv_products(self, tic_id: str) -> tuple[list[dict[str, Any]], bool, str, str | None, str | None]:
        cached = self.cache.get("mast_dv_products", tic_id)
        if cached is not None:
            return (
                list(cached.get("products", [])),
                True,
                str(cached.get("dv_status", "UNAVAILABLE")),
                cached.get("dv_error"),
                cached.get("dv_error_type"),
            )
        products: list[dict[str, Any]] = []
        error_message: str | None = None
        try:
            products = self._prioritize(self._fetch_from_mast(tic_id))
        except Exception as exc:  # noqa: BLE001
            error_message = str(exc)
        dv_status, dv_error, dv_error_type = classify_dv_lookup(products, error_message)
        self.cache.set(
            "mast_dv_products",
            tic_id,
            {
                "products": products,
                "dv_status": dv_status,
                "dv_error": dv_error,
                "dv_error_type": dv_error_type,
            },
            ttl_seconds=ttl_for_dv_status(dv_status),
        )
        return products, False, dv_status, dv_error, dv_error_type

    def get_dv_products_batch(
        self,
        tic_ids: list[str],
    ) -> list[tuple[str, list[dict[str, Any]], bool, str, str | None, str | None]]:
        results: dict[str, tuple[str, list[dict[str, Any]], bool, str, str | None, str | None]] = {}
        max_workers = min(settings.dv_batch_max_concurrency, max(1, len(tic_ids)))
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            future_map = {ex.submit(self.get_dv_products, tic): tic for tic in tic_ids}
            for future in as_completed(future_map):
                tic = future_map[future]
                try:
                    products, cached, dv_status, dv_error, dv_error_type = future.result()
                except Exception as exc:  # noqa: BLE001
                    products, cached = [], False
                    dv_status, dv_error, dv_error_type = classify_dv_lookup(products, str(exc))
                results[tic] = (tic, products, cached, dv_status, dv_error, dv_error_type)
        return [results[tic] for tic in tic_ids]

    def _fetch_from_mast(self, tic_id: str) -> list[dict[str, Any]]:
        obs_rows = self._mast_invoke(
            service="Mast.Caom.Filtered",
            params={
                "columns": "*",
                "filters": [
                    {"paramName": "obs_collection", "values": ["TESS"]},
                    {"paramName": "target_name", "values": [f"TIC {tic_id}", tic_id]},
                ],
            },
        )
        out: list[dict[str, Any]] = []
        for obs in obs_rows:
            obsid = obs.get("obsid") or obs.get("obs_id")
            if obsid is None:
                continue
            products = self._mast_invoke(service="Mast.Caom.Products", params={"obsid": obsid})
            for prod in products:
                subgroup = str(prod.get("productSubGroupDescription", "")).upper()
                if subgroup not in DV_ALLOWED_SUBGROUPS:
                    continue
                data_uri = str(prod.get("dataURI", "")).strip()
                if not data_uri:
                    continue
                out.append(
                    {
                        "kind": subgroup,
                        "product_filename": str(prod.get("productFilename", f"{subgroup}-{tic_id}")),
                        "mast_download_url": self._download_url(data_uri),
                        "obs_collection": prod.get("obs_collection"),
                        "data_uri": data_uri,
                    }
                )
        return out

    def _mast_invoke(self, *, service: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        payload = {"service": service, "params": params, "format": "json"}
        body = self._mast_invoke_raw(payload)
        return parse_mast_invoke_rows(body)

    def _mast_invoke_raw(self, payload: dict[str, Any]) -> object:
        """
        Call MAST /api/v0/invoke using the legacy form field expected by MAST:
        POST request=<json>.
        """
        url = f"{settings.mast_base_url.rstrip('/')}/api/v0/invoke"
        try:
            return post_form_json(url, {"request": json.dumps(payload)})
        except Exception as exc:  # noqa: BLE001
            raise HttpError(str(exc)) from exc

    @staticmethod
    def _download_url(data_uri: str) -> str:
        return f"{settings.mast_base_url.rstrip('/')}/api/v0.1/Download/file?uri={quote(data_uri, safe='')}"

    @staticmethod
    def _prioritize(products: list[dict[str, Any]]) -> list[dict[str, Any]]:
        order = {"DVR": 0, "DVS": 1, "DVM": 2, "DVT": 3}
        return sorted(products, key=lambda p: (order.get(str(p.get('kind')), 99), str(p.get('product_filename', ''))))
