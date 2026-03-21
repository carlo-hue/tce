from __future__ import annotations

import json
import re
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from typing import Any

from .config import settings


class HttpError(RuntimeError):
    pass


def _compact_http_error_body(body: str) -> str:
    text = str(body or "").strip()
    if not text:
        return ""
    if not text.startswith("<"):
        return re.sub(r"\s+", " ", text)[:220]
    try:
        root = ET.fromstring(text)
    except ET.ParseError:
        return re.sub(r"\s+", " ", text)[:220]
    infos: list[str] = []
    for elem in root.iter():
        tag = elem.tag.split("}")[-1].lower()
        if tag != "info":
            continue
        name = str(elem.attrib.get("name", "")).strip()
        value = str(elem.attrib.get("value", "")).strip()
        body_text = " ".join(str(elem.text or "").split())
        part = body_text or value or name
        if part:
            infos.append(part)
    if infos:
        return " | ".join(infos[:2])[:220]
    return re.sub(r"\s+", " ", text)[:220]


def _request(
    method: str,
    url: str,
    *,
    params: dict[str, Any] | None = None,
    json_body: dict[str, Any] | None = None,
    raw_body: bytes | None = None,
    headers: dict[str, str] | None = None,
    timeout: float | None = None,
) -> str:
    if params:
        query = urllib.parse.urlencode({k: v for k, v in params.items() if v is not None})
        sep = "&" if "?" in url else "?"
        url = f"{url}{sep}{query}"
    data_bytes = raw_body
    req_headers = dict(headers or {})
    if json_body is not None:
        data_bytes = json.dumps(json_body).encode("utf-8")
        req_headers.setdefault("Content-Type", "application/json")
    req = urllib.request.Request(url, data=data_bytes, headers=req_headers, method=method.upper())
    try:
        with urllib.request.urlopen(req, timeout=timeout or settings.http_timeout_seconds) as resp:
            return resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        compact_body = _compact_http_error_body(body)
        if compact_body:
            raise HttpError(f"{exc.code} {exc.reason}: {compact_body}") from exc
        raise HttpError(f"{exc.code} {exc.reason}") from exc
    except urllib.error.URLError as exc:
        raise HttpError(str(exc.reason)) from exc


def get_text(url: str, *, params: dict[str, Any] | None = None) -> str:
    return get_text_with_timeout(url, params=params, timeout=settings.http_timeout_seconds)


def get_text_with_timeout(url: str, *, params: dict[str, Any] | None = None, timeout: float | None = None) -> str:
    last_exc: Exception | None = None
    for attempt in range(settings.http_retry_attempts + 1):
        try:
            return _request("GET", url, params=params, timeout=timeout)
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if attempt >= settings.http_retry_attempts:
                break
            time.sleep(0.25 * (attempt + 1))
    raise HttpError(str(last_exc) if last_exc else "Unknown HTTP error")


def post_json(url: str, payload: dict[str, Any]) -> dict[str, Any] | list[Any]:
    last_exc: Exception | None = None
    for attempt in range(settings.http_retry_attempts + 1):
        try:
            text = _request("POST", url, json_body=payload)
            return json.loads(text) if text else {}
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if attempt >= settings.http_retry_attempts:
                break
            time.sleep(0.25 * (attempt + 1))
    raise HttpError(str(last_exc) if last_exc else "Unknown HTTP error")


def post_form_json(url: str, form_fields: dict[str, Any]) -> dict[str, Any] | list[Any]:
    last_exc: Exception | None = None
    encoded = urllib.parse.urlencode({k: v for k, v in form_fields.items() if v is not None}).encode("utf-8")
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    for attempt in range(settings.http_retry_attempts + 1):
        try:
            text = _request("POST", url, raw_body=encoded, headers=headers, timeout=settings.http_timeout_seconds)
            return json.loads(text) if text else {}
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if attempt >= settings.http_retry_attempts:
                break
            time.sleep(0.25 * (attempt + 1))
    raise HttpError(str(last_exc) if last_exc else "Unknown HTTP error")
