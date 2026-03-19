from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _csv_list(raw: str, default: list[str]) -> list[str]:
    items = [p.strip() for p in (raw or "").split(",") if p.strip()]
    return items or default


@dataclass(frozen=True)
class TessTceSettings:
    http_timeout_seconds: float = float(os.getenv("TESS_TCE_HTTP_TIMEOUT_SECONDS", "20"))
    http_retry_attempts: int = int(os.getenv("TESS_TCE_HTTP_RETRY_ATTEMPTS", "2"))
    exoarchive_tap_timeout_seconds: float = float(os.getenv("TESS_TCE_EXOARCHIVE_TAP_TIMEOUT_SECONDS", "6"))
    dv_batch_max_concurrency: int = int(os.getenv("TESS_TCE_DV_BATCH_MAX_CONCURRENCY", "5"))
    mast_tce_bulk_url_template: str = os.getenv(
        "TESS_TCE_MAST_TCE_BULK_URL_TEMPLATE",
        "https://archive.stsci.edu/missions/tess/catalogs/tce/tess_tce_sec{sector}.csv",
    )
    mast_tce_bulk_index_url: str = os.getenv(
        "TESS_TCE_MAST_TCE_BULK_INDEX_URL",
        "https://archive.stsci.edu/tess/bulk_downloads/bulk_downloads_tce.html",
    )
    mast_base_url: str = os.getenv("TESS_TCE_MAST_BASE_URL", "https://mast.stsci.edu")
    exoarchive_tap_url: str = os.getenv(
        "TESS_TCE_EXOARCHIVE_TAP_URL",
        "https://exoplanetarchive.ipac.caltech.edu/TAP/sync",
    )
    cache_path: str = os.getenv(
        "TESS_TCE_SQLITE_CACHE_PATH",
        str(Path(__file__).resolve().parents[2] / "_runtime_data" / "tess_tce" / "cache.db"),
    )
    allowed_download_hosts: list[str] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.dv_batch_max_concurrency < 1:
            object.__setattr__(self, "dv_batch_max_concurrency", 1)
        if self.allowed_download_hosts is None:
            object.__setattr__(
                self,
                "allowed_download_hosts",
                _csv_list(
                    os.getenv("TESS_TCE_ALLOWED_DOWNLOAD_HOSTS", "mast.stsci.edu,archive.stsci.edu,hla.stsci.edu"),
                    ["mast.stsci.edu"],
                ),
            )


settings = TessTceSettings()
