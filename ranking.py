from __future__ import annotations

import math
from dataclasses import dataclass

RANKING_VERSION = "base_v1"


def clip(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


@dataclass(frozen=True)
class RankingInput:
    mes: float
    depth_pct: float
    duration_hr: float
    cdpp_ppm: float | None = None


def compute_base_v1(
    row: RankingInput,
    *,
    min_mes: float = 7.1,
    max_mes: float = 20.0,
    use_cdpp: bool = False,
) -> dict[str, float | str]:
    if max_mes <= min_mes:
        raise ValueError("max_mes must be greater than min_mes")
    mes_norm = clip((row.mes - min_mes) / (max_mes - min_mes), 0.0, 1.0)
    depth_score = 1.0 if row.depth_pct <= 3.0 else math.exp(-(row.depth_pct - 3.0))
    duration_score = math.exp(-((row.duration_hr - 5.0) ** 2) / (2.0 * 5.0**2))
    snr_norm = 0.0
    if use_cdpp:
        if row.cdpp_ppm and row.cdpp_ppm > 0:
            snr_proxy = (row.depth_pct * 1e4) / row.cdpp_ppm
            snr_norm = clip(snr_proxy / 10.0, 0.0, 1.0)
        score = 0.50 * mes_norm + 0.20 * depth_score + 0.15 * duration_score + 0.15 * snr_norm
    else:
        score = 0.60 * mes_norm + 0.25 * depth_score + 0.15 * duration_score
    return {
        "score": clip(score, 0.0, 1.0),
        "mes_norm": mes_norm,
        "depth_score": depth_score,
        "duration_score": duration_score,
        "snr_norm": snr_norm,
        "ranking_version": RANKING_VERSION,
    }

