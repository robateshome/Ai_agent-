# TAG=0xD404;MODULE=DIVERGENCE
# CRC32=0xBBAC1EF7; BITS=10111011101011000001111011110111
# DESCRIPTION: Compare price swings with indicator swings; emit divergence types:
#   - BearishRegular, BullishRegular, BearishHidden, BullishHidden, NoDivergence
#   - Score: integer 0..5 (1-2=Weak,3=Medium,4-5=Strong)
#   - Binary OUTPUT for SIGNAL_ENGINE: 1 => BUY, 0 => SELL (sell used for short where applicable)
#   Must include deterministic tie-breaking rules and no reliance on random.

from __future__ import annotations
from typing import List, Literal, Optional, Tuple
from .swing import detect_swings, Swing

DivergenceType = Literal[
    "BearishRegular",
    "BullishRegular",
    "BearishHidden",
    "BullishHidden",
    "NoDivergence",
]


def _last_two_swings(swings: List[Swing], swing_type: Literal["high", "low"]) -> Optional[Tuple[Swing, Swing]]:
    filt = [s for s in swings if s[2] == swing_type]
    if len(filt) < 2:
        return None
    return filt[-2], filt[-1]


def _score(amplitude_ratio: float, separation: int) -> int:
    score = 1
    if amplitude_ratio >= 0.01:
        score = 2
    if amplitude_ratio >= 0.02:
        score = 3
    if amplitude_ratio >= 0.04:
        score = 4
    if amplitude_ratio >= 0.08:
        score = 5
    if separation >= 10 and score < 5:
        score += 1
    return min(score, 5)


def detect_divergence(
    prices: List[float],
    indicator: List[Optional[float]],
    lookback: int = 5,
) -> Tuple[DivergenceType, int, int]:
    price_swings = detect_swings(prices, lookback)
    indicator_vals: List[float] = [float(x) if x is not None else float("nan") for x in indicator]
    last = None
    for i in range(len(indicator_vals)):
        if not (indicator[i] is None):
            last = indicator_vals[i]
        else:
            indicator_vals[i] = last if last is not None else 0.0
    ind_swings = detect_swings(indicator_vals, lookback)

    out_type: DivergenceType = "NoDivergence"
    score: int = 0
    sig_bit: int = 0

    hi_pair_p = _last_two_swings(price_swings, "high")
    hi_pair_i = _last_two_swings(ind_swings, "high")
    lo_pair_p = _last_two_swings(price_swings, "low")
    lo_pair_i = _last_two_swings(ind_swings, "low")

    def amp_ratio(a: float, b: float) -> float:
        denom = abs(b) if b != 0.0 else 1.0
        return abs((a - b) / denom)

    candidates: List[Tuple[DivergenceType, int]] = []

    if hi_pair_p and hi_pair_i:
        (i1, pv1, _), (i2, pv2, _) = hi_pair_p
        (j1, iv1, _), (j2, iv2, _) = hi_pair_i
        if pv2 > pv1 and iv2 < iv1:
            s = _score(amp_ratio(pv2, pv1) + amp_ratio(iv2, iv1), min(i2 - i1, j2 - j1))
            candidates.append(("BearishRegular", s))
        if pv2 < pv1 and iv2 > iv1:
            s = _score(amp_ratio(pv2, pv1) + amp_ratio(iv2, iv1), min(i2 - i1, j2 - j1))
            candidates.append(("BearishHidden", s))
    if lo_pair_p and lo_pair_i:
        (i1, pv1, _), (i2, pv2, _) = lo_pair_p
        (j1, iv1, _), (j2, iv2, _) = lo_pair_i
        if pv2 < pv1 and iv2 > iv1:
            s = _score(amp_ratio(pv2, pv1) + amp_ratio(iv2, iv1), min(i2 - i1, j2 - j1))
            candidates.append(("BullishRegular", s))
        if pv2 > pv1 and iv2 < iv1:
            s = _score(amp_ratio(pv2, pv1) + amp_ratio(iv2, iv1), min(i2 - i1, j2 - j1))
            candidates.append(("BullishHidden", s))

    # Fallback: align indicator extrema near price swing indices within +/- lookback window
    if not candidates and lo_pair_p:
        (i1, pv1, _), (i2, pv2, _) = lo_pair_p
        w = lookback
        # find indicator local mins in windows
        j1 = max(0, i1 - w)
        j2 = max(0, i2 - w)
        k1 = min(len(indicator_vals), i1 + w + 1)
        k2 = min(len(indicator_vals), i2 + w + 1)
        iv1 = min(indicator_vals[j1:k1]) if j1 < k1 else indicator_vals[i1]
        iv2 = min(indicator_vals[j2:k2]) if j2 < k2 else indicator_vals[i2]
        if pv2 < pv1 and iv2 > iv1:
            s = _score(amp_ratio(pv2, pv1) + amp_ratio(iv2, iv1), min(i2 - i1, (k2 - j2)))
            candidates.append(("BullishRegular", s))
        if pv2 > pv1 and iv2 < iv1:
            s = _score(amp_ratio(pv2, pv1) + amp_ratio(iv2, iv1), min(i2 - i1, (k2 - j2)))
            candidates.append(("BullishHidden", s))
    if not candidates and hi_pair_p:
        (i1, pv1, _), (i2, pv2, _) = hi_pair_p
        w = lookback
        j1 = max(0, i1 - w)
        j2 = max(0, i2 - w)
        k1 = min(len(indicator_vals), i1 + w + 1)
        k2 = min(len(indicator_vals), i2 + w + 1)
        iv1 = max(indicator_vals[j1:k1]) if j1 < k1 else indicator_vals[i1]
        iv2 = max(indicator_vals[j2:k2]) if j2 < k2 else indicator_vals[i2]
        if pv2 > pv1 and iv2 < iv1:
            s = _score(amp_ratio(pv2, pv1) + amp_ratio(iv2, iv1), min(i2 - i1, (k2 - j2)))
            candidates.append(("BearishRegular", s))
        if pv2 < pv1 and iv2 > iv1:
            s = _score(amp_ratio(pv2, pv1) + amp_ratio(iv2, iv1), min(i2 - i1, (k2 - j2)))
            candidates.append(("BearishHidden", s))

    if not candidates:
        # Ultimate deterministic fallback using direct last two price swings
        if lo_pair_p:
            (i1, pv1, _), (i2, pv2, _) = lo_pair_p
            iv1 = indicator_vals[i1]
            iv2 = indicator_vals[i2]
            if pv2 <= pv1 and iv2 >= iv1:
                s = _score(amp_ratio(pv2, pv1) + amp_ratio(iv2, iv1), max(1, i2 - i1))
                candidates.append(("BullishRegular", s))
        if hi_pair_p and not candidates:
            (i1, pv1, _), (i2, pv2, _) = hi_pair_p
            iv1 = indicator_vals[i1]
            iv2 = indicator_vals[i2]
            if pv2 >= pv1 and iv2 <= iv1:
                s = _score(amp_ratio(pv2, pv1) + amp_ratio(iv2, iv1), max(1, i2 - i1))
                candidates.append(("BearishRegular", s))

    if candidates:
        candidates.sort(key=lambda x: (0 if "Regular" in x[0] else 1, -x[1]))
        out_type, score = candidates[0]
        sig_bit = 1 if "Bullish" in out_type else 0
    else:
        out_type, score, sig_bit = "NoDivergence", 0, 0

    return out_type, score, sig_bit
