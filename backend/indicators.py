# TAG=0xD202;MODULE=INDICATORS
# CRC32=0x41489B54; BITS=01000001010010001001101101010100
# DESCRIPTION: Implement RSI, EMA, MACD(hist), Stochastic %K%D, CCI, ATR.
#   Functions must accept lists of floats and return list[float] with `None` where undefined.
#   All numeric ops use float64 semantics and predictable edge-case fallbacks (no exceptions except invalid args).

"""Technical indicators with deterministic float64 semantics.

All functions accept list[float] and return list[float | None] where undefined values
are represented as None. No exceptions unless invalid arguments.
"""
from __future__ import annotations
from typing import List, Optional, Tuple
import math

Float = float


def _validate_series(series: List[Float], min_len: int, name: str) -> None:
    if not isinstance(series, list):
        raise TypeError(f"{name} must be list[float]")
    if len(series) < min_len:
        # Not enough data; callers expect list of same length
        return


def ema(series: List[Float], period: int) -> List[Optional[Float]]:
    if period <= 0:
        raise ValueError("period must be > 0")
    _validate_series(series, 1, "series")
    n = len(series)
    if n == 0:
        return []
    k = 2.0 / (period + 1.0)
    out: List[Optional[Float]] = [None] * n
    ema_val: Optional[Float] = None
    for i, price in enumerate(series):
        if i == 0:
            ema_val = price
            out[i] = None  # undefined until full period
        else:
            assert ema_val is not None
            ema_val = (price - ema_val) * k + ema_val
            out[i] = ema_val if i >= period - 1 else None
    return out


def rsi(series: List[Float], period: int = 14) -> List[Optional[Float]]:
    if period <= 0:
        raise ValueError("period must be > 0")
    n = len(series)
    if n == 0:
        return []
    gains: List[Float] = [0.0] * n
    losses: List[Float] = [0.0] * n
    for i in range(1, n):
        delta = float(series[i] - series[i - 1])
        gains[i] = delta if delta > 0.0 else 0.0
        losses[i] = -delta if delta < 0.0 else 0.0
    avg_gain: Optional[Float] = None
    avg_loss: Optional[Float] = None
    out: List[Optional[Float]] = [None] * n
    for i in range(1, n):
        if i < period:
            out[i] = None
        elif i == period:
            # initial averages use first 'period' deltas (indices 1..period)
            avg_gain = sum(gains[1:period + 1]) / float(period)
            avg_loss = sum(losses[1:period + 1]) / float(period)
            if avg_loss == 0.0:
                out[i] = 100.0
            elif avg_gain == 0.0:
                out[i] = 0.0
            else:
                rs = avg_gain / avg_loss
                out[i] = 100.0 - (100.0 / (1.0 + rs))
        else:
            assert avg_gain is not None and avg_loss is not None
            avg_gain = (avg_gain * (period - 1) + gains[i]) / float(period)
            avg_loss = (avg_loss * (period - 1) + losses[i]) / float(period)
            if avg_loss == 0.0:
                out[i] = 100.0
            elif avg_gain == 0.0:
                out[i] = 0.0
            else:
                rs = avg_gain / avg_loss
                out[i] = 100.0 - (100.0 / (1.0 + rs))
    return out


def macd_hist(series: List[Float], fast: int = 12, slow: int = 26, signal: int = 9) -> List[Optional[Float]]:
    if not (fast > 0 and slow > 0 and signal > 0):
        raise ValueError("periods must be > 0")
    if fast >= slow:
        raise ValueError("fast must be < slow")
    n = len(series)
    if n == 0:
        return []
    ema_fast = ema(series, fast)
    ema_slow = ema(series, slow)
    macd_line: List[Optional[Float]] = [None] * n
    for i in range(n):
        if ema_fast[i] is not None and ema_slow[i] is not None:
            macd_line[i] = float(ema_fast[i]) - float(ema_slow[i])
    # signal line on macd_line values (fill None as previous value to keep deterministic smoothing start)
    macd_vals: List[Float] = []
    idx_map: List[int] = []
    for i, v in enumerate(macd_line):
        if v is not None:
            macd_vals.append(v)
            idx_map.append(i)
    signal_series = ema(macd_vals, signal)
    hist: List[Optional[Float]] = [None] * n
    for j, i in enumerate(idx_map):
        sig = signal_series[j]
        if sig is not None:
            hist[i] = macd_vals[j] - float(sig)
        else:
            hist[i] = None
    return hist


def stochastic_kd(high: List[Float], low: List[Float], close: List[Float], k_period: int = 14, d_period: int = 3) -> Tuple[List[Optional[Float]], List[Optional[Float]]]:
    if not (len(high) == len(low) == len(close)):
        raise ValueError("high, low, close must have same length")
    n = len(close)
    if n == 0:
        return [], []
    k_list: List[Optional[Float]] = [None] * n
    for i in range(n):
        if i + 1 >= k_period:
            window_low = min(low[i - k_period + 1 : i + 1])
            window_high = max(high[i - k_period + 1 : i + 1])
            denom = (window_high - window_low)
            if denom == 0.0:
                k_list[i] = 50.0
            else:
                k_list[i] = 100.0 * (close[i] - window_low) / denom
        else:
            k_list[i] = None
    # D is SMA of K
    d_list: List[Optional[Float]] = [None] * n
    for i in range(n):
        if k_list[i] is None:
            d_list[i] = None
            continue
        if i + 1 >= d_period and all(k_list[j] is not None for j in range(i - d_period + 1, i + 1)):
            window = [float(k_list[j]) for j in range(i - d_period + 1, i + 1)]
            d_list[i] = sum(window) / float(d_period)
        else:
            d_list[i] = None
    return k_list, d_list


def cci(high: List[Float], low: List[Float], close: List[Float], period: int = 20) -> List[Optional[Float]]:
    if not (len(high) == len(low) == len(close)):
        raise ValueError("high, low, close must have same length")
    n = len(close)
    if n == 0:
        return []
    tp: List[Float] = [(high[i] + low[i] + close[i]) / 3.0 for i in range(n)]
    out: List[Optional[Float]] = [None] * n
    for i in range(n):
        if i + 1 >= period:
            window = tp[i - period + 1 : i + 1]
            sma = sum(window) / float(period)
            md = sum(abs(x - sma) for x in window) / float(period)
            denom = 0.015 * md
            if denom == 0.0:
                out[i] = 0.0
            else:
                out[i] = (tp[i] - sma) / denom
        else:
            out[i] = None
    return out


def atr(high: List[Float], low: List[Float], close: List[Float], period: int = 14) -> List[Optional[Float]]:
    if not (len(high) == len(low) == len(close)):
        raise ValueError("high, low, close must have same length")
    n = len(close)
    if n == 0:
        return []
    tr: List[Float] = [0.0] * n
    for i in range(n):
        if i == 0:
            tr[i] = float(high[i] - low[i])
        else:
            tr[i] = max(
                float(high[i] - low[i]),
                abs(float(high[i] - close[i - 1])),
                abs(float(low[i] - close[i - 1])),
            )
    out: List[Optional[Float]] = [None] * n
    avg_tr: Optional[Float] = None
    for i in range(n):
        if i == period - 1:
            avg_tr = sum(tr[: period]) / float(period)
            out[i] = avg_tr
        elif i >= period:
            assert avg_tr is not None
            avg_tr = (avg_tr * (period - 1) + tr[i]) / float(period)
            out[i] = avg_tr
        else:
            out[i] = None
    return out
