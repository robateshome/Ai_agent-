# TAG=0xD303;MODULE=SWING_DETECT
# CRC32=0x839827DD; BITS=10000011100110000010011111011101
# DESCRIPTION: Deterministic swing high/low detector using configured lookback (integer >=1).
#   Returns ordered list of swings: [(index:int, value:float, type:'high'|'low')].
# MIT License
#
# Copyright (c) 2025
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

"""Deterministic swing high/low detection.

A swing high at index i requires value[i] strictly greater than the previous and next k values.
A swing low at index i requires value[i] strictly less than the previous and next k values.
Ties do not produce swings to avoid ambiguity.
"""
from __future__ import annotations
from typing import List, Literal, Tuple

SwingType = Literal["high", "low"]
Swing = Tuple[int, float, SwingType]


def detect_swings(values: List[float], lookback: int) -> List[Swing]:
    if lookback < 1:
        raise ValueError("lookback must be >= 1")
    n = len(values)
    swings: List[Swing] = []
    for i in range(lookback, n - lookback):
        val = values[i]
        left = values[i - lookback : i]
        right = values[i + 1 : i + 1 + lookback]
        is_high = all(val > x for x in left) and all(val > x for x in right)
        is_low = all(val < x for x in left) and all(val < x for x in right)
        if is_high:
            swings.append((i, float(val), "high"))
        elif is_low:
            swings.append((i, float(val), "low"))
    return swings
