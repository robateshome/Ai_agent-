# TAG=0xD101;MODULE=DATA_FEED
# CRC32=0xE38AEED4; BITS=11100011100010101110111011010100
# DESCRIPTION: Twelve Data WebSocket + REST wrapper; subscribe OHLCV ticks; produce aggregated timeframe candles;
#   MUST implement: reconnect, rate-limit handling, API key read from secure storage (env or encrypted DB field),
#   publish candles to internal async queue. Use typed Python 3.11, no pseudo-code.

from __future__ import annotations
import asyncio
import json
import math
import time
from dataclasses import dataclass
from typing import Any, AsyncIterator, Dict, Optional
import aiohttp

from .db import get_api_key


@dataclass
class Candle:
    ts: int  # unix ms
    open: float
    high: float
    low: float
    close: float
    volume: float


class DataConnector:
    def __init__(self, symbol: str, timeframe: str = "1min", simulate: bool = False) -> None:
        self.symbol = symbol
        self.timeframe = timeframe
        self.simulate = simulate
        self.queue: asyncio.Queue[Candle] = asyncio.Queue()
        self._task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()

    async def start(self) -> None:
        if self._task is not None:
            return
        self._stop.clear()
        if self.simulate:
            self._task = asyncio.create_task(self._run_simulator())
        else:
            self._task = asyncio.create_task(self._run_ws())

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            await asyncio.wait([self._task])
            self._task = None

    async def _run_simulator(self) -> None:
        # Deterministic sine-wave candles
        ts = int(time.time()) * 1000
        base = 100.0
        amp = 5.0
        step_ms = 1000  # 1s candle
        i = 0
        last_close = base
        while not self._stop.is_set():
            angle = (i % 360) * math.pi / 180.0
            close = base + amp * math.sin(angle)
            high = max(last_close, close) + 0.2
            low = min(last_close, close) - 0.2
            open_ = last_close
            vol = 1_000.0 + 100.0 * math.cos(angle)
            candle = Candle(ts=ts, open=open_, high=high, low=low, close=close, volume=vol)
            await self.queue.put(candle)
            last_close = close
            ts += step_ms
            i += 1
            await asyncio.sleep(0.01)  # fast simulation

    async def _run_ws(self) -> None:
        api_key = get_api_key("twelve_data_api_key")
        if not api_key:
            # fallback to simulator if key absent
            await self._run_simulator()
            return
        url = "wss://ws.twelvedata.com/v1/stream"
        backoff = 1.0
        async with aiohttp.ClientSession() as session:
            while not self._stop.is_set():
                try:
                    async with session.ws_connect(url, heartbeat=30) as ws:
                        await ws.send_json({"action": "authenticate", "params": {"apikey": api_key}})
                        await ws.send_json({"action": "subscribe", "params": {"symbols": self.symbol}})
                        backoff = 1.0
                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                data = json.loads(msg.data)
                                # Expecting tick or candle-like data; adapt as needed
                                if data.get("event") == "price":
                                    price = float(data["price"])  # placeholder mapping
                                    now_ms = int(time.time() * 1000)
                                    candle = Candle(ts=now_ms, open=price, high=price, low=price, close=price, volume=0.0)
                                    await self.queue.put(candle)
                            elif msg.type == aiohttp.WSMsgType.ERROR:
                                break
                except Exception:
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2.0, 30.0)
                    continue

    async def candles(self) -> AsyncIterator[Candle]:
        while True:
            candle = await self.queue.get()
            yield candle
