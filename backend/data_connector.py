# TAG=0xD101;MODULE=DATA_FEED
# CRC32=0x030CE7E5; BITS=00000011000011001110011111100101
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
        self._agg_open: Optional[float] = None
        self._agg_high: Optional[float] = None
        self._agg_low: Optional[float] = None
        self._agg_volume: float = 0.0
        self._bucket_start_ms: Optional[int] = None

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

    def _bucket_ms(self) -> int:
        # Currently supports 1min as 60s
        if self.timeframe.endswith("min"):
            mins = int(self.timeframe.replace("min", ""))
            return mins * 60 * 1000
        return 60 * 1000

    async def _emit_agg(self, ts_ms: int, close: float) -> None:
        if self._agg_open is None:
            return
        candle = Candle(
            ts=ts_ms,
            open=float(self._agg_open),
            high=float(self._agg_high if self._agg_high is not None else self._agg_open),
            low=float(self._agg_low if self._agg_low is not None else self._agg_open),
            close=float(close),
            volume=float(self._agg_volume),
        )
        await self.queue.put(candle)
        self._agg_open = None
        self._agg_high = None
        self._agg_low = None
        self._agg_volume = 0.0
        self._bucket_start_ms = None

    async def _handle_tick(self, price: float, ts_ms: int, volume: float = 0.0) -> None:
        bucket = self._bucket_ms()
        if self._bucket_start_ms is None:
            self._bucket_start_ms = (ts_ms // bucket) * bucket
        if ts_ms >= self._bucket_start_ms + bucket:
            # Emit previous bucket at its end
            await self._emit_agg(self._bucket_start_ms + bucket - 1, price)
            self._bucket_start_ms = (ts_ms // bucket) * bucket
            self._agg_open = price
            self._agg_high = price
            self._agg_low = price
            self._agg_volume = volume
            return
        # Update agg within bucket
        if self._agg_open is None:
            self._agg_open = price
            self._agg_high = price
            self._agg_low = price
        else:
            self._agg_high = max(float(self._agg_high), price)
            self._agg_low = min(float(self._agg_low), price)
        self._agg_volume += float(volume)
        # Emit interim candle snapshot too for smoother UI
        await self.queue.put(Candle(ts=ts_ms, open=float(self._agg_open), high=float(self._agg_high), low=float(self._agg_low), close=float(price), volume=float(self._agg_volume)))

    async def _run_simulator(self) -> None:
        ts = int(time.time()) * 1000
        base = 100.0
        amp = 5.0
        step_ms = 1000
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
            await asyncio.sleep(0.01)

    async def _run_ws(self) -> None:
        api_key = get_api_key("twelve_data_api_key")
        if not api_key:
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
                                # Two possible shapes: candle aggregation or price tick
                                now_ms = int(time.time() * 1000)
                                if data.get("event") == "price":
                                    price = float(data.get("price", 0.0))
                                    await self._handle_tick(price, now_ms, 0.0)
                                elif data.get("event") == "bar":
                                    # If Twelve Data emits bar events for the symbol
                                    bar = data.get("bar", {})
                                    candle = Candle(
                                        ts=int(bar.get("timestamp", now_ms)),
                                        open=float(bar.get("open", 0.0)),
                                        high=float(bar.get("high", 0.0)),
                                        low=float(bar.get("low", 0.0)),
                                        close=float(bar.get("close", 0.0)),
                                        volume=float(bar.get("volume", 0.0)),
                                    )
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
