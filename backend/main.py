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

from __future__ import annotations
import asyncio
import time
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from .db import init_db, save_api_key, get_api_key
from .signal_engine import SignalEngine, BroadcastHub
from .data_connector import DataConnector
from .indicators import rsi
from .divergence import detect_divergence

app = FastAPI(title="DivergenceBotX")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

init_db()


class ApiKeyIn(BaseModel):
    api_key: str


@app.get("/api/ping")
def api_ping() -> Dict[str, Any]:
    return {"ok": True, "ts": int(time.time() * 1000)}


@app.post("/api/apikey")
def api_key_save(inp: ApiKeyIn) -> Dict[str, Any]:
    save_api_key("twelve_data_api_key", inp.api_key)
    return {"ok": True}


@app.get("/api/apikey")
def api_key_get() -> Dict[str, Any]:
    val = get_api_key("twelve_data_api_key")
    return {"ok": True, "configured": bool(val)}


hub = BroadcastHub()
engine = SignalEngine(symbol="EUR/USD", timeframe="1m", hub=hub)


@app.websocket("/ws/ping")
async def ws_ping(ws: WebSocket) -> None:
    await ws.accept()
    await ws.send_json({"event": "pong", "ts": int(time.time() * 1000)})
    await ws.close()


@app.websocket("/ws/stream")
async def ws_stream(ws: WebSocket) -> None:
    await ws.accept()
    q = await hub.subscribe()
    try:
        while True:
            try:
                msg = await asyncio.wait_for(q.get(), timeout=30.0)
                await ws.send_json(msg)
            except asyncio.TimeoutError:
                await ws.send_json({"event": "keepalive", "ts": int(time.time() * 1000)})
    except WebSocketDisconnect:
        await hub.unsubscribe(q)


# Background task: simple pipeline using RSI to check divergences on simulated data if no key
_prices: List[float] = []
_indicator: List[Optional[float]] = []


async def background_pipeline() -> None:
    dc = DataConnector(symbol="EUR/USD", timeframe="1min", simulate=True)
    await dc.start()
    async for c in dc.candles():
        _prices.append(float(c.close))
        while len(_indicator) < len(_prices):
            _indicator.append(None)
        if len(_prices) >= 2:
            r = rsi(_prices, 14)
            _indicator[:] = r
            dtype, score, sig = detect_divergence(_prices, _indicator, lookback=5)
            if dtype != "NoDivergence":
                await engine.handle_divergence(dtype, score, sig)


@app.on_event("startup")
async def on_startup() -> None:
    asyncio.create_task(background_pipeline())