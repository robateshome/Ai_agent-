# TAG=0xD000;MODULE=BACKEND_MAIN
# CRC32=0x00000000; BITS=00000000000000000000000000000000
# DESCRIPTION: FastAPI app wiring data connector, signals, and websockets; supports live candles and config.
from __future__ import annotations
import asyncio
import time
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from .db import init_db, save_api_key, get_api_key
from .signal_engine import SignalEngine, BroadcastHub
from .data_connector import DataConnector, Candle
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


class StreamConfig(BaseModel):
    symbol: str
    timeframe: str = "1min"


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
_stream_cfg = StreamConfig(symbol="EUR/USD", timeframe="1min")
engine = SignalEngine(symbol=_stream_cfg.symbol, timeframe=_stream_cfg.timeframe, hub=hub)
_dc: Optional[DataConnector] = None
_dc_lock = asyncio.Lock()


async def _restart_connector(symbol: str, timeframe: str) -> None:
    global _dc, engine, _stream_cfg
    async with _dc_lock:
        if _dc is not None:
            await _dc.stop()
        _stream_cfg = StreamConfig(symbol=symbol, timeframe=timeframe)
        engine = SignalEngine(symbol=_stream_cfg.symbol, timeframe=_stream_cfg.timeframe, hub=hub)
        simulate = not bool(get_api_key("twelve_data_api_key"))
        _dc = DataConnector(symbol=_stream_cfg.symbol, timeframe=_stream_cfg.timeframe, simulate=simulate)
        await _dc.start()


@app.post("/api/stream/config")
async def set_stream_config(cfg: StreamConfig) -> Dict[str, Any]:
    await _restart_connector(cfg.symbol, cfg.timeframe)
    return {"ok": True, "symbol": cfg.symbol, "timeframe": cfg.timeframe}


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


_prices: List[float] = []
_indicator: List[Optional[float]] = []


async def background_pipeline() -> None:
    await _restart_connector(_stream_cfg.symbol, _stream_cfg.timeframe)
    assert _dc is not None
    async for c in _dc.candles():
        # Broadcast candle for live chart
        await hub.broadcast({
            "event": "candle",
            "symbol": _stream_cfg.symbol,
            "tf": _stream_cfg.timeframe,
            "ts": c.ts,
            "open": c.open,
            "high": c.high,
            "low": c.low,
            "close": c.close,
            "volume": c.volume,
        })
        # Update series for divergence signals using RSI
        _prices.append(float(c.close))
        while len(_indicator) < len(_prices):
            _indicator.append(None)
        r = rsi(_prices, 14)
        _indicator[:] = r
        dtype, score, sig = detect_divergence(_prices, _indicator, lookback=5)
        if dtype != "NoDivergence":
            await engine.handle_divergence(dtype, score, sig)


@app.on_event("startup")
async def on_startup() -> None:
    asyncio.create_task(background_pipeline())