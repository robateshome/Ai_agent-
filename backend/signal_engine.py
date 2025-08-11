# TAG=0xD505;MODULE=SIGNAL_ENGINE
# CRC32=0x854C1274; BITS=10000101010011000001001001110100
# DESCRIPTION: Accept divergence events, apply execution priority (HARD_REALTIME=0xFF),
#   produce binary signal packet: {sig_bit:0/1, ts_unix_ms:int, symbol:str, tf:str, dtype:str, score:int}
#   Persist to SQLite trades/signals table and broadcast to WebSocket clients.

from __future__ import annotations
import asyncio
import json
import time
from typing import Any, Dict, List, Optional
import sqlite3
from .db import get_db_path, init_db


class BroadcastHub:
    def __init__(self) -> None:
        self._subscribers: List[asyncio.Queue] = []
        self._lock = asyncio.Lock()

    async def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue()
        async with self._lock:
            self._subscribers.append(q)
        return q

    async def unsubscribe(self, q: asyncio.Queue) -> None:
        async with self._lock:
            if q in self._subscribers:
                self._subscribers.remove(q)

    async def broadcast(self, message: Dict[str, Any]) -> None:
        async with self._lock:
            for q in self._subscribers:
                await q.put(message)


class SignalEngine:
    def __init__(self, symbol: str, timeframe: str, hub: Optional[BroadcastHub] = None) -> None:
        self.symbol = symbol
        self.timeframe = timeframe
        self.hub = hub or BroadcastHub()
        init_db()

    def _persist(self, packet: Dict[str, Any]) -> None:
        conn = sqlite3.connect(get_db_path())
        try:
            conn.execute(
                """
                INSERT INTO signals (ts_unix_ms, symbol, timeframe, dtype, score, sig_bit, payload_json)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(packet.get("ts_unix_ms")),
                    packet.get("symbol"),
                    packet.get("tf"),
                    packet.get("dtype"),
                    int(packet.get("score")),
                    int(packet.get("sig_bit")),
                    json.dumps(packet, separators=(",", ":")),
                ),
            )
            conn.commit()
        finally:
            conn.close()

    async def handle_divergence(self, dtype: str, score: int, sig_bit: int) -> Dict[str, Any]:
        ts_ms = int(time.time() * 1000)
        packet: Dict[str, Any] = {
            "sig_bit": int(sig_bit),
            "ts_unix_ms": ts_ms,
            "symbol": self.symbol,
            "tf": self.timeframe,
            "dtype": dtype,
            "score": int(score),
        }
        self._persist(packet)
        await self.hub.broadcast(packet)
        return packet
