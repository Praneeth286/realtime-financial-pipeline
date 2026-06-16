"""FastAPI REST + WebSocket layer serving real-time trade aggregates."""
from __future__ import annotations

import asyncio
import os

import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://app:app@localhost:5432/market")

app = FastAPI(title="Real-Time Financial Pipeline API", version="1.0.0")


def _conn():
    return psycopg2.connect(DATABASE_URL)


AGG_SQL = """
SELECT
    symbol,
    COUNT(*)                              AS trades,
    SUM(size)                             AS volume,
    ROUND(SUM(price * size) / SUM(size), 4) AS vwap,
    (ARRAY_AGG(price ORDER BY ts DESC))[1]  AS last_price,
    MAX(ts)                               AS as_of
FROM trades
WHERE symbol = %s AND ts > NOW() - INTERVAL '5 minutes'
GROUP BY symbol;
"""


def fetch_aggregate(symbol: str) -> dict | None:
    with _conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(AGG_SQL, (symbol.upper(),))
        return cur.fetchone()


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/aggregates/{symbol}")
def aggregates(symbol: str) -> dict:
    row = fetch_aggregate(symbol)
    if not row:
        raise HTTPException(status_code=404, detail=f"No recent trades for {symbol}")
    return row


@app.websocket("/ws/{symbol}")
async def ws_aggregates(websocket: WebSocket, symbol: str) -> None:
    """Push fresh aggregates for a symbol roughly once per second."""
    await websocket.accept()
    try:
        while True:
            row = await asyncio.to_thread(fetch_aggregate, symbol)
            if row:
                await websocket.send_json(row)
            await asyncio.sleep(1.0)
    except WebSocketDisconnect:
        return
