from __future__ import annotations
import os, datetime as dt
from typing import Optional, Iterable, List, Dict, Any
from sqlalchemy import (
    MetaData, String, Integer, BigInteger, Float, Boolean,
    DateTime, ForeignKey, UniqueConstraint, Index, text
)
from sqlalchemy.orm import Mapped, mapped_column, relationship, declarative_base
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.dialects.postgresql import insert as pg_insert

PG_HOST=os.getenv("PG_HOST","db")
PG_PORT=int(os.getenv("PG_PORT","5432"))
PG_DB=os.getenv("PG_DB","trader")
PG_USER=os.getenv("PG_USER","trader")
PG_PASS=os.getenv("PG_PASS","traderpass")

ASYNC_DSN=f"postgresql+asyncpg://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"

convention = {
  "ix": "ix_%(column_0_label)s",
  "uq": "uq_%(table_name)s_%(column_0_name)s",
  "ck": "ck_%(table_name)s_%(constraint_name)s",
  "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
  "pk": "pk_%(table_name)s"
}
metadata = MetaData(naming_convention=convention)
Base = declarative_base(metadata=metadata)

class Symbol(Base):
    __tablename__ = "symbols"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    symbol: Mapped[str] = mapped_column(String(32), unique=True, index=True)

class Candle(Base):
    __tablename__ = "candles"
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    symbol: Mapped[str] = mapped_column(String(32), index=True)
    ts: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), index=True)
    open: Mapped[float] = mapped_column(Float)
    high: Mapped[float] = mapped_column(Float)
    low: Mapped[float] = mapped_column(Float)
    close: Mapped[float] = mapped_column(Float)
    volume: Mapped[float] = mapped_column(Float)
    __table_args__ = (
        UniqueConstraint("symbol","ts", name="uq_candles_symbol_ts"),
        Index("ix_candles_symbol_ts", "symbol","ts"),
    )

class Balance(Base):
    __tablename__ = "balances"
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    asset: Mapped[str] = mapped_column(String(20), index=True)
    free: Mapped[float] = mapped_column(Float)
    locked: Mapped[float] = mapped_column(Float, default=0.0)
    ts: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), index=True, server_default=text("NOW() AT TIME ZONE 'utc'"))

class Order(Base):
    __tablename__ = "orders"
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    symbol: Mapped[str] = mapped_column(String(32), index=True)
    side: Mapped[str] = mapped_column(String(8))      # BUY/SELL
    type: Mapped[str] = mapped_column(String(12))     # LIMIT/MARKET
    price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    qty: Mapped[float] = mapped_column(Float)
    status: Mapped[str] = mapped_column(String(24), index=True)  # NEW, FILLED, PARTIALLY_FILLED, CANCELED
    client_order_id: Mapped[Optional[str]] = mapped_column(String(64), unique=True)
    exch_order_id: Mapped[Optional[str]] = mapped_column(String(64), index=True)
    created_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), server_default=text("NOW() AT TIME ZONE 'utc'"))
    updated_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), server_default=text("NOW() AT TIME ZONE 'utc'"))

class Trade(Base):
    __tablename__ = "trades"
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    symbol: Mapped[str] = mapped_column(String(32), index=True)
    side: Mapped[str] = mapped_column(String(8))
    price: Mapped[float] = mapped_column(Float)
    qty: Mapped[float] = mapped_column(Float)
    fee_asset: Mapped[Optional[str]] = mapped_column(String(16), nullable=True)
    fee: Mapped[float] = mapped_column(Float, default=0.0)
    order_client_id: Mapped[Optional[str]] = mapped_column(String(64), index=True)
    ts: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), server_default=text("NOW() AT TIME ZONE 'utc'"))

engine = create_async_engine(ASYNC_DSN, echo=False, pool_pre_ping=True)
SessionLocal = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

# --- DAL helpers ---
async def upsert_candles(session: AsyncSession, symbol: str, rows):
    if not rows:
        return
    # Build rows for insert
    payload = [
        dict(
            symbol=symbol,
            ts=r["ts"],
            open=r["o"],
            high=r["h"],
            low=r["l"],
            close=r["c"],
            volume=r["v"],
        )
        for r in rows
    ]

    stmt = pg_insert(Candle).values(payload)

    # On duplicate (symbol, ts), update OHLCV with the incoming values
    stmt = stmt.on_conflict_do_update(
        index_elements=["symbol", "ts"],
        set_={
            "open": stmt.excluded.open,
            "high": stmt.excluded.high,
            "low":  stmt.excluded.low,
            "close": stmt.excluded.close,
            "volume": stmt.excluded.volume,
        },
    )
    await session.execute(stmt)

async def insert_balance(session: AsyncSession, asset: str, free: float, locked: float = 0.0):
    b = Balance(asset=asset, free=free, locked=locked)
    session.add(b)

async def insert_order(session: AsyncSession, **kwargs):
    o = Order(**kwargs)
    session.add(o)

async def set_order_status(session: AsyncSession, client_id: str, status: str, price: Optional[float] = None):
    await session.execute(
        text("UPDATE orders SET status=:s, updated_at=NOW() AT TIME ZONE 'utc', price=COALESCE(:p, price) WHERE client_order_id=:cid")
        .bindparams(s=status, cid=client_id, p=price)
    )

async def insert_trade(session: AsyncSession, **kwargs):
    t = Trade(**kwargs)
    session.add(t)
