-- Creates tables if they don't exist (idempotent enough for first boot).
-- All timestamps in UTC.
CREATE TABLE IF NOT EXISTS symbols (
  id       SERIAL PRIMARY KEY,
  symbol   VARCHAR(32) UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS candles (
  id       BIGSERIAL PRIMARY KEY,
  symbol   VARCHAR(32) NOT NULL,
  ts       TIMESTAMPTZ NOT NULL,
  open     DOUBLE PRECISION NOT NULL,
  high     DOUBLE PRECISION NOT NULL,
  low      DOUBLE PRECISION NOT NULL,
  close    DOUBLE PRECISION NOT NULL,
  volume   DOUBLE PRECISION NOT NULL,
  CONSTRAINT uq_candles_symbol_ts UNIQUE (symbol, ts)
);
CREATE INDEX IF NOT EXISTS ix_candles_symbol_ts ON candles(symbol, ts);

CREATE TABLE IF NOT EXISTS balances (
  id       BIGSERIAL PRIMARY KEY,
  asset    VARCHAR(20) NOT NULL,
  free     DOUBLE PRECISION NOT NULL,
  locked   DOUBLE PRECISION NOT NULL DEFAULT 0,
  ts       TIMESTAMPTZ NOT NULL DEFAULT (NOW() AT TIME ZONE 'utc')
);
CREATE INDEX IF NOT EXISTS ix_balances_asset_ts ON balances(asset, ts);

CREATE TABLE IF NOT EXISTS orders (
  id               BIGSERIAL PRIMARY KEY,
  symbol           VARCHAR(32) NOT NULL,
  side             VARCHAR(8)  NOT NULL,   -- BUY/SELL
  type             VARCHAR(12) NOT NULL,   -- LIMIT/MARKET
  price            DOUBLE PRECISION,
  qty              DOUBLE PRECISION NOT NULL,
  status           VARCHAR(24) NOT NULL,   -- NEW/FILLED/...
  client_order_id  VARCHAR(64) UNIQUE,
  exch_order_id    VARCHAR(64),
  created_at       TIMESTAMPTZ NOT NULL DEFAULT (NOW() AT TIME ZONE 'utc'),
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT (NOW() AT TIME ZONE 'utc')
);
CREATE INDEX IF NOT EXISTS ix_orders_symbol ON orders(symbol);
CREATE INDEX IF NOT EXISTS ix_orders_status ON orders(status);
CREATE INDEX IF NOT EXISTS ix_orders_exch_order_id ON orders(exch_order_id);

CREATE TABLE IF NOT EXISTS trades (
  id               BIGSERIAL PRIMARY KEY,
  symbol           VARCHAR(32) NOT NULL,
  side             VARCHAR(8)  NOT NULL,   -- BUY/SELL
  price            DOUBLE PRECISION NOT NULL,
  qty              DOUBLE PRECISION NOT NULL,
  fee_asset        VARCHAR(16),
  fee              DOUBLE PRECISION NOT NULL DEFAULT 0,
  order_client_id  VARCHAR(64),
  ts               TIMESTAMPTZ NOT NULL DEFAULT (NOW() AT TIME ZONE 'utc')
);
CREATE INDEX IF NOT EXISTS ix_trades_symbol_ts ON trades(symbol, ts);
CREATE INDEX IF NOT EXISTS ix_trades_order_client_id ON trades(order_client_id);
