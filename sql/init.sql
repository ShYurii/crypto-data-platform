-- =========================================
-- 1. CREATE SCHEMA (DWH LAYER)
-- =========================================
CREATE SCHEMA IF NOT EXISTS warehouse;

-- =========================================
-- 2. RAW LAYER TABLE
-- =========================================
DROP TABLE IF EXISTS warehouse.bitcoin_prices;

CREATE TABLE warehouse.bitcoin_prices (
    id SERIAL PRIMARY KEY,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    price_usd NUMERIC(38, 18) NOT NULL,
    run_id TEXT NOT NULL UNIQUE,
    symbol TEXT NOT NULL,
    source TEXT NOT NULL
);

-- =========================================
-- 3. AGGREGATION / GOLD LAYER TABLE
-- =========================================
DROP TABLE IF EXISTS warehouse.crypto_statistics;

CREATE TABLE warehouse.crypto_statistics (
    symbol TEXT NOT NULL,

    avg_price NUMERIC(18, 4),
    max_price NUMERIC(18, 4),
    min_price NUMERIC(18, 4),

    records_count BIGINT NOT NULL,

    snapshot_time TIMESTAMP NOT NULL DEFAULT NOW()
);

-- =========================================
-- 4. INDEXES (optional but GOOD PRACTICE)
-- =========================================

-- Fast filtering by symbol
CREATE INDEX idx_crypto_statistics_symbol
ON warehouse.crypto_statistics(symbol);

-- Fast time-based analytics
CREATE INDEX idx_crypto_statistics_snapshot_time
ON warehouse.crypto_statistics(snapshot_time);

-- Fast lookup in raw table
CREATE INDEX idx_bitcoin_prices_symbol
ON warehouse.bitcoin_prices(symbol);

CREATE INDEX idx_bitcoin_prices_created_at
ON warehouse.bitcoin_prices(created_at);

-- =========================================
-- 5. CHECKS (DATA QUALITY LAYER - optional)
-- =========================================

ALTER TABLE warehouse.bitcoin_prices
ADD CONSTRAINT price_positive CHECK (price_usd > 0);

-- =========================================
-- DONE
-- =========================================