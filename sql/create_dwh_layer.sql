CREATE TABLE crypto_statistics (
    symbol VARCHAR(20),
    avg_price NUMERIC,
    min_price NUMERIC,
    max_price NUMERIC,
    updated_at TIMESTAMP DEFAULT NOW()
);
