CREATE TABLE IF NOT EXISTS orders (
    event_id UUID PRIMARY KEY,
    order_id VARCHAR(40) NOT NULL,
    customer_id VARCHAR(40) NOT NULL,
    product_id VARCHAR(40) NOT NULL,
    product_name VARCHAR(100) NOT NULL,
    category VARCHAR(60) NOT NULL,
    region VARCHAR(30) NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price NUMERIC(12, 2) NOT NULL CHECK (unit_price >= 0),
    discount_pct NUMERIC(5, 4) NOT NULL CHECK (discount_pct BETWEEN 0 AND 1),
    gross_amount NUMERIC(14, 2) NOT NULL,
    net_amount NUMERIC(14, 2) NOT NULL,
    status VARCHAR(20) NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_orders_event_time ON orders (event_time DESC);
CREATE INDEX IF NOT EXISTS idx_orders_category ON orders (category);
CREATE INDEX IF NOT EXISTS idx_orders_region ON orders (region);

