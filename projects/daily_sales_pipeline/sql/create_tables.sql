CREATE TABLE IF NOT EXISTS demo_sales_orders (
    order_id INTEGER PRIMARY KEY,
    order_date DATE NOT NULL,
    customer TEXT NOT NULL,
    region TEXT NOT NULL,
    product TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price NUMERIC(10, 2) NOT NULL,
    total_amount NUMERIC(10, 2) NOT NULL
);

CREATE TABLE IF NOT EXISTS demo_sales_summary (
    region TEXT PRIMARY KEY,
    order_count INTEGER NOT NULL,
    total_quantity INTEGER NOT NULL,
    total_amount NUMERIC(10, 2) NOT NULL,
    refreshed_at TIMESTAMP NOT NULL DEFAULT NOW()
);

