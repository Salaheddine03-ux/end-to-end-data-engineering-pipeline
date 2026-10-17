
CREATE TABLE IF NOT EXISTS products (
    product_id INTEGER PRIMARY KEY,
    title TEXT NOT NULL,
    category VARCHAR(100),
    brand VARCHAR(100),
    sku VARCHAR(100),
    price NUMERIC(10,2),
    discount_percentage NUMERIC(5,2),
    rating NUMERIC(3,2),
    stock INTEGER,
    availability_status VARCHAR(50),
    minimum_order_quantity INTEGER,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
);
