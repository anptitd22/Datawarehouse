create database datamart_db;
USE datamart_db;
-- 1. DIMENSION TABLES (với surrogate keys)
-- Dim Date
CREATE TABLE dim_date (
    date_key INT IDENTITY(1,1) PRIMARY KEY,
    date DATE,
    day INT,
    week INT,
    month INT,
    quarter INT,
    year INT
);
-- Dim Customer
CREATE TABLE dim_customer (
    customer_key INT IDENTITY(1,1) PRIMARY KEY, -- Surrogate key
    client_id NVARCHAR(255) NOT NULL UNIQUE, -- Natural key
    name NVARCHAR(255),
    email NVARCHAR(255),
    phone NVARCHAR(50),
    address NVARCHAR(255),
    total_spent DECIMAL(15,2),
    num_order INT
);

-- Dim Product
CREATE TABLE dim_product (
    product_key INT IDENTITY(1,1) PRIMARY KEY, -- Surrogate key
    product_id NVARCHAR(255) NOT NULL UNIQUE, -- Natural key
    name NVARCHAR(255) not null,
    brand NVARCHAR(255),
    category NVARCHAR(255), 
    import_price DECIMAL(15,2) not null,
    selling_price DECIMAL(15,2) not null,
    promotion_price DECIMAL(15,2) not null,
	sold BigInt,
	stock bigint,
    rating DECIMAL(3,2)
);

-- Dim Order (Degenerate Dimension)
CREATE TABLE dim_order (
    order_key INT IDENTITY(1,1) PRIMARY KEY, -- Surrogate key
    order_id NVARCHAR(255) NOT NULL, 
    order_item_id NVARCHAR(255) NOT NULL UNIQUE,-- Natural key
    status NVARCHAR(50) NOT NULL,
    payment_method NVARCHAR(50) NOT NULL
);
-- 2. FACT TABLES (sử dụng surrogate keys để kết nối)
-- Fact Sales
CREATE TABLE fact_sales (
    sales_key INT IDENTITY(1,1) PRIMARY KEY, -- Surrogate key cho fact
    order_key INT NOT NULL, -- Tham chiếu tới dim_order
    customer_key INT NOT NULL, -- Tham chiếu tới dim_customer
    product_key INT NOT NULL, -- Tham chiếu tới dim_product
    date_key INT NOT NULL, -- Tham chiếu tới dim_date
    quantity INT NOT NULL,
    unit_price DECIMAL(15,2) NOT NULL,
    total_price DECIMAL(15,2) NOT NULL,
    cost DECIMAL(15,2) NOT NULL,
    revenue DECIMAL(15,2) NOT NULL,
    profit DECIMAL(15,2) NOT NULL,
    CONSTRAINT fk_sales_order FOREIGN KEY (order_key) REFERENCES dim_order(order_key),
    CONSTRAINT fk_sales_customer FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key),
    CONSTRAINT fk_sales_product FOREIGN KEY (product_key) REFERENCES dim_product(product_key),
    CONSTRAINT fk_sales_date FOREIGN KEY (date_key) REFERENCES dim_date(date_key)
);

-- Fact Status
CREATE TABLE fact_status (
    status_key INT IDENTITY(1,1) PRIMARY KEY, -- Surrogate key cho fact
    order_key INT NOT NULL,
    date_key INT NOT NULL,
    delivered_count INT NOT NULL,
    cancelled_count INT NOT NULL,
    delivery_percentage DECIMAL(5,2) NOT NULL,
    cancel_percentage DECIMAL(5,2) NOT NULL,
    CONSTRAINT fk_status_order FOREIGN KEY (order_key) REFERENCES dim_order(order_key),
    CONSTRAINT fk_status_date FOREIGN KEY (date_key) REFERENCES dim_date(date_key)
);
-- 3. TẠO INDEX ĐỂ TỐI ƯU HIỆU NĂNG
CREATE INDEX idx_fact_sales_date ON fact_sales(date_key);
CREATE INDEX idx_fact_sales_product ON fact_sales(product_key);
CREATE INDEX idx_fact_sales_customer ON fact_sales(customer_key);
CREATE INDEX idx_fact_sales_order ON fact_sales(order_key);
CREATE INDEX idx_fact_status_date ON fact_status(date_key);
CREATE INDEX idx_fact_status_order ON fact_status(order_key);