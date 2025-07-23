CREATE TABLE IF NOT EXISTS customer (
  customer_id SERIAL PRIMARY KEY,
  name VARCHAR(100),
  email VARCHAR(100),
  phone VARCHAR(20),
  address TEXT,
  created_at TIMESTAMP WITH TIME ZONE,
  updated_at TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS products (
  product_id SERIAL PRIMARY KEY,
  product_name VARCHAR(100),
  category VARCHAR(50),
  price NUMERIC,
  stock INTEGER,
  created_at TIMESTAMP WITH TIME ZONE,
  updated_at TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS purchase (
  purchase_id SERIAL PRIMARY KEY,
  customer_id INTEGER REFERENCES customer(customer_id),
  product_id INTEGER REFERENCES products(product_id),
  quantity INTEGER,
  total_price NUMERIC,
  created_at TIMESTAMP WITH TIME ZONE,
  updated_at TIMESTAMP WITH TIME ZONE
);

-- Delete all data from the customer table
TRUNCATE TABLE customer RESTART IDENTITY CASCADE;
TRUNCATE TABLE products RESTART IDENTITY CASCADE;
TRUNCATE TABLE purchase RESTART IDENTITY CASCADE;


-- Insert 20 customers
INSERT INTO customer (name, email, phone, address, created_at, updated_at) VALUES
('Alice Johnson', 'alice@example.com', '1234567890', '123 Main St', NOW(), NOW()),
('Bob Smith', 'bob@example.com', '1234567891', '456 Oak Ave', NOW(), NOW()),
('Charlie Lee', 'charlie@example.com', '1234567892', '789 Pine Rd', NOW(), NOW()),
('Diana King', 'diana@example.com', '1234567893', '321 Maple St', NOW(), NOW()),
('Edward Kim', 'edward@example.com', '1234567894', '654 Cedar Ave', NOW(), NOW()),
('Fiona Grant', 'fiona@example.com', '1234567895', '987 Spruce Rd', NOW(), NOW()),
('George Hall', 'george@example.com', '1234567896', '159 Elm St', NOW(), NOW()),
('Hannah Wu', 'hannah@example.com', '1234567897', '753 Birch Blvd', NOW(), NOW()),
('Ian Davis', 'ian@example.com', '1234567898', '951 Willow Way', NOW(), NOW()),
('Julia Scott', 'julia@example.com', '1234567899', '357 Poplar Ln', NOW(), NOW()),
('Kevin Moore', 'kevin@example.com', '1234567800', '159 Pine Ln', NOW(), NOW()),
('Laura White', 'laura@example.com', '1234567801', '753 Fir Rd', NOW(), NOW()),
('Mike Brown', 'mike@example.com', '1234567802', '951 Ash Ct', NOW(), NOW()),
('Nina Black', 'nina@example.com', '1234567803', '357 Cypress Dr', NOW(), NOW()),
('Oscar Reed', 'oscar@example.com', '1234567804', '123 Redwood St', NOW(), NOW()),
('Paula Green', 'paula@example.com', '1234567805', '456 Hemlock Ave', NOW(), NOW()),
('Quinn Adams', 'quinn@example.com', '1234567806', '789 Dogwood Rd', NOW(), NOW()),
('Rachel Ford', 'rachel@example.com', '1234567807', '321 Palm St', NOW(), NOW()),
('Steve Nash', 'steve@example.com', '1234567808', '654 Beech Ave', NOW(), NOW()),
('Tina Lopez', 'tina@example.com', '1234567809', '987 Sycamore Rd', NOW(), NOW());

-- Insert 20 products
INSERT INTO products (product_name, category, price, stock, created_at, updated_at) VALUES
('Laptop X1', 'Electronics', 999.99, 10, NOW(), NOW()),
('Phone Z3', 'Electronics', 699.99, 15, NOW(), NOW()),
('Headphones', 'Accessories', 59.99, 50, NOW(), NOW()),
('Wireless Mouse', 'Accessories', 29.99, 30, NOW(), NOW()),
('Keyboard Pro', 'Accessories', 89.99, 25, NOW(), NOW()),
('Monitor 24"', 'Electronics', 199.99, 20, NOW(), NOW()),
('USB-C Cable', 'Accessories', 9.99, 100, NOW(), NOW()),
('Smart Watch', 'Wearables', 199.99, 18, NOW(), NOW()),
('Fitness Tracker', 'Wearables', 129.99, 12, NOW(), NOW()),
('Tablet 10"', 'Electronics', 399.99, 8, NOW(), NOW()),
('Bluetooth Speaker', 'Audio', 49.99, 40, NOW(), NOW()),
('Desk Lamp', 'Home', 24.99, 60, NOW(), NOW()),
('Backpack', 'Bags', 39.99, 22, NOW(), NOW()),
('Camera', 'Electronics', 549.99, 6, NOW(), NOW()),
('Microphone', 'Audio', 89.99, 15, NOW(), NOW()),
('Gaming Chair', 'Furniture', 299.99, 10, NOW(), NOW()),
('HDMI Cable', 'Accessories', 14.99, 70, NOW(), NOW()),
('Router', 'Networking', 129.99, 12, NOW(), NOW()),
('External HDD', 'Storage', 79.99, 14, NOW(), NOW()),
('SSD 1TB', 'Storage', 149.99, 10, NOW(), NOW());

-- Insert 20 purchases
INSERT INTO purchase (customer_id, product_id, quantity, total_price, created_at, updated_at) VALUES
(1, 1, 1, 999.99, NOW(), NOW()),
(2, 2, 1, 699.99, NOW(), NOW()),
(3, 3, 2, 119.98, NOW(), NOW()),
(4, 4, 1, 29.99, NOW(), NOW()),
(5, 5, 1, 89.99, NOW(), NOW()),
(6, 6, 1, 199.99, NOW(), NOW()),
(7, 7, 3, 29.97, NOW(), NOW()),
(8, 8, 1, 199.99, NOW(), NOW()),
(9, 9, 1, 129.99, NOW(), NOW()),
(10, 10, 1, 399.99, NOW(), NOW()),
(11, 11, 2, 99.98, NOW(), NOW()),
(12, 12, 1, 24.99, NOW(), NOW()),
(13, 13, 1, 39.99, NOW(), NOW()),
(14, 14, 1, 549.99, NOW(), NOW()),
(15, 15, 1, 89.99, NOW(), NOW()),
(16, 16, 1, 299.99, NOW(), NOW()),
(17, 17, 2, 29.98, NOW(), NOW()),
(18, 18, 1, 129.99, NOW(), NOW()),
(19, 19, 1, 79.99, NOW(), NOW()),
(20, 20, 1, 149.99, NOW(), NOW());