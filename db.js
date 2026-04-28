const Database = require('better-sqlite3');
const path = require('path');

const db = new Database(process.env.DB_PATH || path.join(__dirname, 'orders.db'));

db.exec(`
  CREATE TABLE IF NOT EXISTS orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    identifier TEXT,
    purchase_month TEXT,
    purchase_date TEXT,
    purchase_week INTEGER,
    brand TEXT,
    amazon_order_id TEXT,
    order_status TEXT,
    product_name TEXT,
    sku TEXT,
    asin TEXT,
    item_status TEXT,
    quantity INTEGER,
    item_price REAL,
    item_tax REAL,
    row_hash TEXT UNIQUE
  );

  CREATE TABLE IF NOT EXISTS returns (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    purchase_month TEXT,
    purchase_date TEXT,
    return_month TEXT,
    return_year TEXT,
    return_date TEXT,
    return_week INTEGER,
    order_id TEXT,
    sku TEXT,
    asin TEXT,
    fnsku TEXT,
    product_name TEXT,
    quantity INTEGER,
    disposition TEXT,
    reason TEXT,
    status TEXT,
    gender TEXT,
    brand TEXT,
    customer_comments TEXT,
    row_hash TEXT UNIQUE
  );

  CREATE TABLE IF NOT EXISTS upload_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    filename TEXT,
    file_hash TEXT UNIQUE,
    type TEXT,
    rows_added INTEGER,
    rows_skipped INTEGER,
    uploaded_by TEXT,
    uploaded_at TEXT DEFAULT (datetime('now'))
  );

  CREATE INDEX IF NOT EXISTS idx_orders_month ON orders(purchase_month);
  CREATE INDEX IF NOT EXISTS idx_orders_week ON orders(purchase_week, purchase_date);
  CREATE INDEX IF NOT EXISTS idx_orders_brand ON orders(brand);
  CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(order_status);
  CREATE INDEX IF NOT EXISTS idx_orders_status_month ON orders(order_status, purchase_month);
  CREATE INDEX IF NOT EXISTS idx_orders_date ON orders(purchase_date);
  CREATE INDEX IF NOT EXISTS idx_returns_month ON returns(return_month);
  CREATE INDEX IF NOT EXISTS idx_returns_reason ON returns(reason);
  CREATE INDEX IF NOT EXISTS idx_returns_brand ON returns(brand);
  CREATE INDEX IF NOT EXISTS idx_returns_date ON returns(return_date);
  CREATE INDEX IF NOT EXISTS idx_returns_disposition ON returns(disposition, return_month);
`);

module.exports = db;
