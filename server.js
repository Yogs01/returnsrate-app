const express = require('express');
const multer = require('multer');
const XLSX = require('xlsx');
const crypto = require('crypto');
const path = require('path');
const fs = require('fs');
const db = require('./db');

const app = express();
const PORT = process.env.PORT || 3001;

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// Increase timeout to 10 minutes for large file uploads
const upload = multer({
  dest: process.env.UPLOADS_PATH || path.join(__dirname, 'uploads'),
  limits: { fileSize: 200 * 1024 * 1024 } // 200MB max
});

function parseNum(v) {
  if (v === null || v === undefined || v === '') return null;
  const n = parseFloat(String(v).replace(/[^0-9.\-]/g, ''));
  return isNaN(n) ? null : n;
}

function parseDate(v) {
  if (!v) return null;
  if (typeof v === 'number') {
    const d = XLSX.SSF.parse_date_code(v);
    return `${d.y}-${String(d.m).padStart(2,'0')}-${String(d.d).padStart(2,'0')}`;
  }
  const s = String(v).trim();
  const m = s.match(/^(\d{4}-\d{2}-\d{2})/);
  return m ? m[1] : s;
}

function getWeek(dateStr) {
  if (!dateStr) return null;
  const d = new Date(dateStr);
  if (isNaN(d)) return null;
  const jan1 = new Date(d.getFullYear(), 0, 1);
  return Math.ceil(((d - jan1) / 86400000 + jan1.getDay() + 1) / 7);
}

function hash(...parts) {
  return crypto.createHash('md5').update(parts.join('|')).digest('hex');
}

function fileHash(filePath) {
  return crypto.createHash('md5').update(fs.readFileSync(filePath)).digest('hex');
}

// Insert orders
const insertOrder = db.prepare(`
  INSERT OR IGNORE INTO orders
  (identifier, purchase_month, purchase_date, purchase_week, brand, amazon_order_id,
   order_status, product_name, sku, asin, item_status, quantity, item_price, item_tax, row_hash)
  VALUES (@identifier, @purchase_month, @purchase_date, @purchase_week, @brand, @amazon_order_id,
   @order_status, @product_name, @sku, @asin, @item_status, @quantity, @item_price, @item_tax, @row_hash)
`);

// Insert returns
const insertReturn = db.prepare(`
  INSERT OR IGNORE INTO returns
  (purchase_month, purchase_date, return_month, return_year, return_date, return_week,
   order_id, sku, asin, fnsku, product_name, quantity, disposition, reason, status,
   gender, brand, customer_comments, row_hash)
  VALUES (@purchase_month, @purchase_date, @return_date_parsed, @return_year, @return_date, @return_week,
   @order_id, @sku, @asin, @fnsku, @product_name, @quantity, @disposition, @reason, @status,
   @gender, @brand, @customer_comments, @row_hash)
`);

function buildOrderRecord(row) {
  const pd = parseDate(row['Purchase Date2'] || row['purchase-date']);
  return {
    identifier: String(row['Identifier'] || row['Duplicate Id'] || '').trim(),
    purchase_month: String(row['Purchase Month'] || '').trim(),
    purchase_date: pd,
    purchase_week: getWeek(pd),
    brand: String(row['Brand'] || '').trim(),
    amazon_order_id: String(row['amazon-order-id'] || '').trim(),
    order_status: String(row['order-status'] || '').trim(),
    product_name: String(row['product-name'] || '').trim(),
    sku: String(row['sku'] || '').trim(),
    asin: String(row['asin'] || '').trim(),
    item_status: String(row['item-status'] || '').trim(),
    quantity: parseNum(row['quantity']) || 0,
    item_price: parseNum(row['item-price']),
    item_tax: parseNum(row['item-tax']),
    row_hash: hash(row['Identifier'] || row['Duplicate Id'] || '', row['asin'] || '', row['purchase-date'] || row['Purchase Date2'] || '', row['item-price'] || ''),
  };
}

function buildReturnRecord(row) {
  const rd = parseDate(row['Return Date2'] || row['Return Date'] || row['return-date']);
  const pd = parseDate(row['Purchase Date'] || '');
  return {
    purchase_month: String(row['Purchase Month'] || '').trim(),
    purchase_date: pd,
    return_date_parsed: String(row['Return Month'] || '').trim(),
    return_year: String(row['Return Year'] || '').trim(),
    return_date: rd,
    return_week: getWeek(rd),
    order_id: String(row['Order ID'] || row['order-id'] || '').trim(),
    sku: String(row['SKU'] || row['sku'] || '').trim(),
    asin: String(row['ASIN'] || row['asin'] || '').trim(),
    fnsku: String(row['FNSKU'] || row['fnsku'] || '').trim(),
    product_name: String(row['Product Name'] || row['product-name'] || '').trim(),
    quantity: parseNum(row['Quantity'] || row['quantity']) || 1,
    disposition: String(row['Detailed-disposition'] || '').trim(),
    reason: String(row['Reason'] || '').trim(),
    status: String(row['Status'] || '').trim(),
    gender: String(row['Gender'] || '').trim(),
    brand: String(row['Brand'] || '').trim(),
    customer_comments: String(row['Customer-comments'] || '').trim(),
    row_hash: hash(row['Order ID'] || '', row['ASIN'] || row['asin'] || '', rd || '', row['Reason'] || ''),
  };
}

// In-memory job store
const jobs = {};

function processFileAsync(filePath, dataType, uploadedBy, filename, jobId) {
  const job = jobs[jobId];
  try {
    let wb;
    try {
      wb = XLSX.readFile(filePath, { cellDates: false });
    } catch (e) {
      job.status = 'error'; job.error = 'Cannot parse file: ' + e.message;
      try { fs.unlinkSync(filePath); } catch(_) {}
      return;
    }

    const readSheet = (name) => {
      const sheet = wb.Sheets[name] || wb.Sheets[wb.SheetNames[0]];
      if (!sheet) return [];
      const raw = XLSX.utils.sheet_to_json(sheet, { defval: '' });
      return raw.map(r => Object.fromEntries(Object.entries(r).map(([k, v]) => [k.trim(), v])));
    };

    let ordersAdded = 0, ordersSkipped = 0, returnsAdded = 0, returnsSkipped = 0;
    const hasOrders = wb.SheetNames.includes('All Orders Raw') || dataType === 'orders';
    const hasReturns = wb.SheetNames.includes('Return Raw') || dataType === 'returns';

    if (hasOrders) {
      const sheetName = wb.SheetNames.includes('All Orders Raw') ? 'All Orders Raw' : wb.SheetNames[0];
      const rows = readSheet(sheetName);
      console.log(`[${jobId}] orders sheet="${sheetName}" rows=${rows.length}`);
      const BATCH = 5000;
      for (let i = 0; i < rows.length; i += BATCH) {
        db.transaction((batch) => {
          for (const row of batch) {
            const r = insertOrder.run(buildOrderRecord(row));
            if (r.changes > 0) ordersAdded++; else ordersSkipped++;
          }
        })(rows.slice(i, i + BATCH));
        job.progress = Math.round((i + BATCH) / rows.length * 100);
      }
    }

    if (hasReturns) {
      const sheetName = wb.SheetNames.includes('Return Raw') ? 'Return Raw' : (hasOrders ? null : wb.SheetNames[0]);
      if (sheetName) {
        const rows = readSheet(sheetName);
        console.log(`[${jobId}] returns sheet="${sheetName}" rows=${rows.length}`);
        db.transaction((rows) => {
          for (const row of rows) {
            const r = insertReturn.run(buildReturnRecord(row));
            if (r.changes > 0) returnsAdded++; else returnsSkipped++;
          }
        })(rows);
      }
    }

    if (!hasOrders && !hasReturns) {
      const rows = readSheet(wb.SheetNames[0]);
      const firstKey = rows[0] ? Object.keys(rows[0])[0] : '';
      const isReturn = firstKey.includes('Return') || firstKey.includes('Order ID');
      db.transaction((rows) => {
        for (const row of rows) {
          if (isReturn) {
            const r = insertReturn.run(buildReturnRecord(row));
            if (r.changes > 0) returnsAdded++; else returnsSkipped++;
          } else {
            const r = insertOrder.run(buildOrderRecord(row));
            if (r.changes > 0) ordersAdded++; else ordersSkipped++;
          }
        }
      })(rows);
    }

    const fHash = fileHash(filePath);
    db.prepare('INSERT OR IGNORE INTO upload_log (filename, file_hash, type, rows_added, rows_skipped, uploaded_by) VALUES (?,?,?,?,?,?)')
      .run(filename, fHash, dataType, ordersAdded + returnsAdded, ordersSkipped + returnsSkipped, uploadedBy);

    job.status = 'done';
    job.ordersAdded = ordersAdded; job.ordersSkipped = ordersSkipped;
    job.returnsAdded = returnsAdded; job.returnsSkipped = returnsSkipped;
    job.progress = 100;
    console.log(`[${jobId}] done — orders +${ordersAdded}, returns +${returnsAdded}`);
  } catch(e) {
    console.error(`[${jobId}] error:`, e.message);
    jobs[jobId].status = 'error'; jobs[jobId].error = e.message;
  } finally {
    try { fs.unlinkSync(filePath); } catch(_) {}
  }
}

// POST /api/upload — responds immediately, processes in background
app.post('/api/upload', upload.single('file'), (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'No file uploaded' });
  const uploadedBy = req.body.uploaded_by || 'Team Member';
  const dataType = req.body.data_type || 'auto';
  const jobId = crypto.randomBytes(8).toString('hex');
  jobs[jobId] = { status: 'processing', progress: 0, ordersAdded: 0, ordersSkipped: 0, returnsAdded: 0, returnsSkipped: 0 };
  // Respond immediately — processing happens in background
  res.json({ success: true, jobId, processing: true });
  // Start processing after response is sent
  setImmediate(() => processFileAsync(req.file.path, dataType, uploadedBy, req.file.originalname, jobId));
});

// POST /api/import-orders — accepts pre-built JSON rows (used by seed-remote.js)
app.post('/api/import-orders', (req, res) => {
  const rows = req.body.rows;
  if (!Array.isArray(rows)) return res.status(400).json({ error: 'rows array required' });
  let added = 0, skipped = 0;
  db.transaction((rows) => {
    for (const rec of rows) {
      const r = insertOrder.run(rec);
      if (r.changes > 0) added++; else skipped++;
    }
  })(rows);
  res.json({ added, skipped });
});

// GET /api/job/:id — poll for job status
app.get('/api/job/:id', (req, res) => {
  const job = jobs[req.params.id];
  if (!job) return res.status(404).json({ error: 'Job not found' });
  res.json(job);
});

// GET /api/summary
app.get('/api/summary', (req, res) => {
  const orderStats = db.prepare(`SELECT COUNT(*) as total, SUM(CASE WHEN order_status='Shipped' THEN 1 ELSE 0 END) as shipped, SUM(CASE WHEN order_status='Cancelled' THEN 1 ELSE 0 END) as cancelled, SUM(CASE WHEN order_status='Shipped' THEN item_price ELSE 0 END) as revenue, SUM(CASE WHEN order_status='Shipped' THEN quantity ELSE 0 END) as units FROM orders`).get();
  const returnStats = db.prepare(`SELECT COUNT(*) as total, SUM(quantity) as units FROM returns`).get();

  const monthly = db.prepare(`
    SELECT
      o.purchase_month as month,
      MIN(o.purchase_date) as month_start,
      COUNT(CASE WHEN o.order_status='Shipped' THEN 1 END) as orders,
      SUM(CASE WHEN o.order_status='Shipped' THEN o.item_price ELSE 0 END) as revenue,
      SUM(CASE WHEN o.order_status='Shipped' THEN o.quantity ELSE 0 END) as units,
      COUNT(CASE WHEN o.order_status='Cancelled' THEN 1 END) as cancelled,
      COALESCE(r.return_count, 0) as returns,
      COALESCE(r.return_units, 0) as return_units
    FROM orders o
    LEFT JOIN (
      SELECT purchase_month, COUNT(*) as return_count, SUM(quantity) as return_units
      FROM returns GROUP BY purchase_month
    ) r ON o.purchase_month = r.purchase_month
    WHERE o.purchase_month != ''
    GROUP BY o.purchase_month
    ORDER BY MIN(o.purchase_date) ASC
  `).all();

  const weekly = db.prepare(`
    SELECT
      CAST(strftime('%Y', o.purchase_date) AS TEXT) as year,
      o.purchase_week as week,
      MIN(o.purchase_date) as week_start,
      COUNT(CASE WHEN o.order_status='Shipped' THEN 1 END) as orders,
      SUM(CASE WHEN o.order_status='Shipped' THEN o.item_price ELSE 0 END) as revenue,
      SUM(CASE WHEN o.order_status='Shipped' THEN o.quantity ELSE 0 END) as units,
      COALESCE(r.return_count, 0) as returns
    FROM orders o
    LEFT JOIN (
      SELECT return_week, CAST(strftime('%Y', return_date) AS TEXT) as year, COUNT(*) as return_count
      FROM returns WHERE return_date != '' GROUP BY year, return_week
    ) r ON o.purchase_week = r.return_week AND strftime('%Y', o.purchase_date) = r.year
    WHERE o.purchase_date != '' AND o.purchase_week IS NOT NULL
    GROUP BY strftime('%Y', o.purchase_date), o.purchase_week
    ORDER BY o.purchase_date ASC
  `).all();

  const byReason = db.prepare(`SELECT reason, COUNT(*) as count, SUM(quantity) as units FROM returns WHERE reason != '' GROUP BY reason ORDER BY count DESC`).all();
  const byDisposition = db.prepare(`SELECT disposition, COUNT(*) as count FROM returns WHERE disposition != '' GROUP BY disposition ORDER BY count DESC`).all();
  const byBrand = db.prepare(`SELECT brand, COUNT(*) as count, SUM(quantity) as units FROM returns WHERE brand != '' AND brand != '-' GROUP BY brand ORDER BY count DESC LIMIT 20`).all();
  const byGender = db.prepare(`SELECT gender, COUNT(*) as count FROM returns WHERE gender != '' GROUP BY gender ORDER BY count DESC`).all();

  res.json({ orderStats, returnStats, monthly, weekly, byReason, byDisposition, byBrand, byGender });
});

// GET /api/returns — paginated
app.get('/api/returns', (req, res) => {
  const page = parseInt(req.query.page) || 1;
  const limit = 50;
  const offset = (page - 1) * limit;
  const search = req.query.search || '';
  const reason = req.query.reason || '';
  const brand = req.query.brand || '';
  const month = req.query.month || '';
  const disposition = req.query.disposition || '';

  let where = 'WHERE 1=1';
  const params = [];
  if (search) { where += ' AND (product_name LIKE ? OR sku LIKE ? OR asin LIKE ? OR order_id LIKE ?)'; params.push(`%${search}%`,`%${search}%`,`%${search}%`,`%${search}%`); }
  if (reason) { where += ' AND reason = ?'; params.push(reason); }
  if (brand) { where += ' AND brand = ?'; params.push(brand); }
  if (month) { where += ' AND return_month = ?'; params.push(month); }
  if (disposition) { where += ' AND disposition = ?'; params.push(disposition); }

  const total = db.prepare(`SELECT COUNT(*) as n FROM returns ${where}`).get(...params).n;
  const records = db.prepare(`SELECT * FROM returns ${where} ORDER BY return_date DESC LIMIT ? OFFSET ?`).all(...params, limit, offset);
  res.json({ records, total, page, pages: Math.ceil(total / limit) });
});

// GET /api/filters
app.get('/api/filters', (req, res) => {
  const reasons = db.prepare(`SELECT DISTINCT reason FROM returns WHERE reason != '' ORDER BY reason`).all().map(r => r.reason);
  const brands = db.prepare(`SELECT DISTINCT brand FROM returns WHERE brand != '' AND brand != '-' ORDER BY brand`).all().map(r => r.brand);
  const months = db.prepare(`SELECT DISTINCT return_month FROM returns WHERE return_month != '' ORDER BY MIN(return_date) DESC`).all().map(r => r.return_month);
  const dispositions = db.prepare(`SELECT DISTINCT disposition FROM returns WHERE disposition != '' ORDER BY disposition`).all().map(r => r.disposition);
  const orderMonths = db.prepare(`SELECT DISTINCT purchase_month FROM orders WHERE purchase_month != '' ORDER BY MIN(purchase_date) DESC`).all().map(r => r.purchase_month);
  res.json({ reasons, brands, months, dispositions, orderMonths });
});

// GET /api/uploads
app.get('/api/uploads', (req, res) => {
  const logs = db.prepare('SELECT * FROM upload_log ORDER BY uploaded_at DESC LIMIT 50').all();
  res.json(logs);
});

// DELETE /api/uploads/reset — clears upload log so files can be re-imported
app.delete('/api/uploads/reset', (req, res) => {
  db.prepare('DELETE FROM upload_log').run();
  res.json({ success: true });
});

const server = app.listen(PORT, () => {
  console.log(`\n✅ Orders & Returns App running at http://localhost:${PORT}\n`);
});
server.setTimeout(600000); // 10 minutes for large uploads
