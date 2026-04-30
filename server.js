const express = require('express');
const multer = require('multer');
const XLSX = require('xlsx');
const crypto = require('crypto');
const path = require('path');
const fs = require('fs');
const db = require('./db');

const app = express();
const PORT = process.env.PORT || 3001;

app.use(express.json({ limit: '50mb' }));
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

// POST /api/import-returns — accepts pre-built JSON rows (used by seed-returns-remote.js)
// Inserts new rows OR updates disposition on existing rows (fixes empty disposition)
const updateReturnDisposition = db.prepare(`
  UPDATE returns SET disposition=@disposition WHERE row_hash=@row_hash AND (disposition IS NULL OR disposition='')
`);
app.post('/api/import-returns', (req, res) => {
  const rows = req.body.rows;
  if (!Array.isArray(rows)) return res.status(400).json({ error: 'rows array required' });
  let added = 0, updated = 0, skipped = 0;
  db.transaction((rows) => {
    for (const rec of rows) {
      const r = insertReturn.run(rec);
      if (r.changes > 0) {
        added++;
      } else {
        // Row exists — update disposition if it was blank
        const u = updateReturnDisposition.run({ disposition: rec.disposition, row_hash: rec.row_hash });
        if (u.changes > 0) updated++; else skipped++;
      }
    }
  })(rows);
  res.json({ added, updated, skipped });
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

// GET /api/period-stats — Last Month / 3 Months / 12 Months breakdown
// Returns are attributed to the PURCHASE MONTH of the original order (matched via Order ID)
app.get('/api/period-stats', (req, res) => {
  const oMonths = db.prepare(`
    SELECT strftime('%Y-%m', purchase_date) as month FROM orders
    WHERE purchase_date != '' AND purchase_date IS NOT NULL
    GROUP BY month ORDER BY month DESC
  `).all().map(r => r.month).filter(Boolean);

  function calc(om) {
    if (!om.length) return { orders_units: 0, returns_total: 0, sellable: 0, unsellable: 0, dispositions: [] };
    const inP = om.map(() => '?').join(',');

    // Match Excel: SUM(quantity) for all statuses except 'On Trial' (Cancelled has qty=0 so no effect)
    const orders_units = db.prepare(`
      SELECT SUM(quantity) as v FROM orders
      WHERE strftime('%Y-%m', purchase_date) IN (${inP}) AND order_status != 'On Trial'
    `).get(...om)?.v || 0;

    // Returns: use SUM(r.quantity) to match Excel's Return Qty calculation
    const ret = db.prepare(`
      SELECT
        SUM(r.quantity) as total,
        SUM(CASE WHEN r.disposition='SELLABLE' THEN r.quantity ELSE 0 END) as sellable,
        SUM(CASE WHEN r.disposition!='' AND r.disposition!='SELLABLE' THEN r.quantity ELSE 0 END) as unsellable
      FROM returns r
      JOIN orders o ON r.order_id = o.amazon_order_id
      WHERE strftime('%Y-%m', o.purchase_date) IN (${inP})
    `).get(...om);

    const dispositions = db.prepare(`
      SELECT r.disposition, SUM(r.quantity) as count
      FROM returns r
      JOIN orders o ON r.order_id = o.amazon_order_id
      WHERE strftime('%Y-%m', o.purchase_date) IN (${inP})
        AND r.disposition != '' AND r.disposition != 'SELLABLE'
      GROUP BY r.disposition ORDER BY count DESC
    `).all(...om);

    return {
      orders_units,
      returns_total: ret?.total || 0,
      sellable:      ret?.sellable || 0,
      unsellable:    ret?.unsellable || 0,
      dispositions
    };
  }

  res.json({
    lastMonth:    calc(oMonths.slice(0, 1)),
    last3Months:  calc(oMonths.slice(0, 3)),
    last12Months: calc(oMonths.slice(0, 12)),
  });
});

// GET /api/summary
app.get('/api/summary', (req, res) => {
  // Match Excel: count/sum all orders except 'On Trial'. Cancelled has qty=0 so doesn't affect units.
  const orderStats = db.prepare(`SELECT
    COUNT(*) as total,
    SUM(CASE WHEN order_status='Shipped' THEN 1 ELSE 0 END) as shipped,
    SUM(CASE WHEN order_status='Cancelled' THEN 1 ELSE 0 END) as cancelled,
    SUM(CASE WHEN order_status='Shipped' THEN item_price ELSE 0 END) as revenue,
    SUM(CASE WHEN order_status != 'On Trial' THEN quantity ELSE 0 END) as units
  FROM orders`).get();
  const returnStats = db.prepare(`SELECT COUNT(*) as total, SUM(quantity) as units FROM returns`).get();

  // Orders by purchase month
  const monthlyOrders = db.prepare(`
    SELECT strftime('%Y-%m', purchase_date) as month, MIN(purchase_date) as month_start,
      SUM(CASE WHEN order_status != 'On Trial' THEN quantity ELSE 0 END) as orders,
      SUM(CASE WHEN order_status='Shipped' THEN item_price ELSE 0 END) as revenue,
      SUM(CASE WHEN order_status != 'On Trial' THEN quantity ELSE 0 END) as units,
      COUNT(CASE WHEN order_status='Cancelled' THEN 1 END) as cancelled
    FROM orders WHERE purchase_date != '' AND purchase_date IS NOT NULL
    GROUP BY month ORDER BY month ASC
  `).all();
  // Returns attributed to PURCHASE MONTH of the original order via Order ID join
  const monthlyReturns = db.prepare(`
    SELECT strftime('%Y-%m', o.purchase_date) as month,
      SUM(r.quantity) as return_count, SUM(r.quantity) as return_units
    FROM returns r
    JOIN orders o ON r.order_id = o.amazon_order_id
    WHERE o.purchase_date != '' AND o.purchase_date IS NOT NULL
    GROUP BY month
  `).all();
  const rMap = {};
  monthlyReturns.forEach(r => { rMap[r.month] = r; });
  const monthly = monthlyOrders.map(o => ({
    ...o,
    returns: rMap[o.month]?.return_count || 0,
    return_units: rMap[o.month]?.return_units || 0
  }));

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

// DELETE /api/returns/cleanup-empty-disposition — removes old returns rows that have no disposition
// (leftover from earlier uploads before disposition was captured correctly)
app.delete('/api/returns/cleanup-empty-disposition', (req, res) => {
  const before = db.prepare('SELECT COUNT(*) as n FROM returns').get().n;
  db.prepare("DELETE FROM returns WHERE disposition IS NULL OR disposition = ''").run();
  const after = db.prepare('SELECT COUNT(*) as n FROM returns').get().n;
  res.json({ deleted: before - after, remaining: after });
});

// ─────────────────────────────────────────────────────────────────────────────
// INVENTORY AGING
// ─────────────────────────────────────────────────────────────────────────────

const insertAging = db.prepare(`
  INSERT OR IGNORE INTO inventory_aging
  (snapshot_date, sku, fnsku, asin, product_name, condition, available,
   age_0_90, age_91_180, age_181_270, age_271_365, age_365_455, age_455_plus,
   sold_t7, sold_t30, sold_t60, sold_t90, sell_through,
   recommended_action, recommended_removal_qty, unfulfillable_qty,
   storage_type, your_price, sales_rank, estimated_storage_cost,
   supplier, brand, row_hash)
  VALUES
  (@snapshot_date,@sku,@fnsku,@asin,@product_name,@condition,@available,
   @age_0_90,@age_91_180,@age_181_270,@age_271_365,@age_365_455,@age_455_plus,
   @sold_t7,@sold_t30,@sold_t60,@sold_t90,@sell_through,
   @recommended_action,@recommended_removal_qty,@unfulfillable_qty,
   @storage_type,@your_price,@sales_rank,@estimated_storage_cost,
   @supplier,@brand,@row_hash)
`);

const insertUnfulfillable = db.prepare(`
  INSERT OR IGNORE INTO inventory_unfulfillable
  (snapshot_date, sku, fnsku, asin, product_name, condition,
   unfulfillable_category, quantity, brand, supplier, row_hash)
  VALUES
  (@snapshot_date,@sku,@fnsku,@asin,@product_name,@condition,
   @unfulfillable_category,@quantity,@brand,@supplier,@row_hash)
`);

function buildAgingRecord(row) {
  const pn = v => { const n = parseNum(v); return n === null ? 0 : n; };
  const snap = parseDate(row['snapshot-date'] || row['Snapshot Date'] || '');
  const sku  = String(row['sku'] || row['SKU'] || '').trim();
  const asin = String(row['asin'] || row['ASIN'] || '').trim();
  return {
    snapshot_date:          snap || '',
    sku,
    fnsku:                  String(row['fnsku'] || row['FNSKU'] || '').trim(),
    asin,
    product_name:           String(row['product-name'] || row['Product Name'] || '').trim(),
    condition:              String(row['condition'] || row['Condition'] || '').trim(),
    available:              pn(row['afn-fulfillable-quantity'] || row['available'] || row['Available'] || row['qty-fulfillable'] || 0),
    age_0_90:               pn(row['inv-age-0-to-90-days']   || row['0-90 Days']   || 0),
    age_91_180:             pn(row['inv-age-91-to-180-days']  || row['91-180 Days']  || 0),
    age_181_270:            pn(row['inv-age-181-to-270-days'] || row['181-270 Days'] || 0),
    age_271_365:            pn(row['inv-age-271-to-365-days'] || row['271-365 Days'] || 0),
    age_365_455:            pn(row['inv-age-365-to-455-days'] || row['inv-age-366-to-455-days'] || row['365-455 Days'] || row['366-455 Days'] || 0),
    age_455_plus:           pn(row['inv-age-455-plus-days']   || row['inv-age-456-plus-days']   || row['455+ Days']   || row['456+ Days']  || 0),
    sold_t7:                pn(row['afn-sold-units-past-7-days']  || row['T7']  || 0),
    sold_t30:               pn(row['afn-sold-units-past-30-days'] || row['T30'] || 0),
    sold_t60:               pn(row['afn-sold-units-past-60-days'] || row['T60'] || 0),
    sold_t90:               pn(row['afn-sold-units-past-90-days'] || row['T90'] || 0),
    sell_through:           parseNum(row['sell-through'] || row['Sell Through'] || null),
    recommended_action:     String(row['recommended-action'] || row['Recommended Action'] || '').trim(),
    recommended_removal_qty:pn(row['recommended-removal-quantity'] || row['Removal Qty'] || 0),
    unfulfillable_qty:      pn(row['your-unfulfillable-quantity'] || row['unfulfillable-quantity'] || row['Unfulfillable'] || 0),
    storage_type:           String(row['storage-type'] || row['Storage Type'] || '').trim(),
    your_price:             parseNum(row['your-price'] || row['Price'] || null),
    sales_rank:             parseNum(row['sales-rank'] || row['Sales Rank'] || null),
    estimated_storage_cost: parseNum(row['estimated-storage-cost-per-unit'] || row['total-estimated-storage-cost'] || row['Est. Storage Cost'] || null),
    supplier:               String(row['supplier'] || row['Supplier'] || '').trim(),
    brand:                  String(row['brand'] || row['Brand'] || '').trim(),
    row_hash:               hash(snap || '', sku, asin),
  };
}

function buildUnfulfillableRecord(row) {
  const snap = parseDate(row['snapshot-date'] || row['Snapshot Date'] || '');
  const sku  = String(row['sku'] || row['SKU'] || '').trim();
  const asin = String(row['asin'] || row['ASIN'] || '').trim();
  // Try to find the category from column names
  const cats = ['Damaged','Customer Damaged','Distributor Damaged','Carrier Damaged','Defective','Expired','Warehouse Damaged'];
  let category = String(row['unfulfillable-category'] || row['Unfulfillable Category'] || row['disposition'] || '').trim();
  if (!category) {
    for (const c of cats) {
      if (row[c] || row[c.toLowerCase()] || row[c.toLowerCase().replace(/ /g,'-')]) { category = c; break; }
    }
  }
  return {
    snapshot_date:       snap || '',
    sku,
    fnsku:               String(row['fnsku'] || row['FNSKU'] || '').trim(),
    asin,
    product_name:        String(row['product-name'] || row['Product Name'] || '').trim(),
    condition:           String(row['condition'] || row['Condition'] || '').trim(),
    unfulfillable_category: category,
    quantity:            parseInt(row['quantity'] || row['Quantity'] || row['qty'] || 0) || 0,
    brand:               String(row['brand'] || row['Brand'] || '').trim(),
    supplier:            String(row['supplier'] || row['Supplier'] || '').trim(),
    row_hash:            hash(snap || '', sku, asin, category),
  };
}

// POST /api/upload-aging
app.post('/api/upload-aging', upload.single('file'), (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'No file uploaded' });
  const jobId = crypto.randomBytes(8).toString('hex');
  jobs[jobId] = { status: 'processing', progress: 0 };
  res.json({ success: true, jobId });

  setImmediate(() => {
    const job = jobs[jobId];
    try {
      const wb = XLSX.readFile(req.file.path, { cellDates: false });
      const readSheet = name => {
        const s = wb.Sheets[name] || null;
        if (!s) return [];
        return XLSX.utils.sheet_to_json(s, { defval: '' })
          .map(r => Object.fromEntries(Object.entries(r).map(([k,v]) => [k.trim(), v])));
      };

      // Find Raw Data sheet (or first sheet)
      const rawName = wb.SheetNames.find(n => /raw/i.test(n)) || wb.SheetNames[0];
      const unfulfName = wb.SheetNames.find(n => /unfulfill/i.test(n));

      let agingAdded = 0, agingSkipped = 0, unfulfAdded = 0;
      const rawRows = readSheet(rawName);
      console.log(`[aging ${jobId}] sheet="${rawName}" rows=${rawRows.length}`);

      // Log column names from first row
      if (rawRows[0]) console.log('Columns:', Object.keys(rawRows[0]).slice(0, 15).join(', '));

      db.transaction(rows => {
        for (const row of rows) {
          const rec = buildAgingRecord(row);
          const r = insertAging.run(rec);
          if (r.changes > 0) agingAdded++; else agingSkipped++;
        }
      })(rawRows);
      job.progress = 80;

      if (unfulfName) {
        const unfRows = readSheet(unfulfName);
        db.transaction(rows => {
          for (const row of rows) {
            const r = insertUnfulfillable.run(buildUnfulfillableRecord(row));
            if (r.changes > 0) unfulfAdded++;
          }
        })(unfRows);
      }

      job.status = 'done';
      job.agingAdded = agingAdded;
      job.agingSkipped = agingSkipped;
      job.unfulfAdded = unfulfAdded;
      job.progress = 100;
      console.log(`[aging ${jobId}] done — aging +${agingAdded}, unfulfilable +${unfulfAdded}`);
    } catch(e) {
      console.error(`[aging ${jobId}] error:`, e.message);
      job.status = 'error'; job.error = e.message;
    } finally {
      try { fs.unlinkSync(req.file.path); } catch(_) {}
    }
  });
});

// GET /api/aging/snapshots
app.get('/api/aging/snapshots', (req, res) => {
  const dates = db.prepare(`SELECT DISTINCT snapshot_date FROM inventory_aging WHERE snapshot_date != '' ORDER BY snapshot_date DESC`).all().map(r => r.snapshot_date);
  res.json(dates);
});

// GET /api/aging/stats?snapshot=YYYY-MM-DD
app.get('/api/aging/stats', (req, res) => {
  const snap = req.query.snapshot;
  const where = snap ? `WHERE snapshot_date = ?` : `WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM inventory_aging)`;
  const params = snap ? [snap] : [];

  const totals = db.prepare(`
    SELECT
      COUNT(*) as sku_count,
      SUM(available) as total_units,
      SUM(age_0_90) as age_0_90,
      SUM(age_91_180) as age_91_180,
      SUM(age_181_270) as age_181_270,
      SUM(age_271_365) as age_271_365,
      SUM(age_365_455) as age_365_455,
      SUM(age_455_plus) as age_455_plus,
      SUM(unfulfillable_qty) as unfulfillable,
      SUM(recommended_removal_qty) as removal_qty,
      SUM(estimated_storage_cost) as total_storage_cost
    FROM inventory_aging ${where}
  `).get(...params);

  const byActionFixed = db.prepare(`
    SELECT recommended_action, COUNT(*) as skus, SUM(available) as units
    FROM inventory_aging ${where} AND recommended_action != ''
    GROUP BY recommended_action ORDER BY skus DESC
  `).all(...params);

  const byBrand = db.prepare(`
    SELECT brand, COUNT(*) as skus, SUM(available) as units,
      SUM(age_0_90) as age_0_90, SUM(age_91_180) as age_91_180,
      SUM(age_181_270+age_271_365+age_365_455+age_455_plus) as aged_180_plus
    FROM inventory_aging ${where} AND brand != ''
    GROUP BY brand ORDER BY units DESC LIMIT 20
  `).all(...params);

  const byStorage = db.prepare(`
    SELECT storage_type, COUNT(*) as skus, SUM(available) as units
    FROM inventory_aging ${where} AND storage_type != ''
    GROUP BY storage_type ORDER BY units DESC
  `).all(...params);

  res.json({ totals, byAction: byActionFixed, byBrand, byStorage });

});

// GET /api/aging/items?snapshot=&page=&search=&brand=&action=&storage=&aged=
app.get('/api/aging/items', (req, res) => {
  const snap     = req.query.snapshot || '';
  const page     = parseInt(req.query.page) || 1;
  const limit    = 50;
  const offset   = (page - 1) * limit;
  const search   = req.query.search || '';
  const brand    = req.query.brand || '';
  const action   = req.query.action || '';
  const storage  = req.query.storage || '';
  const aged     = req.query.aged || '';   // '90', '180', '365'

  const latestSnap = db.prepare(`SELECT MAX(snapshot_date) as d FROM inventory_aging`).get()?.d || '';
  const useSnap = snap || latestSnap;

  let where = 'WHERE snapshot_date = ?';
  const params = [useSnap];
  if (search)  { where += ' AND (product_name LIKE ? OR sku LIKE ? OR asin LIKE ?)'; params.push(`%${search}%`,`%${search}%`,`%${search}%`); }
  if (brand)   { where += ' AND brand = ?'; params.push(brand); }
  if (action)  { where += ' AND recommended_action = ?'; params.push(action); }
  if (storage) { where += ' AND storage_type = ?'; params.push(storage); }
  if (aged === '90')  { where += ' AND (age_91_180+age_181_270+age_271_365+age_365_455+age_455_plus) > 0'; }
  if (aged === '180') { where += ' AND (age_181_270+age_271_365+age_365_455+age_455_plus) > 0'; }
  if (aged === '365') { where += ' AND (age_365_455+age_455_plus) > 0'; }

  const total = db.prepare(`SELECT COUNT(*) as n FROM inventory_aging ${where}`).get(...params).n;
  const records = db.prepare(`
    SELECT * FROM inventory_aging ${where}
    ORDER BY (age_181_270+age_271_365+age_365_455+age_455_plus) DESC, available DESC
    LIMIT ? OFFSET ?
  `).all(...params, limit, offset);

  res.json({ records, total, page, pages: Math.ceil(total / limit) });
});

// GET /api/aging/filters
app.get('/api/aging/filters', (req, res) => {
  const snap = db.prepare(`SELECT MAX(snapshot_date) as d FROM inventory_aging`).get()?.d || '';
  const brands   = db.prepare(`SELECT DISTINCT brand FROM inventory_aging WHERE brand != '' ORDER BY brand`).all().map(r => r.brand);
  const actions  = db.prepare(`SELECT DISTINCT recommended_action FROM inventory_aging WHERE recommended_action != '' ORDER BY recommended_action`).all().map(r => r.recommended_action);
  const storages = db.prepare(`SELECT DISTINCT storage_type FROM inventory_aging WHERE storage_type != '' ORDER BY storage_type`).all().map(r => r.storage_type);
  res.json({ brands, actions, storages, latestSnapshot: snap });
});

// POST /api/import-aging — batch insert from seed script
app.post('/api/import-aging', (req, res) => {
  const rows = req.body.rows;
  if (!Array.isArray(rows)) return res.status(400).json({ error: 'rows array required' });
  let added = 0, skipped = 0;
  db.transaction(rows => {
    for (const rec of rows) {
      const r = insertAging.run(rec);
      if (r.changes > 0) added++; else skipped++;
    }
  })(rows);
  res.json({ added, skipped });
});

// DELETE /api/aging/reset — clear all aging data
app.delete('/api/aging/reset', (req, res) => {
  db.prepare('DELETE FROM inventory_aging').run();
  db.prepare('DELETE FROM inventory_unfulfillable').run();
  res.json({ success: true });
});

const server = app.listen(PORT, () => {
  console.log(`\n✅ Orders & Returns App running at http://localhost:${PORT}\n`);
});
server.setTimeout(600000); // 10 minutes for large uploads
