const express = require('express');
const multer = require('multer');
const XLSX = require('xlsx');
const crypto = require('crypto');
const path = require('path');
const fs = require('fs');
const zlib = require('zlib');
const db = require('./db');

const app = express();
const PORT = process.env.PORT || 3001;

// ─── Gzip compression for all JSON responses ─────────────────────────────────
app.use((req, res, next) => {
  const ae = req.headers['accept-encoding'] || '';
  if (!ae.includes('gzip')) return next();
  const _json = res.json.bind(res);
  res.json = (data) => {
    const buf = Buffer.from(JSON.stringify(data));
    zlib.gzip(buf, (err, compressed) => {
      if (err) return _json(data);
      res.set({ 'Content-Encoding': 'gzip', 'Content-Type': 'application/json', 'Vary': 'Accept-Encoding' });
      res.send(compressed);
    });
  };
  next();
});

// ─── Cache-Control for static assets ─────────────────────────────────────────
app.use(express.static(path.join(__dirname, 'public'), {
  setHeaders(res, filePath) {
    if (filePath.endsWith('.html')) res.set('Cache-Control', 'no-cache');
    else res.set('Cache-Control', 'public, max-age=86400'); // 1 day for CSS/JS
  }
}));

app.use(express.json({ limit: '50mb' }));

// ─── Server-side filter cache (invalidated on every upload) ──────────────────
// /api/filters is expensive (5 DISTINCT queries across 300K rows) but only
// changes when new data is uploaded. Cache it for up to 5 minutes.
let _filtersCache = null;
let _filtersCacheTs = 0;
const FILTERS_TTL = 5 * 60 * 1000; // 5 minutes
function invalidateFiltersCache() { _filtersCache = null; _filtersCacheTs = 0; }

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

// Add gender column to orders if it doesn't exist yet (one-time migration)
try { db.exec(`ALTER TABLE orders ADD COLUMN gender TEXT DEFAULT ''`); } catch(e) {}
db.exec(`CREATE INDEX IF NOT EXISTS idx_orders_gender ON orders(gender)`);

// Add style_name column to orders (core model name, stripped of brand/color/size)
try { db.exec(`ALTER TABLE orders ADD COLUMN style_name TEXT DEFAULT ''`); } catch(e) {}
db.exec(`CREATE INDEX IF NOT EXISTS idx_orders_style_name ON orders(brand, style_name)`);

// Insert orders
const insertOrder = db.prepare(`
  INSERT OR IGNORE INTO orders
  (identifier, purchase_month, purchase_date, purchase_week, brand, amazon_order_id,
   order_status, product_name, style_name, sku, asin, item_status, quantity, item_price, item_tax, gender, row_hash)
  VALUES (@identifier, @purchase_month, @purchase_date, @purchase_week, @brand, @amazon_order_id,
   @order_status, @product_name, @style_name, @sku, @asin, @item_status, @quantity, @item_price, @item_tax, @gender, @row_hash)
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

// One canonical spelling per brand — no duplicates.
// All case variants (SOREL / sorel / Sorel) are normalised to this form via canonicalizeBrand().
const KNOWN_BRANDS = [
  'Good Smile Company','Teenage Mutant Ninja Turtles',
  'Dungeons & Dragons','Under Armour','Diamond Select','Orange Rouge',
  'The Loyal Subjects','Max Factory','Square Enix','Games Workshop',
  'Hiya Toys','Hot Toys','Sen-Ti-Nel','Warhammer 40k',
  'Jason Markk','SAXX Underwear Co.','SAXX','Hey Dude',
  'Fjallraven','Hydro Flask','Loungefly','CamelBak','ThreeZero',
  'Mezco','Funko','Hasbro','Mattel','Bandai','Capcom','NECA',
  'Megahouse','Furyu','SEGA','Vionic','Timberland','Saucony','Rockport',
  'Reebok','Merrell','Garmont','Brooks','K-Swiss','ASICS',
  'Clarks','Chaco','Marmot','Kelty','Altra','OOFOS','Hestra','Sperry',
  'DenTek','Sharpie','HOKA','KEEN','ECCO','Muck',
  'Smith','Cole Haan','adidas','Nike','Veja','Crocs',
  'Birkenstock','Salomon','Teva','UGG','Bogs','Sorel','Danner','Oboz',
  'New Balance','Puma','Osprey','Gregory','Deuter','Thule','Patagonia',
  'Columbia','Arc\'teryx','Cotopaxi','Eagle Creek','Warhammer',
  'Keds','REEF','Caterpillar','CAT Footwear','On',
  'Blundstone','Vans','Converse','Skechers','Dr. Martens','Hunter',
  'Baffin','Kamik','Muck Boot','Pendleton','Woolrich',
  'Henley Hansen','Helly Hansen','The North Face','Black Diamond',
  'Darn Tough','Smartwool','Wigwam','Thorlos','Bombas'
].sort((a, b) => b.length - a.length);

// Infer gender from an explicit value (CSV column) or product name patterns.
// Called at record-build time so the result is stored — no LIKE scans at query time.
function inferGender(rawGender, productName) {
  const g = String(rawGender || '').trim();
  // Treat blank or literal "Unknown" (from Amazon CSV) as unresolved — try product name instead
  if (g && g.toLowerCase() !== 'unknown') return g;
  const p = String(productName || '').toLowerCase();
  if (p.includes("women's") || p.includes('womens') || p.includes(' (w)')) return 'Women';
  if (p.includes('girls'))                                                   return 'Girls';
  if (p.includes("men's")  || p.includes('mens'))                           return 'Men';
  if (p.includes('boys'))                                                    return 'Boys';
  if (p.includes(' (m)'))                                                    return 'Men';
  return '';  // genuinely unknown — stored as empty, displayed as "Unknown"
}

// Fill gender for returns rows whose gender column is blank or 'Unknown'.
// Pass 1 — uses the return's own product_name (fast, no join).
// Pass 2 — uses the matched order's product_name (catches cases where the
//           returns CSV product name differs from the orders CSV product name).
// 'Unknown' (literal string from Amazon CSV) is treated the same as blank —
// we try to override it with a real value inferred from product name.
// Safe to call on every startup and after every upload: WHERE clauses limit
// work to unresolved rows only, so re-runs are cheap no-ops.
function backfillReturnGender() {
  try {
    // Rows eligible for backfill: blank, NULL, or literal 'Unknown' from CSV
    const blankGender = `(gender IS NULL OR gender = '' OR gender = 'Unknown')`;

    const patternWhere = `(
      product_name LIKE '%Women''s%' OR product_name LIKE '%Womens%' OR product_name LIKE '% (W)%'
      OR product_name LIKE '%Girls%'
      OR product_name LIKE '%Men''s%'  OR product_name LIKE '%Mens%'
      OR product_name LIKE '%Boys%'
      OR product_name LIKE '% (M)%'
    )`;
    const genderCase = `CASE
      WHEN product_name LIKE '%Women''s%' OR product_name LIKE '%Womens%'
        OR product_name LIKE '% (W)%'                          THEN 'Women'
      WHEN product_name LIKE '%Girls%'                          THEN 'Girls'
      WHEN product_name LIKE '%Men''s%' OR product_name LIKE '%Mens%'
                                                                THEN 'Men'
      WHEN product_name LIKE '%Boys%'                           THEN 'Boys'
      WHEN product_name LIKE '% (M)%'                           THEN 'Men'
      ELSE ''
    END`;

    // Pass 1: infer from returns.product_name
    const r1 = db.prepare(`
      UPDATE returns SET gender = ${genderCase}
      WHERE ${blankGender} AND ${patternWhere}
    `).run();

    // Pass 2: infer from matching orders.product_name for rows still unresolved
    const orderPatternWhere = patternWhere.replace(/product_name/g, 'o.product_name');
    const orderGenderCase   = genderCase.replace(/product_name/g, 'o.product_name');
    const r2 = db.prepare(`
      UPDATE returns SET gender = (
        SELECT ${orderGenderCase}
        FROM orders o
        WHERE o.amazon_order_id = returns.order_id AND o.sku = returns.sku
        LIMIT 1
      )
      WHERE ${blankGender}
        AND EXISTS (
          SELECT 1 FROM orders o
          WHERE o.amazon_order_id = returns.order_id AND o.sku = returns.sku
            AND ${orderPatternWhere}
        )
    `).run();

    const total = r1.changes + r2.changes;
    if (total > 0) {
      console.log(`[backfill] gender: ${r1.changes} from return names, ${r2.changes} from order names`);
      invalidateFiltersCache();
    }
    return total;
  } catch(e) {
    console.error('[backfill] gender error:', e.message);
    return 0;
  }
}
// Fill gender for orders rows that have a blank gender column.
// Uses the same patterns as inferGender() / backfillReturnGender().
function backfillOrderGender() {
  try {
    const blankGender = `(gender IS NULL OR gender = '' OR gender = 'Unknown')`;
    const patternWhere = `(
      product_name LIKE '%Women''s%' OR product_name LIKE '%Womens%' OR product_name LIKE '% (W)%'
      OR product_name LIKE '%Girls%'
      OR product_name LIKE '%Men''s%'  OR product_name LIKE '%Mens%'
      OR product_name LIKE '%Boys%'
      OR product_name LIKE '% (M)%'
    )`;
    const genderCase = `CASE
      WHEN product_name LIKE '%Women''s%' OR product_name LIKE '%Womens%'
        OR product_name LIKE '% (W)%'                          THEN 'Women'
      WHEN product_name LIKE '%Girls%'                          THEN 'Girls'
      WHEN product_name LIKE '%Men''s%' OR product_name LIKE '%Mens%'
                                                                THEN 'Men'
      WHEN product_name LIKE '%Boys%'                           THEN 'Boys'
      WHEN product_name LIKE '% (M)%'                           THEN 'Men'
      ELSE ''
    END`;
    const r = db.prepare(`UPDATE orders SET gender = ${genderCase} WHERE ${blankGender} AND ${patternWhere}`).run();
    if (r.changes > 0) console.log(`[backfill] orders gender: ${r.changes} rows`);
    return r.changes;
  } catch(e) {
    console.error('[backfill] orders gender error:', e.message);
    return 0;
  }
}
// Backfill style_name for all orders rows where it is blank.
// Reads rows in JS and applies extractStyleName() — avoids complex SQLite string manipulation.
function backfillStyleName() {
  try {
    const rows = db.prepare(`SELECT id, product_name, brand FROM orders WHERE style_name IS NULL OR style_name = ''`).all();
    if (!rows.length) return 0;
    const upd = db.prepare(`UPDATE orders SET style_name = ? WHERE id = ?`);
    let changed = 0;
    db.transaction(() => {
      for (const row of rows) {
        const sn = extractStyleName(row.product_name || '', row.brand || '');
        if (sn) { upd.run(sn, row.id); changed++; }
      }
    })();
    if (changed > 0) console.log(`[backfill] style_name: ${changed} rows`);
    return changed;
  } catch(e) {
    console.error('[backfill] style_name error:', e.message);
    return 0;
  }
}
// Startup backfills are deferred until after app.listen() so Railway's health
// check succeeds before the (potentially slow) LIKE-scan migrations run.

// Returns the canonical KNOWN_BRANDS spelling for a brand (case-insensitive match).
// E.g. "SOREL" → "Sorel", "hoka" → "HOKA", "merrell" → "Merrell".
// Falls back to the original string trimmed if not in the list.
function canonicalizeBrand(brand) {
  if (!brand || brand === '-' || brand === '0') return brand;
  const lower = brand.trim().toLowerCase();
  for (const kb of KNOWN_BRANDS) {
    if (kb.toLowerCase() === lower) return kb;
  }
  return brand.trim();
}

// Extract the core model/style name from a full Amazon product name.
// Strips: brand prefix, color/size suffixes (after " - " or " | "), gender markers.
// E.g. "HOKA Arahi 8 Women's - White/Ice Water" → "Arahi 8"
//      "Blundstone 500 Series Boot - Brown - Size 9" → "500 Series Boot"
function extractStyleName(productName, brand) {
  if (!productName) return '';
  let s = productName.trim();

  // 1. Strip brand prefix (supplied brand takes priority)
  if (brand) {
    const safe = brand.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    s = s.replace(new RegExp(`^${safe}\\s+`, 'i'), '').trim();
  }
  // Also try all KNOWN_BRANDS as fallback
  for (const kb of KNOWN_BRANDS) {
    if (s.toLowerCase().startsWith(kb.toLowerCase() + ' ')) {
      s = s.slice(kb.length).trim();
      break;
    }
  }

  // 2. Drop everything from the first " - " or " | " separator onward
  //    (Amazon uses these to delimit color and size in product listings)
  const sepIdx = s.search(/\s+-\s+|\s+\|\s+/);
  if (sepIdx > 0) s = s.slice(0, sepIdx).trim();

  // 3. Strip trailing gender markers
  s = s.replace(/\s+(?:Women'?s?|Men'?s?|Kids?|Boys?|Girls?|Unisex|\(W\)|\(M\))\s*$/i, '').trim();

  return s || productName.trim();
}

function extractBrandFromName(productName) {
  if (!productName || productName === '-') return null;
  const p = productName.trim();
  // 1. Known brand as prefix — always returns the canonical spelling
  for (const brand of KNOWN_BRANDS) {
    if (p.toLowerCase().startsWith(brand.toLowerCase())) return brand;
  }
  // 2. Chaco Z-model codes (Z1, Z/1, Z/2, Z/X2, MEGA Z etc.)
  if (/^(Z\d|Z\/\d|Z\/X|MEGA Z)/i.test(p)) return 'Chaco';

  // 3. Warhammer kits by known unit/book names
  const warhammerKeywords = ['XV8','Nurglings','Kill Team','Spearhead','Knight Household',
    'Horus Heresy','Chaos Space','Space Marine','Age of Sigmar','Blood Angels',
    'Ork ','Eldar','Necron','Tau ','Tyranid'];
  for (const kw of warhammerKeywords) {
    if (p.toLowerCase().includes(kw.toLowerCase())) return 'Warhammer';
  }

  // 4. "Brand - Product" dash pattern
  const m = p.match(/^([A-Za-z0-9][^-]{1,30}?)\s*[-–]\s+\S/);
  if (m) {
    const c = m[1].trim();
    if (c.length >= 2 && c.length <= 30) return c;
  }
  return null;
}

// Auto-fill blank brands AND canonicalize existing brand spellings.
// Called after every upload or JSON import. Returns { orders, returns } counts fixed.
function runBrandFix() {
  const updateOrders = db.prepare(`UPDATE orders  SET brand = ? WHERE id = ?`);
  const updateRets   = db.prepare(`UPDATE returns SET brand = ? WHERE id = ?`);

  // Pass 1: fill in blank brands from product_name
  const emptyOrders = db.prepare(`
    SELECT id, product_name FROM orders
    WHERE (brand IS NULL OR brand = '' OR brand = '-' OR brand = '0')
      AND product_name IS NOT NULL AND product_name != ''
  `).all();
  let fixedOrders = 0;
  db.transaction(() => {
    for (const row of emptyOrders) {
      const brand = extractBrandFromName(row.product_name);
      if (brand) { updateOrders.run(brand, row.id); fixedOrders++; }
    }
  })();

  const emptyRets = db.prepare(`
    SELECT id, product_name FROM returns
    WHERE (brand IS NULL OR brand = '' OR brand = '-' OR brand = '0')
      AND product_name IS NOT NULL AND product_name != ''
  `).all();
  let fixedReturns = 0;
  db.transaction(() => {
    for (const row of emptyRets) {
      const brand = extractBrandFromName(row.product_name);
      if (brand) { updateRets.run(brand, row.id); fixedReturns++; }
    }
  })();

  // Pass 2: canonicalize existing non-blank brands (e.g. "SOREL" → "Sorel", "HOKA" → "HOKA")
  // This removes duplicates caused by case mismatches between raw file data and brand detector.
  const allOrders = db.prepare(`SELECT id, brand FROM orders WHERE brand IS NOT NULL AND brand != '' AND brand != '-' AND brand != '0'`).all();
  db.transaction(() => {
    for (const row of allOrders) {
      const canonical = canonicalizeBrand(row.brand);
      if (canonical !== row.brand) { updateOrders.run(canonical, row.id); fixedOrders++; }
    }
  })();

  const allRets = db.prepare(`SELECT id, brand FROM returns WHERE brand IS NOT NULL AND brand != '' AND brand != '-' AND brand != '0'`).all();
  db.transaction(() => {
    for (const row of allRets) {
      const canonical = canonicalizeBrand(row.brand);
      if (canonical !== row.brand) { updateRets.run(canonical, row.id); fixedReturns++; }
    }
  })();

  if (fixedOrders + fixedReturns > 0)
    console.log(`[brand-fix] orders=${fixedOrders} returns=${fixedReturns}`);
  return { orders: fixedOrders, returns: fixedReturns };
}

function buildOrderRecord(row) {
  const pd = parseDate(row['Purchase Date2'] || row['purchase-date']);
  const rawBrand = String(row['Brand'] || '').trim();
  const productName = String(row['product-name'] || '').trim();
  const brand = canonicalizeBrand(
    (rawBrand && rawBrand !== '-' && rawBrand !== '0')
      ? rawBrand
      : (extractBrandFromName(productName) || rawBrand)
  );
  return {
    identifier: String(row['Identifier'] || row['Duplicate Id'] || '').trim(),
    purchase_month: String(row['Purchase Month'] || '').trim(),
    purchase_date: pd,
    purchase_week: getWeek(pd),
    brand,
    amazon_order_id: String(row['amazon-order-id'] || '').trim(),
    order_status: String(row['order-status'] || '').trim(),
    product_name: productName,
    style_name: extractStyleName(productName, brand),
    sku: String(row['sku'] || '').trim(),
    asin: String(row['asin'] || '').trim(),
    item_status: String(row['item-status'] || '').trim(),
    quantity: parseNum(row['quantity']) || 0,
    item_price: parseNum(row['item-price']),
    item_tax: parseNum(row['item-tax']),
    gender: inferGender('', productName),
    row_hash: hash(row['Identifier'] || row['Duplicate Id'] || '', row['asin'] || '', row['purchase-date'] || row['Purchase Date2'] || '', row['item-price'] || ''),
  };
}

function buildReturnRecord(row) {
  const rd = parseDate(row['Return Date2'] || row['Return Date'] || row['return-date']);
  const pd = parseDate(row['Purchase Date'] || '');
  // Derive return_month and return_year from date if not explicitly in the file (Amazon CSV format)
  const rdObj = rd ? new Date(rd + 'T00:00:00') : null;
  const derivedMonth = rdObj ? rdObj.toLocaleString('en-US', { month: 'long', year: 'numeric' }) : '';
  const derivedYear  = rdObj ? String(rdObj.getFullYear()) : '';
  const reason = String(row['Reason'] || row['reason'] || '').trim();
  return {
    purchase_month:     String(row['Purchase Month'] || '').trim(),
    purchase_date:      pd,
    return_date_parsed: String(row['Return Month'] || '').trim() || derivedMonth,
    return_year:        String(row['Return Year']  || '').trim() || derivedYear,
    return_date:        rd,
    return_week:        getWeek(rd),
    order_id:           String(row['Order ID'] || row['order-id'] || '').trim(),
    sku:                String(row['SKU']  || row['sku']  || '').trim(),
    asin:               String(row['ASIN'] || row['asin'] || '').trim(),
    fnsku:              String(row['FNSKU']|| row['fnsku']|| '').trim(),
    product_name:       String(row['Product Name'] || row['product-name'] || '').trim(),
    quantity:           parseNum(row['Quantity'] || row['quantity']) || 1,
    disposition:        String(row['Detailed-disposition'] || row['detailed-disposition'] || '').trim(),
    reason,
    status:             String(row['Status'] || row['status'] || '').trim(),
    gender:             inferGender(row['Gender'] || row['gender'] || '', row['Product Name'] || row['product-name'] || ''),
    brand:              canonicalizeBrand(String(row['Brand'] || row['brand'] || '').trim()),
    customer_comments:  String(row['Customer-comments'] || row['customer-comments'] || '').trim(),
    row_hash:           hash(String(row['Order ID'] || row['order-id'] || '').trim(), String(row['ASIN'] || row['asin'] || '').trim(), rd || '', reason),
  };
}

// In-memory job store
const jobs = {};

// Stream a sheet row-by-row using ExcelJS streaming API.
// Only one row is in memory at a time — safe for 300K+ row sheets.
// Calls batchCallback(rows[]) synchronously for each batch.
// Returns a Promise<number> (total data rows processed).
const ExcelJS = require('exceljs');

function processSheetBatched(filePath, sheetName, batchSize, batchCallback) {
  return new Promise((resolve, reject) => {
    const options = {
      entries: 'emit',
      sharedStrings: 'cache',
      hyperlinks: 'ignore',
      styles: 'ignore',
      worksheets: 'emit',
    };
    const workbookReader = new ExcelJS.stream.xlsx.WorkbookReader(filePath, options);
    let headers = null, batch = [], total = 0, found = false;

    workbookReader.on('worksheet', worksheetReader => {
      if (worksheetReader.name !== sheetName) {
        // Skip non-target sheets — just consume rows without processing
        worksheetReader.on('row', () => {});
        return;
      }
      found = true;

      worksheetReader.on('row', row => {
        if (row.number === 1) {
          // Header row
          headers = [];
          row.eachCell({ includeEmpty: true }, (cell, colNum) => {
            headers[colNum - 1] = cell.value !== null && cell.value !== undefined
              ? String(cell.value).trim() : '';
          });
          return;
        }
        if (!headers) return;

        const obj = {};
        headers.forEach((h, i) => {
          const cell = row.getCell(i + 1);
          let v = cell.value;
          // ExcelJS returns rich text as objects — flatten to string
          if (v && typeof v === 'object' && v.richText) v = v.richText.map(r => r.text).join('');
          if (v && typeof v === 'object' && v.result !== undefined) v = v.result; // formula result
          obj[h] = v !== null && v !== undefined ? v : '';
        });

        batch.push(obj);
        total++;
        if (batch.length >= batchSize) {
          batchCallback(batch);
          batch = [];
        }
      });

      worksheetReader.on('end', () => {
        if (batch.length > 0) { batchCallback(batch); batch = []; }
      });

      worksheetReader.on('error', reject);
    });

    workbookReader.on('end', () => resolve(total));
    workbookReader.on('error', reject);
    workbookReader.read();
  });
}

async function processFileAsync(filePath, dataType, uploadedBy, filename, jobId) {
  const job = jobs[jobId];
  try {
    // Step 1: peek at sheet names only (very fast — no cell data read)
    let sheetNames;
    try {
      const peek = XLSX.readFile(filePath, { bookSheets: true });
      sheetNames = peek.SheetNames;
    } catch (e) {
      job.status = 'error'; job.error = 'Cannot parse file: ' + e.message;
      try { fs.unlinkSync(filePath); } catch(_) {}
      return;
    }
    console.log(`[${jobId}] sheets=${JSON.stringify(sheetNames)}`);

    const BATCH = 3000; // smaller batch = less peak memory
    let ordersAdded = 0, ordersSkipped = 0, returnsAdded = 0, returnsSkipped = 0;
    const hasOrders  = sheetNames.includes('All Orders Raw') || dataType === 'orders';
    const hasReturns = sheetNames.includes('Return Raw')     || dataType === 'returns';

    if (hasOrders) {
      const sheetName = sheetNames.includes('All Orders Raw') ? 'All Orders Raw' : sheetNames[0];
      console.log(`[${jobId}] streaming orders sheet="${sheetName}"…`);
      let rowsSeen = 0;
      const total = await processSheetBatched(filePath, sheetName, BATCH, (batch) => {
        db.transaction((b) => {
          for (const row of b) {
            const r = insertOrder.run(buildOrderRecord(row));
            if (r.changes > 0) ordersAdded++; else ordersSkipped++;
          }
        })(batch);
        rowsSeen += batch.length;
        job.progress = Math.min(48, Math.round(rowsSeen / 330000 * 48));
      });
      console.log(`[${jobId}] orders done: total=${total} added=${ordersAdded} skipped=${ordersSkipped}`);
    }

    if (hasReturns) {
      const sheetName = sheetNames.includes('Return Raw') ? 'Return Raw' : (hasOrders ? null : sheetNames[0]);
      if (sheetName) {
        console.log(`[${jobId}] streaming returns sheet="${sheetName}"…`);
        let rowsSeen = 0;
        const total = await processSheetBatched(filePath, sheetName, BATCH, (batch) => {
          db.transaction((b) => {
            for (const row of b) {
              const r = insertReturn.run(buildReturnRecord(row));
              if (r.changes > 0) returnsAdded++; else returnsSkipped++;
            }
          })(batch);
          rowsSeen += batch.length;
          job.progress = 50 + Math.min(48, Math.round(rowsSeen / 47000 * 48));
        });
        console.log(`[${jobId}] returns done: total=${total} added=${returnsAdded} skipped=${returnsSkipped}`);
      }
    }

    if (!hasOrders && !hasReturns) {
      const sheetName = sheetNames[0];
      let isReturn = null;
      await processSheetBatched(filePath, sheetName, BATCH, (batch) => {
        if (isReturn === null) {
          const colsLower = Object.keys(batch[0] || {}).map(k => k.toLowerCase());
          isReturn = colsLower.some(k =>
            k === 'return-date' || k === 'return date' || k === 'return date2' ||
            k === 'detailed-disposition' || k === 'return month'
          );
          console.log(`[${jobId}] auto-detect: isReturn=${isReturn}`);
        }
        db.transaction((b) => {
          for (const row of b) {
            if (isReturn) {
              const r = insertReturn.run(buildReturnRecord(row));
              if (r.changes > 0) returnsAdded++; else returnsSkipped++;
            } else {
              const r = insertOrder.run(buildOrderRecord(row));
              if (r.changes > 0) ordersAdded++; else ordersSkipped++;
            }
          }
        })(batch);
      });
    }

    // Auto-dedup: remove duplicate orders caused by hash mismatches between
    // Excel date formats (.txt ISO timestamps vs .xlsx serial dates)
    if (hasOrders || (!hasOrders && !hasReturns)) {
      const beforeDedup = db.prepare('SELECT COUNT(*) as n FROM orders').get().n;
      db.prepare(`DELETE FROM orders WHERE purchase_date < '2000-01-01' AND purchase_date != ''`).run();
      db.prepare(`
        DELETE FROM orders WHERE id NOT IN (
          SELECT id FROM (
            SELECT id, ROW_NUMBER() OVER (
              PARTITION BY amazon_order_id, sku
              ORDER BY
                CASE WHEN item_price IS NOT NULL AND item_price != 0 THEN 0 ELSE 1 END,
                CASE WHEN purchase_date > '2000-01-01' THEN 0 ELSE 1 END,
                id ASC
            ) AS rn FROM orders
          ) ranked WHERE rn = 1
        )
      `).run();
      const afterDedup = db.prepare('SELECT COUNT(*) as n FROM orders').get().n;
      const dupsRemoved = beforeDedup - afterDedup;
      if (dupsRemoved > 0) console.log(`[${jobId}] auto-dedup removed ${dupsRemoved} duplicate order rows`);
      job.dupsRemoved = dupsRemoved;
    }

    // Auto-fix blank brands from product name
    const brandFix = runBrandFix();
    job.brandFixed = brandFix.orders + brandFix.returns;

    const fHash = fileHash(filePath);
    db.prepare('INSERT OR IGNORE INTO upload_log (filename, file_hash, type, rows_added, rows_skipped, uploaded_by) VALUES (?,?,?,?,?,?)')
      .run(filename, fHash, dataType, ordersAdded + returnsAdded, ordersSkipped + returnsSkipped, uploadedBy);

    invalidateFiltersCache(); // new data means filters may have changed
    backfillReturnGender();   // fill gender for any newly added returns with blank gender
    backfillOrderGender();    // fill gender for any newly added orders with blank gender
    backfillStyleName();      // fill style_name for any newly added orders
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
// mode=replace: clears orders/returns tables first, then imports fresh from file
app.post('/api/upload', upload.single('file'), (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'No file uploaded' });
  const uploadedBy = req.body.uploaded_by || 'Team Member';
  const dataType   = req.body.data_type   || 'auto';
  const mode       = req.body.mode        || 'append'; // 'append' or 'replace'
  const jobId = crypto.randomBytes(8).toString('hex');
  jobs[jobId] = { status: 'processing', progress: 0, mode, ordersAdded: 0, ordersSkipped: 0, returnsAdded: 0, returnsSkipped: 0 };

  // If replace mode, wipe relevant tables BEFORE importing
  if (mode === 'replace') {
    if (dataType === 'orders' || dataType === 'auto') {
      db.prepare('DELETE FROM orders').run();
      console.log(`[${jobId}] replace mode: cleared orders table`);
    }
    if (dataType === 'returns' || dataType === 'auto') {
      db.prepare('DELETE FROM returns').run();
      console.log(`[${jobId}] replace mode: cleared returns table`);
    }
    // Clear upload log too so file hash check doesn't block re-import
    db.prepare('DELETE FROM upload_log').run();
  }

  res.json({ success: true, jobId, processing: true, mode });
  setImmediate(() => processFileAsync(req.file.path, dataType, uploadedBy, req.file.originalname, jobId).catch(e => {
    console.error(`[${jobId}] fatal error:`, e.message);
    jobs[jobId].status = 'error'; jobs[jobId].error = e.message;
  }));
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
      // Auto-fill blank brand from product name before inserting
      if (!rec.brand || rec.brand === '-' || rec.brand === '0') {
        rec.brand = extractBrandFromName(rec.product_name || '') || rec.brand || '';
      }
      const r = insertReturn.run(rec);
      if (r.changes > 0) {
        added++;
      } else {
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
      // Auto-fill blank brand from product name before inserting
      if (!rec.brand || rec.brand === '-' || rec.brand === '0') {
        rec.brand = extractBrandFromName(rec.product_name || '') || rec.brand || '';
      }
      if (!rec.style_name) rec.style_name = extractStyleName(rec.product_name || '', rec.brand || '');
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
  // Always use the last COMPLETE calendar month as the reference point.
  // This prevents a partial current month (e.g. May 1st data) from
  // becoming "Last Month" and showing misleadingly low numbers.
  const now = new Date();
  const currentYM = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;

  const oMonths = db.prepare(`
    SELECT strftime('%Y-%m', purchase_date) as month FROM orders
    WHERE purchase_date != '' AND purchase_date IS NOT NULL
      AND strftime('%Y-%m', purchase_date) < ?
    GROUP BY month ORDER BY month DESC
  `).all(currentYM).map(r => r.month).filter(Boolean);

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

// GET /api/filters — results cached for 5 min (invalidated on upload)
app.get('/api/filters', (req, res) => {
  try {
    if (_filtersCache && Date.now() - _filtersCacheTs < FILTERS_TTL) {
      return res.json(_filtersCache);
    }
    const reasons = db.prepare(`SELECT DISTINCT reason FROM returns WHERE reason != '' ORDER BY reason`).all().map(r => r.reason);
    const brands = db.prepare(`SELECT DISTINCT brand FROM orders WHERE brand != '' AND brand != '-' AND brand != '0' ORDER BY brand`).all().map(r => r.brand);
    const months = db.prepare(`SELECT DISTINCT return_month FROM returns WHERE return_month != '' ORDER BY return_month DESC`).all().map(r => r.return_month);
    const dispositions = db.prepare(`SELECT DISTINCT disposition FROM returns WHERE disposition != '' ORDER BY disposition`).all().map(r => r.disposition);
    const orderMonths = db.prepare(`
      SELECT strftime('%Y-%m', purchase_date) as month
      FROM orders WHERE purchase_date IS NOT NULL AND purchase_date != ''
      GROUP BY month ORDER BY month DESC
    `).all().map(r => r.month).filter(Boolean);
    const weeks = db.prepare(`
      SELECT strftime('%Y', purchase_date) as year,
        purchase_week as week, MIN(purchase_date) as start_date
      FROM orders WHERE purchase_date != '' AND purchase_week IS NOT NULL
      GROUP BY year, week ORDER BY year DESC, week DESC LIMIT 156
    `).all();
    _filtersCache = { reasons, brands, months, dispositions, orderMonths, weeks };
    _filtersCacheTs = Date.now();
    res.json(_filtersCache);
  } catch(e) {
    console.error('filters error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// Gender is pre-computed and stored at insert/backfill time by inferGender().
// Queries just need a null/empty → 'Unknown' fallback — no expensive LIKE scans.
const GENDER_EXPR = `CASE WHEN r.gender IS NULL OR r.gender = '' THEN 'Unknown' ELSE r.gender END`;

// GET /api/return-rate?groupBy=sku|style|brand&since=YYYY-MM-DD&month=&week=&year=&brand=&style=&page=&sort=rate|returns|orders&sortDir=asc|desc
app.get('/api/return-rate', (req, res) => {
  const groupBy  = ['sku', 'style', 'brand'].includes(req.query.groupBy) ? req.query.groupBy : 'sku';
  const since    = req.query.since  || '';
  const month    = req.query.month  || '';
  const week     = req.query.week   ? parseInt(req.query.week)  : null;
  const year     = req.query.year   ? String(req.query.year)    : '';
  const brand    = req.query.brand  || '';
  const style    = req.query.style  || '';
  const sort     = ['rate', 'returns', 'orders'].includes(req.query.sort) ? req.query.sort : 'rate';
  const sortDir  = req.query.sortDir === 'asc' ? 'ASC' : 'DESC';
  const minOrders = Math.max(1, parseInt(req.query.minOrders) || 1);
  const page     = Math.max(1, parseInt(req.query.page) || 1);
  const limit    = 50;
  const offset   = (page - 1) * limit;

  const groupCol = groupBy === 'brand' ? 'brand'
                 : groupBy === 'style' ? 'product_name'
                 : 'sku';

  // Build filter clauses for the orders table — used in both subqueries (with/without table prefix)
  const buildFilters = (prefix = '') => {
    const p = prefix ? `${prefix}.` : '';
    const clauses = [`${p}order_status != 'On Trial'`, `${p}${groupCol} != ''`];
    const vals = [];
    if (since) { clauses.push(`${p}purchase_date >= ?`);                   vals.push(since); }
    if (month) { clauses.push(`strftime('%Y-%m', ${p}purchase_date) = ?`); vals.push(month); }
    if (week)  { clauses.push(`${p}purchase_week = ?`);                    vals.push(week);  }
    if (year)  { clauses.push(`strftime('%Y', ${p}purchase_date) = ?`);    vals.push(year);  }
    if (brand) { clauses.push(`${p}brand = ?`);                            vals.push(brand); }
    if (style && groupBy !== 'style') { clauses.push(`${p}product_name LIKE ?`); vals.push(`%${style}%`); }
    return { sql: clauses.join(' AND '), params: vals };
  };

  // Orders aggregated WITHOUT the returns join — avoids quantity being multiplied
  // by number of return records per order
  const oF = buildFilters();   // no prefix — selects directly from orders
  const rF = buildFilters('o'); // 'o' prefix — orders aliased as o in the returns subquery

  const orderSQL = sort === 'returns' ? `returns_qty ${sortDir}, return_rate ${sortDir}`
                 : sort === 'orders'  ? `orders_qty ${sortDir}`
                 : `return_rate ${sortDir}, returns_qty ${sortDir}`;

  // Aggregate orders and returns SEPARATELY, then join the aggregates.
  // This prevents the classic JOIN-inflation bug where an order with N return records
  // gets its quantity counted N times in SUM(o.quantity).
  const coreSql = `
    SELECT
      oa.name,
      oa.sample_name,
      oa.sample_brand,
      oa.orders_qty,
      COALESCE(ra.returns_qty, 0)                                                AS returns_qty,
      ROUND(COALESCE(ra.returns_qty, 0) * 100.0 / NULLIF(oa.orders_qty, 0), 1)  AS return_rate
    FROM (
      SELECT
        ${groupCol}                                                               AS name,
        MAX(product_name)                                                         AS sample_name,
        MAX(brand)                                                                AS sample_brand,
        SUM(quantity)                                                             AS orders_qty
      FROM orders
      WHERE ${oF.sql}
      GROUP BY ${groupCol}
    ) oa
    LEFT JOIN (
      SELECT o.${groupCol} AS name, SUM(r.quantity) AS returns_qty
      FROM returns r
      JOIN orders o ON r.order_id = o.amazon_order_id AND r.sku = o.sku
      WHERE ${rF.sql}
      GROUP BY o.${groupCol}
    ) ra ON ra.name = oa.name
    WHERE oa.orders_qty >= ${minOrders}
  `;

  // All params = order-side params + return-side params (each subquery bound separately)
  const allParams = [...oF.params, ...rF.params];

  try {
    const stats   = db.prepare(`SELECT COUNT(*) as total, MAX(return_rate) as max_rate FROM (${coreSql})`).get(...allParams);
    const records = db.prepare(`${coreSql} ORDER BY ${orderSQL} LIMIT ? OFFSET ?`).all(...allParams, limit, offset);
    const topRow  = db.prepare(`${coreSql} ORDER BY return_rate DESC, returns_qty DESC LIMIT 1`).get(...allParams);

    // Stat-card totals: apply all active filters but WITHOUT the `${groupCol} != ''`
    // constraint used in the grouped table. That constraint changes per view (sku/product_name/brand),
    // so including it makes the overall rate fluctuate when you switch between By SKU / By Style /
    // By Brand — whichever column has the most blank values lowers the denominator and inflates the
    // rate. The totals should always reflect the same universe of orders regardless of grouping.
    const buildStatsFilters = () => {
      const clauses = [`order_status != 'On Trial'`];
      const vals = [];
      if (since) { clauses.push(`purchase_date >= ?`);                   vals.push(since); }
      if (month) { clauses.push(`strftime('%Y-%m', purchase_date) = ?`); vals.push(month); }
      if (week)  { clauses.push(`purchase_week = ?`);                    vals.push(week);  }
      if (year)  { clauses.push(`strftime('%Y', purchase_date) = ?`);    vals.push(year);  }
      if (brand) { clauses.push(`brand = ?`);                            vals.push(brand); }
      if (style) { clauses.push(`product_name LIKE ?`);                  vals.push(`%${style}%`); }
      return { sql: clauses.join(' AND '), params: vals };
    };
    const sF = buildStatsFilters();

    const totalOrders = db.prepare(
      `SELECT SUM(quantity) as n FROM orders WHERE ${sF.sql}`
    ).get(...sF.params)?.n || 0;

    // Returns: count returns whose source order matches all active filters (via EXISTS join).
    let totalReturns;
    if (!since && !month && !week && !year && !brand && !style) {
      // No filters — just count everything (matches dashboard exactly)
      totalReturns = db.prepare(`SELECT COALESCE(SUM(quantity), 0) as n FROM returns`).get()?.n || 0;
    } else {
      // Filtered — count returns linked to orders that match all active filters via EXISTS.
      // Include o.sku = r.sku so this is consistent with the main coreSql JOIN condition
      // (avoids inflated counts from returns whose SKU doesn't match the ordered SKU).
      const exClauses = [`o.amazon_order_id = r.order_id`, `o.sku = r.sku`, `o.order_status != 'On Trial'`];
      const exParams  = [];
      if (since) { exClauses.push(`o.purchase_date >= ?`);                    exParams.push(since); }
      if (month) { exClauses.push(`strftime('%Y-%m', o.purchase_date) = ?`);  exParams.push(month); }
      if (week)  { exClauses.push(`o.purchase_week = ?`);                     exParams.push(week);  }
      if (year)  { exClauses.push(`strftime('%Y', o.purchase_date) = ?`);     exParams.push(year);  }
      if (brand) { exClauses.push(`o.brand = ?`);                             exParams.push(brand); }
      if (style) { exClauses.push(`o.product_name LIKE ?`);                   exParams.push(`%${style}%`); }
      totalReturns = db.prepare(
        `SELECT COALESCE(SUM(r.quantity), 0) as n FROM returns r
         WHERE EXISTS (SELECT 1 FROM orders o WHERE ${exClauses.join(' AND ')})`
      ).get(...exParams)?.n || 0;
    }

    const avgRate = totalOrders > 0 ? (totalReturns / totalOrders * 100).toFixed(1) : null;

    res.json({
      records, total: stats.total, page, pages: Math.ceil(stats.total / limit),
      groupBy, avgRate, totalOrders, totalReturns,
      maxRate: stats.max_rate != null ? parseFloat(stats.max_rate).toFixed(1) : null,
      topRow
    });
  } catch(e) {
    console.error('return-rate error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// GET /api/gender-breakdown — returns breakdown by Men / Women / Other
// Respects the same since/month/week/year/brand/style filters as /api/return-rate.
// Gender comes from the returns.gender column populated during upload.
app.get('/api/gender-breakdown', (req, res) => {
  const since = req.query.since || '';
  const month = req.query.month || '';
  const week  = req.query.week  ? parseInt(req.query.week) : null;
  const year  = req.query.year  ? String(req.query.year)   : '';
  const brand = req.query.brand || '';
  const style = req.query.style || '';

  // Orders filter with 'o.' prefix (for the JOIN)
  const oClauses = [`o.order_status != 'On Trial'`];
  const oParams  = [];
  if (since) { oClauses.push(`o.purchase_date >= ?`);                   oParams.push(since); }
  if (month) { oClauses.push(`strftime('%Y-%m', o.purchase_date) = ?`); oParams.push(month); }
  if (week)  { oClauses.push(`o.purchase_week = ?`);                    oParams.push(week);  }
  if (year)  { oClauses.push(`strftime('%Y', o.purchase_date) = ?`);    oParams.push(year);  }
  if (brand) { oClauses.push(`o.brand = ?`);                            oParams.push(brand); }
  if (style) { oClauses.push(`o.product_name LIKE ?`);                  oParams.push(`%${style}%`); }
  const oWhere = oClauses.join(' AND ');

  const hasFilters = !!(since || month || week || year || brand || style);

  // Bucket expression: collapse Girls→Other, Boys→Other, blank→Other
  const BUCKET = (col) => `CASE WHEN ${col} = 'Men' THEN 'Men' WHEN ${col} = 'Women' THEN 'Women' ELSE 'Other' END`;

  try {
    // ── Orders (sales) by gender bucket ─────────────────────────────────────
    // Plain orders filter (no alias needed — single table)
    const plainClauses = [`order_status != 'On Trial'`];
    const plainParams  = [];
    if (since) { plainClauses.push(`purchase_date >= ?`);                   plainParams.push(since); }
    if (month) { plainClauses.push(`strftime('%Y-%m', purchase_date) = ?`); plainParams.push(month); }
    if (week)  { plainClauses.push(`purchase_week = ?`);                    plainParams.push(week);  }
    if (year)  { plainClauses.push(`strftime('%Y', purchase_date) = ?`);    plainParams.push(year);  }
    if (brand) { plainClauses.push(`brand = ?`);                            plainParams.push(brand); }
    if (style) { plainClauses.push(`product_name LIKE ?`);                  plainParams.push(`%${style}%`); }
    const plainWhere = plainClauses.join(' AND ');

    const orderRows = db.prepare(`
      SELECT ${BUCKET('gender')} AS g, SUM(quantity) AS qty
      FROM orders WHERE ${plainWhere} GROUP BY g
    `).all(...plainParams);

    // ── Returns by gender bucket ─────────────────────────────────────────────
    // No-filter path: count all returns directly (avoids excluding unmatched returns)
    let returnRows;
    if (!hasFilters) {
      returnRows = db.prepare(`
        SELECT ${BUCKET(`CASE WHEN r.gender IS NULL OR r.gender = '' THEN 'Other' ELSE r.gender END`)} AS g,
               SUM(r.quantity) AS qty
        FROM returns r GROUP BY g
      `).all();
    } else {
      returnRows = db.prepare(`
        SELECT ${BUCKET(`CASE WHEN r.gender IS NULL OR r.gender = '' THEN 'Other' ELSE r.gender END`)} AS g,
               SUM(r.quantity) AS qty
        FROM returns r
        JOIN orders o ON r.order_id = o.amazon_order_id AND r.sku = o.sku
        WHERE ${oWhere} GROUP BY g
      `).all(...oParams);
    }

    // ── Merge into the three fixed buckets ──────────────────────────────────
    const ordMap = Object.fromEntries(orderRows.map(r => [r.g, r.qty]));
    const retMap = Object.fromEntries(returnRows.map(r => [r.g, r.qty]));
    const buckets = ['Men', 'Women', 'Other'].map(g => {
      const orders  = ordMap[g]  || 0;
      const returns = retMap[g]  || 0;
      const rate    = orders > 0 ? +(returns / orders * 100).toFixed(1) : null;
      return { gender: g, orders_qty: orders, returns_qty: returns, return_rate: rate };
    });

    const totalOrders  = buckets.reduce((s, b) => s + b.orders_qty,  0);
    const totalReturns = buckets.reduce((s, b) => s + b.returns_qty, 0);
    const totalRate    = totalOrders > 0 ? +(totalReturns / totalOrders * 100).toFixed(1) : null;

    res.json({ buckets, total: { orders_qty: totalOrders, returns_qty: totalReturns, return_rate: totalRate } });
  } catch(e) {
    console.error('gender-breakdown error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// GET /api/brand-detail — drill-down analysis for a single brand / sku / style
// Returns return reasons, disposition breakdown, and top SKUs for the selected dimension.
// Respects the same since/month/week/year time filters as /api/return-rate.
app.get('/api/brand-detail', (req, res) => {
  const brand      = req.query.brand      || '';
  const sku        = req.query.sku        || '';
  const style      = req.query.style      || '';
  const style_name = req.query.style_name || '';
  const since = req.query.since || '';
  const month = req.query.month || '';
  const week  = req.query.week  ? parseInt(req.query.week) : null;
  const year  = req.query.year  ? String(req.query.year)   : '';

  // Build WHERE clauses with optional table alias prefix
  const buildWhere = (prefix = '') => {
    const p = prefix ? `${prefix}.` : '';
    const clauses = [`${p}order_status != 'On Trial'`];
    const vals = [];
    if (brand)      { clauses.push(`${p}brand = ?`);        vals.push(brand); }
    if (sku)        { clauses.push(`${p}sku = ?`);          vals.push(sku);   }
    if (style_name) { clauses.push(`${p}style_name = ?`);   vals.push(style_name); }
    else if (style) { clauses.push(`${p}product_name = ?`); vals.push(style); }
    if (since) { clauses.push(`${p}purchase_date >= ?`);                     vals.push(since); }
    if (month) { clauses.push(`strftime('%Y-%m', ${p}purchase_date) = ?`);   vals.push(month); }
    if (week)  { clauses.push(`${p}purchase_week = ?`);                      vals.push(week);  }
    if (year)  { clauses.push(`strftime('%Y', ${p}purchase_date) = ?`);      vals.push(year);  }
    return { sql: clauses.join(' AND '), params: vals };
  };

  const dF = buildWhere();    // no prefix — direct FROM orders WHERE ...
  const oF = buildWhere('o'); // 'o' prefix — inside JOINs where orders is aliased as o

  try {
    // Total orders for this dimension (no join needed)
    const ordersQty = db.prepare(`SELECT COALESCE(SUM(quantity), 0) as n FROM orders WHERE ${dF.sql}`)
      .get(...dF.params)?.n || 0;

    // Total returns linked to those orders — join on BOTH order_id AND sku to match
    // the same logic used in the main /api/return-rate coreSql, preventing inflated counts
    // from returns whose SKU doesn't match any order line item for that order_id.
    const returnsQty = db.prepare(`
      SELECT COALESCE(SUM(r.quantity), 0) as n
      FROM returns r
      JOIN orders o ON r.order_id = o.amazon_order_id AND r.sku = o.sku
      WHERE ${oF.sql}
    `).get(...oF.params)?.n || 0;

    // Return reasons — same join condition
    const reasons = db.prepare(`
      SELECT r.reason, SUM(r.quantity) as qty
      FROM returns r
      JOIN orders o ON r.order_id = o.amazon_order_id AND r.sku = o.sku
      WHERE ${oF.sql} AND r.reason != ''
      GROUP BY r.reason ORDER BY qty DESC LIMIT 20
    `).all(...oF.params);

    // Disposition breakdown — same join condition
    const dispositions = db.prepare(`
      SELECT r.disposition, SUM(r.quantity) as qty
      FROM returns r
      JOIN orders o ON r.order_id = o.amazon_order_id AND r.sku = o.sku
      WHERE ${oF.sql} AND r.disposition != ''
      GROUP BY r.disposition ORDER BY qty DESC
    `).all(...oF.params);

    // Top returned Styles — separate subqueries to avoid JOIN-inflation bug,
    // same r.sku = o.sku condition for consistency with the main table
    const topStyles = db.prepare(`
      SELECT
        oa.product_name, oa.orders_qty,
        COALESCE(ra.returns_qty, 0)                                                  AS returns_qty,
        ROUND(COALESCE(ra.returns_qty, 0) * 100.0 / NULLIF(oa.orders_qty, 0), 1)    AS return_rate
      FROM (
        SELECT product_name, SUM(quantity) AS orders_qty
        FROM orders WHERE ${dF.sql} AND product_name != ''
        GROUP BY product_name HAVING orders_qty >= 3
      ) oa
      LEFT JOIN (
        SELECT o.product_name, SUM(r.quantity) AS returns_qty
        FROM returns r
        JOIN orders o ON r.order_id = o.amazon_order_id AND r.sku = o.sku
        WHERE ${oF.sql}
        GROUP BY o.product_name
      ) ra ON ra.product_name = oa.product_name
      ORDER BY return_rate DESC, returns_qty DESC
      LIMIT 15
    `).all(...dF.params, ...oF.params);

    // Gender breakdown for this brand/sku/style — Sales / Returns / Rate per bucket
    const BUCKET_EXPR = `CASE WHEN gender = 'Men' THEN 'Men' WHEN gender = 'Women' THEN 'Women' ELSE 'Other' END`;

    // Orders by gender: prefer the matched return's gender over orders.gender.
    // This fixes brands like Blundstone whose product names have no gender keywords
    // but whose returns CSV has explicit "Men"/"Women" values.
    // LEFT JOIN to a deduped return-gender subquery; fall back to o.gender if no return match.
    const resolvedGender = `
      CASE WHEN rg.gender IN ('Men','Women') THEN rg.gender
           WHEN o.gender  IN ('Men','Women') THEN o.gender
           ELSE '' END`;
    const gOrdRows = db.prepare(`
      SELECT CASE WHEN ${resolvedGender} = 'Men'   THEN 'Men'
                  WHEN ${resolvedGender} = 'Women' THEN 'Women'
                  ELSE 'Other' END AS g,
             SUM(o.quantity) AS qty
      FROM orders o
      LEFT JOIN (
        SELECT r.order_id, r.sku,
          CASE WHEN SUM(CASE WHEN r.gender = 'Men'   THEN 1 ELSE 0 END) > 0 THEN 'Men'
               WHEN SUM(CASE WHEN r.gender = 'Women' THEN 1 ELSE 0 END) > 0 THEN 'Women'
               ELSE '' END AS gender
        FROM returns r GROUP BY r.order_id, r.sku
      ) rg ON rg.order_id = o.amazon_order_id AND rg.sku = o.sku
      WHERE ${dF.sql} GROUP BY g
    `).all(...dF.params);

    // Returns by gender bucket (uses r.gender — unambiguous)
    const gRetRows = db.prepare(`
      SELECT ${BUCKET_EXPR.replace(/gender/g, 'r.gender')} AS g, SUM(r.quantity) AS qty
      FROM returns r
      JOIN orders o ON r.order_id = o.amazon_order_id AND r.sku = o.sku
      WHERE ${oF.sql} GROUP BY g
    `).all(...oF.params);

    const gOrdMap = Object.fromEntries(gOrdRows.map(r => [r.g, r.qty]));
    const gRetMap = Object.fromEntries(gRetRows.map(r => [r.g, r.qty]));
    const genders = ['Men', 'Women', 'Other'].map(g => {
      const orders_qty  = gOrdMap[g] || 0;
      const returns_qty = gRetMap[g] || 0;
      const return_rate = orders_qty > 0 ? +(returns_qty / orders_qty * 100).toFixed(1) : null;
      return { gender: g, orders_qty, returns_qty, return_rate };
    }).filter(g => g.orders_qty > 0 || g.returns_qty > 0);
    const gTotal = {
      orders_qty:  genders.reduce((s, g) => s + g.orders_qty,  0),
      returns_qty: genders.reduce((s, g) => s + g.returns_qty, 0),
    };
    gTotal.return_rate = gTotal.orders_qty > 0 ? +(gTotal.returns_qty / gTotal.orders_qty * 100).toFixed(1) : null;

    // Attach percentage to each reason / disposition
    const totalR = reasons.reduce((s, r) => s + r.qty, 0);
    const totalD = dispositions.reduce((s, r) => s + r.qty, 0);
    reasons.forEach(r => { r.pct = totalR > 0 ? +(r.qty / totalR * 100).toFixed(1) : 0; });
    dispositions.forEach(r => { r.pct = totalD > 0 ? +(r.qty / totalD * 100).toFixed(1) : 0; });

    const returnRate = ordersQty > 0 ? (returnsQty / ordersQty * 100).toFixed(1) : null;

    res.json({
      summary: { orders_qty: ordersQty, returns_qty: returnsQty, return_rate: returnRate },
      reasons,
      dispositions,
      genders,
      genderTotal: gTotal,
      topStyles
    });
  } catch(e) {
    console.error('brand-detail error:', e.message);
    res.status(500).json({ error: e.message });
  }
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

// DELETE /api/orders/cleanup-bad — removes orders rows that were accidentally
// created from a returns CSV being mis-detected as orders (no order_id, no purchase_date, no order_status)
app.delete('/api/orders/cleanup-bad', (req, res) => {
  const before = db.prepare('SELECT COUNT(*) as n FROM orders').get().n;
  db.prepare(`DELETE FROM orders WHERE (amazon_order_id IS NULL OR amazon_order_id = '')
              AND (purchase_date IS NULL OR purchase_date = '')
              AND (order_status IS NULL OR order_status = '')`).run();
  const after = db.prepare('SELECT COUNT(*) as n FROM orders').get().n;
  console.log(`cleanup-bad: removed ${before - after} bad order rows`);
  res.json({ deleted: before - after, remaining: after });
});

// DELETE /api/orders/dedup — removes duplicate order rows keeping the best-quality record per (amazon_order_id, sku)
// "Best quality" = has a valid purchase_date (not corrupted), then longest identifier (Excel-style preferred)
app.delete('/api/orders/dedup', (req, res) => {
  try {
    const before = db.prepare('SELECT COUNT(*) as n FROM orders').get().n;

    // 1. Remove rows with clearly bad/corrupted purchase_date (e.g. "-4589-12-...")
    db.prepare(`DELETE FROM orders WHERE purchase_date < '2000-01-01' AND purchase_date != ''`).run();

    // 2. For each (amazon_order_id, sku) pair that has duplicates, keep only the row with the
    //    lowest rowid (first inserted = most stable, typically from the structured Excel upload)
    db.prepare(`
      DELETE FROM orders
      WHERE rowid NOT IN (
        SELECT MIN(rowid)
        FROM orders
        GROUP BY amazon_order_id, sku
      )
    `).run();

    const after = db.prepare('SELECT COUNT(*) as n FROM orders').get().n;
    console.log(`dedup: removed ${before - after} duplicate/corrupt order rows, ${after} remain`);
    res.json({ removed: before - after, remaining: after });
  } catch(e) {
    console.error('dedup error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// GET /api/reset-all — wipes ALL data (orders + returns + upload log) for a full clean re-import
app.get('/api/reset-all', (req, res) => {
  const orders  = db.prepare('SELECT COUNT(*) as n FROM orders').get().n;
  const returns = db.prepare('SELECT COUNT(*) as n FROM returns').get().n;
  db.prepare('DELETE FROM orders').run();
  db.prepare('DELETE FROM returns').run();
  db.prepare('DELETE FROM upload_log').run();
  res.send(`<h2>Full Reset Done</h2><p>Deleted: <b>${orders.toLocaleString()}</b> orders and <b>${returns.toLocaleString()}</b> returns</p><p>Database is now empty. Re-upload your Excel file to restore data.</p><br><a href="/">← Back to Dashboard</a>`);
});

// GET /api/returns/reset — wipes all returns so you can re-import from a single clean source
app.get('/api/returns/reset', (req, res) => {
  const before = db.prepare('SELECT COUNT(*) as n FROM returns').get().n;
  db.prepare('DELETE FROM returns').run();
  res.send(`<h2>Returns Reset Done</h2><p>Deleted: <b>${before}</b> return records</p><p>Returns table is now empty.</p><br><p>Now re-upload your Excel file to restore clean data.</p><br><a href="/">← Back to Dashboard</a>`);
});

// GET /api/returns/diagnostic — shows breakdown to identify duplicate sources
app.get('/api/returns/diagnostic', (req, res) => {
  const total        = db.prepare(`SELECT COUNT(*) as n FROM returns`).get().n;
  const withOrderId  = db.prepare(`SELECT COUNT(*) as n FROM returns WHERE order_id != '' AND order_id IS NOT NULL`).get().n;
  const noOrderId    = db.prepare(`SELECT COUNT(*) as n FROM returns WHERE order_id = '' OR order_id IS NULL`).get().n;
  const dateRange    = db.prepare(`SELECT MIN(return_date) as earliest, MAX(return_date) as latest FROM returns WHERE return_date != ''`).get();
  const byYear       = db.prepare(`SELECT strftime('%Y', return_date) as yr, COUNT(*) as cnt FROM returns WHERE return_date != '' GROUP BY yr ORDER BY yr`).all();
  const sampleNoId   = db.prepare(`SELECT order_id, asin, return_date, reason, disposition FROM returns WHERE order_id = '' OR order_id IS NULL LIMIT 3`).all();
  const sampleWithId = db.prepare(`SELECT order_id, asin, return_date, reason, disposition FROM returns WHERE order_id != '' LIMIT 3`).all();
  res.json({ total, withOrderId, noOrderId, dateRange, byYear, sampleNoId, sampleWithId });
});

// GET /api/orders/dedup — same as DELETE but accessible via browser URL
app.get('/api/orders/dedup', (req, res) => {
  const before = db.prepare('SELECT COUNT(*) as n FROM orders').get().n;
  db.prepare(`
    DELETE FROM orders
    WHERE id NOT IN (
      SELECT id FROM (
        SELECT id,
          ROW_NUMBER() OVER (
            PARTITION BY amazon_order_id, sku
            ORDER BY
              CASE WHEN item_price IS NOT NULL AND item_price != 0 THEN 0 ELSE 1 END,
              CASE WHEN purchase_date IS NOT NULL AND purchase_date != '' THEN 0 ELSE 1 END,
              id ASC
          ) as rn
        FROM orders
        WHERE amazon_order_id != '' AND sku != ''
      ) ranked
      WHERE rn = 1
    )
    AND amazon_order_id != ''
    AND sku != ''
  `).run();
  const after = db.prepare('SELECT COUNT(*) as n FROM orders').get().n;
  console.log(`dedup orders: removed ${before - after} duplicate rows, ${after} remain`);
  res.send(`<h2>Orders Dedup Done</h2><p>Removed: <b>${before - after}</b> duplicates</p><p>Remaining: <b>${after}</b> orders</p><br><a href="/api/returns/dedup">Run Returns Dedup next →</a>`);
});

// GET /api/returns/dedup — same as DELETE but accessible via browser URL
app.get('/api/returns/dedup', (req, res) => {
  const before = db.prepare('SELECT COUNT(*) as n FROM returns').get().n;
  db.prepare(`
    DELETE FROM returns
    WHERE id NOT IN (
      SELECT id FROM (
        SELECT id,
          ROW_NUMBER() OVER (
            PARTITION BY order_id, asin, return_date, reason
            ORDER BY
              CASE WHEN disposition != '' THEN 0 ELSE 1 END,
              id ASC
          ) as rn
        FROM returns
        WHERE order_id != '' AND return_date != ''
      ) ranked
      WHERE rn = 1
    )
    AND order_id != ''
    AND return_date != ''
  `).run();
  const after = db.prepare('SELECT COUNT(*) as n FROM returns').get().n;
  console.log(`dedup returns: removed ${before - after} duplicate rows, ${after} remain`);
  res.send(`<h2>Returns Dedup Done</h2><p>Removed: <b>${before - after}</b> duplicates</p><p>Remaining: <b>${after}</b> returns</p><br><a href="/">← Back to Dashboard</a>`);
});

// DELETE /api/orders/dedup — removes duplicate order rows keeping best data per amazon_order_id + sku
app.delete('/api/orders/dedup', (req, res) => {
  const before = db.prepare('SELECT COUNT(*) as n FROM orders').get().n;
  db.prepare(`
    DELETE FROM orders
    WHERE id NOT IN (
      SELECT id FROM (
        SELECT id,
          ROW_NUMBER() OVER (
            PARTITION BY amazon_order_id, sku
            ORDER BY
              CASE WHEN item_price IS NOT NULL AND item_price != 0 THEN 0 ELSE 1 END,
              CASE WHEN purchase_date IS NOT NULL AND purchase_date != '' THEN 0 ELSE 1 END,
              id ASC
          ) as rn
        FROM orders
        WHERE amazon_order_id != '' AND sku != ''
      ) ranked
      WHERE rn = 1
    )
    AND amazon_order_id != ''
    AND sku != ''
  `).run();
  const after = db.prepare('SELECT COUNT(*) as n FROM orders').get().n;
  console.log(`dedup orders: removed ${before - after} duplicate rows, ${after} remain`);
  res.json({ deleted: before - after, remaining: after });
});

// DELETE /api/returns/dedup — removes duplicate return rows caused by the same
// return being uploaded multiple times with different column casing (Excel vs Amazon CSV).
// Keeps the row with the best data (non-empty disposition) per unique order+asin+date+reason.
app.delete('/api/returns/dedup', (req, res) => {
  const before = db.prepare('SELECT COUNT(*) as n FROM returns').get().n;
  // For each group of (order_id, asin, return_date, reason), keep only the row
  // that has the most complete data (prefer non-empty disposition), then by lowest id.
  db.prepare(`
    DELETE FROM returns
    WHERE id NOT IN (
      SELECT id FROM (
        SELECT id,
          ROW_NUMBER() OVER (
            PARTITION BY order_id, asin, return_date, reason
            ORDER BY
              CASE WHEN disposition != '' THEN 0 ELSE 1 END,
              id ASC
          ) as rn
        FROM returns
        WHERE order_id != '' AND return_date != ''
      ) ranked
      WHERE rn = 1
    )
    AND order_id != ''
    AND return_date != ''
  `).run();
  const after = db.prepare('SELECT COUNT(*) as n FROM returns').get().n;
  console.log(`dedup returns: removed ${before - after} duplicate rows, ${after} remain`);
  res.json({ deleted: before - after, remaining: after });
});

// GET /api/normalize-brands — one-time backfill: canonicalizes all brand spellings in the DB
// (e.g. "SOREL" → "Sorel", "HOKA" → "HOKA", "merrell" → "Merrell")
// Safe to run multiple times — only updates rows where the value actually changes.
app.get('/api/normalize-brands', (req, res) => {
  try {
    const result = runBrandFix();
    console.log(`normalize-brands: orders_fixed=${result.orders} returns_fixed=${result.returns}`);
    res.json({ success: true, orders_fixed: result.orders, returns_fixed: result.returns });
  } catch(e) {
    console.error('normalize-brands error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// GET /api/fix-brands — one-time migration: fills in brand for orders where brand is blank/dash/0
// by extracting it from the product_name using known brand list + dash-prefix pattern
app.get('/api/fix-brands', (req, res) => {
  // Fetch all orders with bad brand
  const badRows = db.prepare(`
    SELECT id, product_name FROM orders
    WHERE (brand IS NULL OR brand = '' OR brand = '-' OR brand = '0')
      AND product_name IS NOT NULL AND product_name != '' AND product_name != '-'
  `).all();

  const updateStmt = db.prepare(`UPDATE orders SET brand = ? WHERE id = ?`);
  let fixed = 0, skipped = 0;

  db.transaction(() => {
    for (const row of badRows) {
      const brand = extractBrandFromName(row.product_name);
      if (brand) {
        updateStmt.run(brand, row.id);
        fixed++;
      } else {
        skipped++;
      }
    }
  })();

  // Also fix returns table
  const badReturns = db.prepare(`
    SELECT id, product_name FROM returns
    WHERE (brand IS NULL OR brand = '' OR brand = '-' OR brand = '0')
      AND product_name IS NOT NULL AND product_name != '' AND product_name != '-'
  `).all();

  const updateReturnStmt = db.prepare(`UPDATE returns SET brand = ? WHERE id = ?`);
  let fixedReturns = 0;

  db.transaction(() => {
    for (const row of badReturns) {
      const brand = extractBrandFromName(row.product_name);
      if (brand) { updateReturnStmt.run(brand, row.id); fixedReturns++; }
    }
  })();

  console.log(`fix-brands: orders fixed=${fixed} skipped=${skipped}, returns fixed=${fixedReturns}`);
  res.send(`
    <h2>Brand Fix Done ✅</h2>
    <p>Orders updated: <b>${fixed}</b> (${skipped} could not be extracted)</p>
    <p>Returns updated: <b>${fixedReturns}</b></p>
    <br><a href="/">← Back to Dashboard</a>
  `);
});

// DELETE /api/returns/cleanup-empty-disposition — removes old returns rows that have no disposition
// (leftover from earlier uploads before disposition was captured correctly)
app.delete('/api/returns/cleanup-empty-disposition', (req, res) => {
  const before = db.prepare('SELECT COUNT(*) as n FROM returns').get().n;
  db.prepare("DELETE FROM returns WHERE disposition IS NULL OR disposition = ''").run();
  const after = db.prepare('SELECT COUNT(*) as n FROM returns').get().n;
  res.json({ deleted: before - after, remaining: after });
});

// GET /api/version — returns current git commit hash (used to verify Railway deployment)
app.get('/api/version', (req, res) => {
  const { execSync } = require('child_process');
  let commit = 'unknown';
  try { commit = execSync('git rev-parse --short HEAD', { cwd: __dirname }).toString().trim(); } catch(e) {}
  res.json({ commit, built: new Date().toISOString() });
});

// GET /api/debug/gender — diagnose gender backfill status
app.get('/api/debug/gender', (req, res) => {
  const brand = req.query.brand || 'Sorel';
  const total     = db.prepare(`SELECT COUNT(*) as n FROM returns WHERE brand = ?`).get(brand).n;
  const unknown   = db.prepare(`SELECT COUNT(*) as n FROM returns WHERE brand = ? AND (gender IS NULL OR gender = '' OR gender = 'Unknown')`).get(brand).n;
  const hasOrder  = db.prepare(`SELECT COUNT(*) as n FROM returns r WHERE r.brand = ? AND (r.gender IS NULL OR r.gender = '' OR r.gender = 'Unknown') AND EXISTS (SELECT 1 FROM orders o WHERE o.amazon_order_id = r.order_id AND o.sku = r.sku)`).get(brand).n;
  const hasGender = db.prepare(`SELECT COUNT(*) as n FROM returns r WHERE r.brand = ? AND (r.gender IS NULL OR r.gender = '' OR r.gender = 'Unknown') AND EXISTS (SELECT 1 FROM orders o WHERE o.amazon_order_id = r.order_id AND o.sku = r.sku AND (o.product_name LIKE '%Women''s%' OR o.product_name LIKE '% (W)%' OR o.product_name LIKE '%Men''s%' OR o.product_name LIKE '% (M)%' OR o.product_name LIKE '%Girls%' OR o.product_name LIKE '%Boys%'))`).get(brand).n;
  const sampleUnknown = db.prepare(`SELECT r.order_id, r.sku, r.product_name AS ret_name, o.product_name AS ord_name, r.gender FROM returns r LEFT JOIN orders o ON o.amazon_order_id = r.order_id AND o.sku = r.sku WHERE r.brand = ? AND (r.gender IS NULL OR r.gender = '' OR r.gender = 'Unknown') LIMIT 5`).all(brand);
  res.json({ brand, total, unknown, hasOrder, hasGender, sampleUnknown });
});

// GET /api/brand-styles — returns styles for a brand grouped by style_name (core model name)
// Used by the Brand → Style drill-down section on the return-rate page.
app.get('/api/brand-styles', (req, res) => {
  const brand = req.query.brand || '';
  const since = req.query.since || '';
  const month = req.query.month || '';
  const week  = req.query.week  ? parseInt(req.query.week) : null;
  const year  = req.query.year  ? String(req.query.year)  : '';
  if (!brand) return res.json({ records: [], total: 0 });

  const clauses = [`order_status != 'On Trial'`, `brand = ?`, `style_name != ''`];
  const params  = [brand];
  if (since) { clauses.push(`purchase_date >= ?`);                   params.push(since); }
  if (month) { clauses.push(`strftime('%Y-%m', purchase_date) = ?`); params.push(month); }
  if (week)  { clauses.push(`purchase_week = ?`);                    params.push(week);  }
  if (year)  { clauses.push(`strftime('%Y', purchase_date) = ?`);    params.push(year);  }
  const oWhere = clauses.join(' AND ');

  const rClauses = clauses.map(c => c.replace(/\b(purchase_date|purchase_week|order_status|brand|style_name)\b/g, 'o.$1'));
  const rWhere   = rClauses.join(' AND ');

  const records = db.prepare(`
    SELECT
      oa.style_name        AS name,
      oa.orders_qty,
      COALESCE(ra.returns_qty, 0)                                                AS returns_qty,
      ROUND(COALESCE(ra.returns_qty, 0) * 100.0 / NULLIF(oa.orders_qty, 0), 1)  AS return_rate
    FROM (
      SELECT style_name, SUM(quantity) AS orders_qty
      FROM orders WHERE ${oWhere}
      GROUP BY style_name
    ) oa
    LEFT JOIN (
      SELECT o.style_name, SUM(r.quantity) AS returns_qty
      FROM returns r
      JOIN orders o ON r.order_id = o.amazon_order_id AND r.sku = o.sku
      WHERE ${rWhere}
      GROUP BY o.style_name
    ) ra ON ra.style_name = oa.style_name
    ORDER BY return_rate DESC, returns_qty DESC
  `).all(...params, ...params);

  res.json({ records, total: records.length });
});

const server = app.listen(PORT, () => {
  console.log(`\n✅ Orders & Returns App running at http://localhost:${PORT}\n`);
  // Run backfills after the server is up so Railway's health check succeeds first
  setImmediate(() => {
    backfillReturnGender();
    backfillOrderGender();
    backfillStyleName();
  });
});
server.setTimeout(600000); // 10 minutes for large uploads
