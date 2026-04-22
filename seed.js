const XLSX = require('./node_modules/xlsx');
const crypto = require('crypto');
const db = require('./db');

const FILE = 'C:/Users/Reynaldo Macinas Jr/Downloads/Monthly Orders and Returns.xlsx';

function parseNum(v) {
  if (v === null || v === undefined || v === '') return null;
  const n = parseFloat(String(v).replace(/[^0-9.\-]/g, ''));
  return isNaN(n) ? null : n;
}
function parseDate(v) {
  if (!v) return null;
  if (typeof v === 'number') { const d = XLSX.SSF.parse_date_code(v); return `${d.y}-${String(d.m).padStart(2,'0')}-${String(d.d).padStart(2,'0')}`; }
  const m = String(v).trim().match(/^(\d{4}-\d{2}-\d{2})/); return m ? m[1] : String(v).trim();
}
function getWeek(ds) {
  if (!ds) return null;
  const d = new Date(ds); if (isNaN(d)) return null;
  const j = new Date(d.getFullYear(),0,1);
  return Math.ceil(((d-j)/86400000+j.getDay()+1)/7);
}
function hash(...p) { return crypto.createHash('md5').update(p.join('|')).digest('hex'); }

console.log('Reading Excel file…');
const wb = XLSX.readFile(FILE, { cellDates: false });

// --- RETURNS (fast, 42k rows) ---
console.log('Importing returns…');
const retRaw = XLSX.utils.sheet_to_json(wb.Sheets['Return Raw'], { defval: '' });
const retRows = retRaw.map(r => Object.fromEntries(Object.entries(r).map(([k,v]) => [k.trim(),v])));
console.log('Return rows:', retRows.length);

const insertReturn = db.prepare(`INSERT OR IGNORE INTO returns
  (purchase_month,purchase_date,return_month,return_year,return_date,return_week,order_id,sku,asin,fnsku,product_name,quantity,disposition,reason,status,gender,brand,customer_comments,row_hash)
  VALUES(@purchase_month,@purchase_date,@return_date_parsed,@return_year,@return_date,@return_week,@order_id,@sku,@asin,@fnsku,@product_name,@quantity,@disposition,@reason,@status,@gender,@brand,@customer_comments,@row_hash)`);

let rAdded=0, rSkipped=0;
db.transaction((rows) => {
  for (const row of rows) {
    const rd = parseDate(row['Return Date2']||row['Return Date']);
    const pd = parseDate(row['Purchase Date']||'');
    const rec = {
      purchase_month: String(row['Purchase Month']||'').trim(),
      purchase_date: pd,
      return_date_parsed: String(row['Return Month']||'').trim(),
      return_year: String(row['Return Year']||'').trim(),
      return_date: rd,
      return_week: getWeek(rd),
      order_id: String(row['Order ID']||'').trim(),
      sku: String(row['SKU']||'').trim(),
      asin: String(row['ASIN']||'').trim(),
      fnsku: String(row['FNSKU']||'').trim(),
      product_name: String(row['Product Name']||'').trim(),
      quantity: parseNum(row['Quantity'])||1,
      disposition: String(row['Detailed-disposition']||'').trim(),
      reason: String(row['Reason']||'').trim(),
      status: String(row['Status']||'').trim(),
      gender: String(row['Gender']||'').trim(),
      brand: String(row['Brand']||'').trim(),
      customer_comments: String(row['Customer-comments']||'').trim(),
      row_hash: hash(row['Order ID']||'', row['ASIN']||'', rd||'', row['Reason']||''),
    };
    const r = insertReturn.run(rec);
    if (r.changes > 0) rAdded++; else rSkipped++;
  }
})(retRows);
console.log(`Returns: ${rAdded} added, ${rSkipped} skipped`);

// --- ORDERS (large, ~1M rows — process in batches) ---
console.log('\nImporting orders (this will take a few minutes)…');
const ws = wb.Sheets['All Orders Raw'];
const ordRaw = XLSX.utils.sheet_to_json(ws, { defval: '' });
const ordRows = ordRaw.map(r => Object.fromEntries(Object.entries(r).map(([k,v]) => [k.trim(),v])));
console.log('Order rows:', ordRows.length);

const insertOrder = db.prepare(`INSERT OR IGNORE INTO orders
  (identifier,purchase_month,purchase_date,purchase_week,brand,amazon_order_id,order_status,product_name,sku,asin,item_status,quantity,item_price,item_tax,row_hash)
  VALUES(@identifier,@purchase_month,@purchase_date,@purchase_week,@brand,@amazon_order_id,@order_status,@product_name,@sku,@asin,@item_status,@quantity,@item_price,@item_tax,@row_hash)`);

let oAdded=0, oSkipped=0;
const BATCH=10000;
for (let i=0; i<ordRows.length; i+=BATCH) {
  const batch = ordRows.slice(i, i+BATCH);
  db.transaction((rows) => {
    for (const row of rows) {
      const pd = parseDate(row['Purchase Date2']||row['purchase-date']);
      const rec = {
        identifier: String(row['Identifier']||row['Duplicate Id']||'').trim(),
        purchase_month: String(row['Purchase Month']||'').trim(),
        purchase_date: pd,
        purchase_week: getWeek(pd),
        brand: String(row['Brand']||'').trim(),
        amazon_order_id: String(row['amazon-order-id']||'').trim(),
        order_status: String(row['order-status']||'').trim(),
        product_name: String(row['product-name']||'').trim(),
        sku: String(row['sku']||'').trim(),
        asin: String(row['asin']||'').trim(),
        item_status: String(row['item-status']||'').trim(),
        quantity: parseNum(row['quantity'])||0,
        item_price: parseNum(row['item-price']),
        item_tax: parseNum(row['item-tax']),
        row_hash: hash(row['Identifier']||row['Duplicate Id']||'', row['asin']||'', row['Purchase Date2']||row['purchase-date']||'', String(row['item-price']||'')),
      };
      const r = insertOrder.run(rec);
      if (r.changes > 0) oAdded++; else oSkipped++;
    }
  })(batch);
  if ((i/BATCH+1) % 10 === 0) process.stdout.write(`  ${i+BATCH} / ${ordRows.length} rows processed…\n`);
}
console.log(`Orders: ${oAdded} added, ${oSkipped} skipped`);

const fHash = crypto.createHash('md5').update(require('fs').readFileSync(FILE)).digest('hex');
db.prepare('INSERT OR IGNORE INTO upload_log (filename,file_hash,type,rows_added,rows_skipped,uploaded_by) VALUES (?,?,?,?,?,?)')
  .run('Monthly Orders and Returns.xlsx', fHash, 'both', oAdded+rAdded, oSkipped+rSkipped, 'Initial Import');

console.log('\n✅ Done! Run: node server.js');
