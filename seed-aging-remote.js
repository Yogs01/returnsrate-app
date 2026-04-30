/**
 * seed-aging-remote.js
 * Reads the Inventory Aging Report Excel file and pushes to Railway.
 * Run: node seed-aging-remote.js
 */

const XLSX   = require('xlsx');
const crypto = require('crypto');
const https  = require('https');

const FILE  = 'C:/Users/Reynaldo Macinas Jr/OneDrive/Desktop/Inventory Aging Report.xlsx';
const HOST  = 'returnsrate-app-production.up.railway.app';
const BATCH = 500;

function parseNum(v) {
  if (v === null || v === undefined || v === '') return null;
  const n = parseFloat(String(v).replace(/[^0-9.\-]/g, ''));
  return isNaN(n) ? null : n;
}

function pn(v) { const n = parseNum(v); return n === null ? 0 : n; }

function parseDate(v) {
  if (!v) return null;
  if (typeof v === 'number') {
    const d = XLSX.SSF.parse_date_code(v);
    return `${d.y}-${String(d.m).padStart(2,'0')}-${String(d.d).padStart(2,'0')}`;
  }
  const m = String(v).trim().match(/^(\d{4}-\d{2}-\d{2})/);
  return m ? m[1] : String(v).trim();
}

function hash(...p) { return crypto.createHash('md5').update(p.join('|')).digest('hex'); }

function postJSON(path, body) {
  return new Promise((resolve, reject) => {
    const data = JSON.stringify(body);
    const req = https.request({
      hostname: HOST, path, method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(data) },
      timeout: 60000
    }, res => {
      let buf = '';
      res.on('data', c => buf += c);
      res.on('end', () => {
        try { resolve(JSON.parse(buf)); } catch(e) { resolve({ raw: buf }); }
      });
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('Request timed out')); });
    req.write(data);
    req.end();
  });
}

// POST to /api/import-aging (batch endpoint we'll add below)
async function postBatch(rows) {
  return postJSON('/api/import-aging', { rows });
}

function buildRecord(row) {
  const snap = parseDate(row['snapshot-date'] || row['Snapshot Date'] || '');
  const sku  = String(row['sku'] || row['SKU'] || '').trim();
  const asin = String(row['asin'] || row['ASIN'] || '').trim();
  return {
    snapshot_date:           snap || '',
    sku,
    fnsku:                   String(row['fnsku'] || row['FNSKU'] || '').trim(),
    asin,
    product_name:            String(row['product-name'] || row['Product Name'] || '').trim(),
    condition:               String(row['condition'] || row['Condition'] || '').trim(),
    available:               pn(row['afn-fulfillable-quantity'] || row['available'] || row['Available'] || 0),
    age_0_90:                pn(row['inv-age-0-to-90-days']    || row['0-90 Days']   || 0),
    age_91_180:              pn(row['inv-age-91-to-180-days']   || row['91-180 Days']  || 0),
    age_181_270:             pn(row['inv-age-181-to-270-days']  || row['181-270 Days'] || 0),
    age_271_365:             pn(row['inv-age-271-to-365-days']  || row['271-365 Days'] || 0),
    age_365_455:             pn(row['inv-age-365-to-455-days']  || row['inv-age-366-to-455-days'] || row['365-455 Days'] || row['366-455 Days'] || 0),
    age_455_plus:            pn(row['inv-age-455-plus-days']    || row['inv-age-456-plus-days']   || row['455+ Days']   || row['456+ Days']   || 0),
    sold_t7:                 pn(row['afn-sold-units-past-7-days']  || row['T7']  || 0),
    sold_t30:                pn(row['afn-sold-units-past-30-days'] || row['T30'] || 0),
    sold_t60:                pn(row['afn-sold-units-past-60-days'] || row['T60'] || 0),
    sold_t90:                pn(row['afn-sold-units-past-90-days'] || row['T90'] || 0),
    sell_through:            parseNum(row['sell-through'] || row['Sell Through'] || null),
    recommended_action:      String(row['recommended-action'] || row['Recommended Action'] || '').trim(),
    recommended_removal_qty: pn(row['recommended-removal-quantity'] || row['Removal Qty'] || 0),
    unfulfillable_qty:       pn(row['your-unfulfillable-quantity'] || row['unfulfillable-quantity'] || row['Unfulfillable'] || 0),
    storage_type:            String(row['storage-type'] || row['Storage Type'] || '').trim(),
    your_price:              parseNum(row['your-price'] || row['Price'] || null),
    sales_rank:              parseNum(row['sales-rank'] || row['Sales Rank'] || null),
    estimated_storage_cost:  parseNum(row['estimated-storage-cost-per-unit'] || row['total-estimated-storage-cost'] || null),
    supplier:                String(row['supplier'] || row['Supplier'] || '').trim(),
    brand:                   String(row['brand'] || row['Brand'] || '').trim(),
    row_hash:                hash(snap || '', sku, asin),
  };
}

async function run() {
  console.log('Reading', FILE, '...');
  const wb = XLSX.readFile(FILE, { cellDates: false });
  console.log('Sheets:', wb.SheetNames.join(', '));

  // Find Raw Data sheet
  const rawName = wb.SheetNames.find(n => /raw/i.test(n)) || wb.SheetNames[0];
  console.log(`Using sheet: "${rawName}"`);

  const sheet = wb.Sheets[rawName];
  const raw   = XLSX.utils.sheet_to_json(sheet, { defval: '' });
  const rows  = raw.map(r => Object.fromEntries(Object.entries(r).map(([k,v]) => [k.trim(), v])));
  console.log(`Total rows: ${rows.length.toLocaleString()}`);

  if (rows[0]) {
    console.log('Columns (first 15):', Object.keys(rows[0]).slice(0,15).join(', '));
    const sample = buildRecord(rows[0]);
    console.log('Sample snap:', sample.snapshot_date, '| age_0_90:', sample.age_0_90, '| age_181_270:', sample.age_181_270, '| action:', sample.recommended_action);
  }

  const records = rows.map(buildRecord);
  let totalAdded = 0, totalSkipped = 0;
  const batches = Math.ceil(records.length / BATCH);

  for (let i = 0; i < records.length; i += BATCH) {
    const batch = records.slice(i, i + BATCH);
    const batchNum = Math.floor(i / BATCH) + 1;
    process.stdout.write(`Batch ${batchNum}/${batches} (rows ${i+1}-${Math.min(i+BATCH,records.length)})... `);
    try {
      const result = await postBatch(batch);
      const a = result.added  ?? 0;
      const s = result.skipped ?? 0;
      totalAdded   += a;
      totalSkipped += s;
      console.log(`+${a} added, ${s} skipped`);
    } catch(e) {
      console.log(`ERROR: ${e.message} — retrying...`);
      try {
        const result = await postBatch(batch);
        totalAdded   += result.added   ?? 0;
        totalSkipped += result.skipped ?? 0;
        console.log('retry ok');
      } catch(e2) { console.log(`retry failed: ${e2.message}`); }
    }
  }

  console.log(`\n✅ Done!`);
  console.log(`   +${totalAdded.toLocaleString()} new rows added`);
  console.log(`   ${totalSkipped.toLocaleString()} already exist (skipped)`);
  console.log('View dashboard: https://returnsrate-app-production.up.railway.app/inventory.html');
}

run().catch(console.error);
