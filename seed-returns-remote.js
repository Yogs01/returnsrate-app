/**
 * seed-returns-remote.js
 * Reads the Return Raw sheet from the Excel file and pushes to Railway.
 * Also fixes empty "disposition" column for any already-uploaded returns.
 * Run: node seed-returns-remote.js
 */

const XLSX  = require('xlsx');
const crypto = require('crypto');
const https = require('https');

const FILE  = 'C:/Users/Reynaldo Macinas Jr/Downloads/Monthly Orders and Returns.xlsx';
const HOST  = 'returnsrate-app-production.up.railway.app';
const BATCH = 500;

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
  const m = String(v).trim().match(/^(\d{4}-\d{2}-\d{2})/);
  return m ? m[1] : String(v).trim();
}
function getWeek(ds) {
  if (!ds) return null;
  const d = new Date(ds); if (isNaN(d)) return null;
  const j = new Date(d.getFullYear(),0,1);
  return Math.ceil(((d-j)/86400000+j.getDay()+1)/7);
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

async function run() {
  console.log('Reading', FILE, '...');
  const wb = XLSX.readFile(FILE, { cellDates: false });

  // Find the Return Raw sheet
  const sheetName = wb.SheetNames.find(s => s === 'Return Raw') || wb.SheetNames[0];
  console.log(`Using sheet: "${sheetName}"`);

  const sheet = wb.Sheets[sheetName];
  const raw = XLSX.utils.sheet_to_json(sheet, { defval: '' });
  const rows = raw.map(r => Object.fromEntries(Object.entries(r).map(([k,v]) => [k.trim(), v])));
  console.log(`Total return rows: ${rows.length.toLocaleString()}`);

  // Show sample column names to confirm headers
  if (rows[0]) console.log('Columns:', Object.keys(rows[0]).slice(0, 10).join(', '));

  const records = rows.map(row => {
    const rd = parseDate(row['Return Date2'] || row['Return Date'] || row['return-date']);
    const pd = parseDate(row['Purchase Date'] || row['purchase-date'] || '');
    return {
      purchase_month:      String(row['Purchase Month'] || '').trim(),
      purchase_date:       pd,
      return_date_parsed:  String(row['Return Month'] || '').trim(),
      return_year:         String(row['Return Year'] || '').trim(),
      return_date:         rd,
      return_week:         getWeek(rd),
      order_id:            String(row['Order ID'] || row['order-id'] || '').trim(),
      sku:                 String(row['SKU'] || row['sku'] || '').trim(),
      asin:                String(row['ASIN'] || row['asin'] || '').trim(),
      fnsku:               String(row['FNSKU'] || row['fnsku'] || '').trim(),
      product_name:        String(row['Product Name'] || row['product-name'] || '').trim(),
      quantity:            parseNum(row['Quantity'] || row['quantity']) || 1,
      disposition:         String(row['Detailed-disposition'] || '').trim(),
      reason:              String(row['Reason'] || '').trim(),
      status:              String(row['Status'] || '').trim(),
      gender:              String(row['Gender'] || '').trim(),
      brand:               String(row['Brand'] || '').trim(),
      customer_comments:   String(row['Customer-comments'] || '').trim(),
      row_hash: hash(
        row['Order ID'] || '',
        row['ASIN'] || row['asin'] || '',
        parseDate(row['Return Date2'] || row['Return Date'] || '') || '',
        row['Reason'] || ''
      ),
    };
  });

  // Show a sample disposition to verify it's being read correctly
  const sampleDisp = records.slice(0, 5).map(r => r.disposition);
  console.log('Sample dispositions:', sampleDisp);

  let totalAdded = 0, totalUpdated = 0, totalSkipped = 0;
  const batches = Math.ceil(records.length / BATCH);

  for (let i = 0; i < records.length; i += BATCH) {
    const batch = records.slice(i, i + BATCH);
    const batchNum = Math.floor(i / BATCH) + 1;
    process.stdout.write(`Batch ${batchNum}/${batches} (rows ${i+1}-${Math.min(i+BATCH,records.length)})... `);
    try {
      const result = await postJSON('/api/import-returns', { rows: batch });
      const a = result.added  ?? 0;
      const u = result.updated ?? 0;
      const s = result.skipped ?? 0;
      totalAdded   += a;
      totalUpdated += u;
      totalSkipped += s;
      console.log(`+${a} added, ${u} disposition fixed, ${s} skipped`);
    } catch(e) {
      console.log(`ERROR: ${e.message} — retrying...`);
      try {
        const result = await postJSON('/api/import-returns', { rows: batch });
        totalAdded   += result.added   ?? 0;
        totalUpdated += result.updated ?? 0;
        totalSkipped += result.skipped ?? 0;
        console.log('retry ok');
      } catch(e2) { console.log(`retry failed: ${e2.message}`); }
    }
  }

  console.log(`\n✅ Done!`);
  console.log(`   +${totalAdded.toLocaleString()} new rows added`);
  console.log(`   ${totalUpdated.toLocaleString()} existing rows updated (disposition fixed)`);
  console.log(`   ${totalSkipped.toLocaleString()} already up to date`);
  console.log('View dashboard: https://returnsrate-app-production.up.railway.app');
}

run().catch(console.error);
