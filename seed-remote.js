/**
 * seed-remote.js
 * Reads orders.xlsx locally and pushes data directly to Railway in batches.
 * Run: node seed-remote.js
 */

const XLSX = require('xlsx');
const crypto = require('crypto');
const https = require('https');

const FILE   = 'C:/Users/Reynaldo Macinas Jr/Downloads/orders.xlsx';
const HOST   = 'returnsrate-app-production.up.railway.app';
const BATCH  = 500;    // rows per POST (smaller = safer, avoids body size issues)
const CONCUR = 1;      // one batch at a time (safe)

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

const KNOWN_BRANDS = [
  'Good Smile Company','GOOD SMILE COMPANY','Teenage Mutant Ninja Turtles',
  'Dungeons & Dragons','Under Armour','Diamond Select','Orange Rouge',
  'The Loyal Subjects','Max Factory','Square Enix','Games Workshop',
  'Hiya Toys','Hot Toys','Sen-Ti-Nel','Sen-ti-nel','Warhammer 40k',
  'Jason Markk','SAXX Underwear Co.','SAXX','Hey Dude','HEYDUDE',
  'Fjallraven','Hydro Flask','Loungefly','CamelBak','ThreeZero',
  'Mezco','Funko','FUNKO','Hasbro','Mattel','Bandai','Capcom','NECA',
  'Megahouse','Furyu','SEGA','Vionic','Timberland','Saucony','Rockport',
  'Reebok','Merrell','MERRELL','GARMONT','Brooks','K-Swiss','ASICS',
  'Clarks','Chaco','MARMOT','Kelty','ALTRA','OOFOS','Hestra','Sperry',
  'DenTek','Sharpie','HOKA','Hoka','KEEN','ECCO','MUCK','Muck',
  'Smith','SMITH','Cole Haan','adidas','Nike','Veja','Crocs',
  'Birkenstock','Salomon','Teva','UGG','Bogs','Sorel','Danner','Oboz',
  'New Balance','Puma','Osprey','Gregory','Deuter','Thule','Patagonia',
  'Columbia','Arc\'teryx','Cotopaxi','Eagle Creek','Warhammer',
  'Keds','REEF','Caterpillar','CAT Footwear','On'
].sort((a,b) => b.length - a.length);

function extractBrand(p) {
  if (!p || p === '-') return null;
  for (const b of KNOWN_BRANDS) {
    if (p.toLowerCase().startsWith(b.toLowerCase())) return b;
  }
  if (/^(Z\d|Z\/\d|Z\/X|MEGA Z)/i.test(p)) return 'Chaco';
  const warhammerKw = ['XV8','Nurglings','Kill Team','Spearhead','Knight Household',
    'Horus Heresy','Chaos Space','Space Marine','Age of Sigmar','Blood Angels',
    'Ork ','Eldar','Necron','Tau ','Tyranid'];
  for (const kw of warhammerKw) {
    if (p.toLowerCase().includes(kw.toLowerCase())) return 'Warhammer';
  }
  const m = p.match(/^([A-Za-z0-9][^-]{1,30}?)\s*[-–]\s+\S/);
  if (m) { const c = m[1].trim(); if (c.length >= 2 && c.length <= 30) return c; }
  return null;
}

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
  const sheet = wb.Sheets[wb.SheetNames[0]];
  const raw = XLSX.utils.sheet_to_json(sheet, { defval: '' });
  const rows = raw.map(r => Object.fromEntries(Object.entries(r).map(([k,v]) => [k.trim(),v])));
  console.log(`Total rows: ${rows.length.toLocaleString()}`);

  // Build records
  console.log('Building records...');
  const records = rows.map(row => {
    const pd = parseDate(row['Purchase Date2'] || row['purchase-date']);
    return {
      identifier: String(row['Identifier'] || row['Duplicate Id'] || '').trim(),
      purchase_month: String(row['Purchase Month'] || '').trim(),
      purchase_date: pd,
      purchase_week: getWeek(pd),
      brand: (() => { const raw = String(row['Brand']||'').trim(); const pn = String(row['product-name']||'').trim(); return (raw && raw!=='-' && raw!=='0') ? raw : (extractBrand(pn) || raw); })(),
      amazon_order_id: String(row['amazon-order-id'] || '').trim(),
      order_status: String(row['order-status'] || '').trim(),
      product_name: String(row['product-name'] || '').trim(),
      sku: String(row['sku'] || '').trim(),
      asin: String(row['asin'] || '').trim(),
      item_status: String(row['item-status'] || '').trim(),
      quantity: parseNum(row['quantity']) || 0,
      item_price: parseNum(row['item-price']),
      item_tax: parseNum(row['item-tax']),
      row_hash: hash(
        row['Identifier'] || row['Duplicate Id'] || '',
        row['asin'] || '',
        row['purchase-date'] || row['Purchase Date2'] || '',
        String(row['item-price'] || '')
      )
    };
  });

  let totalAdded = 0, totalSkipped = 0;
  const batches = Math.ceil(records.length / BATCH);

  for (let i = 0; i < records.length; i += BATCH) {
    const batch = records.slice(i, i + BATCH);
    const batchNum = Math.floor(i / BATCH) + 1;
    process.stdout.write(`Batch ${batchNum}/${batches} (rows ${i+1}-${Math.min(i+BATCH, records.length)})... `);
    try {
      const result = await postJSON('/api/import-orders', { rows: batch });
      const a = result.added ?? result.ordersAdded ?? 0;
      const s = result.skipped ?? result.ordersSkipped ?? 0;
      totalAdded   += a;
      totalSkipped += s;
      console.log(`+${a} added, ${s} skipped`);
    } catch(e) {
      console.log(`ERROR: ${e.message} — retrying...`);
      try {
        const result = await postJSON('/api/import-orders', { rows: batch });
        const a = result.added ?? result.ordersAdded ?? 0;
        const s = result.skipped ?? result.ordersSkipped ?? 0;
        totalAdded   += a;
        totalSkipped += s;
        console.log(`retry ok: +${a} added`);
      } catch(e2) {
        console.log(`retry failed: ${e2.message}`);
      }
    }
  }

  console.log(`\n✅ Done! Total: +${totalAdded.toLocaleString()} added, ${totalSkipped.toLocaleString()} skipped`);
  console.log('View dashboard: https://returnsrate-app-production.up.railway.app');
}

run().catch(console.error);
