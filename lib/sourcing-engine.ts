// ==========================================
// Amazon Wholesale Sourcing Engine
// Ported from Python universal_agent.py
// ==========================================

import * as XLSX from 'xlsx';

// ---- Types ----
export interface RunConfig {
  min_roi_ready: number;
  priority_roi: number;
  borderline_min: number;
  min_drops_90d: number;
  max_offers_total: number;
  max_fba_offers: number;
  max_amazon_bb_pct: number;
  max_bb_stddev: number;
  max_bb_winners: number;
  max_trend_drop: number;
  new_listing_days: number;
  min_rating: number;
  min_review_count: number;
}

export const DEFAULT_CONFIG: RunConfig = {
  min_roi_ready: 13.0,
  priority_roi: 15.0,
  borderline_min: 10.0,
  min_drops_90d: 5.0,
  max_offers_total: 20.0,
  max_fba_offers: 12.0,
  max_amazon_bb_pct: 30.0,
  max_bb_stddev: 25.0,
  max_bb_winners: 15.0,
  max_trend_drop: -40.0,
  new_listing_days: 14,
  min_rating: 0,
  min_review_count: 0,
};

export interface ProcessingResult {
  ready: Record<string, any>[];
  borderline: Record<string, any>[];
  removed: Record<string, any>[];
  unmatched: Record<string, any>[];
  warnings: string[];
  vendor: string;
  confidence: string;
  supplierMapping: Record<string, string | null>;
}

// ---- Field Aliases ----
const SUPPLIER_FIELD_ALIASES: Record<string, string[]> = {
  upc: ['upc code', 'upc', 'ean', 'barcode', 'gtin', 'upc/ean'],
  cost: ['std pkg cust cost', 'cost', 'wholesale', 'net', 'price', 'unit cost', 'cust cost', 'promo price'],
  mpn: ['part number', 'vendor part number', 'your part number', 'mpn', 'sku', 'item#'],
  description: ['item description', 'title', 'description', 'product name', 'name'],
  brand: ['brand name', 'brand', 'manufacturer'],
  vendor: ['vendor name', 'vendor', 'supplier', 'distributor'],
  moq: ['minimum order qty', 'moq', 'min qty', 'min order'],
  list_price: ['std pkg list price', 'msrp', 'list price'],
  hazmat: ['hazmat item', 'hazmat', 'hazardous'],
  state_restricted: ['state restricted', 'restricted states'],
  promo_price: ['promo price'],
  promo_start: ['promo start date'],
  promo_end: ['promo end date'],
};

const KEEPA_REQUIRED_MAP: Record<string, string> = {
  asin: 'ASIN',
  title: 'Title',
  buybox_current: 'Buy Box: Current',
  buybox_90d: 'Buy Box: 90 days avg.',
  fba_fee: 'FBA Pick&Pack Fee',
  drops_90d: 'Sales Rank: Drops last 90 days',
  monthly_sold: 'Bought in past month',
  trend_pct: '90 days change % monthly sold',
  offers_total: 'Total Offer Count',
  fba_offers: 'New Offer Count: Current',
  amazon_bb_pct: 'Buy Box: % Amazon 90 days',
  amazon_current: 'Amazon: Current',
  bb_stddev: 'Buy Box: Standard Deviation 90 days',
  bb_winners: 'Buy Box: Winner Count 90 days',
  flipability: 'Buy Box: Flipability 90 days',
  imported_by_code: 'Imported by Code',
  prod_upc: 'Product Codes: UPC',
  prod_ean: 'Product Codes: EAN',
  prod_gtin: 'Product Codes: GTIN',
  prod_partnumber: 'Product Codes: PartNumber',
  is_hazmat: 'Is HazMat',
  is_heat_sensitive: 'Is heat sensitive',
  adult_product: 'Adult Product',
  variation_attributes: 'Variation Attributes',
  listed_since: 'Listed since',
  brand: 'Brand',
  manufacturer: 'Manufacturer',
  weight_g: 'Package: Weight (g)',
  reviews_rating: 'Reviews: Rating',
  reviews_count: 'Reviews: Rating Count',
};

// ---- Utility Functions ----
function normalizeHeader(s: any): string {
  return String(s ?? '').trim().toLowerCase().replace(/\s+/g, ' ');
}

function safeFloat(v: any): number | null {
  if (v === null || v === undefined || v === '' || v === '-' || v === '—') return null;
  if (typeof v === 'number') return isNaN(v) ? null : v;
  const t = String(v).trim().replace('%', '').replace(/,/g, '');
  if (!t) return null;
  const n = parseFloat(t);
  return isNaN(n) ? null : n;
}

function digitsOnly(v: any): string {
  if (v === null || v === undefined) return '';
  return String(v).replace(/\D/g, '');
}

function normalizeUpcOutput(v: any): string | null {
  let d = digitsOnly(v);
  if (!d) return null;
  if (d.length === 14 && d.startsWith('00')) d = d.slice(-12);
  else if (d.length === 13 && d.startsWith('0')) d = d.slice(-12);
  else if (d.length > 12) return d;
  else d = d.padStart(12, '0');
  return d;
}

function matchingCodeVariants(v: any): string[] {
  const d = digitsOnly(v);
  if (!d) return [];
  const out = new Set<string>();
  out.add(d);
  if (d.length <= 12) out.add(d.padStart(12, '0'));
  if (d.length === 13 && d.startsWith('0')) out.add(d.slice(-12));
  if (d.length === 14 && d.startsWith('00')) out.add(d.slice(-12));
  return Array.from(out).filter((x: string) => x.length > 0);
}

function yn(v: any): string {
  if (v === null || v === undefined || v === '' || v === '-' || v === '—') return 'N';
  const s = String(v).trim().toLowerCase();
  return ['y', 'yes', 'true', '1'].includes(s) ? 'Y' : 'N';
}

function keepaBool(v: any): string {
  if (v === null || v === undefined || v === '' || v === '-' || v === '—') return 'N';
  const s = String(v).trim().toLowerCase();
  return ['yes', 'y', 'true', '1'].includes(s) ? 'Y' : 'N';
}

/**
 * Smart currency conversion: auto-detects if values are in cents or dollars.
 * When keepaInCents=true, divides by 100. When false, returns as-is.
 */
function keepaToDollars(v: any, keepaInCents: boolean): number | null {
  const x = safeFloat(v);
  if (x === null) return null;
  if (keepaInCents) return Math.round((x / 100.0) * 100) / 100;
  return Math.round(x * 100) / 100;
}

/**
 * For Amazon BB% - if value is 0-1 fraction, convert to 0-100 percentage.
 * If already 0-100, keep as-is.
 */
function toPercentage(v: any): number | null {
  const x = safeFloat(v);
  if (x === null) return null;
  // Amazon BB% from Keepa: can be 0-1 fraction or 0-100 percentage
  if (x >= 0 && x <= 1.0) return Math.round(x * 100.0 * 100) / 100;
  return Math.round(x * 100) / 100;
}

/**
 * BB StdDev is in the same units as BuyBox (dollars or cents).
 * We store as dollar amount; the relative % is computed at filter time.
 */
function keepaStdDev(v: any, keepaInCents: boolean): number | null {
  const x = safeFloat(v);
  if (x === null) return null;
  if (keepaInCents) return Math.round((x / 100.0) * 100) / 100;
  return Math.round(x * 100) / 100;
}

/**
 * Auto-detect if Keepa monetary values are in cents or dollars.
 * Strategy: check the FBA Fee values — if median FBA > 50, data is likely in cents.
 * FBA fees for typical products range $2-$20. In cents that would be 200-2000.
 */
function detectKeepaFormat(keepaData: Record<string, any>[], fbaColName: string | null): boolean {
  if (!fbaColName) return false;
  const fbaVals: number[] = [];
  for (const r of keepaData) {
    const v = safeFloat(r[fbaColName]);
    if (v !== null && v > 0) {
      fbaVals.push(v);
      if (fbaVals.length >= 200) break;
    }
  }
  if (fbaVals.length === 0) return false; // default to dollars
  fbaVals.sort((a, b) => a - b);
  const median = fbaVals[Math.floor(fbaVals.length / 2)];
  // If median FBA > 50, it's likely in cents (typical FBA fee in dollars is $3-$15)
  return median > 50;
}

function gramsToLbs(v: any): number | null {
  const x = safeFloat(v);
  if (x === null) return null;
  return Math.round((x / 453.6) * 100) / 100;
}

function normalizeFlipability(v: any): string | null {
  if (v === null || v === undefined || v === '' || v === '-' || v === '—') return null;
  const s = String(v).trim().toLowerCase();
  if (['low', 'medium', 'high'].includes(s)) return s;
  const x = safeFloat(v);
  if (x === null) return s;
  if (x <= 3) return 'low';
  if (x <= 7) return 'medium';
  return 'high';
}

function variationFlag(v: any): string {
  if (v === null || v === undefined || v === '' || v === '-' || v === '—') return 'N';
  return String(v).trim() ? 'Y' : 'N';
}

function parseDate(v: any): Date | null {
  if (v === null || v === undefined || v === '' || v === '-' || v === '—') return null;
  if (v instanceof Date) return isNaN(v.getTime()) ? null : v;
  const str = String(v).trim();
  // Try common formats
  const formats = [
    /^(\d{1,2})\/(\d{1,2})\/(\d{2})$/, // M/D/YY
    /^(\d{1,2})\/(\d{1,2})\/(\d{4})$/, // M/D/YYYY
    /^(\d{4})-(\d{2})-(\d{2})/, // YYYY-MM-DD
  ];
  const m1 = str.match(formats[0]);
  if (m1) {
    const yr = parseInt(m1[3], 10);
    return new Date(yr < 50 ? 2000 + yr : 1900 + yr, parseInt(m1[1], 10) - 1, parseInt(m1[2], 10));
  }
  const m2 = str.match(formats[1]);
  if (m2) return new Date(parseInt(m2[3], 10), parseInt(m2[1], 10) - 1, parseInt(m2[2], 10));
  const m3 = str.match(formats[2]);
  if (m3) return new Date(parseInt(m3[1], 10), parseInt(m3[2], 10) - 1, parseInt(m3[3], 10));
  const d = new Date(str);
  return isNaN(d.getTime()) ? null : d;
}

function splitMultiCodes(v: any, stripNonDigits = true): string[] {
  const s = String(v ?? '');
  const parts = s.split(/[,/;]/).map((p: string) => p.trim()).filter((p: string) => p && p.toLowerCase() !== 'nan');
  if (stripNonDigits) return parts.map((p: string) => digitsOnly(p)).filter((p: string) => p.length > 0);
  return parts;
}

// ---- Supplier Processing ----
function detectSupplierColumns(columns: string[]): Record<string, string | null> {
  const normalized: Record<string, string> = {};
  for (const c of columns) normalized[c] = normalizeHeader(c);
  const mapping: Record<string, string | null> = {};
  for (const k of Object.keys(SUPPLIER_FIELD_ALIASES)) mapping[k] = null;

  for (const [unified, aliases] of Object.entries(SUPPLIER_FIELD_ALIASES)) {
    const scores: [number, string][] = [];
    for (const [c, nc] of Object.entries(normalized)) {
      let score = 0;
      for (const alias of aliases) {
        const a = normalizeHeader(alias);
        if (nc === a) score = Math.max(score, 100);
        else if (nc.includes(a)) score = Math.max(score, 60 + a.length);
      }
      if (score > 0) scores.push([score, c]);
    }
    if (scores.length > 0) {
      scores.sort((a: [number, string], b: [number, string]) => b[0] - a[0]);
      mapping[unified] = scores[0][1];
    }
  }
  return mapping;
}

function parseWorkbook(buffer: Buffer, fileName: string): Record<string, any>[][] {
  const wb = XLSX.read(buffer, { type: 'buffer', cellDates: true });
  const sheets: Record<string, any>[][] = [];
  for (const name of wb.SheetNames) {
    const ws = wb.Sheets[name];
    if (!ws) continue;
    const data = XLSX.utils.sheet_to_json(ws, { defval: '' }) as Record<string, any>[];
    sheets.push(data);
  }
  return sheets;
}

export function parseCSVExport(buffer: Buffer): Record<string, any>[] {
  return parseCSV(buffer);
}

function parseCSV(buffer: Buffer): Record<string, any>[] {
  const text = decodeCSVText(buffer);

  // Detect the best separator by checking the header line
  const firstLine = text.split(/\r?\n/)[0] ?? '';
  const separators = [
    { sep: ';', count: (firstLine.match(/;/g) ?? []).length },
    { sep: '\t', count: (firstLine.match(/\t/g) ?? []).length },
    { sep: '|', count: (firstLine.match(/\|/g) ?? []).length },
    { sep: ',', count: (firstLine.match(/,/g) ?? []).length },
  ];
  // Sort by count descending — the separator that appears most in the header wins
  separators.sort((a, b) => b.count - a.count);

  for (const { sep, count } of separators) {
    if (count === 0) continue;
    const wb = XLSX.read(text, { type: 'string', FS: sep });
    const ws = wb.Sheets[wb.SheetNames[0]];
    if (!ws) continue;
    const data = XLSX.utils.sheet_to_json(ws, { defval: '' }) as Record<string, any>[];
    if (data.length > 0) {
      const keys = Object.keys(data[0] ?? {});
      // Only accept if we have real columns (not __EMPTY) and at least 2 meaningful ones
      const realKeys = keys.filter((k: string) => !k.startsWith('__EMPTY'));
      if (realKeys.length >= 2) return data;
    }
  }

  // Fallback: let XLSX auto-detect
  const wb = XLSX.read(text, { type: 'string' });
  const ws = wb.Sheets[wb.SheetNames[0]];
  return ws ? (XLSX.utils.sheet_to_json(ws, { defval: '' }) as Record<string, any>[]) : [];
}

function decodeCSVText(buffer: Buffer): string {
  const encodings = ['utf-8', 'windows-1251', 'latin1'];
  for (const encoding of encodings) {
    try {
      const decoder = new TextDecoder(encoding, { fatal: encoding === 'utf-8' });
      const text = decoder.decode(buffer);
      if (text.trim()) return text.replace(/^\uFEFF/, '');
    } catch {
      // Try the next common supplier-file encoding.
    }
  }
  return buffer.toString('utf-8').replace(/^\uFEFF/, '');
}

export function isKeepaFile(columns: string[]): boolean {
  const normalized = columns.map((c: string) => normalizeHeader(c));
  const keepaSignatures = ['asin', 'buy box: current', 'sales rank: drops last 90 days', 'fba pick&pack fee', 'imported by code'];
  let matches = 0;
  for (const sig of keepaSignatures) {
    if (normalized.some((n: string) => n.includes(sig))) matches++;
  }
  return matches >= 2;
}

function normalizeSupplier(
  data: Record<string, any>[],
  fileName: string,
  warnings: string[]
): { rows: Record<string, any>[]; vendor: string; mapping: Record<string, string | null> } {
  const filtered = data.filter((r: Record<string, any>) => Object.values(r ?? {}).some((v: any) => v !== '' && v !== null && v !== undefined));
  if (filtered.length === 0) {
    warnings.push(`No supplier data rows found in ${fileName}`);
    return { rows: [], vendor: 'UNKNOWN', mapping: {} };
  }
  const columns = Object.keys(filtered[0] ?? {});
  const mapping = detectSupplierColumns(columns);

  if (!mapping.cost) warnings.push(`STRUCTURE WARNING: No cost column detected in ${fileName}`);
  if (!mapping.upc) warnings.push(`STRUCTURE WARNING: No UPC column detected in ${fileName}`);

  const today = new Date();
  const rows: Record<string, any>[] = [];
  for (const r of filtered) {
    const upcRaw = mapping.upc ? r[mapping.upc] : null;
    const costRaw = mapping.cost ? r[mapping.cost] : null;
    const moqRaw = mapping.moq ? r[mapping.moq] : null;
    const promoRaw = mapping.promo_price ? r[mapping.promo_price] : null;
    const promoEndRaw = mapping.promo_end ? r[mapping.promo_end] : null;

    let cost = safeFloat(costRaw);
    const moq = safeFloat(moqRaw) || 1;
    const promoPrice = safeFloat(promoRaw);
    const promoEnd = parseDate(promoEndRaw);

    if (promoPrice !== null && promoEnd && promoEnd > today) cost = promoPrice;
    if (cost === null || cost <= 0) continue;

    const unitCost = moq > 0 ? cost / moq : cost;
    rows.push({
      'UPC': normalizeUpcOutput(upcRaw),
      'UPC_raw': upcRaw,
      'MPN': mapping.mpn && r[mapping.mpn] !== '' ? String(r[mapping.mpn] ?? '').trim() || null : null,
      'Description': mapping.description && r[mapping.description] !== '' ? String(r[mapping.description] ?? '').trim() || null : null,
      'Brand': mapping.brand && r[mapping.brand] !== '' ? String(r[mapping.brand] ?? '').trim() || null : null,
      'Vendor': mapping.vendor && r[mapping.vendor] !== '' ? String(r[mapping.vendor] ?? '').trim() || null : null,
      'Cost $': Math.round(unitCost * 10000) / 10000,
      'MOQ': Math.floor(moq) || 1,
      'HazMat': mapping.hazmat ? yn(r[mapping.hazmat]) : 'N',
      'State Restricted': mapping.state_restricted && r[mapping.state_restricted] !== '' ? String(r[mapping.state_restricted] ?? '').trim() || null : null,
    });
  }

  const vendors = [...new Set(rows.map((r: Record<string, any>) => r['Vendor']).filter((v: any) => v && String(v).trim()))];
  let vendor = 'UNKNOWN';
  if (vendors.length === 1) vendor = String(vendors[0]);
  else if (vendors.length > 1) vendor = 'MULTI_VENDOR';

  return { rows, vendor, mapping };
}

// ---- Keepa Processing ----
function findKeepaColumn(columns: string[], targetName: string): string | null {
  const target = normalizeHeader(targetName);
  for (const c of columns) {
    if (normalizeHeader(c) === target) return c;
  }
  // Partial match
  for (const c of columns) {
    if (normalizeHeader(c).includes(target) || target.includes(normalizeHeader(c))) return c;
  }
  return null;
}

function buildKeepaIndex(
  keepaData: Record<string, any>[],
  warnings: string[]
): Map<string, Record<string, any>[]> {
  const idx = new Map<string, Record<string, any>[]>();
  if (keepaData.length === 0) return idx;

  const columns = Object.keys(keepaData[0] ?? {});
  // Map keepa keys to actual column names
  const colMap: Record<string, string | null> = {};
  for (const [key, target] of Object.entries(KEEPA_REQUIRED_MAP)) {
    colMap[key] = findKeepaColumn(columns, target);
  }

  // Auto-detect if monetary values are in cents or dollars
  const keepaInCents = detectKeepaFormat(keepaData, colMap.fba_fee);
  // Log to server console only — not a user-facing warning
  console.log(`[Sourcing Engine] Keepa format auto-detected: prices in ${keepaInCents ? 'CENTS (÷100)' : 'DOLLARS (as-is)'}`);

  for (const r of keepaData) {
    const asinCol = colMap.asin;
    const asin = asinCol ? String(r[asinCol] ?? '').trim() : '';
    if (!asin || asin.toLowerCase() === 'nan') continue;

    const rec: Record<string, any> = {
      'ASIN': asin,
      'Title': colMap.title ? r[colMap.title] : null,
      'Brand': colMap.brand ? r[colMap.brand] : null,
      'Manufacturer': colMap.manufacturer ? r[colMap.manufacturer] : null,
      'BuyBox $': keepaToDollars(colMap.buybox_current ? r[colMap.buybox_current] : null, keepaInCents),
      'BuyBox 90d $': keepaToDollars(colMap.buybox_90d ? r[colMap.buybox_90d] : null, keepaInCents),
      'FBA Fee $': keepaToDollars(colMap.fba_fee ? r[colMap.fba_fee] : null, keepaInCents),
      'Drops 90d': safeFloat(colMap.drops_90d ? r[colMap.drops_90d] : null),
      'Monthly Sold': safeFloat(colMap.monthly_sold ? r[colMap.monthly_sold] : null),
      'Trend %': safeFloat(colMap.trend_pct ? r[colMap.trend_pct] : null),
      'Offers Total': safeFloat(colMap.offers_total ? r[colMap.offers_total] : null),
      'FBA Offers': safeFloat(colMap.fba_offers ? r[colMap.fba_offers] : null),
      'Amazon BB%': toPercentage(colMap.amazon_bb_pct ? r[colMap.amazon_bb_pct] : null),
      'Amazon $': keepaToDollars(colMap.amazon_current ? r[colMap.amazon_current] : null, keepaInCents),
      'BB StdDev $': keepaStdDev(colMap.bb_stddev ? r[colMap.bb_stddev] : null, keepaInCents),
      'BB Winners': safeFloat(colMap.bb_winners ? r[colMap.bb_winners] : null),
      'Flipability': normalizeFlipability(colMap.flipability ? r[colMap.flipability] : null),
      'HazMat': keepaBool(colMap.is_hazmat ? r[colMap.is_hazmat] : null),
      'Meltable': keepaBool(colMap.is_heat_sensitive ? r[colMap.is_heat_sensitive] : null),
      'Adult': keepaBool(colMap.adult_product ? r[colMap.adult_product] : null),
      'Variation': variationFlag(colMap.variation_attributes ? r[colMap.variation_attributes] : null),
      'Listed since': parseDate(colMap.listed_since ? r[colMap.listed_since] : null),
      'Weight lbs': colMap.weight_g ? gramsToLbs(r[colMap.weight_g]) : null,
      'Rating': safeFloat(colMap.reviews_rating ? r[colMap.reviews_rating] : null),
      'Review Count': safeFloat(colMap.reviews_count ? r[colMap.reviews_count] : null),
    };

    const keySpecs: [string, any][] = [
      ['imported_by_code', colMap.imported_by_code ? r[colMap.imported_by_code] : null],
      ['prod_upc', colMap.prod_upc ? r[colMap.prod_upc] : null],
      ['prod_ean', colMap.prod_ean ? r[colMap.prod_ean] : null],
      ['prod_gtin', colMap.prod_gtin ? r[colMap.prod_gtin] : null],
      ['prod_partnumber', colMap.prod_partnumber ? r[colMap.prod_partnumber] : null],
    ];

    for (const [keyName, rawVal] of keySpecs) {
      if (rawVal === null || rawVal === undefined || rawVal === '') continue;
      if (keyName === 'prod_partnumber') {
        const parts = splitMultiCodes(rawVal, false);
        for (const p of parts) {
          const key = `${keyName}::${p.trim()}`;
          if (!idx.has(key)) idx.set(key, []);
          idx.get(key)!.push(rec);
        }
      } else {
        const parts = splitMultiCodes(rawVal, true);
        for (const p of parts) {
          for (const code of matchingCodeVariants(p)) {
            const key = `${keyName}::${code}`;
            if (!idx.has(key)) idx.set(key, []);
            idx.get(key)!.push(rec);
          }
        }
      }
    }
  }
  return idx;
}

/**
 * Score a Keepa record by data completeness (higher = more useful data).
 */
function keepaRecordScore(rec: Record<string, any>): number {
  let score = 0;
  if (rec['BuyBox $'] !== null && rec['BuyBox $'] > 0) score += 10;
  if (rec['FBA Fee $'] !== null && rec['FBA Fee $'] > 0) score += 10;
  if (rec['Drops 90d'] !== null && rec['Drops 90d'] > 0) score += 5;
  if (rec['Offers Total'] !== null) score += 2;
  if (rec['Monthly Sold'] !== null) score += 2;
  if (rec['BB Winners'] !== null) score += 1;
  return score;
}

function exactKeepaMatch(
  sRow: Record<string, any>,
  keepaIdx: Map<string, Record<string, any>[]>
): { match: Record<string, any> | null; matchType: string | null; attempts: string[]; ambiguous: boolean } {
  const attempts: string[] = [];
  const upc = sRow['UPC'] as string | null;
  const mpn = (sRow['MPN'] as string || '').trim();

  // Collect all candidate ASINs across all matching methods
  const allCandidates = new Map<string, { rec: Record<string, any>; matchType: string; score: number }>();

  if (upc) {
    // Try imported_by_code first (highest priority — exact match with user's input)
    for (const k of ['imported_by_code', 'prod_upc', 'prod_ean', 'prod_gtin']) {
      attempts.push(k);
      const key = `${k}::${upc}`;
      const vals = keepaIdx.get(key) ?? [];
      for (const rec of vals) {
        const asin = rec['ASIN'];
        if (!allCandidates.has(asin)) {
          allCandidates.set(asin, { rec, matchType: k, score: keepaRecordScore(rec) });
        }
      }
      // If exactly 1 ASIN found via imported_by_code, return immediately
      if (k === 'imported_by_code' && allCandidates.size === 1) {
        const entry = Array.from(allCandidates.values())[0];
        return { match: entry.rec, matchType: entry.matchType, attempts, ambiguous: false };
      }
    }

    // Multiple ASINs found for same UPC — pick the best one (most complete data)
    if (allCandidates.size > 1) {
      const sorted = Array.from(allCandidates.values()).sort((a, b) => b.score - a.score);
      // If top candidate has significantly more data, use it
      if (sorted[0].score >= 15 && sorted[0].score > sorted[1].score) {
        return { match: sorted[0].rec, matchType: sorted[0].matchType + ' (best_of_' + allCandidates.size + ')', attempts, ambiguous: false };
      }
      // Otherwise truly ambiguous
      return { match: null, matchType: 'ambiguous', attempts, ambiguous: true };
    }
    if (allCandidates.size === 1) {
      const entry = Array.from(allCandidates.values())[0];
      return { match: entry.rec, matchType: entry.matchType, attempts, ambiguous: false };
    }
  }

  if (mpn) {
    attempts.push('prod_partnumber');
    const key = `prod_partnumber::${mpn}`;
    const vals = keepaIdx.get(key) ?? [];
    const unique = new Map<string, Record<string, any>>();
    for (const rec of vals) unique.set(rec['ASIN'], rec);
    if (unique.size === 1) return { match: Array.from(unique.values())[0], matchType: 'prod_partnumber', attempts, ambiguous: false };
    if (unique.size > 1) {
      // Try to pick the best one
      const sorted = Array.from(unique.values())
        .map(rec => ({ rec, score: keepaRecordScore(rec) }))
        .sort((a, b) => b.score - a.score);
      if (sorted[0].score >= 15 && sorted[0].score > sorted[1].score) {
        return { match: sorted[0].rec, matchType: 'prod_partnumber (best_of_' + unique.size + ')', attempts, ambiguous: false };
      }
      return { match: null, matchType: 'prod_partnumber', attempts, ambiguous: true };
    }
  }

  return { match: null, matchType: null, attempts, ambiguous: false };
}

// ---- Confidence & Quantity ----
function classifyConfidence(row: Record<string, any>, config: RunConfig): string {
  if (
    (row['Amazon BB%'] ?? 0) === 0 &&
    (row['Drops 90d'] ?? 0) >= 15 &&
    (row['BB StdDev %'] ?? 0) <= 15 &&
    (row['Offers Total'] ?? 999) <= 12 &&
    !row['New Listing']
  ) return 'HIGH';
  if (
    row['ROI %'] !== null &&
    row['ROI %'] !== undefined &&
    row['ROI %'] >= config.min_roi_ready &&
    (row['Offers Total'] ?? 999) <= config.max_offers_total &&
    (row['BB StdDev %'] ?? 999) <= config.max_bb_stddev
  ) return 'MEDIUM';
  return 'LOW';
}

function qtyEngine(row: Record<string, any>): number {
  const moq = row['MOQ'] || 1;
  if (moq > 1) return Math.floor(moq);
  const drops = row['Drops 90d'] || 0;
  if (drops >= 15) return 5;
  if (drops >= 5) return 3;
  return 1;
}

function overallConfidence(readyRows: Record<string, any>[]): string {
  if (readyRows.length === 0) return 'LOW';
  const counts: Record<string, number> = { HIGH: 0, MEDIUM: 0, LOW: 0 };
  for (const r of readyRows) counts[r['Confidence'] ?? 'LOW']++;
  if (counts.HIGH >= counts.MEDIUM + counts.LOW) return 'HIGH';
  if (counts.LOW > counts.HIGH) return 'LOW';
  return 'MEDIUM';
}

function removalRecord(sRow: Record<string, any>, stage: string, reason: string, asin?: string): Record<string, any> {
  return {
    'UPC': sRow['UPC'] ?? null,
    'MPN': sRow['MPN'] ?? null,
    'Description': sRow['Description'] ?? null,
    'Vendor': sRow['Vendor'] ?? null,
    'Stage Removed': stage,
    'Reason': `${reason}${asin ? ` | ASIN ${asin}` : ''}`,
  };
}

// ---- Main Processing ----
export function processFiles(
  supplierBuffers: { name: string; buffer: Buffer }[],
  keepaBuffers: { name: string; buffer: Buffer }[],
  config: RunConfig
): ProcessingResult {
  const warnings: string[] = [];
  const ready: Record<string, any>[] = [];
  const borderline: Record<string, any>[] = [];
  const removed: Record<string, any>[] = [];
  const unmatched: Record<string, any>[] = [];

  // Parse all keepa files into unified data
  let allKeepaRows: Record<string, any>[] = [];
  for (const kf of keepaBuffers) {
    try {
      const sheets = parseWorkbook(kf.buffer, kf.name);
      for (const sheet of sheets) {
        if (sheet.length > 0) allKeepaRows = allKeepaRows.concat(sheet);
      }
    } catch (e: any) {
      warnings.push(`Error reading keepa file ${kf.name}: ${e?.message ?? 'unknown'}`);
    }
  }

  if (allKeepaRows.length === 0) warnings.push('STRUCTURE WARNING: no Keepa rows found');
  const keepaIdx = buildKeepaIndex(allKeepaRows, warnings);

  // Parse supplier file(s)
  let allSupplierRows: Record<string, any>[] = [];
  let vendor = 'UNKNOWN';
  let supplierMapping: Record<string, string | null> = {};

  for (const sf of supplierBuffers) {
    try {
      let rawData: Record<string, any>[];
      if (sf.name.toLowerCase().endsWith('.csv')) {
        rawData = parseCSV(sf.buffer);
      } else {
        const sheets = parseWorkbook(sf.buffer, sf.name);
        rawData = sheets.reduce((acc: Record<string, any>[], s: Record<string, any>[]) => s.length > acc.length ? s : acc, []);
      }
      const result = normalizeSupplier(rawData, sf.name, warnings);
      allSupplierRows = allSupplierRows.concat(result.rows);
      if (result.vendor !== 'UNKNOWN') vendor = result.vendor;
      supplierMapping = result.mapping;
    } catch (e: any) {
      warnings.push(`Error reading supplier file ${sf.name}: ${e?.message ?? 'unknown'}`);
    }
  }

  if (allSupplierRows.length === 0) {
    return { ready: [], borderline: [], removed: [], unmatched: [], warnings, vendor, confidence: 'LOW', supplierMapping };
  }

  const today = new Date();

  // Process each supplier row through 17-stage pipeline
  for (const sRow of allSupplierRows) {
    if (!sRow['Cost $'] || sRow['Cost $'] <= 0) {
      removed.push(removalRecord(sRow, 'STAGE 1', 'No Cost'));
      continue;
    }

    const { match, matchType, attempts, ambiguous } = exactKeepaMatch(sRow, keepaIdx);
    if (ambiguous) {
      removed.push(removalRecord(sRow, 'STAGE 4', 'Multiple ASINs found for exact match key'));
      continue;
    }
    if (!match) {
      unmatched.push({
        'UPC': sRow['UPC'] ?? null,
        'MPN': sRow['MPN'] ?? null,
        'Description': sRow['Description'] ?? null,
        'Vendor': sRow['Vendor'] ?? null,
        'Match Attempted': attempts.length > 0 ? attempts.join(' > ') : 'none',
        'Notes': 'No confirmed exact ASIN match',
      });
      continue;
    }

    // Stage 5-6: Price validation
    const buybox = match['BuyBox $'] ?? match['BuyBox 90d $'];
    if (buybox === null || buybox === undefined || buybox <= 0) {
      removed.push(removalRecord(sRow, 'STAGE 5', 'No confirmed BuyBox price', match['ASIN']));
      continue;
    }
    const fbaFee = match['FBA Fee $'];
    if (fbaFee === null || fbaFee === undefined || fbaFee <= 0) {
      removed.push(removalRecord(sRow, 'STAGE 6', 'No confirmed FBA fee', match['ASIN']));
      continue;
    }

    // Stage 7-8: Unit economics
    const profit = Math.round((buybox - fbaFee - sRow['Cost $']) * 100) / 100;
    const roi = sRow['Cost $'] > 0 ? Math.round((profit / sRow['Cost $']) * 100 * 100) / 100 : null;

    const titleStr = match['Title'] ? String(match['Title']).trim() : null;
    const merged: Record<string, any> = {
      'ASIN': match['ASIN'],
      'UPC': sRow['UPC'],
      'MPN': sRow['MPN'],
      'Description': titleStr && titleStr.toLowerCase() !== 'nan' ? titleStr : sRow['Description'],
      'Brand': match['Brand'] && String(match['Brand']).trim().toLowerCase() !== 'nan' ? String(match['Brand']).trim() : sRow['Brand'],
      'Vendor': sRow['Vendor'],
      'Cost $': Math.round(sRow['Cost $'] * 100) / 100,
      'BuyBox $': Math.round(buybox * 100) / 100,
      'BuyBox 90d $': match['BuyBox 90d $'],
      'FBA Fee $': Math.round(fbaFee * 100) / 100,
      'Profit $': profit,
      'ROI %': roi,
      'Drops 90d': match['Drops 90d'],
      'Monthly Sold': match['Monthly Sold'],
      'Trend %': match['Trend %'],
      'Offers Total': match['Offers Total'],
      'FBA Offers': match['FBA Offers'],
      'Amazon BB%': match['Amazon BB%'] ?? 0,
      'BB StdDev $': match['BB StdDev $'],
      'BB StdDev %': (match['BB StdDev $'] !== null && buybox > 0) ? Math.round((match['BB StdDev $'] / buybox) * 100 * 100) / 100 : null,
      'BB Winners': match['BB Winners'],
      'Flipability': match['Flipability'],
      'HazMat': sRow['HazMat'] === 'Y' || match['HazMat'] === 'Y' ? 'Y' : 'N',
      'Meltable': match['Meltable'] ?? 'N',
      'Adult': match['Adult'] ?? 'N',
      'Variation': match['Variation'] ?? 'N',
      'State Restr.': sRow['State Restricted'] || 'N',
      'Weight lbs': match['Weight lbs'],
      'MOQ': sRow['MOQ'] || 1,
      'Risk Flags': '',
      'Notes': `Match via ${matchType}`,
      'Amazon $': match['Amazon $'],
      'Rating': match['Rating'],
      'Review Count': match['Review Count'],
    };

    // Stage 14: Listing age
    merged['New Listing'] = false;
    const listedSince = match['Listed since'];
    if (listedSince) {
      const daysDiff = Math.floor((today.getTime() - listedSince.getTime()) / (1000 * 60 * 60 * 24));
      if (daysDiff < config.new_listing_days) merged['New Listing'] = true;
    }

    // Stage 8: ROI filter
    if (roi === null || roi < config.borderline_min) {
      removed.push(removalRecord(sRow, 'STAGE 8', `ROI < ${config.borderline_min}%`, match['ASIN']));
      continue;
    }

    // Stage 9: Demand filter
    const drops = merged['Drops 90d'];
    if (drops === null || drops === undefined || drops < config.min_drops_90d) {
      removed.push(removalRecord(sRow, 'STAGE 9', `Drops 90d < ${config.min_drops_90d}`, match['ASIN']));
      continue;
    }
    const trend = merged['Trend %'];
    if (trend !== null && trend !== undefined && trend < config.max_trend_drop) {
      removed.push(removalRecord(sRow, 'STAGE 9', `Trend % < ${config.max_trend_drop}`, match['ASIN']));
      continue;
    }

    // Stage 10: Competition filter
    const offersTotal = merged['Offers Total'];
    if (offersTotal !== null && offersTotal !== undefined && offersTotal > config.max_offers_total) {
      removed.push(removalRecord(sRow, 'STAGE 10', `Total Offer Count > ${config.max_offers_total}`, match['ASIN']));
      continue;
    }
    const fbaOffers = merged['FBA Offers'];
    if (fbaOffers !== null && fbaOffers !== undefined && fbaOffers > config.max_fba_offers) {
      removed.push(removalRecord(sRow, 'STAGE 10', `FBA Offers > ${config.max_fba_offers}`, match['ASIN']));
      continue;
    }

    // Stage 11: Amazon filter
    const amazonCurrent = merged['Amazon $'];
    if (amazonCurrent !== null && amazonCurrent !== undefined && amazonCurrent > 0) {
      removed.push(removalRecord(sRow, 'STAGE 11', 'Amazon current price > 0', match['ASIN']));
      continue;
    }
    const amazonBb = merged['Amazon BB%'];
    if (amazonBb !== null && amazonBb !== undefined && amazonBb > config.max_amazon_bb_pct) {
      removed.push(removalRecord(sRow, 'STAGE 11', `Amazon BB% > ${config.max_amazon_bb_pct}`, match['ASIN']));
      continue;
    }

    // Stage 12: BuyBox stability
    const bbStddevPct = merged['BB StdDev %'];
    if (bbStddevPct !== null && bbStddevPct !== undefined && bbStddevPct > config.max_bb_stddev) {
      removed.push(removalRecord(sRow, 'STAGE 12', `BB StdDev ${bbStddevPct.toFixed(1)}% > ${config.max_bb_stddev}%`, match['ASIN']));
      continue;
    }
    const bbWinners = merged['BB Winners'];
    if (bbWinners !== null && bbWinners !== undefined && bbWinners > config.max_bb_winners) {
      removed.push(removalRecord(sRow, 'STAGE 12', `BB Winners > ${config.max_bb_winners}`, match['ASIN']));
      continue;
    }
    if (merged['Flipability'] === 'low') {
      removed.push(removalRecord(sRow, 'STAGE 12', 'Flipability low', match['ASIN']));
      continue;
    }

    // Stage 13: Risk filter
    if (merged['HazMat'] === 'Y') {
      removed.push(removalRecord(sRow, 'STAGE 13', 'HazMat = Y', match['ASIN']));
      continue;
    }
    if (merged['Meltable'] === 'Y') {
      removed.push(removalRecord(sRow, 'STAGE 13', 'Meltable / heat sensitive = Y', match['ASIN']));
      continue;
    }
    if (merged['Adult'] === 'Y') {
      removed.push(removalRecord(sRow, 'STAGE 13', 'Adult = Y', match['ASIN']));
      continue;
    }
    if (merged['Variation'] === 'Y') {
      removed.push(removalRecord(sRow, 'STAGE 13', 'Variation attributes present', match['ASIN']));
      continue;
    }

    // Stage 13b: Rating filter
    if (config.min_rating > 0) {
      const rating = merged['Rating'];
      if (rating !== null && rating !== undefined && rating < config.min_rating) {
        removed.push(removalRecord(sRow, 'STAGE 13b', `Rating ${rating} < ${config.min_rating}`, match['ASIN']));
        continue;
      }
    }
    if (config.min_review_count > 0) {
      const reviewCount = merged['Review Count'];
      if (reviewCount !== null && reviewCount !== undefined && reviewCount < config.min_review_count) {
        removed.push(removalRecord(sRow, 'STAGE 13b', `Review Count ${reviewCount} < ${config.min_review_count}`, match['ASIN']));
        continue;
      }
    }

    // Stage 15: New listing flag
    if (merged['New Listing']) merged['Risk Flags'] = 'NEW LISTING';

    // Stage 16-17: Quantity & confidence
    merged['Qty'] = qtyEngine(merged);
    merged['Confidence'] = classifyConfidence(merged, config);

    if (roi >= config.min_roi_ready) {
      ready.push(merged);
    } else {
      borderline.push({
        'ASIN': merged['ASIN'],
        'UPC': merged['UPC'],
        'Description': merged['Description'],
        'Vendor': merged['Vendor'],
        'Cost $': merged['Cost $'],
        'BuyBox $': merged['BuyBox $'],
        'ROI %': merged['ROI %'],
        'Profit $': merged['Profit $'],
        'Drops 90d': merged['Drops 90d'],
        'Reason borderline': 'ROI 10-13%',
        'Notes': merged['Notes'],
      });
    }
  }

  const confidence = overallConfidence(ready);
  return { ready, borderline, removed, unmatched, warnings, vendor, confidence, supplierMapping };
}

// ---- Excel Generation ----
export async function generateExcel(result: ProcessingResult): Promise<Buffer> {
  const ExcelJS = (await import('exceljs')).default;
  const wb = new ExcelJS.Workbook();

  // Style helpers
  const headerStyle = {
    font: { bold: true, color: { argb: 'FFFFFFFF' } },
    fill: { type: 'pattern' as const, pattern: 'solid' as const, fgColor: { argb: 'FF1B2A4A' } },
    alignment: { horizontal: 'center' as const, wrapText: true },
  };

  function addSheet(name: string, columns: string[], rows: Record<string, any>[]) {
    const ws = wb.addWorksheet(name);
    ws.addRow(columns);
    const headerRow = ws.getRow(1);
    headerRow.eachCell((cell: any) => {
      cell.font = headerStyle.font;
      cell.fill = headerStyle.fill;
      cell.alignment = headerStyle.alignment;
    });
    for (const r of rows) {
      ws.addRow(columns.map((c: string) => r[c] ?? ''));
    }
    // Auto width
    for (let i = 1; i <= columns.length; i++) {
      const col = ws.getColumn(i);
      col.width = Math.min(Math.max((columns[i - 1]?.length ?? 10) + 4, 12), 40);
    }
  }

  const readyCols = ['ASIN', 'UPC', 'MPN', 'Description', 'Brand', 'Vendor', 'Cost $', 'BuyBox $', 'BuyBox 90d $', 'FBA Fee $', 'Profit $', 'ROI %', 'Drops 90d', 'Monthly Sold', 'Trend %', 'Offers Total', 'FBA Offers', 'Amazon BB%', 'BB StdDev $', 'BB StdDev %', 'BB Winners', 'Rating', 'Review Count', 'HazMat', 'Meltable', 'Adult', 'Variation', 'State Restr.', 'Weight lbs', 'MOQ', 'Qty', 'Confidence', 'Risk Flags', 'Notes'];
  const borderCols = ['ASIN', 'UPC', 'Description', 'Vendor', 'Cost $', 'BuyBox $', 'ROI %', 'Profit $', 'Drops 90d', 'Reason borderline', 'Notes'];
  const removedCols = ['UPC', 'MPN', 'Description', 'Vendor', 'Stage Removed', 'Reason'];
  const unmatchedCols = ['UPC', 'MPN', 'Description', 'Vendor', 'Match Attempted', 'Notes'];

  addSheet('READY ORDER', readyCols, result.ready);
  addSheet('BORDERLINE ROI', borderCols, result.borderline);
  addSheet('REMOVED', removedCols, result.removed);
  addSheet('UNMATCHED', unmatchedCols, result.unmatched);

  const buf = await wb.xlsx.writeBuffer();
  return Buffer.from(buf);
}
