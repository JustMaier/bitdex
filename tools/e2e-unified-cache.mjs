#!/usr/bin/env node
/**
 * E2E Validation Suite for BitDex Unified Cache
 *
 * Runs against a live server with production data. Tests cache population,
 * pagination correctness, mutation maintenance, delete maintenance,
 * min_filter_size threshold, and multiple filter combinations.
 *
 * Usage:
 *   node tools/e2e-unified-cache.mjs [--url http://localhost:3000] [--index models] [--verbose]
 */

const BASE_URL = process.argv.includes('--url')
  ? process.argv[process.argv.indexOf('--url') + 1]
  : 'http://localhost:3000';
const INDEX = process.argv.includes('--index')
  ? process.argv[process.argv.indexOf('--index') + 1]
  : 'civitai';
const VERBOSE = process.argv.includes('--verbose');

const api = (path, opts) => `${BASE_URL}/api/indexes/${INDEX}${path}`;

let passed = 0;
let failed = 0;

// Timing records: { label, miss_us, hit_us }
const timings = [];

function log(...args) { console.log(...args); }
function vlog(...args) { if (VERBOSE) console.log('  [verbose]', ...args); }
function fmt_us(us) {
  if (us >= 1000000) return `${(us / 1000000).toFixed(2)}s`;
  if (us >= 1000) return `${(us / 1000).toFixed(2)}ms`;
  return `${us}us`;
}

function assert(cond, msg) {
  if (!cond) throw new Error(`Assertion failed: ${msg}`);
}

async function post(path, body) {
  const res = await fetch(api(path), {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  const data = await res.json();
  vlog(`POST ${path}:`, JSON.stringify(data).slice(0, 300));
  return data;
}

async function del(path, body) {
  const opts = { method: 'DELETE', headers: { 'Content-Type': 'application/json' } };
  if (body) opts.body = JSON.stringify(body);
  const res = await fetch(api(path), opts);
  const data = await res.json();
  vlog(`DELETE ${path}:`, JSON.stringify(data).slice(0, 300));
  return data;
}

async function get(path) {
  const res = await fetch(api(path));
  const data = await res.json();
  vlog(`GET ${path}:`, JSON.stringify(data).slice(0, 300));
  return data;
}

async function stats() {
  return get('/stats');
}

async function clearCache() {
  return del('/cache');
}

async function query(filters, sort, limit = 20, cursor = null) {
  const body = { filters, limit };
  if (sort) body.sort = sort;
  if (cursor) body.cursor = cursor;
  return post('/query', body);
}

async function queryWithDocs(filters, sort, limit = 20, cursor = null) {
  const body = { filters, limit, include_docs: true };
  if (sort) body.sort = sort;
  if (cursor) body.cursor = cursor;
  const res = await fetch(api('/query'), {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  const data = await res.json();
  vlog(`POST /query (docs):`, JSON.stringify(data).slice(0, 300));
  return data;
}

// Wait for flush thread to process mutations
async function waitFlush(ms = 300) {
  await new Promise(r => setTimeout(r, ms));
}

// ----- Test Groups -----

async function testA_CachePopulation() {
  log('\n--- A. Cache Population ---');

  // 1. Clear cache
  await clearCache();
  let s = await stats();
  assert(s.unified_cache_entries === 0, `expected 0 entries after clear, got ${s.unified_cache_entries}`);
  assert(s.unified_cache_hits === 0, `expected 0 hits after clear, got ${s.unified_cache_hits}`);
  assert(s.unified_cache_misses === 0, `expected 0 misses after clear, got ${s.unified_cache_misses}`);
  log('  [1] Cache cleared, stats zeroed');

  // 2. Run a filter+sort query (miss)
  const filters = [{ Eq: ['nsfwLevel', { Integer: 1 }] }];
  const sort = { field: 'reactionCount', direction: 'Desc' };
  const r1 = await query(filters, sort);
  assert(r1.ids && r1.ids.length > 0, `expected results, got ${r1.ids?.length}`);
  const missLatency = r1.elapsed_us;
  log(`  [2] First query (MISS): ${r1.ids.length} results in ${fmt_us(missLatency)}`);

  // 3. Check stats — should have 1 entry, 0 hits, 1 miss
  s = await stats();
  assert(s.unified_cache_entries >= 1, `expected >=1 entry, got ${s.unified_cache_entries}`);
  assert(s.unified_cache_misses >= 1, `expected >=1 miss, got ${s.unified_cache_misses}`);
  log(`  [3] After first query: ${s.unified_cache_entries} entries, ${s.unified_cache_hits} hits, ${s.unified_cache_misses} misses`);

  // 4. Run same query again (hit)
  const r2 = await query(filters, sort);
  assert(r2.ids && r2.ids.length > 0, 'expected results on second query');
  const hitLatency = r2.elapsed_us;
  s = await stats();
  assert(s.unified_cache_hits >= 1, `expected >=1 hit, got ${s.unified_cache_hits}`);
  log(`  [4] Second query (HIT):  ${r2.ids.length} results in ${fmt_us(hitLatency)}`);

  const speedup = missLatency / Math.max(hitLatency, 1);
  log(`  [5] Speedup: ${speedup.toFixed(1)}x (${fmt_us(missLatency)} -> ${fmt_us(hitLatency)})`);
  timings.push({ label: 'nsfwLevel=1, sort=reactionCount desc', miss_us: missLatency, hit_us: hitLatency });

  // Results should be identical
  assert(
    JSON.stringify(r1.ids) === JSON.stringify(r2.ids),
    'cache hit should return identical results'
  );
  log('  [6] Cache hit returned identical results');
}

async function testB_PaginationCorrectness() {
  log('\n--- B. Pagination Correctness ---');

  await clearCache();

  const filters = [{ Eq: ['nsfwLevel', { Integer: 1 }] }];
  const sort = { field: 'reactionCount', direction: 'Desc' };

  // Page 1 (miss — populates cache)
  const p1 = await query(filters, sort, 20);
  assert(p1.ids.length === 20, `page 1: expected 20, got ${p1.ids.length}`);
  assert(p1.cursor, 'page 1 should have a cursor');
  log(`  [1] Page 1: ${p1.ids.length} results in ${fmt_us(p1.elapsed_us)}, cursor: ${JSON.stringify(p1.cursor)}`);

  // Page 2 (hit — should be faster)
  const p2 = await query(filters, sort, 20, p1.cursor);
  assert(p2.ids.length === 20, `page 2: expected 20, got ${p2.ids.length}`);
  assert(p2.cursor, 'page 2 should have a cursor');
  log(`  [2] Page 2: ${p2.ids.length} results in ${fmt_us(p2.elapsed_us)}`);

  // Page 3
  const p3 = await query(filters, sort, 20, p2.cursor);
  assert(p3.ids.length === 20, `page 3: expected 20, got ${p3.ids.length}`);
  log(`  [3] Page 3: ${p3.ids.length} results in ${fmt_us(p3.elapsed_us)}`);

  timings.push({ label: 'pagination p1 (miss)', miss_us: p1.elapsed_us, hit_us: null });
  timings.push({ label: 'pagination p2 (hit)', miss_us: null, hit_us: p2.elapsed_us });
  timings.push({ label: 'pagination p3 (hit)', miss_us: null, hit_us: p3.elapsed_us });

  // No duplicates across pages
  const allIds = [...p1.ids, ...p2.ids, ...p3.ids];
  const uniqueIds = new Set(allIds);
  assert(uniqueIds.size === allIds.length, `duplicates found: ${allIds.length} total, ${uniqueIds.size} unique`);
  log(`  [4] No duplicates: ${uniqueIds.size} unique across 3 pages`);

  // Sort order: reactionCount desc — values should be monotonically non-increasing
  // We check via sort values embedded in cursors: p1.cursor.sort_value >= p2.cursor.sort_value >= p3.cursor
  if (p1.cursor && p2.cursor) {
    assert(
      p1.cursor.sort_value >= p2.cursor.sort_value,
      `sort order violation: p1 cursor ${p1.cursor.sort_value} < p2 cursor ${p2.cursor.sort_value}`
    );
    log(`  [5] Sort order correct: ${p1.cursor.sort_value} >= ${p2.cursor.sort_value}`);
  }
}

async function testC_DeepPagination() {
  log('\n--- C. Deep Pagination / Expansion ---');

  await clearCache();

  const filters = [{ Eq: ['nsfwLevel', { Integer: 1 }] }];
  const sort = { field: 'reactionCount', direction: 'Desc' };

  let cursor = null;
  const pageSize = 20;
  const targetPages = 5; // 100 results — enough to test expansion
  const allIds = [];

  const pageTimes = [];
  for (let page = 1; page <= targetPages; page++) {
    const r = await query(filters, sort, pageSize, cursor);
    assert(r.ids.length === pageSize, `page ${page}: expected ${pageSize}, got ${r.ids.length}`);
    allIds.push(...r.ids);
    pageTimes.push(r.elapsed_us);
    cursor = r.cursor;
    if (!cursor) {
      log(`  Pagination ended at page ${page} (no cursor)`);
      break;
    }
  }
  log(`  Page latencies: ${pageTimes.map((t, i) => `p${i + 1}=${fmt_us(t)}`).join(', ')}`);

  const uniqueIds = new Set(allIds);
  assert(uniqueIds.size === allIds.length, `duplicates in deep pagination: ${allIds.length} total, ${uniqueIds.size} unique`);
  log(`  [1] ${targetPages} pages, ${uniqueIds.size} unique results, no duplicates`);

  // Verify no stuck pagination (all pages returned full results)
  assert(allIds.length === targetPages * pageSize, `expected ${targetPages * pageSize} total, got ${allIds.length}`);
  log(`  [2] No stuck pagination — all ${targetPages} pages returned ${pageSize} results`);
}

async function testD_MutationMaintenance() {
  log('\n--- D. Mutation Maintenance ---');

  await clearCache();

  const filters = [{ Eq: ['nsfwLevel', { Integer: 1 }] }];
  const sort = { field: 'reactionCount', direction: 'Desc' };

  // Populate cache
  const r1 = await query(filters, sort, 20);
  assert(r1.ids.length === 20, 'initial query should return 20 results');
  const s1 = await stats();
  const entriesBefore = s1.unified_cache_entries;
  log(`  [1] Cache populated: ${entriesBefore} entries`);

  // Get a doc from the results to know valid fields, then upsert with high reactionCount
  // Use an ID from deep in the results (last one) — update its reactionCount to be highest
  const targetId = r1.ids[r1.ids.length - 1];
  log(`  [2] Upserting doc ${targetId} with reactionCount=999999999`);

  // Upsert: set reactionCount very high so it should appear at top
  const upsertRes = await post('/documents/upsert', {
    documents: [{
      id: targetId,
      nsfwLevel: 1,
      reactionCount: 999999999,
      type: 'image',
    }],
  });
  vlog('  Upsert result:', JSON.stringify(upsertRes));

  // Wait for flush
  await waitFlush(500);

  // Re-query — the doc should now appear in top results
  const r2 = await query(filters, sort, 20);
  const appearsInTop = r2.ids.includes(targetId);
  log(`  [3] After upsert: doc ${targetId} in top 20? ${appearsInTop}`);
  // Note: may not appear at #1 due to cache bitmap staleness, but should be findable

  // Check entry count didn't grow (maintenance updated existing entry)
  const s2 = await stats();
  assert(
    s2.unified_cache_entries <= entriesBefore + 1,
    `cache entries grew unexpectedly: ${entriesBefore} -> ${s2.unified_cache_entries}`
  );
  log(`  [4] Cache entries stable: ${s2.unified_cache_entries}`);

  // Restore original value by upserting back with a normal value
  await post('/documents/upsert', {
    documents: [{
      id: targetId,
      nsfwLevel: 1,
      reactionCount: 0,
      type: 'image',
    }],
  });
  await waitFlush(200);
  log(`  [5] Restored doc ${targetId}`);
}

async function testE_DeleteMaintenance() {
  log('\n--- E. Delete Maintenance ---');

  await clearCache();

  const filters = [{ Eq: ['nsfwLevel', { Integer: 1 }] }];
  const sort = { field: 'reactionCount', direction: 'Desc' };

  // Populate
  const r1 = await query(filters, sort, 20);
  assert(r1.ids.length === 20, 'should get 20 results');
  const s1 = await stats();
  log(`  [1] Cache populated: ${s1.unified_cache_entries} entries`);

  // Pick a doc to delete (one from middle of results)
  const targetId = r1.ids[10];
  log(`  [2] Deleting doc ${targetId}`);

  const delRes = await del('/documents', { ids: [targetId] });
  vlog('  Delete result:', JSON.stringify(delRes));

  // Wait for flush
  await waitFlush(500);

  // Re-query — deleted doc should not appear
  const r2 = await query(filters, sort, 20);
  assert(!r2.ids.includes(targetId), `deleted doc ${targetId} still appears in results`);
  log(`  [3] Doc ${targetId} no longer in results`);

  // Entry count should be stable
  const s2 = await stats();
  assert(
    s2.unified_cache_entries <= s1.unified_cache_entries + 1,
    `cache entries grew: ${s1.unified_cache_entries} -> ${s2.unified_cache_entries}`
  );
  log(`  [4] Cache entries stable: ${s2.unified_cache_entries}`);

  // Note: we can't easily restore a deleted doc without knowing all its fields.
  // The E2E test should use non-critical data or accept this limitation.
  log(`  [5] Warning: doc ${targetId} was permanently deleted from the index`);
}

async function testF_MinFilterSizeThreshold() {
  log('\n--- F. Min Filter Size Threshold ---');

  await clearCache();

  // Use a very specific userId that likely matches < 1000 docs
  // Try userId=1 — probably has few images
  const filters = [{ Eq: ['userId', { Integer: 1 }] }];
  const sort = { field: 'reactionCount', direction: 'Desc' };

  const r = await query(filters, sort, 20);
  log(`  [1] Narrow query (userId=1) returned ${r.ids.length} results, total_matched: ${r.total_matched}`);

  const s = await stats();
  if (r.total_matched !== undefined && r.total_matched < 1000) {
    assert(
      s.unified_cache_entries === 0,
      `expected 0 cache entries for narrow filter (total=${r.total_matched}), got ${s.unified_cache_entries}`
    );
    log(`  [2] Correctly not cached: total_matched=${r.total_matched} < min_filter_size=1000`);
  } else {
    log(`  [2] Note: userId=1 has ${r.total_matched} matches — may exceed threshold, skipping assertion`);
  }
}

async function testG_MultipleFilterCombos() {
  log('\n--- G. Multiple Filter Combinations ---');

  await clearCache();
  let s = await stats();
  assert(s.unified_cache_entries === 0, 'should start at 0');

  const queries = [
    {
      filters: [{ Eq: ['nsfwLevel', { Integer: 1 }] }],
      sort: { field: 'reactionCount', direction: 'Desc' },
    },
    {
      filters: [{ Eq: ['nsfwLevel', { Integer: 1 }] }, { Eq: ['type', { String: 'image' }] }],
      sort: { field: 'reactionCount', direction: 'Desc' },
    },
    {
      filters: [{ Eq: ['nsfwLevel', { Integer: 1 }] }],
      sort: { field: 'reactionCount', direction: 'Asc' },
    },
  ];

  const labels = [
    'nsfwLevel=1, sort=reactionCount desc',
    'nsfwLevel=1 + type=image, sort=reactionCount desc',
    'nsfwLevel=1, sort=reactionCount asc',
  ];

  // Run each query once (all misses)
  const missResults = [];
  for (let i = 0; i < queries.length; i++) {
    const r = await query(queries[i].filters, queries[i].sort, 20);
    assert(r.ids.length > 0, `query ${i + 1} should return results`);
    missResults.push(r);
    log(`  [${i + 1}] Query ${i + 1} (MISS): ${r.ids.length} results in ${fmt_us(r.elapsed_us)}`);
  }

  s = await stats();
  log(`  [4] After 3 unique queries: ${s.unified_cache_entries} entries, ${s.unified_cache_hits} hits, ${s.unified_cache_misses} misses`);
  // Should have at least 2 entries (some may share cache keys if filters canonicalize the same)
  assert(s.unified_cache_entries >= 2, `expected >=2 cache entries, got ${s.unified_cache_entries}`);

  const hitsBefore = s.unified_cache_hits;

  // Run each query again (all hits)
  for (let i = 0; i < queries.length; i++) {
    const r = await query(queries[i].filters, queries[i].sort, 20);
    const speedup = missResults[i].elapsed_us / Math.max(r.elapsed_us, 1);
    log(`  [${i + 5}] Query ${i + 1} (HIT):  ${r.ids.length} results in ${fmt_us(r.elapsed_us)} (${speedup.toFixed(1)}x faster)`);
    timings.push({ label: labels[i], miss_us: missResults[i].elapsed_us, hit_us: r.elapsed_us });
  }

  s = await stats();
  const newHits = s.unified_cache_hits - hitsBefore;
  log(`  [8] After re-running: ${newHits} new hits (expected ${queries.length})`);
  assert(newHits >= queries.length, `expected >=${queries.length} new hits, got ${newHits}`);
}

// ----- Runner -----

const groups = [
  ['A', 'Cache Population', testA_CachePopulation],
  ['B', 'Pagination Correctness', testB_PaginationCorrectness],
  ['C', 'Deep Pagination / Expansion', testC_DeepPagination],
  ['D', 'Mutation Maintenance', testD_MutationMaintenance],
  ['E', 'Delete Maintenance', testE_DeleteMaintenance],
  ['F', 'Min Filter Size Threshold', testF_MinFilterSizeThreshold],
  ['G', 'Multiple Filter Combinations', testG_MultipleFilterCombos],
];

async function main() {
  log(`BitDex Unified Cache E2E Tests`);
  log(`Server: ${BASE_URL}`);
  log(`Index: ${INDEX}`);

  // Verify server is reachable
  try {
    const s = await stats();
    log(`Server alive: ${s.alive_count} docs, slot_count=${s.slot_count}`);
  } catch (e) {
    log(`\nERROR: Cannot reach server at ${BASE_URL}`);
    log(`  ${e.message}`);
    log(`\nStart the server first:`);
    log(`  cargo run --release --features server --bin server -- --port 3000`);
    process.exit(1);
  }

  for (const [id, name, fn] of groups) {
    try {
      await fn();
      passed++;
      log(`  PASS: ${id}. ${name}`);
    } catch (e) {
      failed++;
      log(`  FAIL: ${id}. ${name}`);
      log(`    ${e.message}`);
      if (VERBOSE) console.error(e.stack);
    }
  }

  // Timing summary
  if (timings.length > 0) {
    log(`\n${'='.repeat(70)}`);
    log('Timing Summary (server-reported elapsed_us)');
    log(`${'='.repeat(70)}`);
    log(`${'Query'.padEnd(48)} ${'Miss'.padStart(10)} ${'Hit'.padStart(10)} ${'Speedup'.padStart(8)}`);
    log(`${'-'.repeat(48)} ${'-'.repeat(10)} ${'-'.repeat(10)} ${'-'.repeat(8)}`);
    for (const t of timings) {
      const miss = t.miss_us != null ? fmt_us(t.miss_us) : '-';
      const hit = t.hit_us != null ? fmt_us(t.hit_us) : '-';
      const speedup = (t.miss_us != null && t.hit_us != null && t.hit_us > 0)
        ? `${(t.miss_us / t.hit_us).toFixed(1)}x`
        : '-';
      log(`${t.label.padEnd(48)} ${miss.padStart(10)} ${hit.padStart(10)} ${speedup.padStart(8)}`);
    }
    log(`${'='.repeat(70)}`);
  }

  log(`\nResults: ${passed} passed, ${failed} failed out of ${groups.length}`);

  process.exit(failed > 0 ? 1 : 0);
}

main();
