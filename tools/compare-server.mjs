#!/usr/bin/env node
/**
 * BitDex vs Meilisearch Comparison Server
 *
 * Queries both BitDex (localhost:3001) and Meilisearch (feed.civitai.com)
 * with equivalent parameters and returns both result sets for comparison.
 *
 * Usage:
 *   node tools/compare-server.mjs
 *
 * Then query:
 *   GET http://localhost:3002/compare?sort=sortAtUnix:desc&nsfwLevel=1&limit=20
 *   GET http://localhost:3002/compare?sort=reactionCount:desc&nsfwLevel=1&type=image&limit=20&offset=0
 *
 * Query params:
 *   sort        - "field:dir" (e.g. "sortAtUnix:desc", "reactionCount:desc")
 *   limit       - number of results (default 20)
 *   offset      - skip N results (default 0)
 *   nsfwLevel   - exact match filter
 *   type        - exact match filter (e.g. "image", "video")
 *   onSite      - boolean filter
 *   tagIds      - comma-separated tag IDs (IN filter)
 *   userId      - exact match filter
 *   postedTo    - exact match filter (string, mapped)
 *   sortAtUnix_lte - upper bound for sortAtUnix (for paging back in time)
 *   sortAtUnix_gte - lower bound for sortAtUnix
 *   ids         - comma-separated IDs to filter on
 */

import http from 'node:http';

const BITDEX_URL = process.env.BITDEX_URL || 'http://localhost:3001';
const BITDEX_INDEX = 'civitai';
const MEILI_HOST = process.env.MEILI_HOST || 'https://feed.civitai.com';
const MEILI_API_KEY = process.env.MEILI_API_KEY || 'f4bec1da3b9e72d8a1c27e8290c3ad10e6c6a1cf0f5e907133f4c458391ab576';
const MEILI_INDEX = 'metrics_images_v1';
const PORT = 3002;

// --- Meili query ---
async function queryMeili(filter, sort, limit, offset) {
  const body = { filter, sort: sort ? [sort] : [], limit, offset };
  const start = performance.now();
  const res = await fetch(`${MEILI_HOST}/indexes/${MEILI_INDEX}/documents/fetch`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${MEILI_API_KEY}`,
    },
    body: JSON.stringify(body),
  });
  const elapsed = performance.now() - start;
  if (!res.ok) {
    const text = await res.text();
    return { error: `Meili ${res.status}: ${text}`, elapsed_ms: elapsed };
  }
  const data = await res.json();
  return {
    ids: data.results?.map(r => r.id) || [],
    total: data.total,
    results: data.results?.slice(0, 10), // first 10 docs for inspection
    elapsed_ms: elapsed,
  };
}

// --- BitDex query ---
async function queryBitdex(filters, sort, limit, offset) {
  const body = { filters, limit };
  if (sort) body.sort = sort;
  if (offset > 0) body.offset = offset;

  const start = performance.now();
  const res = await fetch(`${BITDEX_URL}/api/indexes/${BITDEX_INDEX}/query?include_docs=true`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  const elapsed = performance.now() - start;
  if (!res.ok) {
    const text = await res.text();
    return { error: `BitDex ${res.status}: ${text}`, elapsed_ms: elapsed };
  }
  const data = await res.json();
  return {
    ids: data.ids || [],
    total_matched: data.total_matched,
    cursor: data.cursor,
    docs: data.docs?.slice(0, 10), // first 10 docs for inspection
    elapsed_ms: elapsed,
    elapsed_us: data.elapsed_us,
  };
}

// --- Build filters from query params ---
function buildFilters(params) {
  const meiliParts = [];
  const bitdexClauses = [];

  // nsfwLevel
  if (params.has('nsfwLevel')) {
    const v = Number(params.get('nsfwLevel'));
    meiliParts.push(`nsfwLevel = ${v}`);
    bitdexClauses.push({ Eq: ['nsfwLevel', { Integer: v }] });
  }

  // type (mapped string in BitDex)
  if (params.has('type')) {
    const v = params.get('type');
    meiliParts.push(`type = "${v}"`);
    bitdexClauses.push({ Eq: ['type', { String: v }] });
  }

  // onSite
  if (params.has('onSite')) {
    const v = params.get('onSite') === 'true';
    meiliParts.push(`onSite = ${v}`);
    bitdexClauses.push({ Eq: ['onSite', { Bool: v }] });
  }

  // tagIds (IN filter)
  if (params.has('tagIds')) {
    const ids = params.get('tagIds').split(',').map(Number);
    meiliParts.push(`tagIds IN [${ids.join(', ')}]`);
    bitdexClauses.push({ In: ['tagIds', ids.map(id => ({ Integer: id }))] });
  }

  // userId
  if (params.has('userId')) {
    const v = Number(params.get('userId'));
    meiliParts.push(`userId = ${v}`);
    bitdexClauses.push({ Eq: ['userId', { Integer: v }] });
  }

  // postedTo (mapped string)
  if (params.has('postedTo')) {
    const v = params.get('postedTo');
    meiliParts.push(`postedTo = "${v}"`);
    bitdexClauses.push({ Eq: ['postedTo', { String: v }] });
  }

  // sortAtUnix range filters
  if (params.has('sortAtUnix_lte')) {
    const v = Number(params.get('sortAtUnix_lte'));
    meiliParts.push(`sortAtUnix <= ${v}`);
    bitdexClauses.push({ Lte: ['sortAtUnix', { Integer: v }] });
  }
  if (params.has('sortAtUnix_gte')) {
    const v = Number(params.get('sortAtUnix_gte'));
    meiliParts.push(`sortAtUnix >= ${v}`);
    bitdexClauses.push({ Gte: ['sortAtUnix', { Integer: v }] });
  }

  // id filter
  if (params.has('ids')) {
    const ids = params.get('ids').split(',').map(Number);
    meiliParts.push(`id IN [${ids.join(', ')}]`);
    bitdexClauses.push({ In: ['id', ids.map(id => ({ Integer: id }))] });
  }

  return {
    meiliFilter: meiliParts.join(' AND '),
    bitdexFilters: bitdexClauses,
  };
}

// --- Build sort from query param ---
function buildSort(sortParam) {
  if (!sortParam) return { meiliSort: null, bitdexSort: null };
  const [field, dir] = sortParam.split(':');

  // BitDex uses "sortAt" for the unix timestamp field (mapped from sortAtUnix)
  const bitdexField = field === 'sortAtUnix' ? 'sortAt' : field;

  return {
    meiliSort: `${field}:${dir}`,
    bitdexSort: { field: bitdexField, direction: dir === 'asc' ? 'Asc' : 'Desc' },
  };
}

// --- Compute comparison stats ---
function compare(meiliIds, bitdexIds) {
  const meiliSet = new Set(meiliIds);
  const bitdexSet = new Set(bitdexIds);
  const intersection = meiliIds.filter(id => bitdexSet.has(id));
  const union = new Set([...meiliIds, ...bitdexIds]);

  // Jaccard similarity
  const jaccard = union.size > 0 ? intersection.length / union.size : 1;

  // Order match: of IDs in both sets, are they in the same order?
  const commonInMeili = meiliIds.filter(id => bitdexSet.has(id));
  const commonInBitdex = bitdexIds.filter(id => meiliSet.has(id));
  let orderMatch = 0;
  const minLen = Math.min(commonInMeili.length, commonInBitdex.length);
  for (let i = 0; i < minLen; i++) {
    if (commonInMeili[i] === commonInBitdex[i]) orderMatch++;
  }

  return {
    meili_count: meiliIds.length,
    bitdex_count: bitdexIds.length,
    intersection: intersection.length,
    jaccard: Math.round(jaccard * 1000) / 1000,
    order_match: minLen > 0 ? `${orderMatch}/${minLen}` : 'n/a',
    only_in_meili: meiliIds.filter(id => !bitdexSet.has(id)).slice(0, 10),
    only_in_bitdex: bitdexIds.filter(id => !meiliSet.has(id)).slice(0, 10),
  };
}

// --- HTTP server ---
const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://localhost:${PORT}`);

  if (url.pathname === '/compare') {
    const params = url.searchParams;
    const limit = Number(params.get('limit') || 20);
    const offset = Number(params.get('offset') || 0);
    const sortParam = params.get('sort');

    const { meiliFilter, bitdexFilters } = buildFilters(params);
    const { meiliSort, bitdexSort } = buildSort(sortParam);

    console.log(`\n--- Query ---`);
    console.log(`  filter: ${meiliFilter || '(none)'}`);
    console.log(`  sort: ${sortParam || '(none)'}`);
    console.log(`  limit: ${limit}, offset: ${offset}`);

    const [meiliResult, bitdexResult] = await Promise.all([
      queryMeili(meiliFilter || undefined, meiliSort, limit, offset).catch(e => ({ error: e.message, elapsed_ms: 0 })),
      queryBitdex(bitdexFilters, bitdexSort, limit, offset).catch(e => ({ error: e.message, elapsed_ms: 0 })),
    ]);

    const comparison = (!meiliResult.error && !bitdexResult.error)
      ? compare(meiliResult.ids, bitdexResult.ids)
      : null;

    const response = {
      query: { filter: meiliFilter, sort: sortParam, limit, offset },
      meili: meiliResult,
      bitdex: bitdexResult,
      comparison,
    };

    console.log(`  meili: ${meiliResult.elapsed_ms?.toFixed(1)}ms, ${meiliResult.ids?.length ?? 0} results`);
    console.log(`  bitdex: ${bitdexResult.elapsed_ms?.toFixed(1)}ms, ${bitdexResult.ids?.length ?? 0} results`);
    if (comparison) {
      console.log(`  jaccard: ${comparison.jaccard}, order: ${comparison.order_match}`);
    }

    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(response, null, 2));
    return;
  }

  // Simple help page
  res.writeHead(200, { 'Content-Type': 'text/html' });
  res.end(`<html><body>
    <h2>BitDex vs Meilisearch Comparison</h2>
    <p>Try these queries (adjust sortAtUnix_lte to ~1 month ago for matching data):</p>
    <ul>
      <li><a href="/compare?sort=sortAtUnix:desc&nsfwLevel=1&limit=20">/compare?sort=sortAtUnix:desc&nsfwLevel=1&limit=20</a></li>
      <li><a href="/compare?sort=reactionCount:desc&nsfwLevel=1&type=image&limit=20">/compare?sort=reactionCount:desc&nsfwLevel=1&type=image&limit=20</a></li>
      <li><a href="/compare?sort=sortAtUnix:desc&nsfwLevel=1&limit=20&offset=20">/compare?sort=sortAtUnix:desc&nsfwLevel=1&limit=20&offset=20</a> (page 2)</li>
      <li><a href="/compare?sort=reactionCount:desc&tagIds=6,7&limit=20">/compare?sort=reactionCount:desc&tagIds=6,7&limit=20</a></li>
      <li><a href="/compare?sort=sortAtUnix:desc&userId=1&limit=10">/compare?sort=sortAtUnix:desc&userId=1&limit=10</a></li>
    </ul>
    <p>Params: sort, limit, offset, nsfwLevel, type, onSite, tagIds, userId, postedTo, sortAtUnix_lte, sortAtUnix_gte, ids</p>
  </body></html>`);
});

server.listen(PORT, () => {
  console.log(`Comparison server running at http://localhost:${PORT}`);
  console.log(`BitDex: ${BITDEX_URL} | Meili: ${MEILI_HOST}/${MEILI_INDEX}`);
  console.log(`\nOpen http://localhost:${PORT} for example queries`);
});
