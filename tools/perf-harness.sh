#!/bin/bash
# BitDex Performance Testing Harness
# Usage: bash tools/perf-harness.sh [warmup|bench|all]
#
# Runs a suite of representative queries and reports latency.
# Designed to test bound cache effectiveness.

BITDEX_URL="${BITDEX_URL:-http://localhost:3001}"
INDEX="civitai"
QUERY_URL="$BITDEX_URL/api/indexes/$INDEX/query"
STATS_URL="$BITDEX_URL/api/indexes/$INDEX/stats"

query() {
  local label="$1"
  local body="$2"
  local result
  result=$(curl -s -X POST "$QUERY_URL" -H "Content-Type: application/json" -d "$body" 2>&1)
  local us=$(echo "$result" | python -c "import json,sys; d=json.load(sys.stdin); print(d.get('elapsed_us',0))" 2>/dev/null)
  local total=$(echo "$result" | python -c "import json,sys; d=json.load(sys.stdin); print(d.get('total_matched',0))" 2>/dev/null)
  local ms=$(python -c "print(f'{${us}/1000:.2f}')")
  printf "  %-50s %10s ms  (%s matched)\n" "$label" "$ms" "$total"
}

warmup() {
  echo "=== WARMUP (triggering lazy loads) ==="
  echo ""

  # Warm up all filter fields
  for field in nsfwLevel availability userId type baseModel; do
    curl -s -X POST "$QUERY_URL" -H "Content-Type: application/json" \
      -d "{\"filters\":[{\"Eq\":[\"$field\",{\"Integer\":1}]}],\"limit\":1}" > /dev/null 2>&1
  done

  # Warm up sort fields
  for sort in sortAt reactionCount commentCount collectedCount; do
    curl -s -X POST "$QUERY_URL" -H "Content-Type: application/json" \
      -d "{\"filters\":[{\"Eq\":[\"userId\",{\"Integer\":1}]}],\"sort\":{\"field\":\"$sort\",\"direction\":\"Desc\"},\"limit\":1}" > /dev/null 2>&1
  done

  # Warm up publishedAtUnix (range filter)
  curl -s -X POST "$QUERY_URL" -H "Content-Type: application/json" \
    -d "{\"filters\":[{\"Lte\":[\"publishedAtUnix\",{\"Integer\":2000000000}]}],\"limit\":1}" > /dev/null 2>&1

  # Warm up boolean fields
  for field in hasMeta onSite poi minor; do
    curl -s -X POST "$QUERY_URL" -H "Content-Type: application/json" \
      -d "{\"filters\":[{\"Eq\":[\"$field\",{\"Bool\":true}]}],\"limit\":1}" > /dev/null 2>&1
  done

  echo "  Warmup complete."
  echo ""
}

seed_bounds() {
  echo "=== SEEDING BOUND CACHE ==="
  echo "  Running representative queries to seed bounds..."
  echo ""

  # These queries will be slow on first run but seed the bound cache
  # for subsequent queries with the same filter+sort combos

  # Primary homepage query: nsfwLevel=1, NOT Private, sortAt desc
  curl -s -X POST "$QUERY_URL" -H "Content-Type: application/json" \
    -d '{"filters":[{"Eq":["nsfwLevel",{"Integer":1}]},{"Not":{"Eq":["availability",{"String":"Private"}]}}],"sort":{"field":"sortAt","direction":"Desc"},"limit":10}' > /dev/null 2>&1

  # Same with reactionCount sort
  curl -s -X POST "$QUERY_URL" -H "Content-Type: application/json" \
    -d '{"filters":[{"Eq":["nsfwLevel",{"Integer":1}]},{"Not":{"Eq":["availability",{"String":"Private"}]}}],"sort":{"field":"reactionCount","direction":"Desc"},"limit":10}' > /dev/null 2>&1

  # Same with commentCount sort
  curl -s -X POST "$QUERY_URL" -H "Content-Type: application/json" \
    -d '{"filters":[{"Eq":["nsfwLevel",{"Integer":1}]},{"Not":{"Eq":["availability",{"String":"Private"}]}}],"sort":{"field":"commentCount","direction":"Desc"},"limit":10}' > /dev/null 2>&1

  # Same with collectedCount sort
  curl -s -X POST "$QUERY_URL" -H "Content-Type: application/json" \
    -d '{"filters":[{"Eq":["nsfwLevel",{"Integer":1}]},{"Not":{"Eq":["availability",{"String":"Private"}]}}],"sort":{"field":"collectedCount","direction":"Desc"},"limit":10}' > /dev/null 2>&1

  echo "  Bounds seeded."
  echo ""
}

bench() {
  echo "=== PERFORMANCE BENCHMARK ==="
  echo ""

  # Get stats
  local stats=$(curl -s "$STATS_URL" 2>&1)
  echo "  Index stats:"
  echo "$stats" | python -c "
import json, sys
d = json.load(sys.stdin)
print(f'    Alive: {d[\"alive_count\"]:,}  Slots: {d[\"slot_count\"]:,}  Density: {d[\"alive_count\"]/d[\"slot_count\"]:.1%}')
print(f'    Bound cache: {d[\"bound_cache_entries\"]} entries ({d[\"bound_cache_bytes\"]} bytes)')
print(f'    Meta index: {d[\"meta_index_entries\"]} entries ({d[\"meta_index_bytes\"]} bytes)')
" 2>/dev/null
  echo ""

  echo "--- Filter-only (no sort) ---"
  query "userId=1 (sparse)" \
    '{"filters":[{"Eq":["userId",{"Integer":1}]}],"limit":10}'
  query "nsfwLevel=1 (dense)" \
    '{"filters":[{"Eq":["nsfwLevel",{"Integer":1}]}],"limit":10}'
  query "nsfwLevel=1 + NOT Private" \
    '{"filters":[{"Eq":["nsfwLevel",{"Integer":1}]},{"Not":{"Eq":["availability",{"String":"Private"}]}}],"limit":10}'
  echo ""

  echo "--- Small result set + sort ---"
  query "userId=1, sortAt desc" \
    '{"filters":[{"Eq":["userId",{"Integer":1}]}],"sort":{"field":"sortAt","direction":"Desc"},"limit":10}'
  query "userId=1, reactionCount desc" \
    '{"filters":[{"Eq":["userId",{"Integer":1}]}],"sort":{"field":"reactionCount","direction":"Desc"},"limit":10}'
  query "userId=4 + type=image, sortAt desc" \
    '{"filters":[{"Eq":["userId",{"Integer":4}]},{"Eq":["type",{"String":"image"}]}],"sort":{"field":"sortAt","direction":"Desc"},"limit":10}'
  echo ""

  echo "--- Large result set + sort (bound cache critical) ---"
  query "nsfwLevel=1 + NOT Private, sortAt desc" \
    '{"filters":[{"Eq":["nsfwLevel",{"Integer":1}]},{"Not":{"Eq":["availability",{"String":"Private"}]}}],"sort":{"field":"sortAt","direction":"Desc"},"limit":10}'
  query "nsfwLevel=1 + NOT Private, reactionCount desc" \
    '{"filters":[{"Eq":["nsfwLevel",{"Integer":1}]},{"Not":{"Eq":["availability",{"String":"Private"}]}}],"sort":{"field":"reactionCount","direction":"Desc"},"limit":10}'
  query "nsfwLevel=1 + NOT Private, commentCount desc" \
    '{"filters":[{"Eq":["nsfwLevel",{"Integer":1}]},{"Not":{"Eq":["availability",{"String":"Private"}]}}],"sort":{"field":"commentCount","direction":"Desc"},"limit":10}'
  query "nsfwLevel=1 + NOT Private, collectedCount desc" \
    '{"filters":[{"Eq":["nsfwLevel",{"Integer":1}]},{"Not":{"Eq":["availability",{"String":"Private"}]}}],"sort":{"field":"collectedCount","direction":"Desc"},"limit":10}'
  echo ""

  echo "--- Re-run same queries (should hit bound cache) ---"
  query "nsfwLevel=1 + NOT Private, sortAt desc (2nd)" \
    '{"filters":[{"Eq":["nsfwLevel",{"Integer":1}]},{"Not":{"Eq":["availability",{"String":"Private"}]}}],"sort":{"field":"sortAt","direction":"Desc"},"limit":10}'
  query "nsfwLevel=1 + NOT Private, reactionCount desc (2nd)" \
    '{"filters":[{"Eq":["nsfwLevel",{"Integer":1}]},{"Not":{"Eq":["availability",{"String":"Private"}]}}],"sort":{"field":"reactionCount","direction":"Desc"},"limit":10}'
  echo ""

  echo "--- Pagination (offset) ---"
  query "userId=1, sortAt desc, offset=10" \
    '{"filters":[{"Eq":["userId",{"Integer":1}]}],"sort":{"field":"sortAt","direction":"Desc"},"limit":10,"offset":10}'
  query "nsfwLevel=1 + NOT Private, sortAt desc, offset=100" \
    '{"filters":[{"Eq":["nsfwLevel",{"Integer":1}]},{"Not":{"Eq":["availability",{"String":"Private"}]}}],"sort":{"field":"sortAt","direction":"Desc"},"limit":10,"offset":100}'
  echo ""

  echo "--- Mixed filters ---"
  query "nsfwLevel=1 + type=image + NOT Private, sortAt" \
    '{"filters":[{"Eq":["nsfwLevel",{"Integer":1}]},{"Eq":["type",{"String":"image"}]},{"Not":{"Eq":["availability",{"String":"Private"}]}}],"sort":{"field":"sortAt","direction":"Desc"},"limit":10}'
  query "nsfwLevel=1 + hasMeta + NOT Private, reactionCount" \
    '{"filters":[{"Eq":["nsfwLevel",{"Integer":1}]},{"Eq":["hasMeta",{"Bool":true}]},{"Not":{"Eq":["availability",{"String":"Private"}]}}],"sort":{"field":"reactionCount","direction":"Desc"},"limit":10}'
  echo ""
}

case "${1:-all}" in
  warmup)
    warmup
    ;;
  seed)
    seed_bounds
    ;;
  bench)
    bench
    ;;
  all)
    warmup
    seed_bounds
    bench
    ;;
  *)
    echo "Usage: $0 [warmup|seed|bench|all]"
    exit 1
    ;;
esac
