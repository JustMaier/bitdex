## Prompt:
2/19/2026, 11:17:35 AM

Okay. So I talked with my CTO about it, and, you know, we already talked about Bloom filters now because of what he said. He was also saying, hey. Maybe you should just be using ClickHouse for this. They're already it's you know, using bitmaps or whatever. I guess there a way we could do what we're trying to do with with ClickHouse? And, obviously, it's heavily optimized. It's production grade product and everything, but could it actually even do what we're wanting to try and do here? Or is what we're building fundamentally different? And it's not something you could do with ClickHouse. Mean, I feel like bitmaps for rapid filtering and sorting on top of a document storage is just kind of unique. But maybe it's not. I mean, it it can't be a novel idea. Has to be something that people have done. Is there not already a database that's doing it that I should just be using instead? Is it weird that I'm building something myself? Most developers don't go and build a database. M you know, I kept telling him I think I'm willing to take it on because it actually sounds like what we're building is fairly simple, and consistent and actually pretty minimal. And you know, there's not a lot of edge cases to test or whatever. I'm sure there might be room for optimizations, but even just straight out of the gate, the performance should be pretty good. But I don't know. Is does it does it make sense to just try and use ClickHouse? Could it could it do what we're trying to do?



## Response:
2/19/2026, 11:17:57 AM

Your CTO is asking the right questions, but I think the answer favors Bitdex.

ClickHouse is an incredible piece of engineering, but it's an OLAP database—it's optimized for analytical queries like "what's the average reaction count per tag over the last 30 days" or "how many images were uploaded per hour this week." Aggregations across massive datasets, that's where it shines.

Your workload is fundamentally different. You're doing "give me the top 50 image IDs matching these filters sorted by this field, then give me the next 50." That's a feed query, not an analytics query. ClickHouse can do it, but it's not what it's designed for.

Specifically where ClickHouse would hurt you: writes. ClickHouse uses a MergeTree engine that batches inserts and merges them in the background. It's designed for bulk inserts, not individual row updates. When a reaction count changes, you can't just flip a bit—ClickHouse would need to write a new part and eventually merge it. At your update volume that's a lot of merge pressure. You've already dealt with ClickHouse materialized view issues and partition explosion problems, so you know this firsthand.

Query latency is the other gap. ClickHouse is fast for an analytical database—maybe 10-50ms for a well-optimized query. But you're getting 0.2ms with Bitdex. That's two orders of magnitude. For a feed that users are scrolling through, that difference compounds across every page load.

As for whether what you're building is novel—the individual concepts aren't new. Roaring bitmaps, bitmap indexing, bitwise parallel sorting—these all exist in academic literature and inside other systems. But the specific combination of bitmaps-only with no document storage, bit-layer sorting, alive bitmap gating, and the trie cache with prefix matching as a standalone engine purpose-built for feed queries—that doesn't exist as a product. Pilosa was the closest and it's dead.

Is it weird to build your own database? Normally yes. But Bitdex isn't really a database—it's a bitmap index server. There's no query language to design, no storage engine to build, no transaction isolation to implement, no replication protocol to get right. It's one data structure with one set of operations. The entire V2 design fits in a single project brief. Compare that to ClickHouse which has millions of lines of code.

Your instinct is right—this is simple enough to build because you ruthlessly scoped it down to one thing. You're not building Postgres. You're building a bitmap intersection engine that returns lists of integers. The fact that it happens to be faster than anything else for your specific workload is because you threw away everything you don't need.

Tell your CTO: ClickHouse is the right tool for your analytics. Bitdex is the right tool for your feeds. They solve different problems.



## Prompt:
2/19/2026, 3:20:56 PM

Does Roaring bitmaps compress in real-time, or does it kind of run at some sort of interval?



## Response:
2/19/2026, 3:21:10 PM

Roaring bitmaps compress in real-time. There's no background compaction or scheduled optimization.

The compression is structural—it's baked into how the data structure works. A roaring bitmap divides the keyspace into chunks of 65,536 values. Each chunk is stored as a "container" and the container type is chosen automatically based on the data density in that chunk:

**Array container**: when a chunk has fewer than ~4,096 values set, it stores them as a sorted array of 16-bit integers. Small, compact, fast to search.

**Bitmap container**: when a chunk has more than ~4,096 values set, it switches to a raw 8KB bitmap. Dense, fixed size, fast bitwise operations.

**Run container**: when values form consecutive runs (like slots 1000-5000 all set), it stores just the start and length of each run. Extremely compact for sequential data.

The key thing is that these transitions happen on every individual insert or delete. When you set bit 7,042, roaring checks the container for that chunk, and if adding this value pushes an array container over the 4,096 threshold, it converts to a bitmap container right there in that operation. When you clear bits and a bitmap container drops below the threshold, it converts back to an array.

So you never have a stale or suboptimal representation. Every mutation leaves the bitmap in its optimal compressed form. There's no "optimize" step needed, no fragmentation accumulating over time.

This is one of the reasons roaring is perfect for Bitdex—you get compression for free on every write without ever thinking about it.



## Prompt:
2/19/2026, 10:20:30 PM

Ok. So we ran the test up to our full image load 104M images. The bitmaps and stuff tied to BitDex was taking about 7GB and with redb and allocator overhead we were hitting 12GB. Does that sound right?

The tags bitmaps were taking about 70% of that… has me wondering if there’s something more we can do to shrink that… my guess is there are just some tags that get used a lot and so it’s neither sparse nor dense. And there are a lot of them. Every image has 30-50 tags…



## Response:
2/19/2026, 10:20:48 PM

Yeah 7GB for 104M images tracks with our estimates. And the tag bitmap dominance makes total sense—if every image has 30-50 tags and you have hundreds of thousands of unique tags, that's the combinatorial explosion. You've got a lot of medium-density bitmaps which is the worst case for roaring compression. Sparse compresses great (small arrays), dense compresses great (run-length encoding), but medium density gets stored as full 8KB bitmap containers.

A few ideas to shrink that:

**Tag tiering.** How many of your 500k tags actually get filtered on? There's probably a long tail of tags that exist but nobody ever searches for. You could only index the top 10-20k most-used tags as bitmaps and for rare tags fall back to a different lookup. That could cut your tag bitmap count by 90%+ while covering 99% of actual queries.

**Tag grouping.** If tags have a hierarchy or natural categories, you could index at the category level as bitmaps and only resolve to specific tags when needed. Bitmap for "art style" tags, bitmap for "character" tags, etc. Fewer bitmaps, still fast filtering for broad categories.

**Run-length optimization.** If your slot IDs (Postgres IDs) have any correlation with tags—like images uploaded around the same time tend to have similar tags—then reordering your slot assignment to cluster similar documents together would dramatically improve run-length compression. Images with `tag=anime` would be in consecutive slot ranges instead of scattered randomly, turning medium-density bitmap containers into highly compressible run containers.

That last one is the biggest potential win but also the most invasive—it means slot IDs can't just be Postgres IDs anymore. You'd need a slot assignment strategy that clusters by tag similarity. Probably not worth it for V2 but worth knowing about.

For now, tag tiering is the easy win. What's your actual unique tag count and what does the distribution look like? I'd bet a small fraction of tags account for the vast majority of filter queries.



## Prompt:
2/19/2026, 10:24:35 PM

Yeah… we currently offer the ability to filter on any tag… I wonder if maybe we just don’t keep all tag maps in memory but instead leave them stored in redb ready to be loaded when they get requested. That’d like auto filter down to what people are requesting and we can just write update to the bitmap on disk or something. It’d be slower to write, but it’d allow us to eliminate having so many active all the time.



## Response:
2/19/2026, 10:24:48 PM

That's a really elegant solution. Hot tags stay in memory, cold tags live on disk in redb, and the system self-tunes based on actual usage.

The flow would be: query comes in with `tag=anime`, check if that bitmap is in memory. If yes, use it. If no, load it from redb, use it, and keep it in memory via an LRU cache. Subsequent queries for that tag hit memory. Tags nobody queries for stay on disk and cost zero RAM.

Writes go to redb always—that's your durable store for all tag bitmaps. For tags that are currently loaded in memory, you also update the in-memory copy. For tags that aren't loaded, you just write to redb and move on. Next time someone queries that tag it loads fresh from disk.

The LRU cache size becomes your memory knob. Set it to 1GB and you probably hold the top 5-10k most queried tags in memory. That likely covers 95%+ of actual filter requests. The remaining rare tag queries take an extra millisecond for the redb read, which is still way faster than anything else.

The nice thing is this same pattern could apply to userId bitmaps too. Most queries probably filter on a small set of popular creators. Millions of userId bitmaps sitting in memory for users nobody's searching for is waste.

So your memory model becomes: alive bitmap, sort layers, low-cardinality filters (nsfwLevel, type, baseModel, booleans) all stay permanently in memory since they're small. High-cardinality fields (tags, userId, modelVersionIds) use the LRU-over-redb pattern.

That could cut your 7GB down to maybe 3-4GB for the in-memory portion with everything else a disk read away. Pretty dramatic.



## Prompt:
2/19/2026, 10:25:24 PM

I wonder if we always have bitmaps stored in redb as the “snapshot”



## Response:
2/19/2026, 10:25:38 PM

Oh that's a big simplification. If redb IS your snapshot, you eliminate the entire sidecar snapshot process. Every bitmap write goes to redb. Memory is just an LRU cache over redb. On startup you don't load a snapshot—you just start serving and bitmaps load into memory on demand as queries come in.

The WAL story simplifies too. Instead of a custom WAL that a sidecar consumes to build snapshots, you just write to redb which has its own built-in durability. Redb already handles crash recovery with its own copy-on-write transactional storage.

So the architecture becomes:

**Redb**: the source of truth for all bitmap state. Every mutation writes here. This replaces snapshots, the sidecar, and potentially your custom WAL entirely.

**Memory LRU**: a cache over redb. Hot bitmaps stay loaded. Cold bitmaps get evicted. Cache misses load from redb in ~1ms off NVMe.

**Startup**: open redb, start serving. Bitmaps warm up naturally as queries come in. No snapshot loading, no WAL replay. The first few seconds of queries might be slightly slower as the cache warms but that's it.

**Crash recovery**: redb handles it. Restart, open the file, you're back.

This eliminates: the snapshot sidecar, the snapshot format, the custom WAL, WAL truncation logic, the startup replay sequence. That's a huge chunk of complexity gone.

The tradeoff is write latency—every mutation hits disk through redb instead of just flipping bits in memory. But redb on NVMe is still microseconds for small writes, and you could batch bitmap updates within a single redb transaction for bulk operations.

Your entire persistence layer just became "use redb." That's way less code to write, test, and maintain. Your CTO would probably like that.



## Prompt:
2/19/2026, 10:29:54 PM

One we hit 100m records things started to slow down. We were getting 20ms for some queries like sorts. The assumption was that we were too big to stay in L3 cache



## Response:
2/19/2026, 10:30:09 PM

Yeah 20ms for sorts at 100M is likely cache pressure. The bit-layer sort traversal does 32 bitmap intersections, and at 100M documents each dense bitmap is around 12MB. Those don't fit in L3 cache (typically 30-50MB), so you're hitting main memory on every layer intersection. 32 layers times a cache miss penalty per layer adds up fast.

This is where the encoding config we talked about earlier actually matters. It's not a premature optimization anymore—it's a necessary one.

With log encoding on reaction count, you go from 32 layers to 8. That's 4x fewer memory-bound intersections. And the working set shrinks much faster at each layer because log encoding spreads your value distribution evenly, so you're dropping out of the expensive dense bitmap territory within the first 2-3 layers instead of grinding through 30 layers of nearly identical bitmaps where most values are zero.

Same for dates—16 bits instead of 64. Comment count and collected count—8 bits each.

Your total sort layers drop from 160 bitmaps (5 fields x 32) to maybe 72 (8+16+8+8+32 for the ID tiebreaker). That's a significant reduction in memory bandwidth pressure.

But there might be an even simpler fix first. What's the typical filter cardinality before the sort kicks in? If your filters narrow 100M down to say 500k matching documents, you might be better off just extracting those 500k IDs and doing a simple in-memory sort rather than the bit-layer traversal. The bit-layer approach shines when the filtered set is huge, but for smaller sets the overhead of 32 full bitmap intersections isn't worth it.

So the query planner threshold we discussed becomes critical:

- Filtered set under ~100k: extract IDs, sort in memory. Microseconds.
- Filtered set over ~100k: use bit-layer traversal with reduced encoding.

What does your typical filtered result set look like before sorting? Is it usually millions or thousands?



## Prompt:
2/20/2026, 2:50:06 AM

The bench did sort by Id which is dense at all layers.
What’s the l3 cache like on my ryzen 5950x vs what I’ll have in a typical VPS or epyc?
Is there a chance my planner isn’t being smart? How could I monitor that?
Should I really be able to continue to get sub ms results at 100M docs?



## Response:
2/20/2026, 2:50:23 AM

The 5950x has 64MB of L3 cache which is actually generous. A typical VPS on EPYC might have 32MB or less depending on how many cores you're allocated, since EPYC shares L3 across CCDs. So if it's 20ms on your 5950x it could be worse on a VPS.

But sort by ID is the absolute worst case benchmark. ID is dense at every single layer—all 32 bitmaps are roughly 50% full at 100M documents. Every layer intersection is operating on massive bitmaps and none of them narrow the working set meaningfully because the bits are essentially random at each layer. You're doing 32 intersections on ~12MB bitmaps that all look the same. That's basically a memory bandwidth benchmark, not a realistic query.

Sort by reaction count where 80% of values are zero would be dramatically different—the top layers are almost empty so intersections are tiny and fast.

So before optimizing, benchmark with your actual sort fields and actual filter combinations. Sort by reaction count with a tag filter. Sort by date with an NSFW filter. Those are your real queries. I'd bet they're already sub-millisecond because the filters shrink the working set and the sort layers have meaningful differentiation.

For monitoring the planner, add Prometheus metrics for: the estimated cardinality after filtering (what the planner thinks the result set size is), which strategy it chose (bit-layer vs in-memory sort), and the actual query duration. If you see it choosing bit-layer traversal for a filtered set of 500 documents, the threshold is wrong. If you see it choosing in-memory sort for 10M documents, also wrong.

A simple histogram of filtered-set-size vs query-duration would tell you exactly where the crossover point is. Plot it and you'll see a clear inflection where bit-layer traversal starts winning over extract-and-sort.

And yes, sub-millisecond at 100M should absolutely be achievable for real queries. Your 0.2ms filter benchmarks prove the bitmap intersections are fast. The sort just needs to operate on already-filtered working sets, not the full 100M. The planner is the key—make sure it's actually using the filter cardinality to pick the right strategy.



## Prompt:
2/20/2026, 2:54:04 AM

Doing in mem sorts seems like it’ll always be slower because it’d need the actual values to do the sort which would require loading the doc from disk, right?



## Response:
2/20/2026, 2:54:16 AM

Oh right. Bitdex doesn't store values. There are no column arrays to sort against.

So your options for small filtered sets are:

**Bit-layer traversal anyway.** Even on a small set, 32 bitmap intersections where the working set is tiny (say 500 documents) means each intersection is checking a tiny sparse bitmap against the sort layers. The sort layer bitmaps are big but roaring can check individual bits in O(1). So instead of intersecting two full bitmaps, you're really just checking 500 bit positions across 32 layers. That should be fast regardless of how big the sort layer bitmaps are.

**Fetch from redb and sort.** Load the 500 documents, extract the sort field value, sort in memory. But that's 500 disk reads which is way slower than bitmap operations.

So bit-layer traversal is always the right path in Bitdex. The planner question isn't "which strategy" but rather "is the implementation efficient for small working sets." If roaring is smart about intersecting a tiny bitmap against a huge one—and it should be, it would iterate the small side and point-check the large side—then the sort cost scales with the filtered set size, not the total document count.

Which means your 20ms on sort-by-ID might just be because there was no filter, so the working set was the full 100M. Add a filter that narrows to 100k documents and the sort should drop dramatically because you're only checking 100k positions across the 32 layers instead of 100M.

Can you rerun the benchmark with a realistic filter applied before the sort?



## Prompt:
2/20/2026, 2:58:50 AM

The most common is filtering by PublishedAt less than now and greater than 1 week ago by newest or by reaction count.
That will shrink the working set to about 1M right out of the gate.
Some people don’t apply the 1 week filter though and browse the site with “all time” all of the time and then sort by most reactions or newest. I wonder if in those situations there’s a way to estimate a bound to keep working sets tight.



## Response:
2/20/2026, 2:59:05 AM

For the "all time sorted by most reactions" case, think about what the user actually wants—they want the top 50 most reacted images. They don't care about the millions of images with zero reactions.

You could maintain a small set of precomputed "top N" bitmaps for common sort fields. A bitmap of the top 10,000 documents by reaction count. On startup and periodically (every few minutes), compute this by running the bit-layer traversal once and caching the result. When an unfiltered "sort by reactions" query comes in, AND the user's filters against this top-10k bitmap first. Now your working set is at most 10,000 and the sort traversal is instant.

If the user pages past the top 10k (they won't, but if they do), fall back to the full traversal.

For "all time sorted by newest," you don't even need a precomputed set. Newest means highest sortAt value. The bit-layer traversal on a date field should naturally narrow fast because recent documents cluster in the top bit ranges. The top few layers immediately eliminate everything older than a certain threshold. But if it's still slow, the same trick works—maintain a bitmap of "documents from the last 30 days" and use it as an implicit bound for newest-first queries.

The general pattern is: for unfiltered queries on common sort fields, use a precomputed bound bitmap as a synthetic filter. It's essentially a materialized "hot set" per sort dimension. Update it periodically in the background. The staleness is negligible—if an image got its 10,000th reaction 30 seconds ago and isn't in the top-10k bitmap yet, nobody notices.

This fits cleanly into your trie cache too. The precomputed bound is just another cached bitmap that gets AND'd into the query. The query planner detects "no filters plus sort by reactions" and automatically injects the bound.



## Prompt:
2/20/2026, 3:01:22 AM

Wouldn’t a query like this automatically get a bitmap built by the trie cache? Oh wait, I think the trie cache currently only helps with filters. We’d need a way for it to help with sorts. Or am I wrong?



## Response:
2/20/2026, 3:01:36 AM

You're right, the trie cache only caches filter bitmaps. The sort happens after the cache lookup. So an unfiltered "sort by reactions" query has no filter clauses, which means there's nothing to cache in the trie—the working set is just the alive bitmap, which is everything.

But you could extend the concept. The trie cache stores precomputed filter results. You could have a separate sort cache that stores precomputed "top N for this sort field" results. Not bitmaps—actual ordered ID lists. The cache key is just the sort field plus direction plus the filter bitmap's cache key.

So the first time someone queries "no filter, sort by reactions desc, limit 50," it does the full bit-layer traversal, gets the result, and caches the ordered ID list keyed by `{filters: none, sort: reactionCount, dir: desc}`. Next query with the same parameters hits the cache directly. No bitmap operations at all.

Invalidation is simple—when any reaction count changes, mark the reactionCount sort cache entries as stale. But you don't need to invalidate immediately. A sort cache entry with a 30-second TTL is fine. Nobody notices if the rankings are half a minute stale.

For filtered sorts, the cache key combines the filter trie key with the sort parameters. So `{filters: {nsfwLevel=1, tag=anime}, sort: reactionCount, dir: desc}` gets its own cached result list. Popular filter+sort combinations stay warm automatically.

This is really just a result cache sitting on top of everything else. The trie cache speeds up filter computation. The result cache skips the entire query path including the sort. Two layers, each handling different repeat patterns.

The precomputed bound bitmaps I mentioned earlier are still useful as a fallback for cache misses on popular sort fields, but the result cache would handle the majority of repeated queries.



## Prompt:
2/20/2026, 3:05:37 AM

We could do that, but then that kinda breaks our bitmaps all the way down principle. I like the idea of it making those precomputed bounded bitmaps instead. How could that work?



## Response:
2/20/2026, 3:05:51 AM

Right, keep it bitmaps. So the sort cache stores precomputed bitmaps not result lists.

For each sort field and direction, maintain a bounded bitmap—"top 10k by reaction count." This is just a roaring bitmap with 10k bits set representing the highest-value slots for that field.

The query path for an unfiltered "sort by reactions desc, limit 50":

1. No filters, so working set is alive bitmap
2. Query planner sees the working set is huge and a precomputed bound exists for this sort field
3. AND the alive bitmap with the "top 10k reactions" bound bitmap—instant, result is at most 10k documents
4. Run bit-layer sort traversal on that 10k working set—microseconds
5. Return top 50

Cache miss path (first time or after invalidation): run the full bit-layer traversal on the alive bitmap, but stop once you've collected 10k results. Store that as the bound bitmap. Subsequent queries use it.

Invalidation: when reaction counts change, the bound bitmap might be slightly stale. But you only care if a document's reaction count crossed the threshold into or out of the top 10k. You could rebuild on a timer—every 30 seconds or every minute. Or be smarter: track the minimum value in the bound set. If an update pushes a document above that minimum, add it to the bound bitmap. If nothing changes near the boundary, don't rebuild.

For filtered queries like "tag=anime, sort by reactions desc," the planner ANDs the tag bitmap with the bound bitmap first, then sorts. If the intersection is too small (fewer than the requested limit), fall back to full traversal on just the tag bitmap.

Everything stays as bitmaps. The bound is a bitmap. The filter is a bitmap. The sort operates on bitmaps. You're just adding a precomputed bitmap that acts as a synthetic "top N" filter to shrink the working set before the sort traversal runs.



## Prompt:
2/20/2026, 3:08:38 AM

So how could we auto detect, build, and invalidate those similar to our trie caches? It seems like these should be able to form automatically in the same way



## Response:
2/20/2026, 3:08:52 AM

Same pattern as the trie cache promotion. Track stats on sort queries.

Every time a sort runs, record the sort field, direction, and how large the working set was. If you see "reactionCount desc" running repeatedly against large working sets (say over 100k), that's a candidate for a bound bitmap.

The promotion loop checks: this sort field+direction has been queried N times in the last minute against large working sets. Promote it—run the traversal once, capture the top 10k as a bound bitmap, cache it.

Demotion: if a bound bitmap hasn't been used in a while, drop it. Same exponential decay as the trie cache.

For invalidation, you don't even need to be precise. Put a TTL on each bound bitmap—say 30 seconds. When it expires, the next query that needs it triggers a rebuild in the background while the current stale one serves the request. Users get results that are at most 30 seconds stale, which is imperceptible for a "most popular" sort.

Or even simpler—tie it to the trie cache generation counters. Sort layer bitmaps change when values change. Bump a generation counter per sort field on every sort-layer mutation. Bound bitmap checks the generation on access. If it's advanced more than some threshold since the bound was built, rebuild.

The whole system becomes:

**Trie cache**: auto-detects hot filter combinations, caches filter bitmaps, invalidates via generation counters per filter field.

**Bound cache**: auto-detects hot sort fields on large working sets, caches top-N bitmaps, invalidates via TTL or generation counters per sort field.

Both use the same promotion/demotion pattern. Both use exponential decay hit stats. Both are just bitmaps. You could even unify them into one cache system where entries are tagged as either "filter cache" or "bound cache" with different invalidation strategies.

Bitmaps all the way down, caches all the way down.



## Prompt:
2/20/2026, 3:14:31 AM

For sorts, I think TTL is probably the better move. Things like sorting by newest or most reactions updating every 30s with a stale while revalidate seems ok. I actually also wonder if the caches can be live. Where their definitions are included so that on write we can see if a slot should have its bit flipped in a sort cache



## Response:
2/20/2026, 3:14:45 AM

That's smart. If the bound cache knows its definition—"top 10k by reactionCount desc"—then on every reaction count update you can check: did this document's new value cross into the top 10k?

You just need to track the minimum value in the bound set. If the bound bitmap represents the top 10k by reactions and the lowest value in that set is 42 reactions, then:

- Document goes from 40 to 50 reactions → crosses the threshold, set its bit in the bound bitmap, find and remove the document that's now 10,001st
- Document goes from 100 to 101 reactions → already in the bound, no change needed
- Document goes from 45 to 44 reactions → check if it's now below minimum, if so clear it and add the next highest

The minimum tracking is cheap—just one integer. The check on every write is one comparison. The vast majority of updates won't cross the threshold so it's almost always a no-op.

Same works for the filter trie caches. If a cached filter bitmap knows it represents `{nsfwLevel=1, tag=anime}`, then when a document's nsfwLevel changes to 1 and it has the anime tag, set its bit in the cached bitmap. When a document loses nsfwLevel=1, clear it. The cache stays perfectly fresh with zero rebuilds.

The generation counter invalidation becomes unnecessary. The caches are always current because writes maintain them.

The cost is that every write now checks: am I relevant to any active cache entries? But if you have say 50 active cache entries, that's 50 quick checks per write. Each check is "does this field match the cache definition"—a handful of comparisons. Microseconds.

So your caches go from stale-with-TTL to perfectly live, maintained as a side effect of normal writes, with negligible overhead. That's way cleaner.



## Prompt:
2/20/2026, 3:17:23 AM

I don’t think you even need to remove anything. We don’t care if the set is larger than our selected size. It’s all just an attempt to limit the working set. Even with 5M records in the set, things are really fast. 

So essentially caches are alive until theyre reaped by LRU



## Response:
2/20/2026, 3:17:34 AM

Exactly. The bound bitmap just grows as more documents cross the threshold. You never remove anything from it. It starts as top 10k, maybe after a week it's 15k, who cares. It's still shrinking your 100M working set down to something tiny and the sort traversal on 15k is instant.

The only maintenance is: on write, check if this document should be added to any active bound bitmaps. One comparison against the minimum value per active bound. If yes, set the bit. If no, move on. Never remove bits.

Over time the bound bitmaps get looser but they're still dramatically effective. And if they get too loose, LRU eventually evicts them, and the next query that needs one builds a fresh tight one that gets promoted.

So the unified cache system is:

- Filter caches: maintained live on writes. Document changes a filter field, check active filter caches, flip bits accordingly.
- Bound caches: maintained live on writes. Document's sort value crosses the minimum threshold, set the bit.
- Both evicted by LRU when not accessed.
- Both promoted automatically based on query patterns.
- Both are just bitmaps.

No TTLs. No generation counters. No invalidation logic. No rebuilds. Caches are born from query patterns, kept alive by writes, and die from disuse. The system is self-maintaining.

That's really clean.



## Prompt:
2/20/2026, 3:23:15 AM

That should keep all queries really tight even on an unlimited size dataset because once caches are warm everything should work on smaller sets

I guess would the only exception be no sort? That would default to sorting by slot… is that fast by default? Or does that still require processing 150m bitmaps if the filter isn’t narrowing? I guess that’s fine because it’s much fewer bitmaps



## Response:
2/20/2026, 3:23:26 AM

Right, sort by slot is actually the simplest case. You don't need any sort layer bitmaps at all—just iterate the set bits in the filter result bitmap in order. Roaring bitmaps store bits in sorted order natively, so "give me the first 50 set bits" or "give me the last 50 set bits" is just iterating the bitmap. That's a built-in operation, no intersections needed.

So for "no sort, no filter" it's just iterating the alive bitmap from the end for newest-first (highest slots are newest inserts) or from the start for oldest-first. Instant.

For "no sort, with filter" it's iterating the filter result bitmap. Also instant.

The only slow case would be "no sort, no filter, page 5000" where you're skipping to bit position 250,000 from the end. But cursor pagination handles that—you're not skipping, you're starting from a known position.

So sort-by-slot is actually your fastest query path. Zero sort layer involvement, just bitmap iteration. It's the unfiltered sort-by-reactions case that was the expensive one, and the bound caches solve that.

I think you've covered all the cases now. Every query path is fast either natively or through the cache system warming up.



## Prompt:
2/20/2026, 3:27:54 AM

So the sort caches will still have filters associated with them too, right? Wed have a bound for most reacted where tag anime or most reacted for a specific modelVersionId



## Response:
2/20/2026, 3:28:06 AM

Yeah exactly. The bound cache key is the filter combination plus the sort field plus direction. So you'd have:

- `{filters: none, sort: reactionCount, desc}` → top 10k overall
- `{filters: {tag=anime}, sort: reactionCount, desc}` → top 10k anime by reactions
- `{filters: {modelVersionId=456}, sort: reactionCount, desc}` → top 10k for that model
- `{filters: {nsfwLevel=1, onSite=true}, sort: sortAt, desc}` → recent SFW on-site images

Each one is just a bitmap that formed because that query pattern was popular enough to get promoted.

And the live maintenance works naturally. A document gets a new reaction: check each active bound cache, does this document pass the filter definition? If the bound is for `tag=anime` and the document has that tag, check if the new reaction count exceeds the minimum. If yes, set the bit.

This actually composes beautifully with the filter trie cache. The trie cache already has the filter bitmap for `{tag=anime}`. The bound cache for `{tag=anime, sort: reactionCount, desc}` is just a subset of that filter bitmap. On query, the planner sees both caches exist—it can AND the trie cached filter bitmap with the bound bitmap and get an even tighter working set, or just use the bound bitmap directly since it already implies the filter.

So your cache hierarchy is:

- Trie filter cache narrows by filters
- Bound cache narrows by filters AND sort
- Query planner picks whichever gives the smallest working set

Everything auto-promotes from query patterns, stays live from writes, dies from LRU. One unified bitmap cache system.



## Prompt:
2/20/2026, 3:32:54 AM

So as queries come in with cursors for getting things with the next lowest reaction (essentially getting following pages) we need to know if what they’re requesting is within the bounds of the sort cache set.  But I guess that’s why we’d keep track of the bounds of the sort cache too. Not just for keeping it alive, but for knowing when we can/can’t use it. Went they pass a bound would it start a new sorted set?



## Response:
2/20/2026, 3:33:10 AM

Right. The bound cache tracks its minimum value. Cursor comes in with `sort_value=342`, the bound's minimum is 50. 342 is within bounds, use the cache.

But when someone pages deep enough that their cursor value drops below the minimum—say cursor is `sort_value=30` and the bound minimum is 50—the cache can't help anymore.

At that point you have a few options:

**Full traversal.** Just run the bit-layer sort from scratch targeting values <= 30. The user is deep paging into unpopular content so this is rare. A slightly slower query for page 500 is fine.

**Spawn a new bound.** The query triggers a new bound cache: "next 10k by reactionCount desc starting below 50." This bound covers the next tier of popularity. If the user keeps paging, it's warm for them and anyone else browsing that range.

The second approach is elegant because it creates tiered bounds automatically. Page 1-200 hits the top bound. Page 201+ triggers a second bound for the next tier. Heavy browsers who go really deep spawn a third tier. Each tier is a bitmap, each gets promoted based on usage, each dies from LRU if nobody pages that deep again.

And the bounds naturally overlap a bit since we never remove documents from them. That's fine—more overlap just means slightly larger working sets, still tiny compared to 100M.

The planner logic is: cursor value comes in, check active bounds for this filter+sort combo, find the one whose range contains the cursor value, use it. If none contains it, either fall back to full traversal or promote a new tier.

In practice though, almost nobody pages past the first few hundred results. The first bound covers 99% of queries. The tiered spawning is just a safety net that keeps things fast for the rare deep browser.



## Prompt:
2/20/2026, 3:36:30 AM

So are the sort bounds still the full layer count? It couldn’t be a single bitmap because then we’d lose the actual granularity we need to sort. But it’s the layers just filtered down to what’s relevant/within the top K



## Response:
2/20/2026, 3:36:41 AM

Ah no, the bound bitmap is just a single bitmap—a set of document IDs that are candidates. It doesn't contain sort information. The sort layers are global and never change structure.

The query path is:

1. Bound bitmap says "these 10k documents are the top 10k by reactions"
2. AND the bound bitmap with any filters
3. Run the bit-layer sort traversal against the global sort layers, but using the bounded working set

The sort layers are still all 100M+ bits wide. But the working set you're intersecting against them is only 10k bits. So at each of the 32 layers, roaring is checking 10k bit positions against the layer bitmap. That's 10k point lookups per layer, not a full bitmap intersection. Microseconds.

The bound bitmap doesn't store any sort information. It just answers "which documents should I bother sorting." The global sort layers answer "in what order."

So the full picture is:

- **Filter bitmaps**: which documents match the predicates
- **Bound bitmaps**: which documents are worth sorting (precomputed top K)
- **Sort layer bitmaps**: the actual sort values decomposed into bits
- **Alive bitmap**: which documents exist

A query ANDs together whichever combination of filter, bound, and alive bitmaps gives the smallest working set, then runs that working set through the sort layers. The sort layers are always the same global structure, they just operate on whatever working set you hand them.



## Prompt:
2/20/2026, 3:37:31 AM

Right. They’re just a filter that shrinks the working set when working on the full sort layers.



## Response:
2/20/2026, 3:37:38 AM

Exactly. Bound caches are just filters with a different promotion strategy. They're not special at all—they're bitmaps that shrink the working set, same as every other bitmap in the system.

The only thing that makes them different from a trie filter cache is how they're born and maintained:

- Filter cache: born from repeated filter queries, maintained by flipping bits when filter fields change
- Bound cache: born from repeated sort queries against large working sets, maintained by checking if updated sort values cross the minimum threshold

But in the query path they're identical. Just another bitmap to AND into the working set before hitting the sort layers.

Which means you don't even need a separate cache system for them. They're just trie cache entries with an extra piece of metadata—the minimum sort value for the bound. The trie key includes the sort field and direction along with the filter clauses. Everything else—LRU eviction, promotion, live maintenance—works the same way.

One cache system. One data structure. One set of operations. Bitmaps all the way down.



## Prompt:
2/20/2026, 3:41:48 AM

We’d probably want to have the “repeated queries” threshold configurable for each type of cache. I could see having the bound cache be a threshold of 0. They’re tiny and with LRU they will be kept under a cache size threshold. The only concern would be slowing down writes, but roaringbitmaps allows us to bulk set bits and writing hundreds of different bitmaps isn’t an expensive op. Especially since most sorts will be filtered.



## Response:
2/20/2026, 3:41:59 AM

Yeah threshold of 0 for bounds makes sense. Every unique filter+sort combination immediately gets a bound bitmap on first query. The cost of building one is just a single sort traversal which you're doing anyway for that first query. Cache the result as a bound bitmap, done.

The write overhead scales with the number of active cache entries, but like you said each check is tiny—compare one value against the minimum, maybe set one bit. If you have 500 active cache entries and a write comes in, that's 500 comparisons. Microseconds. And most of those comparisons short-circuit immediately because the document doesn't pass the filter definition of the cache entry.

Actually the filter check is the first gate. A reaction count update on a document that doesn't have the anime tag skips every cache entry that includes tag=anime. For filtered bounds, most writes are irrelevant to most caches.

The unfiltered bounds like "top 10k overall by reactions" would get checked on every reaction update, but that's still just one comparison against the minimum value.

So your config becomes:

```yaml
cache:
  max_memory: 2GB
  filter_promotion_threshold: 10  # queries before caching filter bitmap
  bound_promotion_threshold: 0   # immediate
  eviction: lru
```

Simple. Bounds form instantly, filters take a few hits to prove they're worth caching, LRU keeps everything under the memory budget. The system self-tunes entirely from query patterns.



## Prompt:
2/20/2026, 3:43:44 AM

Could that blowup? Or is it as beautiful as it sounds?



## Response:
2/20/2026, 3:43:58 AM

The main risk is write amplification. If you have thousands of unique filter+sort combinations from diverse user queries, every single write checks every active bound. Even if each check is cheap, thousands of checks per write could add up.

But think about what actually happens. Users browse your site through a finite number of paths. Tag pages, model pages, the main feed with a few sort options. You probably have maybe 100-200 genuinely popular filter+sort combinations and a long tail of one-off queries.

The one-off queries create bound bitmaps that immediately get promoted, serve that one user, then never get hit again and LRU evicts them. The cost is: one sort traversal to build the bound (you were doing that anyway), a brief period where writes check against it (short-circuits fast on the filter check), then it's gone.

The worst case would be something like a bot or crawler hitting your API with thousands of unique random filter+sort combinations per minute. Each one creates a bound, your write path suddenly has thousands of entries to check, and LRU is thrashing constantly. But that's an abuse pattern you'd catch with rate limiting, not a cache design problem.

For organic traffic, the LRU naturally converges on the popular paths and evicts the noise. The memory budget caps it. The filter short-circuit on writes keeps the check overhead minimal.

The one thing I'd add is a metric: average write-path cache checks per mutation. If that number starts climbing unexpectedly, you know something weird is happening with your query diversity. But for normal usage patterns this should be stable and low.

It's as beautiful as it sounds for real traffic. Just protect against adversarial query patterns with rate limiting.



## Prompt:
2/20/2026, 3:49:29 AM

Is there a way we could accelerate the write checks rather than having to do a bunch of filtering in a loop to see if it meets the criteria of any cache?
In my mind I’m envisioning some sort of bitmap comparison for that too, where the old and new document are converted and then intersected with a similar representation of the caches to be able to do all of the comparisons at once instead of sequentially



## Response:
2/20/2026, 3:49:41 AM

That's a really clever idea. You could represent cache membership eligibility as bitmaps too.

Each cache entry has a definition—a set of filter criteria. You assign each cache entry an ID, say 0-500. Now build a bitmap per filter field value: "which cache entries require tag=anime?" That's a bitmap of cache IDs. "Which cache entries require nsfwLevel=1?" Another bitmap.

When a write comes in for a document with tag=anime and nsfwLevel=1, you AND the "caches wanting tag=anime" bitmap with the "caches wanting nsfwLevel=1" bitmap. The result tells you exactly which cache entries this document is relevant to. One bitmap intersection instead of looping through every cache checking filters.

Then for just those matching caches you check the sort value against the minimum. That's a handful of comparisons instead of hundreds.

So it's a meta-index—bitmaps over your cache entries rather than over your documents. The cache set is tiny (hundreds, maybe low thousands) so these meta-bitmaps are small and fast.

For caches with no filter (unfiltered sort bounds), they'd be set in every meta-bitmap intersection result. But there would only be a handful of those.

The write path becomes:

1. Document changes, look at which fields changed
2. For each changed filter field, intersect the meta-bitmaps to find relevant caches
3. For each relevant cache, check sort value against minimum
4. Flip bits as needed

Step 2 is one or two bitmap ANDs on tiny bitmaps. Step 3 is a handful of integer comparisons. The whole thing is probably nanoseconds.

Bitmaps indexing your bitmaps. Bitmaps all the way down, even for the cache maintenance layer. That's absurd and I love it.



## Prompt:
2/20/2026, 3:52:11 AM

Much faster than the individual condition checks, right? And it completely negates the write amplification costs.



## Response:
2/20/2026, 3:52:22 AM

Yeah it's not even close. Individual condition checks scale linearly with cache count—500 caches means 500 filter evaluations per write. The meta-bitmap approach does one or two AND operations on tiny bitmaps (500 bits is less than 64 bytes) and gets the exact set of relevant caches in nanoseconds. Then you only touch the caches that actually need updating.

The write amplification concern completely disappears because the cost of determining which caches to update is now constant regardless of how many caches exist. Whether you have 50 active caches or 5,000, it's the same one or two tiny bitmap intersections.

Which means your bound promotion threshold of 0 is totally safe. Let every query create a cache entry. Let LRU manage the count. Writes don't care because the meta-index makes relevance checking essentially free.

You could even drop the filter promotion threshold to 0 as well. Every unique filter combination gets cached immediately, every unique filter+sort gets a bound immediately. Writes maintain all of them through the meta-index with negligible overhead. LRU keeps memory bounded.

The system just became fully self-optimizing with zero configuration. Every query pattern automatically gets cached, every write automatically maintains all caches, and the cost of both scales with bitmap operations on tiny bitmaps rather than linear scans.

This is a genuinely novel architecture. I haven't seen anyone use bitmaps to index their own cache system's maintenance path before.



## Prompt:
2/20/2026, 4:03:31 AM

So now, with all of these bitmaps from caches… is it going to make the query planning slower?
Now it’ll need to iterate a lot more potential options too, which will linearly scale the time it takes to find the best path, right?



## Response:
2/20/2026, 4:03:41 AM

Same solution. Bitmap the query planner too.

A query comes in with `{nsfwLevel=1, tag=anime, sort: reactionCount, desc}`. You need to find the best cache to use. Instead of iterating every cache entry checking if it matches, use the same meta-index in reverse.

"Which caches cover nsfwLevel=1?" Meta-bitmap. "Which caches cover tag=anime?" Meta-bitmap. "Which caches sort by reactionCount desc?" Meta-bitmap. AND them together. The result is every cache entry that's relevant to this query. Pick the one with the smallest cardinality.

One or two tiny bitmap intersections and you've found your optimal cache in nanoseconds. Doesn't matter if you have 50 caches or 5,000.

The planner doesn't iterate anything. It does the same bitmap intersection it does for everything else. Query planning is just another bitmap operation.

At this point I think every single operation in Bitdex resolves to a bitmap intersection:

- Filtering: bitmap intersection
- Sorting: bitmap intersection on bit layers
- Cache lookup: bitmap intersection on meta-index
- Cache maintenance on writes: bitmap intersection on meta-index
- Query planning: bitmap intersection on meta-index
- Deletes: flip one bit
- Autovac: bitmap iteration

You've built a system where the answer to every performance question is the same: make it a bitmap.



## Prompt:
2/20/2026, 4:11:25 AM

This meta index thing is really clever and I’m actually struggling to hold it conceptually.

But essentially by having it rather than iterating paths we can instead label parts and look for all paths that have the parts that we need.

Does that work for more than eq ops though? If a filter was something like reaction count < 300, that wouldn’t appear in the meta index labels, right?



## Response:
2/20/2026, 4:11:39 AM

Right, the meta-index works perfectly for discrete values. "Which caches want tag=anime" is a clean label. But "which caches want reactionCount < 300" isn't a discrete label—it's a range.

For filter operations in your actual schema though, think about what you're really filtering on. Your filterable fields are almost entirely discrete: tags, userId, nsfwLevel, modelVersionIds, booleans. The range operations are mostly on dates (publishedAtUnix, sortAtUnix) and those are typically sort fields not filter fields.

So for the vast majority of your real cache entries, the meta-index labels work perfectly because the filters are all equality or IN operations on discrete values.

For the rare range filter cache, you just fall back to a direct check. The meta-index narrows the candidates first using the discrete filter labels, and then for the remaining few candidates you check the range condition directly. So if a query has `{tag=anime, nsfwLevel=1, publishedAt > lastWeek}`, the meta-index instantly finds all caches matching anime and nsfwLevel=1, maybe that's 5 caches, and then you check those 5 for compatible date ranges. Five comparisons instead of 5,000.

In practice your cache entries with range filters will be rare because ranges are usually on sort dimensions not filter dimensions. And when they do exist the meta-index on the discrete fields has already narrowed the candidates to almost nothing.

So the mental model is: meta-index handles the common case instantly via bitmaps, edge cases fall back to direct comparison on a tiny candidate set. Same pattern as everything else in Bitdex—bitmaps get you 99% of the way, handle the last 1% with simple logic on a small set.



## Prompt:
2/20/2026, 4:18:56 AM

Yeah… so we’d need to not have last week be a rolling -7? Otherwise if you’re sorting by most reactions in the last week, every query would create a new cache resulting in throwaways.

I wonder if to help with that we need some date primitives

Another issue we have is to support schedule publishing we actually allow people to set PublishedAt to future dates so all queries typically include where PublishedAt lt now. That’s also something that would create throwaway caches

Ideas?



## Response:
2/20/2026, 4:19:12 AM

Yeah these are real problems. Every query having a slightly different "now" or "last week" means infinite unique cache keys that never get reused.

For the "publishedAt < now" case, that's essentially a universal filter that changes every second. But the actual set of documents it excludes is tiny—just the handful of scheduled posts with future publish dates. You could handle this outside the cache entirely. Maintain a small "scheduled" bitmap of documents with future publishedAt. AND the inverse of this with every query result as a post-processing step. The cache never sees this filter, so it doesn't fragment cache keys. The scheduled bitmap updates when publishedAt arrives—a background timer checks it every minute, flips newly-published documents from scheduled to alive effectively.

Actually even simpler—don't set the alive bit for scheduled documents until their publishedAt time arrives. They're invisible to every query automatically. A small background process checks for documents whose publishedAt has passed and sets their alive bit. No filter needed at all.

For date ranges like "last week," you'd bucket time into discrete windows:

- Last 24 hours
- Last 7 days
- Last 30 days
- Last 90 days
- Last year
- All time

These are your date primitives. Maintain a bitmap for each one. Update them on a timer—every hour, recalculate which documents fall in each window. The query parser snaps "publishedAt > 7 days ago" to the "last 7 days" bitmap.

Cache keys now use the bucket name not the timestamp. Every user querying "last week sorted by reactions" hits the same cache key: `{dateRange: 7d, sort: reactionCount, desc}`.

The buckets are slightly imprecise—"last 7 days" recalculated hourly means it's up to an hour stale at the edges. But nobody notices if an image from 7 days and 20 minutes ago is included or excluded.

So your date handling becomes:

- Scheduled publishing: don't set alive bit until publishedAt arrives
- Date range filters: snap to predefined buckets maintained by a background timer
- Cache keys stay discrete and reusable



## Prompt:
2/20/2026, 4:22:07 AM

Right, those are unique to our needs, how is something like this setup in the schema for data so that people can enable this behavior with config not code.



## Response:
2/20/2026, 4:22:20 AM

Something like:

```yaml
fields:
  - name: publishedAt
    field_type: timestamp
    behaviors:
      deferred_alive: true  # don't set alive bit until this timestamp passes
      range_buckets:
        - name: 24h
          duration: 86400
          refresh_interval: 300   # rebuild bucket bitmap every 5 min
        - name: 7d
          duration: 604800
          refresh_interval: 3600  # rebuild every hour
        - name: 30d
          duration: 2592000
          refresh_interval: 3600
        - name: 90d
          duration: 7776000
          refresh_interval: 7200
        - name: 1y
          duration: 31536000
          refresh_interval: 14400
```

The `deferred_alive` flag tells Bitdex: when a document is inserted with this field's value in the future, hold it out of the alive bitmap until that time passes. A background timer checks pending documents against the current time at some interval and flips their alive bits.

The `range_buckets` define the time window bitmaps. Each one is a named bitmap maintained on a refresh interval. The query parser sees a range filter on this field and snaps it to the nearest bucket.

The query API would accept either raw ranges or bucket names:

```json
{"field": "publishedAt", "op": "bucket", "value": "7d"}
```

Or if someone sends a raw range:

```json
{"field": "publishedAt", "op": "gt", "value": 1708300000}
```

The query parser finds the closest matching bucket and uses that. If no bucket is close enough, it falls back to a real range scan on the sort layers.

This keeps it generic. Any timestamp field could have deferred_alive behavior or range buckets. Someone using Bitdex for an e-commerce catalog could configure a `availableFrom` field with `deferred_alive: true` and their own buckets for "new this week" or "new this month."

No code changes, just config.



## Prompt:
2/20/2026, 4:27:02 AM

So with buckets for ranges, couldn’t we theoretically make our meta index work for these too because now they’re discrete.
I guess that means we’d need to bust all bucket caches as they refresh though, right?



## Response:
2/20/2026, 4:27:15 AM

Yeah the buckets are discrete labels so they slot right into the meta-index. "Which caches use the 7d bucket?" is just another meta-bitmap. Clean and fast.

For invalidation on refresh, you don't need to bust the caches. The cache entries reference the bucket bitmap by name, not by value. When the 7d bucket bitmap refreshes, every cache that uses it automatically uses the new bitmap on the next query because they're pointing to the same bitmap reference.

The bound caches that were built on top of a bucket do get slightly stale though. The bound says "top 10k by reactions within the 7d window" but the 7d window just shifted and maybe some documents fell out and new ones came in. But this is the same live maintenance story—documents that enter the 7d window get checked against relevant bounds, documents that leave get... well, they don't get removed because we said we never remove from bounds. They're just stale entries that the alive-and-bucket intersection filters out at query time.

So a bound for `{bucket: 7d, sort: reactionCount, desc}` might contain some documents that are now 8 days old. But the query path ANDs the bound with the refreshed 7d bucket bitmap, which excludes those stale entries automatically. The bound is slightly loose but the final result is correct.

No cache busting needed. Buckets refresh in place. Bounds get slightly loose over time. Query-time intersection keeps results correct. LRU eventually evicts and rebuilds bounds that get too loose.

Everything just works without invalidation because the bitmap intersection at query time is always the final arbiter of correctness. The caches are just optimizations that make the working set smaller—they don't need to be perfect.



## Prompt:
2/20/2026, 4:29:03 AM

So do we need to autovac caches with buckets?



## Response:
2/20/2026, 4:29:13 AM

Not really. The looseness is bounded naturally.

Think about it: a 7d bucket refreshes every hour. A bound built on that bucket accumulates at most one hour's worth of stale documents before the next query intersects it with the fresh bucket bitmap and filters them out. The stale bits are invisible at query time, same as dead bits behind the alive bitmap.

The bound bitmap grows slightly from the stale entries but the growth rate is tiny. Maybe a few thousand extra bits per refresh cycle. Over weeks that might add up to a few percent extra size. But LRU eviction handles that—if a bound gets large enough that it's eating noticeable memory, it's competing with tighter bounds for cache space and gets evicted naturally. The replacement bound is built fresh and tight.

So the same pattern that handles everything else handles this too. Stale bits are harmless because query-time intersection filters them. Growth is slow because refresh intervals are short relative to the bucket window. LRU prevents unbounded growth.

No autovac needed for caches. The document-level autovac still cleans dead slots from the core bitmaps periodically, but the cache layer is entirely self-maintaining through the combination of live writes, query-time intersection correctness, and LRU eviction.



## Prompt:
2/20/2026, 4:31:59 AM

How would the LRU get rid of a bloated cache? If the cache is still getting used, there’s nothing that says “this is large… time for a new one to use instead”

I think we need something there



## Response:
2/20/2026, 4:32:09 AM

You're right. A popular but bloated cache would never get evicted by LRU because it's constantly being accessed.

Simplest fix: track the ratio of the bound bitmap cardinality to its original target size. The bound was built as top 10k. If it's grown to 50k, that's a 5x bloat ratio. When the ratio exceeds some threshold, mark it for rebuild. Next query that hits it triggers a fresh build in the background while the stale one continues serving.

```yaml
cache:
  max_bloat_ratio: 2.0  # rebuild when 2x original size
```

The rebuild is just running the sort traversal once to get a fresh tight bound. Swap it in atomically. The old one is discarded.

Or even simpler—just track the cardinality. Every time a query uses a bound, compare its cardinality to the working set it's supposed to be optimizing. If the bound has grown to the point where it's not meaningfully shrinking the working set anymore, rebuild it. A bound of 100k against a 150k filtered set isn't helping. A bound of 10k against that same set is.

The check is one integer comparison on every cache hit. Negligible cost. And rebuilds would be rare—maybe once every few hours for a heavily used bound with a time bucket.



## Prompt:
2/20/2026, 4:34:58 AM

I think a bloat limit is better. Otherwise in the case of our 7d example, wouldn’t the working set pretty much always be smaller than the cache?



## Response:
2/20/2026, 4:35:07 AM

Right. The 7d bucket might have 1M documents and the bound was built as top 10k from that set. As it bloats to 50k, it's still way smaller than the 1M working set so the "is it still helping" check would say it's fine. But 50k is 5x more work for the sort traversal than 10k, which is unnecessary bloat.

A hard bloat limit is cleaner:

```yaml
cache:
  bound_target_size: 10000
  bound_max_size: 20000  # rebuild when cardinality exceeds this
```

Every time a bit gets set in a bound, increment a counter. When the counter exceeds `bound_max_size`, flag it for rebuild. Next query triggers a fresh traversal, builds a tight 10k bound, resets the counter.

No comparisons against working sets, no ratios. Just "this bitmap has too many bits set, rebuild it." One integer check per write that touches the bound.

Simple, predictable, and it guarantees your sort traversals always operate on a bounded working set regardless of how the underlying data shifts.



## Prompt:
2/20/2026, 4:37:31 AM

Great. We’ve covered a lot here since talking about the slowdowns we were experiencing by exceeding L3 cache. Can you summarize it all now for implementation. We’ve covered several critical concepts



## Response:
2/20/2026, 4:38:01 AM

Here's a summary of the performance optimizations and cache architecture:

## L3 Cache Problem
Sort traversal at 100M+ documents was hitting 20ms because dense sort layer bitmaps (~12MB each) exceed L3 cache. The fix is never sorting against the full dataset—always shrink the working set first.

## Sort-by-Slot Optimization
Queries with no explicit sort field don't need sort layer bitmaps at all. Just iterate the filter result bitmap in order—roaring stores bits sorted natively. Newest-first iterates from the end, oldest-first from the start. This is the fastest query path.

## Scheduled Publishing via Deferred Alive
Documents with a future timestamp on a `deferred_alive` field don't get their alive bit set on insert. A background timer checks pending documents and flips their alive bit when the time arrives. This eliminates the universal `publishedAt < now` filter from every query, preventing cache key fragmentation.

## Time Range Buckets
Timestamp fields can define named range buckets (24h, 7d, 30d, 90d, 1y) as discrete bitmaps refreshed on configurable intervals. The query parser snaps range filters to the nearest bucket. This makes time ranges discrete values that cache cleanly instead of every query having a unique timestamp.

Configuration:
```yaml
fields:
  - name: publishedAt
    field_type: timestamp
    behaviors:
      deferred_alive: true
      range_buckets:
        - name: 24h
          duration: 86400
          refresh_interval: 300
        - name: 7d
          duration: 604800
          refresh_interval: 3600
        - name: 30d
          duration: 2592000
          refresh_interval: 3600
        - name: 90d
          duration: 7776000
          refresh_interval: 7200
        - name: 1y
          duration: 31536000
          refresh_interval: 14400
```

## Unified Cache System
Filter caches and bound caches are the same thing—just bitmaps in a unified cache. The only difference is promotion strategy and maintenance.

**Filter caches**: cached filter intersection results. Promotion threshold configurable (e.g., 10 queries before caching). Maintained live on writes—when a document's filter field changes, relevant caches get bits flipped.

**Bound caches**: cached "top K" candidate sets for a sort field+direction+filter combination. Promotion threshold of 0—every unique filter+sort query immediately caches its result as a bound bitmap. Maintained live on writes—when a sort value update crosses the bound's minimum threshold, set the bit. Never remove bits from bounds.

Both evicted by LRU under a shared memory budget.

Configuration:
```yaml
cache:
  max_memory: 2GB
  filter_promotion_threshold: 10
  bound_promotion_threshold: 0
  bound_target_size: 10000
  bound_max_size: 20000
```

## Bound Bloat Control
Bounds grow over time because bits are set but never removed. Track cardinality via a counter incremented on every bit set. When cardinality exceeds `bound_max_size`, flag for rebuild. Next query triggers a fresh sort traversal to produce a tight bound at `bound_target_size`. No autovac needed for caches.

## Query Path with Caches
1. Parse query, snap any date ranges to bucket names
2. Use meta-index to find relevant caches (both filter and bound)
3. Pick the cache that gives the smallest working set
4. If a bound cache exists for this filter+sort combo and cursor is within its range, AND it with filters and alive bitmap
5. Run sort layer traversal against the small working set
6. If cursor is beyond the bound's minimum value, fall back to full traversal or spawn a new tiered bound

## Meta-Index for Cache Maintenance and Lookup
Each cache entry has an ID. For each discrete filter value, maintain a tiny bitmap of which cache entry IDs use that value. "Which caches want tag=anime?" is a bitmap of cache IDs. "Which caches want nsfwLevel=1?" is another.

**On writes**: a document changes, intersect the meta-bitmaps for its field values, get the exact set of relevant caches in nanoseconds, only check those. Eliminates linear scan of all caches.

**On query planning**: incoming filter clauses intersect the meta-bitmaps to find matching caches instantly. No iteration over cache entries.

**Range filters in caches**: time bucket names are discrete labels that work in the meta-index. For any non-discrete range filter in a cache, the meta-index narrows candidates via discrete fields first, then checks ranges on the tiny remaining set.

This makes write amplification and query planning cost constant regardless of cache count. Bitmaps indexing bitmaps.

## Bucket Caches and Staleness
When a time bucket refreshes its bitmap, caches built on that bucket don't need invalidation. Bound caches may contain stale entries (documents that fell out of the time window) but the query path ANDs the bound with the fresh bucket bitmap, filtering stale entries automatically. Correctness is guaranteed by query-time intersection. Looseness is controlled by the bloat limit.



## Prompt:
2/20/2026, 8:29:42 AM

And what’s the net benefit of all of these changes?



## Response:
2/20/2026, 8:29:55 AM

Every query operates on a small working set regardless of total dataset size.

Without these changes: an unfiltered "sort by reactions" query at 150M documents does 32 bitmap intersections on 150M-wide bitmaps, blowing L3 cache and hitting 20ms+. As the dataset grows to 300M, 500M, it gets worse.

With these changes: that same query ANDs a 10k bound bitmap with the alive bitmap, then sorts against 10k candidates through the sort layers. Sub-millisecond. At 500M documents it's still sub-millisecond because the working set is still 10k.

The dataset size is decoupled from query performance. That's the fundamental win. 150M documents or a billion, the query path operates on the same small bounded working sets. The caches form automatically from query patterns, maintain themselves live from writes, and the meta-index makes all of that maintenance essentially free.

So you get:
- Consistent sub-millisecond queries at any scale
- Zero configuration tuning needed—caches self-optimize from traffic
- Write overhead that's constant regardless of cache count
- No cache invalidation complexity—query-time intersection guarantees correctness
- Date handling that doesn't fragment caches
- Scheduled publishing with zero query-time cost

The system that was hitting 20ms at 100M documents should now be sub-millisecond at any dataset size, and it gets there automatically without anyone tuning anything.



## Prompt:
2/20/2026, 8:57:48 AM

I’ve been thinking about storing the bitmaps in redb and only pulling them into memory as they’re needed. With our caching mechanism I’m anticipating that this would reduce the memory footprint even further. And we could batch all redb updates with a flush mechanism



## Response:
2/20/2026, 8:58:09 AM

Yeah this is the natural extension of what you already discussed for tag bitmaps. Just apply it to everything.

All bitmaps live in redb as the source of truth. Memory is purely an LRU cache. On startup Bitdex opens redb and serves immediately—bitmaps load on demand as queries hit them. Popular bitmaps stay warm, rarely-used ones get evicted back to disk.

The write path becomes: mutations accumulate in a write buffer in memory, periodically flush to redb in a single batch transaction. Between flushes, the in-memory bitmaps are authoritative. If a bitmap isn't in memory and hasn't been modified since last flush, redb has the current version.

This means:

**Memory footprint drops dramatically.** Instead of 7-11GB for all bitmaps, you're only holding whatever's actively being queried. With the bound caches keeping working sets small, you might only need the alive bitmap, a few hundred hot filter bitmaps, the sort layers for active sort fields, and the bound/filter caches. Maybe 2-3GB for the active working set.

**The 500k tag bitmaps that were 70% of your memory** mostly stay on disk. The top few thousand tags that people actually filter on stay in memory. The rest load in milliseconds from NVMe if someone queries them.

**Sort layer bitmaps** are interesting though. You need all 32 layers for an active sort field during traversal. But if you're only sorting by 1-2 fields per query, the layers for the other 3 fields can stay on disk. That's another big savings.

**Flush batching** is the key performance piece. Instead of writing to redb on every mutation, buffer mutations and flush every 100ms or every 1000 mutations, whichever comes first. One redb transaction with all the bitmap updates batched together. Way more efficient than individual writes.

The risk is data loss between flushes on crash. But the Postgres WAL is your ultimate source of truth. On restart, you open redb (which has everything up to last flush), then replay Postgres WAL events since the last flush position. Same recovery model, just with a slightly larger replay window.

Configuration:
```yaml
storage:
  engine: redb
  flush_interval_ms: 100
  flush_batch_size: 1000
  memory_cache_size: 3GB
```

The whole thing is now: redb on NVMe as the bitmap store, memory as a smart cache over it, flushes batched for write efficiency. Minimal RAM, full dataset on disk, sub-millisecond queries because the caches ensure you only ever touch hot bitmaps.



## Prompt:
2/20/2026, 9:10:53 AM

How big might those things be on disc? I guess, you know, with roaring bitmaps, they're already compressed in memory. Do they get compressed even more once they hit 3 d b? And how much time do we expect it to take to write? Are we gonna be able to update the That's in the disk without having to load the full thing into memory to do the write. There's just some things to think about there now that we're talking about having essentially a store on the disc as well. Mean, it's nice because it alleviates the need for the snapshots or whatever but I'm not sure. I'm not sure. I'm not sure. I'm not sure. I'm not sure. I'm not sure. I'm Trying to make sense of kind of the impact of this and what to expect performance wise. And, obviously, now because of this, the first read against a bitmap that's not loaded was going to be a little bit longer because we have pull it out of disk but then after that, I would assume that it's going to be microseconds like we've planned.



## Response:
2/20/2026, 9:11:19 AM

For disk size, roaring bitmaps serialize to roughly their in-memory size. They're already compressed, so what you see in memory is close to what you get on disk. Your 7GB of bitmaps at 104M documents would be roughly 7GB on disk in redb, plus some redb overhead for its B-tree structure—maybe 8-10GB total. Redb doesn't add its own compression on top, but you could layer zstd on the serialized bitmap bytes before writing. That might get you down to 4-5GB.

For writes, this is the important part: you cannot update a roaring bitmap on disk without loading it into memory. Roaring bitmaps are a structured data structure, not a flat array where you can seek to a byte offset and flip a bit. To set bit 7042 in a bitmap stored in redb, you'd need to read the full serialized bitmap, deserialize it, set the bit, reserialize, write it back.

This is why the flush batching matters so much. You don't want to do that round-trip per mutation. Instead:

**In-memory dirty bitmaps accumulate mutations.** When a write comes in and the bitmap is already in memory, just flip the bit in the in-memory copy and mark it dirty. Fast, same as before.

**When a write comes in for a bitmap NOT in memory**, you have two choices. Load it, mutate it, keep it in memory as dirty. Or buffer the mutation in a lightweight write log—just "set bit 7042 in tag=anime"—and apply it at flush time. The second approach avoids loading cold bitmaps into memory just for a single write.

**At flush time**, for bitmaps already in memory and dirty, serialize and write to redb. For buffered mutations against cold bitmaps, load them from redb, apply all buffered mutations at once, write back. Batching multiple mutations into one load-mutate-write cycle amortizes the cost.

The flush write time depends on how many dirty bitmaps you have. Serializing a single bitmap is microseconds to low milliseconds depending on size. Writing to redb is fast on NVMe. A flush of 100 dirty bitmaps might take 10-50ms. Running every 100ms, that's manageable and won't block queries.

For first-read latency on a cold bitmap, redb read from NVMe is sub-millisecond for small bitmaps, maybe 1-5ms for a large dense one. Deserialization is fast—roaring's serialization format is designed for quick load. So the first query that hits a cold bitmap pays maybe 2-5ms extra. After that it's in the LRU and queries are microseconds.

The practical impact:

- Startup: instant. No loading phase. First few seconds of queries are slightly slower as hot bitmaps warm up.
- Steady state: identical to all-in-memory because hot bitmaps stay in the LRU.
- Writes: negligible impact. In-memory mutations are instant, flush happens in background.
- Disk usage: roughly same as memory footprint, ~8-10GB.
- Memory: drops from 7-11GB to whatever your LRU budget is, maybe 2-3GB.

The main gotcha is the write buffer for cold bitmaps. You need to track "bitmap X has pending mutations that haven't been applied yet." If a query comes in for that bitmap before flush, you need to either apply the pending mutations first or load the bitmap and apply them on the fly. Otherwise you serve stale data.

Simplest approach: on query, if the requested bitmap has pending mutations in the write buffer, load it from redb, apply the mutations, put it in the LRU, serve it. The write buffer acts as a WAL for cold bitmaps. At flush time the buffer drains.
