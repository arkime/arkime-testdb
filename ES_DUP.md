# Notes on Building an Elasticsearch 7.10 Drop-in

These are pragmatic, hard-won learnings from implementing enough of the
Elasticsearch 7.10.x HTTP surface to run **Arkime** (`db.pl init`, the
viewer's `/api/sessions`, Cont3xt history, etc.) against a single-node
Rust backend (`arkimedb`) sitting on top of `redb` + roaring bitmaps.
"ES" below means Elasticsearch unless otherwise noted.

The goal of this document is so that the next person (or the next refactor)
doesn't relearn each landmine the hard way.

---

## 0. Strategy

* **Be a duck**. Real clients don't care what's underneath as long as the
  responses look right. Implement endpoints driven by client failures —
  not by the ES reference docs top-down.
* Keep an Arkime checkout next to the project; grep `db/db.pl`,
  `viewer/arkimeparser.jison`, and the viewer source whenever a request
  shape is unclear. They are the spec for "what the client actually sends."
* Log every unrecognized query clause / 4xx / 5xx with the raw body —
  that one log line is worth ten guesses.

---

## 1. Version / handshake

* `GET /` must return `version.number` ≥ 7.10.0 or Arkime refuses to start
  with `Currently using Elasticsearch version X which isn't supported`.
  We return `7.10.10`.
* `GET /_cluster/health`, `GET /_nodes`, `GET /_cluster/state`,
  `GET /_cat/indices`, `GET /_cat/templates`, `GET /_cat/aliases`,
  `GET /_cat/health` all need plausible shapes — Arkime's bootstrap
  pings several of them.
* `Content-Type: application/json` everywhere; Arkime's elasticsearch6.js
  client is picky.
* `Accept: */*` is fine but watch for `?format=json` on `_cat/*`.

---

## 2. Indices, templates, aliases

### Index lifecycle
* `PUT /:idx` — create with optional `{settings, mappings, aliases}`.
* `DELETE /:idx` — multi-index delete (commas, wildcards). Be careful to
  resolve aliases first.
* `POST /:idx/_close` / `_open` — return `{acknowledged:true}`.
* Alias resolution must happen at *every* read/write entry point.
  Arkime points at aliases like `tests_sessions3` that map to many
  daily indices `tests_sessions3-YYMMDD`.

### Templates (legacy `_template`)
This is the single most subtle part of the whole compatibility layer.

* Templates have an integer `order` (default 0). When **multiple**
  templates' `index_patterns` match a new index, ES applies them in
  ascending order of `order` — so the **highest-order** template wins
  for any conflicting key. **Apply higher-order templates _first_** in
  your code so a "first-wins" merge gives the same end-state.
* `dynamic_templates` from all matching templates are concatenated. When
  matching a new field, ES walks the merged list in order and picks the
  first match — which means, again, the *higher-order* template's dts
  must come first in your merged list, otherwise its matches will be
  preempted by a lower-order catch-all like
  `match_mapping_type: string → keyword`.
* Real bug we hit: ECS template (`order:1`,
  `strings_as_keyword: string→keyword`) was being merged before the
  Arkime template (`order:99`, `*Ip→ip`). Result: every new IP field
  including `dns.mailserverIp` got typed as keyword and IP queries
  returned 0 hits. Fix: sort matching templates by `order` DESC before
  applying.
* `_index_template` (composable) wraps everything under
  `template:{settings, mappings, aliases}`. Old `_template` puts them at
  top level. Accept both.
* `index_patterns` may be a string OR an array of strings.
* When you persist a template, store the raw bytes, not your parsed
  representation — that way you don't lose unknown keys when re-emitting.

### Dynamic templates entry shape
```
{ "template_word_split":
    { "match":"*Tokens", "mapping": { "type":"text", "analyzer":"wordSplit", "norms":false } } }
```
* The outer key is just a name. The inner has zero or more of:
  `match`, `path_match`, `unmatch`, `path_unmatch`,
  `match_mapping_type`, `mapping`. We OR them together (any matches → match).
* `match` checks the **leaf** field name; `path_match` checks the full
  dotted path. Both use ES-style globs (`*` matches across `.`).
* `match_mapping_type` values we observed: `"string"`, `"long"`,
  `"double"`, `"boolean"`, `"object"`.

### Aliases
* Arkime hits `_aliases` (list & batch update), `/:idx/_alias/:name`,
  `/:idx/_aliases/:name` (PUT/DELETE), `/_alias`, `/_alias/:name`.
* `_aliases` POST takes `{actions:[{add:{index,alias}},{remove:{...}}]}`.
* Some operations apply to alias targets (deletes, updates, searches,
  counts) — others just to the alias name (puts of mapping).

---

## 3. Mappings & field types

### Type mapping (ES → ours)
| ES                        | Internal `FieldType` |
| ---                       | ---                  |
| `keyword`, `constant_keyword` | Keyword          |
| `text`                    | Text                 |
| `ip`                      | Ip                   |
| `long`, `integer`, `short`, `byte` | I64         |
| `unsigned_long`           | U64                  |
| `float`, `double`, `half_float`, `scaled_float` | F64 |
| `boolean`                 | Bool                 |
| `date`, `date_nanos`      | Timestamp            |
| `object`, `nested`, `flattened` | Json           |

### Flattening
* ES *displays* nested objects as nested in `_source` but *indexes*
  them under dotted paths (`dns.host`, `source.ip`, …). Your indexer
  must walk objects and join with `.`. Keep `_source` byte-faithful for
  hit hydration; index a flattened view for queries.
* Arrays of objects: walk into each element with the same path. Arrays
  of scalars: index all elements as separate postings under the same
  field.

### `copy_to`
* `"dns.host": { "type":"keyword", "copy_to":["dns.hostTokens"] }`.
* At index time: any string written to the source field is also
  indexed (after the target's analysis) under each target.
* Round-trip `copy_to` in `_mapping` output. Accept both string and
  array forms on input.

### Text analysis (`wordSplit`)
* Arkime's custom `wordSplit` analyzer = `pattern` tokenizer (default
  `\W+`) + `lowercase` filter. We implemented one tokenizer
  (`tokenize_text`) that splits on non-(alnum|`_`) and lowercases per
  char. Use it both at index time (Text field) and at query time
  (`match`/`match_phrase`).
* `match`/`match_phrase` = AND of per-token Term queries. We don't yet
  enforce phrase order; suffices for Arkime's typical single-token
  hostname queries. (Backlog: positional postings.)

### Dynamic field discovery
* When a doc carries a field not in the schema, run:
  `schema → dynamic_templates → name-based fallback`. Cache the result
  in the schema so subsequent docs/queries skip the lookup.
* **Persist** dynamically-discovered fields. Don't only persist on
  explicit mapping PUTs. Otherwise on restart you lose every type you
  inferred and the next ingest re-infers from possibly-different first
  values.

---

## 4. Search request quirks

ES is generous about input shapes; clients depend on that generosity.
Be lenient on the way in, strict on the way out.

* `size` and `from` come as numbers OR numeric strings. Use a custom
  deserializer.
* `sort` may be:
  * a string: `"firstPacket"`,
  * a single object: `{"firstPacket":"asc"}` or `{"firstPacket":{"order":"asc","missing":"_last"}}`,
  * an array of either,
  * an empty object `{}` (silently ignore),
  * `null`.
* `_source` may be a bool, a string, an array of strings, or an
  object `{includes:[], excludes:[]}`.
* `fields` is an array — entries can be strings or objects with
  `field` + `format`. We just take the field names.
* `track_total_hits`: `true`, `false`, or an integer cap. We treat
  anything truthy as exact and `false` as "best-effort, may be capped"
  (still room to optimize that path).
* `rest_total_hits_as_int=true` (query param): emit `hits.total` as a
  raw integer, not the 7.x `{value, relation}` object. Arkime sets this.
* `?preference=primaries` and `?ignore_unavailable=true`: accept,
  ignore.

### Hits shape
```json
{
  "took": 0,
  "timed_out": false,
  "_shards": {"total":1,"successful":1,"skipped":0,"failed":0},
  "hits": {
     "total": 3,                 // or {value,relation} unless rest_total_hits_as_int
     "max_score": null,
     "hits": [{"_index":...,"_id":...,"_score":null,"_source":{...},"fields":{...}}]
  }
}
```
* When `_source:false`, omit `_source` (don't emit `null`).
* `fields` projection always wraps each value in an array — even
  scalars. (Arkime knows this and unpacks `[0]`.)

---

## 5. Query DSL — what we've actually implemented

* `bool` with `must`, `should`, `must_not`, `filter`,
  `minimum_should_match` (numeric only — the ES string forms like
  `"75%"` are uncommon in Arkime's traffic).
* `term`, `terms`, `match`, `match_phrase` — note Text-field tokenization
  branch.
* `range` — `gt`/`gte`/`lt`/`lte`. Date strings need granularity-aware
  rounding (see §7).
* `exists`. Maintain a per-field "exists" bitmap at index time.
* `prefix` → translate to a `wildcard` with the literal value escaped
  (`*`, `?`, `\`) and a trailing `*`.
* `wildcard` — ES anchors implicitly; our matcher walks the per-field
  values list and uses a tiny glob matcher.
* `regexp` — ES anchors implicitly (add `^…$`). Use a Rust regex
  flavor; turn ES character class quirks into Rust ones.
* `ids` — translate to `Term` on `_id`.
* `constant_score` / `filter` — unwrap and recurse on inner.
* CIDR: the ES `term` query on an `ip` field accepts `"a.b.c.d/n"` and
  internally converts to a range over the IPv6-mapped `u128`. We
  detect the `/` and emit a `Cidr` op.

When you don't recognize a clause, return 400 with the clause name in
the body — invaluable for adding the next one.

---

## 6. Aggregations

Arkime uses (so far): `terms`, `cardinality`, `stats`,
`date_histogram`, `histogram`, `filters`, plus nested sub-aggs.
Implement compile + run as two phases over a result bitmap. `min_doc_count`
and `size` matter; `order` (`_count`/`_term`/sub-agg metric) matters.

---

## 7. Dates

* Accept `epoch_millis`, `epoch_second`, ISO 8601 with or without TZ,
  date-only, and ES "date math" expressions like `now-1h/d`.
* **Granularity rounding** — the most overlooked bit. `lte` of a
  second-precision string covers the *whole second* (`.999`):
  | Input            | gran (ms) |
  | ---              | ---       |
  | `2014-02-26`     | 86_400_000 |
  | `2014-02-26T10`  | 3_600_000  |
  | `2014-02-26T10:27` | 60_000   |
  | `2014-02-26T10:27:57` | 1_000 |
  | `2014-02-26T10:27:57.123` | 1 |
  Apply `(gran-1)` to upper bounds (`lt`, `lte`); `gte`/`gt` use the raw
  ms. Mismatched rounding silently drops valid hits and you'll lose
  hours debugging.
* Always honor the offset in the input. `2014-02-26T10:27:57-05:00` is
  not the same epoch as `2014-02-26T10:27:57Z`.

---

## 8. CRUD endpoints

* `POST /:idx/_doc` (auto id), `PUT /:idx/_doc/:id`,
  `PUT /:idx/_create/:id`, `POST /:idx/_update/:id`,
  `DELETE /:idx/_doc/:id`.
* **`?op_type=create`** on `PUT /:idx/_doc/:id` must route to the create
  path (fail if exists). Same for `op_type:"create"` inside a bulk
  `index` action header.
* `_update` body shapes:
  * `{doc:{...}}` — partial merge.
  * `{doc:{...}, doc_as_upsert:true}` — upsert with the doc.
  * `{doc:{...}, upsert:{...}}` — merge if exists, else insert upsert.
  * `{upsert:{...}}` — insert if missing, else noop.
  * `{script:{...}}` — TODO (Arkime barely uses this).
* `_bulk` is NDJSON: action header line, then (for index/create/update)
  a payload line. `delete` has only the header. Trailing newline
  optional. Always reply with `{took, errors, items:[…]}` even when
  some items failed.
* `_mget` — accept either `{docs:[{_index,_id},…]}` or
  `{ids:[…]}` (with the index in the path).

### Refresh
We treat `?refresh=true|wait_for|false` as no-ops because writes are
durable on commit in our backend.

---

## 9. Pagination: scroll & search_after

* Arkime uses **scroll**. Open with `?scroll=2m` on the initial search
  → response contains `_scroll_id`. Continue with
  `POST /_search/scroll` `{scroll:"2m", scroll_id:"…"}`. End with
  `DELETE /_search/scroll`.
* Hold the materialized hit-id list (or a cursor over the sorted
  bitmap) keyed by an opaque scroll id. TTL sweeper desirable.
* `search_after` is simpler but Arkime doesn't currently use it.

---

## 10. Counts

* `GET/POST /:idx/_count` and global `/_count`. Always emit
  `{count: <int>, _shards: {...}}`. **Never** wrap `count` as
  `{value, relation}` regardless of `rest_total_hits_as_int`.

---

## 11. Common Arkime-specific gotchas

* Arkime stores some IPs as integers historically; new indices use ES's
  `ip` string form. Both must round-trip.
* Field naming: Arkime's parser maps user expressions to db fields via
  `arkimeparser.jison`. Many "alias-ish" names like
  `dns.host.tokens` actually live as `dns.hostTokens` (a `text` field
  populated via `copy_to` from `dns.host`).
* Many `*Cnt` companion fields are integers maintained by the Arkime
  capture side — we just store/query them as longs.
* "Counts as integers" — anything ending in `Cnt` or `cnt` is a `long`.
* Arkime queries often build a giant `bool` of `exists` clauses. Our
  per-field exists bitmap makes this O(fields) bitmap-or — fast.

---

## 12. Error & status conventions

* On parse failure, return 400 with the parser message and the offending
  body. Arkime swallows generic 500s; specific 400s show up in its log.
* On a missing index that the client expected, behave as ES would:
  * `ignore_unavailable=true` → empty hits.
  * Else → 404 `{error:{type:"index_not_found_exception",index:"…"}}`.
* `index_already_exists_exception` (400) and
  `version_conflict_engine_exception` (409) are real shapes Arkime
  branches on.
* `result` field on write responses: `created` / `updated` / `deleted` /
  `noop`. Bulk items reflect the action they were issued under
  (`index` not `create`, etc.).

---

## 13. Performance & storage notes

(Only the bits that influenced the API surface; engine internals live
elsewhere.)

* Postings per `(field, value)` as roaring bitmaps; per-field exists
  bitmap; per-field sorted value list for terms/wildcard/regex/sort.
* Sort stability: ES is not stable across docs with equal sort keys; we
  break ties on `_id` for reproducibility. Don't accidentally promise
  more than ES.
* Dictionary-encode keyword values per field for storage; a single
  `value_id → bitmap` lookup is the hot path.
* `_source` is the only thing we hydrate for hits — keep it adjacent to
  the doc id table.

---

## 14. Things we haven't done yet (and probably should)

* `script` updates / runtime fields.
* Highlighting.
* `function_score`, `dis_max`, `boosting`, scoring at all (we return
  `_score:null` everywhere — ES allows this when `track_scores:false`,
  which Arkime is fine with).
* Positional `match_phrase` (currently AND of tokens).
* `min_should_match` percentage forms.
* Nested datatype with proper nested context (we flatten).
* Index settings honored beyond the trivial echo.
* Composable `_index_template` `composed_of` chains.
* Field caps (`/_field_caps`) — easy add, just hasn't come up.
* Refresh interval, replicas, shards — all reported as 1/0 always.
* Snapshot/restore, ILM, security — not needed for Arkime single-node.

---

## 15. The "next failure" loop

Whenever a real client fails:
1. Find the failing request in the log (we log body + response code).
2. Reproduce against ES and against arkimedb side-by-side with `curl`.
3. Diff the response shapes.
4. Decide: is the gap *parsing* (be more lenient), *behavior* (match ES
   semantics), or *unsupported feature* (implement)?
5. Add a synthetic minimal repro test before fixing — these accumulate
   into a regression suite that's worth its weight in gold.
6. Wipe `/tmp/arkimedb-arkime/` whenever schema-shape changes are
   involved; existing schemas are not retroactively fixed by improved
   inference.

---

## Appendix: more landmines discovered after the initial doc

### Top-level `/_search` (no index)

The viewer (via multies and the `_count`/`_search` helpers) sends
`POST /_search` without an index. This must be routed to "all live
collections" — same as `_all` or `*`. A missing route there returns
405 "allow: HEAD,PUT,DELETE,GET" because axum picks up the method-only
route; factor a `search_impl(Option<String> idx, …)` and register both
`/_search` and `/:idx/_search` against it.

### Terms lookup (`terms: { field: { index, id, path } }`)

Arkime uses terms lookups extensively (Cont3xt, session search with
IP shortcuts, etc.). ES resolves them server-side: GET the referenced
doc, pull the array at `path`, use as the terms array. Resolve BEFORE
deserializing into your typed `SearchRequest` — the lookup form fails
type-checking as "terms value must be array". Walk the query JSON and
substitute in place; recurse into `bool.{must,should,must_not,filter}`
so the `must_not: [{ terms: {ip: {index,id,path}} }]` inversion form
works too. Missing doc → empty array (matches ES).

### Bare query flags (`?flat_settings`, `?pretty`, `?refresh`)

Clients send bare flag-style query params with no value. Axum +
serde_urlencoded deserializes `?flat_settings` to `Some("")` and
`Option<bool>` *fails* ("invalid type: unit value, expected a
boolean"), which axum turns into a plaintext
`Failed to deserialize query string: …` 400 body that then blows up
JSON.pm in db.pl. Always type these as `Option<String>` and treat
empty, `true`, `1`, `yes` as on; `false` / `0` / `no` as off.

### `flat_settings` response shape

Under `?flat_settings=true`, ES flattens the nested `settings.index.*`
tree into dotted keys directly at `settings`:

```
settings: {"index.number_of_shards": "1", "index.refresh_interval": "60s", ...}
```

`db.pl`'s `checkPreviousSettings` reads
`$stemplate->{settings}->{"index.number_of_shards"}` — so without
flattening you get a pile of "Use of uninitialized value" warnings
and bogus "Previous number of shards was 0" messages.

### `fields:["*"]` / glob expansion

The viewer sends `fields:["*"]` for session detail. ES expands it
against the mapping. Simplest faithful implementation: flatten the
hydrated doc to leaf paths (arrays are leaves — don't descend), then
glob-match each requested pattern (`*`, `?`) against those paths. Same
matcher used for `?` in the field names.

### Per-hit `sort` is REQUIRED by multies

`multies.js`'s `sortResultsAndTruncate` reads `hit.sort[i].length` —
if sort was requested on the request, every hit MUST carry a `sort:
[…]` array, or multies crashes with `Cannot read properties of
undefined (reading 'length')`. Extract from the hydrated doc using the
sort field(s); if the value is an array, take the first element (ES
emits one scalar per sort clause). Populate only when sort was
requested, not speculatively.

### Unhandled promise rejections in multies

If arkimedb returns a shape multies doesn't expect, the crash can be
buried. Inject `process.on('unhandledRejection' | 'uncaughtException')`
handlers at the top of `multies.js` that print full stack + `err.meta`
for the elasticsearch-js ResponseError. This saves hours of guessing.

### Histogram (not date_histogram) is Arkime's timeline agg

Arkime's session-timeline uses `histogram` with `field=firstPacket`
(or `lastPacket`, `@timestamp`, or `packetRange`) and a **ms-integer**
interval. It is *not* `date_histogram`. Two consequences:

1. You must support integer `histogram` on `date`/`long` fields with
   interval in milliseconds.
2. Sub-aggs on integer histograms are real aggs, not just counts —
   see the per-bucket-per-collection trap below.

### Per-bucket sub-agg bitmaps must be per-collection

A subtle, expensive bug when a single `_search` spans multiple
collections (e.g. `tests_sessions3-*`): the aggs harness passes a
`HashMap<Collection, RoaringBitmap>` ("sets") into each bucket's
sub-aggs. If you OR bucket bitmaps across collections into a single
`RoaringBitmap` and then apply it to every collection, you over-count
dramatically (row ids collide across collections — each collection
has its own `next_row_id` sequence). Every bucket's sub-agg inputs
MUST stay per-collection — keep `HashMap<CollectionName,
RoaringBitmap>` per bucket and pass the subset through as-is.

Symptom: `source.packetsHisto` returned 2980 for a bucket that should
have been 101 — exactly `N_collections × correct`.

### Range-typed fields (`integer_range`, `ip_range`) and `spanning=true`

Arkime's `spanning=true` timeline switches the histogram field to
`packetRange` (ES `integer_range`). We store ranges as two flattened
sub-fields `packetRange.gte` / `packetRange.lte`; the bare field has
no postings. When a histogram's main field has zero postings, fall
back to pair lookup of `<f>.gte`/`<f>.lte` and span each bucket that
`[gte, lte]` overlaps (inclusive on both ends). ES returns the same
effective shape for range-field histograms.

### Histogram bucket-key arithmetic

Aligned bucket start = `floor(v/iv) * iv`. Store *that* (the aligned
start value, in whatever units the field is — ms, bytes, etc.) as the
bucket key and emit it directly. If you accidentally store
`floor(v/iv)` (the bucket index) and multiply again at emit time you
get i64 overflow; on a 1-day interval over epoch-ms timestamps that
quickly saturates to `9223372036854776000`.

### `aggs` vs `aggregations`

Every real ES client sends `aggregations:` as often as `aggs:`. Both
are accepted by ES. Add `#[serde(alias = "aggregations")]` to the
`aggs` field of your typed `SearchRequest` or half the clients look
like they returned no aggregations at all.

### On-update unindex

In-memory postings/exists bitmaps are by (field, value) → row_id_bm.
On reindex of an existing doc, the new postings are added on top of
the old ones — stale values still match queries. Before indexing the
new version, call a `PostingsIndex::remove_row(row_id)` that iterates
every bitmap and drops the row. Without this, updating a field like
`users` from `"alice"` to `""` still matches `term:{users:"alice"}`.

Symptom in api-shortcuts.t: a shortcut re-shared from users=alice to
roles=["x"] was still visible to alice.

### Tombstones need posting cleanup too (adjacent gotcha)

On delete, we tombstone the row; queries filter by tombstone. But if
you do any schema-wide scans (terms agg, wildcard over a field), the
stale row_id is still in the posting bitmap. For correctness, either
AND every bitmap with the live-rows bitmap before iterating, or run
`remove_row` on delete as well as on reindex. The former is cheaper
but easy to forget — prefer the latter for any new code paths.

That's it. Stay client-driven, stay lenient on input, be exact on
output, and keep a copy of the client's source code open. Most
"Elasticsearch compatibility" work is really "Elasticsearch *quirks*
compatibility" work.

---

## 16. What is actually **required** from an ES implementation for Arkime

If you're building another drop-in from scratch, this is the minimum
surface, in roughly the order Arkime will exercise it:

### Absolutely required (Arkime won't start without these)

1. **Handshake** — `GET /` returning `version.number >= 7.10.0`.
   Arkime hard-refuses anything `< 7.10.0` with `isn't supported`.
2. **Bootstrap pings** — `GET /_cluster/health`, `GET /_nodes`,
   `GET /_cat/indices`, `GET /_cat/templates`, `GET /_cat/aliases`.
   Plausible shapes, not zero-bytes.
3. **Templates** — list / get / put / delete for both `_template`
   **and** `_index_template`. `?flat_settings=true` response shape
   matters (see §App). Template `order` + merge semantics (§2).
4. **Index lifecycle** — `PUT /:idx`, `DELETE /:idx`, `_close`/`_open`,
   `_stats`, `_mapping` (get/put with dynamic+static+copy_to), aliases
   (all five route variants).
5. **Write CRUD** — `_doc` (put / post auto-id), `_create`, `_update`
   (all four body shapes, §8), `_delete`, `_bulk` NDJSON, `_mget`.
   `op_type=create` must really 409 on duplicates.
6. **Read CRUD** — `_doc` GET, `_mget`, `_search` with at minimum
   `bool/term/terms/range/match/match_phrase/exists/wildcard/prefix/
    regexp/ids/constant_score/filter`, `_count`, `_search/scroll`
   open+continue+delete.
7. **Aggs** — `terms`, `cardinality`, `stats`, `date_histogram`,
   `histogram` (integer + on date/long fields), `filters`, nested
   sub-aggs. `min_doc_count`, `size`, `order`.
8. **Dates** — epoch_ms/s, ISO-8601, date-only, `now-1h/d` math, TZ
   offsets, *and granularity rounding* (§7). Get this wrong and
   upper-bound queries silently drop valid hits.
9. **Per-hit sort array** (§App) when sort was requested — multies
   crashes without it.

### Required in specific flows (you will hit these or silently corrupt data)

10. **Alias resolution at every read AND write entry point**, including
    inside `_bulk`, `_count`, `_search`, `_update_by_query`,
    `_delete_by_query`. Arkime aliases `tests_sessions3` →
    `tests_sessions3-YYMMDD` daily indices.
11. **CIDR semantics on `term` over `ip`** — `"term": {"ip": "10/8"}`
    must internally expand to a range over IPv6-mapped u128.
12. **`copy_to`** round-tripped AND honored at index time (§3). Arkime
    relies on `dns.host` → `dns.hostTokens` text-analysis.
13. **Higher-order template wins on conflicting keys AND on
    `dynamic_templates` ordering** (§2). Miss this and IP fields get
    typed `keyword`.
14. **Persist dynamically inferred fields** (§3). Otherwise restart ⇒
    re-infer ⇒ different types from a different first doc.
15. **Tombstones + posting removal on reindex/delete** (§App). Stale
    postings cause "old users still see deleted content" bugs.
16. **Per-bucket sub-agg bitmaps are per-collection** (§App). Merging
    across collections over-counts by `N_collections`.
17. **Histogram bucket-key arithmetic** — store aligned value, not
    index (§App).
18. **`rest_total_hits_as_int` and `flat_settings` flags** must be
    honored — Arkime's db.pl parses with these shapes literally.
19. **Terms-lookup rewriting** before typed query deserialization (§App).
20. **Bare query flags** (`?pretty`, `?flat_settings`, `?refresh` with
    no value) must not 400 (§App).

### Safe to stub (Arkime ignores or only logs)

* Scoring: everywhere we return `_score:null` is fine.
* `function_score`, `dis_max`, `boosting`, nested datatype context,
  runtime fields, `script` updates.
* `ILM`, `snapshot`, `security`, `_enrich`, SQL endpoint.
* Replicas/shards/refresh_interval — echo whatever was set; return
  plausible numbers.
* `_field_caps`, highlighting — hasn't come up but easy adds.

### Required HTTP/transport behaviour

* **Response code discipline.** `404 index_not_found_exception` (with
  the typed body), `400 index_already_exists_exception`,
  `409 version_conflict_engine_exception` are branched on by Arkime.
  Generic 500s get swallowed; typed 400s surface in the log.
* **NDJSON for `_bulk`** — trailing newline optional, header line then
  payload line for index/create/update, header-only for delete.
* **Query-param leniency** — `size`/`from` as strings or numbers;
  booleans as `true/false/1/0/yes/no/""`.
* **`POST /_search` with no index** must resolve to "all live
  collections" (same as `_all`/`*`). Easy to miss because axum will
  otherwise 405 on it.

---

## 17. Performance & durability learnings (single-node)

The baseline naive impl ran Arkime's full perl test suite
(`tests.pl --viewer`, 52 files, 5101 tests) in **422s**. After the
following changes it runs in **~175s** (−59%). What mattered, in
descending order of impact:

### 1. `Durability::Eventual` on every write transaction (−56% alone)

In redb 2.x (and similarly in LMDB/sled/rocksdb), the default commit
fsyncs before returning. Arkime's bootstrap does thousands of
one-document writes (`tests_files/_doc/test-N?refresh=true` for every
pcap, plus `tests_sequence` increments); each fsync dominated. Setting
`Durability::Eventual` (fsync queued, not blocking) took the full
run from 422s → 184s.

For a drop-in ES node: **default to eventual durability on write**.
The real ES default is not a per-commit fsync either; its
`index.translog.durability` defaults to `request` but with large
write buffers and async flush. `refresh=true` is about visibility,
not durability.

### 2. `refresh` is a no-op if your backend already makes writes visible

ES's `refresh` exists because Lucene batches writes into an in-memory
buffer that isn't searchable until flushed into a searcher segment.
If your backend writes directly to the queryable index (our redb +
in-memory postings do), `_refresh`, `_flush`, and
`?refresh=true|wait_for` are all no-ops — and should literally return
immediately without doing anything.

This took us from 184s → 175s because the original impl was
*re-persisting already-persisted tombstones* on every `_refresh`.

### 3. Bulk writes: group contiguous same-collection ops into one
transaction (−small but removes a future bottleneck)

`_bulk` with N index ops on the same collection was N separate
`begin_write()/commit()` round-trips. Group them — one tx, N doc
writes, one commit. In-memory postings still update per-op so the
user-visible ordering is preserved; only the disk tx is amortized.
Doesn't help much at Arkime's test-suite scale but scales with
ingest-rate linearly.

### 4. Everything else was small change

* Parallel hydrate-on-startup with rayon: helped startup only.
* Match-all fast path in `_count` (skip compile + eval, return
  `row_count()`): helps `GET /:idx/_count` with no body.
* Gating per-request access log behind an env var (`ARKIMEDB_HTTP_LOG`):
  logging 23k lines/run is measurable.
* Lowering zstd from 9 → 3 on the compress path: write-time win but
  also increases disk, not much changed at this scale.

### What did NOT help

* Adding row-level posting indexes to make `remove_row` surgical —
  memory overhead > time saved at Arkime test scale. Reverted.
* Per-row exists tracking. Reverted.
* Bulk-tx grouping *with* aggressive batching of postings updates
  after commit — caused races where disk rows existed but postings
  didn't yet. Kept the per-op postings update.

### Where the floor is

After the above, arkimedb's total HTTP time was ~42s of the 175s wall.
The remaining ~130s is viewer startup, capture processes, and the
perl harness itself — not reachable from the DB side. Any further
win requires harness-level changes (warm viewers, parallel files).

### Storage reality: minimum file size dominates

redb 2.6 has a 1,589,248-byte minimum file size. Arkime creates 346
collection files (mostly empty daily indices across 25 years of test
pcaps). 346 × 1.55MB = **~536MB of the 543MB on disk**. Real
compressed data is <20MB. The only path to small-on-disk is one
shared redb per "family" (e.g. all `tests_sessions3-*` in one file
with a table per day). This is a large refactor we haven't done.

### Durability requirements as seen by Arkime

Arkime itself never calls a fsync-style endpoint between its writes
and its reads. It does issue `?refresh=true` on critical writes
(`tests_files`, `tests_users`), which we treat as no-ops because our
writes are queryable on commit. No test regressed from this.

What Arkime **does** need:
* Writes visible to immediately-following reads, across connections.
* Tombstones honored immediately (delete-then-read must return
  `found:false`).
* Atomic per-doc writes (no torn writes mid-request).
* Crash durability for **completed** restarts. We do sync the
  background fsync queue on shutdown; on a hard crash we can lose
  the last ~1s of writes. Arkime does not rely on per-request
  durability for correctness.

---

## 18. Concurrency under load (tokio + shared locks)

These bugs didn't appear until the viewer fanned out concurrent
searches while a bulk ingest was in-flight. Single-request benchmarks
were always green. Some were textbook, some were subtle.

### 18.1 Axum handlers must not run sync work directly

axum spawns each request on a tokio worker. Any synchronous work —
redb transactions, roaring-bitmap scans, JSON (de)ser for a 50-hit
response — **blocks that worker thread** for the whole duration. With
the default ~N-core worker pool, `N` concurrent searches park every
worker and the accept loop stops making progress. Symptom: `curl /`
hangs, but a fresh TCP connection still accepts at the kernel level
(somaxconn queue), so `ss`/`lsof` shows lots of ESTABLISHED
connections and `sample` shows all workers in `park`.

Fix: wrap every sync hot path in `tokio::task::spawn_blocking`. A
one-line helper (`blocking(move || …)`) keeps call sites small:

```rust
async fn blocking<F, T>(f: F) -> Result<T>
  where F: FnOnce() -> Result<T> + Send + 'static, T: Send + 'static
{ tokio::task::spawn_blocking(f).await.unwrap() }
```

Wrap at minimum: `_search` (compile + eval + hydrate), `_count` on
non-match_all, `_bulk`, every CRUD handler, agg execution. Leave
`GET /`, `_cluster/health`, `_cat/*` alone — those are cheap.

### 18.2 Shared index structures need a declared lock order

`PostingsIndex` has a top-level `map: RwLock<…>` and per-field
`RwLock<…>` inside each value. Insert acquired `map.write()` then
`per_field.write()`; `for_each_value` acquired `per_field.read()`
then `map.read()`. Under concurrent search + ingest (after §18.3
removed the coarse serializer), this is a classic AB-BA deadlock —
both threads hold one lock and wait for the other. Tokio workers
park forever, process survives, `GET /` still answers because `/` is
cheap and a different worker handles it.

Rule: pick a single direction and document it at the top of the
file. We chose **per_field before map, and always only the structure
you're about to mutate**. Add a `// LOCK ORDER: per_field → map`
comment directly above each acquisition.

### 18.3 Don't serialize reads behind a bulk `reindex_lock`

Early on, every read path took `collection.reindex_lock.read()` to
exclude bulk writes. This works for correctness but turns the server
single-reader-while-writing: a long bulk starves every search. ES
itself allows reads to observe pre-refresh state; it never blocks
reads behind writes.

Fix: drop the read-side of `reindex_lock` everywhere. Accept that a
search may observe a half-applied bulk (some postings updated, some
not) — that's the same semantic ES gives before `refresh`. The
per-structure RwLocks inside `PostingsIndex` still protect internal
consistency; §18.2 just has to be right.

Keep `reindex_lock.write()` in bulk itself, sorted by Arc pointer
across the collections a bulk touches, to prevent bulk-bulk deadlock.

### 18.4 Per-doc update lock for the tagging race

Arkime's tagging issues `_update` with `doc:{...}` on the same session
concurrently from multiple workers. Read-modify-write races produced
"last writer wins" losses — tags dropped, not duplicated.

Fix: a `DashMap<String, Arc<Mutex<()>>>` keyed by `(index, id)` held
for the full read-merge-write of a single doc. No global lock, no
interference across docs. Combined with §18.3 this is the right
granularity: per-doc atomicity, no per-collection stop-the-world.

### 18.5 Cache invalidation scope = per collection, not global

`sort_cache` (row→Scalar map for sort/agg fast paths, §19) lives on
`Collection`. Writes to collection A should invalidate A's cache,
never B's. Arkime search fans out across 300+ daily session
collections; a single `_update` invalidating them all turned the
"warm" timeline queries back into "cold" ones.

Rule: keep caches on the smallest object that owns the underlying
data. Clear them in the same write path that touches that data.

### 18.6 `DELETE /:idx` must actually remove the file

An early version marked the collection deleted in the catalog, freed
the in-memory handles, and left the `.redb` file on disk. Next
startup re-opened it and the index reappeared. Arkime's rotation
tests (`db.pl expire`) silently fell back to "index still here."

Always close the redb handle, `fs::remove_file` the data file, and
only then drop the catalog entry. Test by restarting — if the index
comes back, you forgot the unlink.

---

## 19. Query-shape fast paths

Two Arkime query shapes dominate wall time; both benefit from the
same trick — a per-collection **row→Scalar cache** (invalidated on
write) consulted instead of walking postings.

### 19.1 Sort by an indexed field + small `size` — extremum-first

Arkime's `/api/sessions` issues `sort:[{firstPacket:asc}] size:50`
across every daily sessions index. Naive: materialize all hits,
stable-sort, truncate. That's O(N log N) with N = millions.

Fast path:

1. Per collection, cache `(min, max)` of the sort field
   (`sort_range`), computed once.
2. At query time, partition collections into "indexed on this field"
   vs "unindexed" (the sort field is `_missing` for them).
3. Process indexed collections in extremum order (smallest-`min`
   first for asc, largest-`max` first for desc). For each, consult
   `sort_cache[field]` (row_id → Scalar), push into a bounded heap
   sized to `from + size`.
4. Early-stop: once the heap is full and the next collection's
   extremum is worse than the heap's worst, skip every remaining
   collection. One posting list per collection, then done.
5. For `missing:_last` with sorted-asc: unindexed collections
   contribute `_missing` rows — only materialize them if they'd
   land in the page. Same for `missing:_first` at the front.

Measured: 60k-hit firstPacket sort **5s → ~15ms**.

The `sort_cache` must:
* Be an `Arc<AHashMap<RowId, Scalar>>` so reads are lock-free once
  obtained.
* Be keyed by field name (most collections need only 1–2 fields).
* Be cleared on every write to the collection it lives on (§18.5).

### 19.2 Histogram with metric sub-aggs — reuse the same cache

Arkime's annotation query:

```json
{"histogram":{"field":"lastPacket","interval":3600000,"min_doc_count":1},
 "aggs":{"source.packets":{"sum":{"field":"source.packets"}},
         "destination.packets":{"sum":...}, /* 6 total */ }}
```

Generic path: for each bucket, for each sub-agg, iterate every
unique value of the field and do `(value_bm & bucket_bm).len()`.
Cost ≈ `buckets × subs × unique_values_of_field`. With 24 buckets,
6 subs, thousands of unique byte-counts per collection, ~8M bitmap
ANDs. **48 seconds.**

Fast path — activate when ALL sub-aggs are metric (sum/min/max/avg/
value_count) and the histogram and sub-agg fields are indexed:

1. Build `sort_cache` for the histogram field and each metric
   field (reuses §19.1 infra, shared invalidation).
2. Single pass over the matching-rows bitmap:
   ```
   for r in bm.iter() {
     let key = floor(hist_map[r] / iv) * iv;
     doc_count[key] += 1;
     for (i, sub_map) in sub_maps.iter().enumerate() {
       let v = sub_map[r]; // skip if missing
       acc[i][key] += (v, 1, min(v), max(v));
     }
   }
   ```
3. Emit buckets in key-sorted order.

Turns quadratic into linear. Measured: **48s → ~1.1s cold,
~350ms warm**.

Fall back to the generic path for any non-metric sub-agg, missing
index, or zero interval. Same structure works for `date_histogram`;
just store ms keys directly.

### 19.3 Min/max-pruning across collections

For date-typed sort fields across many collections, a further
optimisation: cache `(min, max)` per (collection, field). Process
the best-first collection, fill the top-K heap, then for every
remaining collection compare its cached extremum to the heap's
worst — if the collection **can't** contribute a value better than
what's already in the heap, skip it entirely (no posting walk, no
hydration). For cold caches the gain is modest; once extrema are
warm (they live in memory and almost never invalidate) multi-index
sorts become effectively single-index.

---

## 20. Operational & release lessons

### 20.1 Releases need a workflow dispatch, not just a tag

Pushing `v0.1.X` to the repo's default-branch tag list does **not**
publish a GitHub Release that produces the
`/releases/latest/download/arkimedb-linux-${arch}` assets Arkime
downloads. The `release.yml` workflow triggers on `workflow_dispatch`,
so after tagging run:

```
git tag vX.Y.Z && git push origin vX.Y.Z
gh workflow run release.yml --ref vX.Y.Z -R <owner>/<repo>
```

Skip step 2 and the tag is there but no binary — Arkime CI silently
reuses the last released version.

### 20.2 `addUser.js` cache bug = cache a negative result, forget to invalidate

Symptom: `addUser.js -n testuser admin admin admin --createOnly`
reported "user already exists" while
`GET /tests_users/_doc/admin` returned 404. Root cause: an
in-process "known user ids" cache populated at startup was never
invalidated on delete, so the second invocation in a test loop saw
a stale positive. Arkime's own user caches work the same way — if
you add any per-process cache over a redb table, wire a
**write-path invalidator** for it at the same moment you write the
table, not at some later "refresh."

### 20.3 `arkimedb_version` in `GET /`

Arkime's own `version.number` on `/` is pinned to `7.10.10` for
compatibility, which makes "what am I actually running?" hard to
answer in the field. Emit an extra `arkimedb_version:
env!("CARGO_PKG_VERSION")` alongside the ES version block. Costs
nothing, doesn't trip any ES client, invaluable during rolling
upgrades.

### 20.4 Stale-postings bugs have a recognisable shape

If you find yourself debugging "a deleted/retagged thing still
matches a search," the answer is almost always **postings updated,
row not removed from old value's bitmap first**. §15 tombstones
aside — if your reindex path only inserts, you will hit this. The
cheapest workaround is ANDing every read bitmap with the
"live rows" bitmap; the correct fix is `remove_row` before every
index of an existing doc.

### 20.5 redb file-count is the disk floor

redb's 1.5MB minimum file × 346 empty daily collections = most of
the on-disk size for a tests_* dataset. If disk is the #1 priority
(it is for us), plan the schema around **one redb per "family"**
with a table per day, not a redb per day. Retrofitting this is
painful; doing it early isn't. We chose "monthly sessions in one
redb" as a compromise (§checkpoint 025).

---

