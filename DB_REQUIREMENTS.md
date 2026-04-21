# Arkime Data Store Requirements

This document specifies, in vendor‑neutral terms, what any data store must
provide in order to back Arkime. It is derived from a full analysis of
`capture/`, `viewer/`, and `db/db.pl` (see `ELASTICSEARCH_USAGE.md` for the
current Elasticsearch/OpenSearch implementation).

Terminology used here is deliberately generic:

- **Record** – a single stored object (what ES calls a document, what SQL
  calls a row).
- **Collection** – a named set of records (index / table / bucket).
- **Field** – a typed attribute of a record (column / property / mapping).
- **Partition** – a time‑ or key‑scoped subdivision of a collection used
  for lifecycle management (time index / partition / table shard).
- **Logical name / alias** – a stable name that resolves to one or more
  physical collections.

Requirements are labelled **MUST** (required to run Arkime as it is today),
**SHOULD** (required for feature parity / performance parity), and **MAY**
(nice‑to‑have / used by a subset of features).

---

## 1. Data Model & Types

### 1.1 Native field types (MUST)

The store must support these typed fields with correct indexing / comparison
semantics (not just string storage):

| Type              | Used for                                                | Required operations |
|-------------------|---------------------------------------------------------|---------------------|
| **IPv6 address**  | `source.ip`, `destination.ip`, any `*Ip` field          | exact, CIDR / subnet match, range, sort |
| IPv4 address      | same; IPv4 addresses are a subset of IPv6 in Arkime     | exact, CIDR, range, sort (MAY be folded into IPv6) |
| **Keyword / exact string** | hostnames, domains, protocols, node names, tags, roles, user ids | exact, wildcard (`*`, `?`), simple regular expression, sort, exists |
| Tokenized text (MAY) | payload substrings, email bodies, HTTP headers that are searched by token | full word / token match with a pluggable tokenizer (at minimum a word‑split tokenizer); prefix / substring match over tokens. Optional — if the store doesn't support tokenization, Arkime can fall back to substring / regex over keyword fields. |
| **Integer (64‑bit signed)** | ports, byte/packet counters, counts, version numbers | exact, range, sort, sum, min, max, avg |
| **Unsigned 32‑bit / 64‑bit** | AS numbers, some counters                       | same as integer |
| **Short / boolean**| flags (`locked`, `enabled`, feature toggles)           | exact, exists |
| **Timestamp**     | `firstPacket`, `lastPacket`, `created`, `lastUsed`, …   | millisecond precision, exact, range, sort, date‑bucketing (see §3) |
| **Arrays of primitives** | `vlan.id[]`, `fileId[]`, `tags[]`, `huntId[]`, `packetPos[]`, `packetLen[]` | contains / contains‑any / contains‑all, length, index‑access for some consumers |
| **Nested / structured arrays** | `cert[]`, `dns.answers[]`, `http.request[]`, errors in hunts | query on a child field while preserving object boundaries (i.e. conditions on the same array element, not OR'd across elements) |
| **Opaque object** | user settings, column configs, parliament settings      | stored and returned verbatim; not necessarily queryable |

Notes:

- Fields often need to be usable **as both exact and free‑text** (for
  example, an HTTP host searched as `host == example.com` and as
  `host == /exa.*/`). The store must allow indexing the same value with
  multiple representations, or expose a single type that supports both.
- IPv4 and IPv6 are mixed in the same field. A single IP type that
  accepts either is strongly preferred.

### 1.2 Schema flexibility (MUST)

- Arkime installs an **initial schema** at cluster init time (field
  catalog, per‑collection field definitions).
- After init, plugins and new protocol parsers contribute additional
  fields at capture time. The store **MUST** allow adding new fields to
  an existing collection on the fly, without downtime and without
  rewriting existing records.
- All fields **MUST** be indexed automatically on ingest — Arkime does
  not maintain a separate "index this field" step; any field that
  appears in a record must immediately be queryable and aggregatable
  without an explicit index‑creation call.
- All fields **MUST** be treated as arrays by default. A field may hold
  a single value or multiple values in the same record, and the same
  field may be scalar in one record and multi‑valued in another.
  Predicates, sorts, and aggregations must work correctly in both
  cases (e.g. equality matches if *any* element matches; aggregations
  count each element).
- A **schema inference / dynamic mapping** mechanism that assigns a type
  based on field‑name conventions (e.g. `*Ip → ipv6`, `*Tokens →
  tokenized text`, otherwise keyword) is strongly desirable; absent
  that, Arkime's capture pipeline must have a way to declare the type
  of each new field out‑of‑band.
- The store **MUST** store and return opaque JSON‑like substructures
  without demanding a full schema for them (user settings, column
  configs, etc.).

### 1.3 Schema metadata (MUST)

- Per collection, a small metadata blob (schema version, Arkime DB
  version, custom properties) must be readable and updatable atomically.
  Arkime's upgrade logic keys off this.

### 1.4 Record identity (MUST)

- Records **MUST** be addressable by a caller‑supplied primary key
  (string up to a few hundred bytes).
- The store **MUST** also support auto‑generated ids.
- `get by id` and `delete by id` must be O(1).

---

## 2. Query Capabilities

### 2.1 Boolean composition (MUST)

Arbitrary nesting of:

- conjunction (AND)
- disjunction (OR)
- negation (NOT)
- required / optional / excluded (must / should / must_not)

No fixed depth limit.

### 2.2 Leaf predicates (MUST)

For every indexed field, the store must support:

- **Equality** / set membership (IN).
- **Existence / non‑existence** (field is present / absent).
- **Range** (`<`, `<=`, `>`, `>=`, between) on numeric / timestamp / IP.
- **CIDR / subnet** on IP fields.
- **Wildcard** (`*`, `?`) on keyword fields.
- **Simple regular expression** on keyword fields — character classes,
  alternation, quantifiers (`*`, `+`, `?`, `{n,m}`), anchors; full PCRE
  is not required.
- **Token / phrase match** on tokenized text (MAY — only if tokenized
  text is supported per §1.1).
- **Array‑contains** predicates on array fields.
- **Nested predicates** (condition on the same element of an array of
  objects).

### 2.3 Derived fields and composite predicates (SHOULD)

- Arkime's "all‑IP" / "all‑port" virtual fields expand to a disjunction
  over several real fields. The store doesn't need to understand this
  natively, but the generated query must be expressible (it is, via
  OR‑of‑terms today).

### 2.4 Sorting and pagination (MUST)

- Multi‑key sort, ascending or descending per key.
- Stable tie‑break by a unique field (e.g. id).
- Handling of missing values (`missing: first | last`).
- **Offset/limit** pagination for small result windows (up to ~10k).
- **Cursor pagination** that can stream through arbitrarily large
  result sets (hundreds of millions). A scrolling cursor or
  `search_after` / keyset pagination is acceptable. The cursor must
  either provide a stable snapshot or be tolerant of concurrent writes
  without silently dropping or duplicating records.
- Concurrent use of many cursors from many viewer processes.

### 2.5 Query cancellation (MAY)

- A client‑supplied opaque id can be attached to a query and later used
  to cancel it.
- Long‑running queries should not hold cluster resources forever, but
  explicit cancellation is optional — a server‑side timeout is
  acceptable.

### 2.6 Multi‑query batching (SHOULD)

- The ability to submit N independent queries in one request and
  receive N result sets (hunts currently use this).

### 2.7 Field projection (MAY)

- By default, a search **MUST** return the full record for each hit
  (either the original ingested document or a faithful reconstruction
  — the store may choose). Arkime does not require the byte‑exact
  source document.
- The store **SHOULD** optionally accept a caller‑supplied list of
  fields to return (projection / "include fields"), so the viewer can
  reduce payload size for wide session records. This is an
  optimization; callers that omit the list must still get every field.

---

## 3. Aggregations

Aggregations are central to the Arkime UI. The store **MUST** support at
least the following, computed server‑side over the full matching set
(not just the returned page):

### 3.1 Metric aggregations (MUST)

- `count` of matching records.
- `count_distinct` (approximate acceptable; must scale to 10⁹ cardinality
  per time slice).
- `sum`, `min`, `max`, `avg` of numeric fields.
- `percentiles` (MAY) — not used today but commonly wanted.

### 3.2 Bucket aggregations (MUST)

- **Terms** bucket: group by the distinct values of a field, return the
  top N with counts, optionally with sub‑metrics. Must support:
  - size / top‑N limit (up to at least several thousand buckets).
  - minimum bucket size filter.
  - ordering by count or by a sub‑metric.
- **Histogram** bucket: group by fixed‑width numeric intervals.
- **Date histogram** bucket: group by calendar or fixed time intervals
  (ms, s, m, h, d, w, M, y), with a caller‑supplied interval.
- **Two‑field span bucketing** (MUST): given a record with two numeric
  or timestamp fields naming a start and an end (e.g. `firstPacket` /
  `lastPacket`), the store must be able to place that record into
  every histogram/date‑histogram bucket its `[start, end]` span
  overlaps, so that time‑series of long‑lived sessions are counted in
  every interval they were active in — not only the interval
  containing the start. The start and end can be two independent
  fields on the record; a dedicated "range type" is not required.
- **Range** buckets — caller‑supplied list of `[from, to)` ranges over
  a numeric/IP/timestamp field (MAY).

### 3.3 Nested and multi‑level aggregations (MUST)

- Every bucket aggregation must accept one or more **sub‑aggregations**
  (metric or bucket). The UI heavily uses patterns like “date histogram
  → sum(bytes) + sum(packets)”, and “terms(country) → count + sum(bytes)”.
- At least 3 levels of nesting must be supported.

### 3.4 Performance expectations (SHOULD)

- Aggregations over a time slice containing **10⁸–10⁹** records, with
  low‑cardinality terms, should complete in interactive time (order of
  seconds) on commodity hardware. This implies columnar storage or an
  equivalent.
- Aggregations must honor the same predicate set as regular queries
  (they run on the matching subset).

---

## 4. Writes and Mutation

### 4.1 Bulk ingest (MUST)

- A bulk / batch insert API capable of sustaining **millions of
  records per minute per cluster** from many capture processes.
- Individual batches of at least several MB / several tens of thousands
  of records.
- Per‑record success/failure reporting.
- Back‑pressure / flow control signaling so producers can throttle.

### 4.2 Durability and visibility (MUST)

- **Get by id must be real‑time**: once a write has been acknowledged,
  a subsequent `get by id` of that record **MUST** return the new
  value. Arkime relies on this for counters, user/view/hunt updates,
  and sequence allocation.
- **Search / query is not required to be real‑time**: it is acceptable
  for bulk‑ingested session records to take some bounded time (seconds
  to a minute) before they appear in searches and aggregations. Arkime
  already tolerates a background "refresh" model for session data.
- A caller should be able to choose, per request, whether the write:
  1. is acknowledged after buffering (fast, may not yet appear in
     searches);
  2. is acknowledged once searchable;
  3. blocks until replicated (durable).
- Default is (1) for sessions (bulk); (2) for audit, views, hunts, file
  metadata when searches immediately follow; (3) for counters.

### 4.3 Refresh / flush (MUST)

- The store **MUST** expose a **refresh** operation, scoped to a
  collection (or the whole cluster), that forces all pending writes
  to become visible to subsequent searches and aggregations before the
  call returns.
- Used by tests, by regression fixtures, after batch loads, and after
  admin operations where the viewer must immediately see the new
  state. Today Arkime uses this heavily (`POST /_refresh` in db.pl,
  tests, and some viewer code paths).
- Refresh must be safe to call concurrently and should complete in
  seconds even under sustained ingest.

### 4.4 Atomic counters (MUST)

- A primitive equivalent to compare‑and‑set / conditional put, keyed
  by a record id. Arkime uses this for file‑number sequences shared by
  many capture nodes.
- Performance target: at least a few thousand increments/sec per
  counter.

### 4.5 Optimistic concurrency (MUST)

- Every record should carry a version / sequence token that the client
  can read and later assert on update (“update only if version still
  X”).
- Retries on conflict (up to N) must be expressible.

### 4.6 Partial update and server‑side mutation (MUST)

- The store **MUST** support partial updates to specific fields of a
  record without requiring the client to resend the full record, and
  with conflict handling as in 4.4.
- For array fields, the store **SHOULD** allow server‑side
  add/remove/dedup operations (Arkime adds tags and hunt ids to session
  records). If the store does not support server‑side mutation logic,
  the application will do read‑modify‑write with optimistic
  concurrency; the store must then make that cheap (single‑key
  read and write in the same round trip is acceptable).

### 4.7 Bulk delete / delete‑by‑query (SHOULD)

- Delete all records matching a predicate, ideally as a background job
  whose progress can be queried and which can be cancelled.

### 4.8 External versioning (SHOULD)

- For backup/restore and cross‑cluster sync, the store **SHOULD**
  accept writes whose version is supplied by the client, and reject
  older‑version writes. This preserves version continuity across
  export/import.

---

## 5. Collection Lifecycle

### 5.1 Logical naming / aliasing (MUST)

- The application always references a stable logical name
  (e.g. `users`, `stats`, `sessions`). The store must provide a way
  to map a logical name to:
  - a single physical collection, or
  - a set of physical collections (a "view" spanning them).
- Changing the mapping (to swap in an upgraded version of a collection,
  or to include/exclude a partition) must be atomic from the
  application's point of view.

### 5.2 Time partitioning (MUST)

- Session and audit data are partitioned by time (hourly / daily / weekly
  / monthly). The store must support:
  - Creating a new partition cheaply.
  - Querying across a range of partitions in one query, with
    partitions resolved automatically by a predicate on the time field
    or by partition name pattern.
  - Dropping a whole partition as a single fast operation (retention).
  - Applying per‑partition settings (replication, storage tier,
    read‑only flag).

### 5.3 Retention / tiering (SHOULD)

- Policy‑based retention: automatically move partitions older than X to
  a colder / cheaper storage tier, and delete them at Y.
- Force‑merge / compact on tier transition.
- Ability to mark partitions read‑only.

### 5.4 Snapshots and backup (SHOULD)

- Consistent snapshot of a collection or selected partitions.
- Incremental backup.
- Restore to a new cluster preserving record ids and versions.

### 5.5 Compaction / optimization (SHOULD)

- Ability to merge/compact a partition once it stops being written to,
  reducing read overhead on historical data.

### 5.6 Resharding / rebalancing (SHOULD)

- Add/remove storage nodes without downtime, with automatic rebalancing.
- Change replication factor per collection or partition.
- Reduce shard/partition count of an existing dataset (equivalent of
  "shrink") when moving data from hot to cold.

### 5.7 Reindex / rewrite (SHOULD)

- A server‑side job that reads all records of collection A, optionally
  transforms them, and writes them into collection B. Must be
  parallelizable and resumable.

---

## 6. Scale, Performance and Reliability

### 6.1 Ingest scale (MUST)

- Sustained ingest of **≥ 100 M records per time slice** (e.g. per
  day, sometimes per hour on large deployments) per cluster, across
  many writers.
- Ingest should be linearly scalable by adding storage nodes.

### 6.2 Query scale (MUST)

- Point queries by id: sub‑10 ms.
- Filtered scans over a day of sessions with correct indexes: seconds.
- Aggregations over a day of sessions: interactive (see §3.4).

### 6.3 Footprint (SHOULD)

- Storage overhead per session record (including all indexes) should
  stay comparable to current deployments (rough budget: ~1–2 KB per
  session on disk after compression).

### 6.4 High availability (MUST)

- **Multi‑node clustering**: the store **MUST** run as a cluster of
  multiple storage/query nodes that cooperate to serve a single
  logical dataset. A single‑node deployment may be supported for dev
  or tiny sites, but production deployments require ≥ 3 nodes for
  quorum and capacity.
- Data and query load are distributed across nodes; adding nodes
  increases both storage and throughput.
- **Server‑side result merging**: for any query, aggregation, or
  cursor, the store **MUST** internally fan the work out to the nodes
  that hold relevant data and return a single merged result to the
  client. Arkime sends one request to one endpoint and receives one
  fully merged response — it does not merge per‑node partials itself.
  This applies to hit lists (sorted/limited), bucket aggregations
  (re‑aggregated by key), metric aggregations (sum/min/max/avg),
  counts, and distinct counts.
- Configurable replication factor per collection.
- Tolerates single‑node failure with no data loss and degraded but
  continuing reads/writes.
- Recovery without manual intervention once the node returns.

### 6.5 Cluster coordination (MUST)

- In a multi‑node deployment, the store **MUST** provide a cluster‑wide
  coordination mechanism — typically one or more "coordinator" /
  "control‑plane" nodes (dedicated, or elected among data nodes) — that
  is authoritative for:
  - the **global schema / field catalog** (every node sees the same
    field types and dynamic‑mapping rules);
  - the **collection → node / partition → node placement map** (which
    nodes currently hold which partitions, including primaries and
    replicas);
  - **membership** (which nodes are in the cluster, which are up,
    which are joining/leaving);
  - **collection lifecycle events** (create, drop, alias swap,
    partition rollover, tier transitions — all applied cluster‑wide
    atomically);
  - **rebalancing decisions** when nodes are added, removed, or fail.
- Schema changes (new field, new collection, alias swap) made through
  any node **MUST** be reflected on all other nodes before the
  operation is acknowledged, so that subsequent writes / queries from
  any client against any node see the new state.
- The coordination layer **MUST** tolerate the loss of a minority of
  coordinator nodes (quorum‑based — typically 3 coordinators survives
  1 failure, 5 survives 2). Coordinators and data nodes may be the
  same physical nodes or separated, at the operator's choice.
- A client (Arkime capture, viewer, or db.pl) only needs to know
  *some* cluster endpoint; the cluster itself routes the request to
  the right node(s) and is responsible for knowing where everything
  lives.

### 6.6 Consistency model (MUST)

- Per‑record read‑your‑writes for operations that opt in (see 4.2).
- Globally eventually consistent writes for high‑volume bulk ingest.
- Monotonic counters via 4.4.

### 6.7 Observability (MUST)

- Per‑collection and per‑partition stats: record count, byte size,
  physical shard/segment count, write/read rates.
- Per‑node stats: CPU, heap/memory, disk used/free, queue depths.
- Cluster health: overall status, relocating/unassigned shards,
  pending tasks.
- Task list with per‑task progress and cancel.
- These are consumed by Arkime's stats UI and by db.pl's `expire` and
  `info` commands.

---

## 7. Security

- **Authentication**: the store **MUST** support username/password.
  API key / bearer token auth **MAY** additionally be supported, but is
  not required.
- **Transport**: the store **MUST** be reachable over both plain HTTP
  and HTTPS (TLS). Arkime supports insecure HTTP for trusted local
  deployments and HTTPS for everything else, selected by config /
  URL scheme.
- **Transport security (HTTPS mode)**: TLS with server certificate
  validation.

Arkime itself also needs:

- A dedicated collection for **user accounts** that stores a salted
  password hash, optional TOTP secret, roles, per‑user settings; or an
  external provider (LDAP/OIDC) — the user collection currently lives
  in the data store but can be moved to SQLite/Redis/LMDB already.

---

## 8. Deployment & Operations Constraints

- **Open source** license compatible with Arkime's Apache‑2.0 codebase.
- Runs on commodity Linux x86_64 and arm64.
- Horizontally scalable; single‑node install also supported for small
  deployments.
- Either an HTTP/JSON API **or** a widely‑available driver for C and
  Node.js. (Arkime's capture is in C; viewer is in Node.js; admin
  script is in Perl. If the store exposes HTTP/JSON, no language‑
  specific driver is required.)
- Stable wire protocol across minor versions.

---

## 9. Summary Checklist

A candidate store is viable only if it checks all MUSTs:

- [ ] Typed fields including IPv6, keyword, 64‑bit numerics,
      millisecond timestamps, arrays, nested arrays of objects, opaque
      JSON.
- [ ] Dynamic / extensible schema without downtime.
- [ ] Rich predicate language: equality, IN, existence, range, CIDR,
      keyword exact/wildcard/simple‑regexp, nested, array contains,
      arbitrarily nested AND/OR/NOT.
- [ ] Sort, offset+limit, and large‑cursor pagination.
- [ ] Aggregations: count, distinct count, sum/min/max/avg, terms,
      histogram, date histogram, two‑field span bucketing, multi‑level
      nesting, all honoring the predicate set.
- [ ] Bulk ingest sustainable at 10⁸+ records/day/cluster with
      back‑pressure.
- [ ] Per‑record optimistic concurrency and partial updates; atomic
      counters; external versioning.
- [ ] Logical names / aliases; atomic swap.
- [ ] Time partitioning with cheap create and cheap drop; per‑partition
      settings and tiering.
- [ ] Snapshot/backup/restore with id and version preservation.
- [ ] HA with configurable replication; multi‑node cluster with
      coordinator nodes managing global schema and partition
      placement, automatic rebalancing, and server‑side merged query
      results.
- [ ] Auth (user/password, HTTP and HTTPS/TLS).
- [ ] Observability: collection/partition/node/cluster stats.
- [ ] Open source, horizontally scalable, HTTP/JSON API or C and
      Node.js driver.
