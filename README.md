# arkime-testdb

A tiny, single-node, Elasticsearch 7.10-compatible HTTP server designed to back
[Arkime](https://github.com/arkime/arkime) for **development and CI** use.

Written in Rust on top of [redb](https://github.com/cberner/redb) with
[roaring bitmaps](https://github.com/RoaringBitmap/roaring-rs) for inverted
indices. Disk footprint is the primary optimization target, performance second,
memory third.

> ⚠️ **Not a production Elasticsearch/OpenSearch replacement.** This is a
> minimum-viable ES 7.10 surface scoped to what Arkime's viewer, capture,
> cont3xt, wiseService, db.pl, and the regression test suite actually exercise.
> There is no replication, no cluster, no shard rebalancing, and large swaths
> of the real ES API are missing or stubbed.

## Goals

1. **Disk space** — tiny on-disk footprint. The full Arkime test corpus lands
   in ~20&nbsp;MB across two redb files (`sessions.redb`, `other.redb`).
2. **Performance** — the full `tests.pl --viewer` suite (~5,100 tests) runs
   in ~160&nbsp;s on a laptop.
3. **Memory** — modest RSS; no JVM.

## Status

Runs the full Arkime regression test suite (`tests/tests.pl --viewer`) green.
The `db.pl` `init`/`upgrade`/`info`/`expire`/`optimize` commands all work.
Viewer, capture, cont3xt, and wiseService all talk to it happily.

## Build & run

```sh
cargo build --release
./target/release/arkimedb serve --data-dir ./data --port 9200
```

Then point Arkime at `http://localhost:9200` as you would Elasticsearch.

### Prebuilt binaries

Download from [Releases](https://github.com/arkime/arkime-testdb/releases).
Linux amd64/arm64 and macOS arm64 are published.

On macOS the downloaded binary is quarantined by Gatekeeper. Strip the
quarantine attribute before first run:

```sh
xattr -d com.apple.quarantine arkimedb-macos-arm64
chmod +x arkimedb-macos-arm64
./arkimedb-macos-arm64 serve --data-dir ./data --port 9200
```

### Useful flags

| Flag | Description |
| --- | --- |
| `--data-dir <path>` | Where redb files live (default `./data`). |
| `--port <n>` | HTTP port (default `9200`). |
| `--debug` | Log every request: `[debug] <ts> <peer> METHOD /url -> STATUS <bytes>B (ms)`. Also via `ARKIMEDB_DEBUG=1`. |

## What's implemented

Enough of ES 7.10 to make Arkime happy. Roughly:

- **Index ops** — create/delete/exists, aliases, settings, mappings, refresh
  (no-op by default; eventual durability).
- **Document ops** — get, index, update, delete, bulk, mget.
- **Search** — `_search`, `_count`, `_msearch`, scroll, PIT, `fields`
  projection, `_source` filtering.
- **Query DSL subset** — `bool`, `term`/`terms`, `match`, `range`,
  `query_string` / Lucene `q=`, `exists`, `prefix`, `wildcard`, `regexp`, CIDR.
- **Aggregations** — `terms`, `date_histogram`, `cardinality`, `stats`, nested.
- **Cluster/cat** — `/_cluster/health`, `/_nodes[/stats]`, `/_cat/*`,
  `/_ilm/policy`, `/_plugins/_ism/policies` (stubs for ILM/ISM).
- **Version** — reports `7.10.10` so Arkime's version gate is satisfied.

See `ES_DUP.md` for a detailed requirements/compatibility rundown and
performance notes collected while building this.

## Storage layout

All non-session indices share a single `other.redb` file; all session
indices share `sessions.redb`. This dramatically cuts disk usage compared to
one file per index when you have many tiny indices (which Arkime does).

## License

[Apache License 2.0](./LICENSE). Same as Arkime.
