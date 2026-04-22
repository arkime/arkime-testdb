//! arkimedb-http — Elasticsearch-compatible HTTP/JSON subset.
//!
//! Only the surface Arkime actually touches is implemented. Everything else
//! returns a descriptive 400.

use std::sync::Arc;
use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, Request, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post, put, head},
    Json, Router,
};
use serde::Deserialize;
use serde_json::{json, Value as J};

use arkimedb_core::{Error, Result};
use arkimedb_storage::{Engine, engine::{BulkOp, BulkKind, BulkOutcome}};
use arkimedb_query::{SearchRequest, search::execute as run_search};

pub struct AppState {
    pub engine: Arc<Engine>,
    pub start: std::time::Instant,
    pub scrolls: parking_lot::RwLock<ahash::AHashMap<String, ScrollState>>,
    pub cluster_settings: parking_lot::RwLock<ClusterSettings>,
    /// Per-(collection,id) async mutex for serializing read-modify-write
    /// script updates. Without this, two concurrent `addtags`/`removetags`
    /// calls for the same document race and one update is lost (read both
    /// see old tags, both write their own version, last writer wins).
    /// Arkime's viewer fans out tag updates with `eachLimit(10)` and the
    /// same session id can appear in the list multiple times.
    pub update_locks: parking_lot::Mutex<ahash::AHashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

/// Whether per-request access lines are written to stderr. Controlled via
/// the `ARKIMEDB_HTTP_LOG=1` env var at startup. Default off — the Arkime
/// test suite generates tens of thousands of requests per run and writing
/// an access line for each one added measurable wall time.
static HTTP_LOG: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
static HTTP_DEBUG: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

pub fn init_http_log_from_env() {
    if std::env::var("ARKIMEDB_HTTP_LOG").ok().as_deref() == Some("1") {
        HTTP_LOG.store(true, std::sync::atomic::Ordering::Relaxed);
    }
    if std::env::var("ARKIMEDB_DEBUG").ok().as_deref() == Some("1") {
        HTTP_DEBUG.store(true, std::sync::atomic::Ordering::Relaxed);
    }
}

pub fn set_http_debug(on: bool) {
    HTTP_DEBUG.store(on, std::sync::atomic::Ordering::Relaxed);
}

#[derive(Default)]
pub struct ClusterSettings {
    pub persistent: serde_json::Map<String, J>,
    pub transient: serde_json::Map<String, J>,
}

pub struct ScrollState {
    pub targets: Vec<String>,
    pub req: SearchRequest,
    pub next_from: usize,
    pub ttl_until: std::time::Instant,
}

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        // root
        .route("/",                              get(root).head(root))
        .route("/_cluster/health",               get(cluster_health))
        .route("/_cluster/health/:target",       get(cluster_health_scoped))
        .route("/_cluster/state",                get(cluster_state))
        .route("/_cat",                          get(cat_index))
        .route("/_cat/indices",                  get(cat_indices_all))
        .route("/_cat/indices/:target",          get(cat_indices_scoped))
        .route("/_cat/aliases",                  get(cat_aliases))
        .route("/_refresh",                      post(refresh_all).get(refresh_all))
        .route("/_flush",                        post(flush_all).get(flush_all))
        .route("/_flush/synced",                 post(flush_all).get(flush_all))
        .route("/_stats",                        get(cluster_stats))
        .route("/_stats/docs",                   get(cluster_stats_docs))
        .route("/_stats/docs,store",             get(cluster_stats_docs))
        .route("/_nodes",                        get(nodes))
        .route("/_nodes/stats",                  get(nodes_stats))
        .route("/_nodes/stats/:metrics",         get(nodes_stats))
        .route("/_nodes/:target/stats",          get(nodes_stats))
        .route("/_nodes/:target/stats/:metrics", get(nodes_stats))
        .route("/_nodes/:target",                get(nodes))
        .route("/_cluster/settings",             get(cluster_settings).put(cluster_settings_put))
        .route("/_cluster/reroute",              post(cluster_reroute))
        .route("/_cluster/allocation/explain",   get(allocation_explain).post(allocation_explain))
        .route("/_cat/nodes",                    get(cat_nodes))
        .route("/_cat/templates",                get(cat_templates))
        .route("/_cat/templates/:pattern",       get(cat_templates_scoped))
        .route("/_cat/shards",                   get(cat_shards_all))
        .route("/_cat/shards/:target",           get(cat_shards_scoped))
        .route("/_cat/aliases/:pattern",         get(cat_aliases_scoped))
        .route("/_cat/health",                   get(cat_health))
        .route("/_cat/master",                   get(cat_master))
        .route("/_cat/allocation",               get(cat_allocation))
        .route("/_cat/count",                    get(cat_count_all))
        .route("/_cat/count/:target",            get(cat_count_scoped))
        .route("/_cat/recovery",                 get(cat_recovery))
        .route("/_tasks",                        get(tasks_list))
        // ILM / ISM (OpenSearch) — not implemented; return empty object
        // so db.pl `esGet(..., 1)` + from_json() sees parseable JSON.
        .route("/_ilm/policy",                    get(empty_object))
        .route("/_ilm/policy/:name",              get(empty_object).put(empty_object).delete(empty_object))
        .route("/_plugins/_ism/policies",         get(empty_object))
        .route("/_plugins/_ism/policies/:name",   get(empty_object).put(empty_object).delete(empty_object))
        .route("/_alias",                        get(alias_list_all))
        .route("/_alias/:alias",                 get(alias_list_by))
        .route("/_bulk",                         post(bulk))
        .route("/_msearch",                      post(msearch))
        .route("/_search",                       post(search_all).get(search_all))
        .route("/_count",                        post(count_all).get(count_all))
        .route("/_aliases",                      post(update_aliases))
        .route("/_search/scroll",                post(scroll_continue).get(scroll_continue_get).delete(scroll_delete))
        // index templates (legacy _template)
        .route("/_template",                     get(template_list))
        .route("/_template/:names",              get(template_get).head(template_head).put(template_put).post(template_put).delete(template_delete))
        .route("/_index_template",               get(template_list))
        .route("/_index_template/:names",        get(template_get).head(template_head).put(template_put).post(template_put).delete(template_delete))
        // per-index
        .route("/:idx",                          head(index_exists).put(create_index).delete(delete_index).get(get_index))
        .route("/:idx/_refresh",                 post(refresh_one).get(refresh_one))
        .route("/:idx/_flush",                   post(flush_one).get(flush_one))
        .route("/:idx/_stats",                   get(index_stats))
        .route("/:idx/_stats/:metric",           get(index_stats))
        .route("/:idx/_alias",                   get(alias_of_index))
        .route("/:idx/_alias/:alias",            put(put_alias).delete(delete_alias).get(alias_of_index_name))
        .route("/:idx/_settings",                get(get_settings).put(put_settings).post(put_settings))
        .route("/:idx/_bulk",                    post(bulk_scoped))
        .route("/:idx/_search",                  post(search).get(search))
        .route("/:idx/_count",                   post(count).get(count))
        .route("/:idx/_delete_by_query",         post(delete_by_query))
        .route("/:idx/_update_by_query",         post(update_by_query))
        .route("/:idx/_msearch",                 post(msearch_scoped))
        .route("/:idx/_mapping",                 get(get_mapping).put(put_mapping).post(put_mapping))
        .route("/:idx/_mappings",                get(get_mapping).put(put_mapping).post(put_mapping))
        .route("/:idx/_doc",                     post(index_doc_auto_id))
        .route("/:idx/_doc/:id",                 put(index_doc).get(get_doc).delete(delete_doc).post(index_doc))
        .route("/:idx/_source/:id",              get(get_source).head(head_source))
        .route("/:idx/_create/:id",              put(create_doc).post(create_doc))
        .route("/:idx/_update/:id",              post(update_doc))
        .route("/_mget",                         post(mget).get(mget))
        .route("/:idx/_mget",                    post(mget_scoped).get(mget_scoped))
        .layer(middleware::from_fn(log_requests))
        .layer(tower_http::decompression::RequestDecompressionLayer::new())
        .layer(axum::extract::DefaultBodyLimit::max(1024 * 1024 * 1024))
        .with_state(state)
}

async fn log_requests(req: Request<axum::body::Body>, next: Next) -> Response {
    let method = req.method().clone();
    let uri = req.uri().clone();
    let peer = req
        .extensions()
        .get::<axum::extract::ConnectInfo<std::net::SocketAddr>>()
        .map(|c| c.0.to_string())
        .unwrap_or_else(|| "-".to_string());
    // Check ?pretty (value-less or "true"/"1") before the request is consumed.
    let pretty = uri.query().map_or(false, |q| {
        q.split('&').any(|kv| {
            let mut it = kv.splitn(2, '=');
            let k = it.next().unwrap_or("");
            let v = it.next().unwrap_or("");
            k == "pretty" && (v.is_empty() || v == "true" || v == "1")
        })
    });
    let t0 = std::time::Instant::now();
    let mut resp = next.run(req).await;
    // Always advertise the Elasticsearch product header. The official
    // @elastic/elasticsearch JS client (>=7.14) verifies this on every
    // response and otherwise rejects with ProductNotSupportedError, which
    // takes down clients permanently (e.g. multies repeatedly exiting).
    resp.headers_mut().insert(
        "X-Elastic-Product",
        axum::http::HeaderValue::from_static("Elasticsearch"),
    );
    let dt = t0.elapsed();
    let log_plain = HTTP_LOG.load(std::sync::atomic::Ordering::Relaxed);
    let log_debug = HTTP_DEBUG.load(std::sync::atomic::Ordering::Relaxed);
    if log_debug {
        // Buffer the full response body so we can report exact byte count.
        // Slightly slower than streaming, but only on when --debug is set.
        let status = resp.status().as_u16();
        let (mut parts, body) = resp.into_parts();
        let bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap_or_default();
        let nbytes = bytes.len();
        let ts = time::OffsetDateTime::now_utc()
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap_or_default();
        eprintln!(
            "[debug] {} {} {} {} -> {} {}B ({:.2}ms)",
            ts, peer, method, uri, status, nbytes, dt.as_secs_f64() * 1000.0
        );
        if pretty {
            let is_json = parts.headers
                .get(axum::http::header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .map_or(false, |v| v.contains("json"));
            if is_json {
                let pretty_bytes: Vec<u8> = match serde_json::from_slice::<J>(&bytes) {
                    Ok(v) => serde_json::to_vec_pretty(&v).unwrap_or_else(|_| bytes.to_vec()),
                    Err(_) => bytes.to_vec(),
                };
                parts.headers.insert(
                    axum::http::header::CONTENT_LENGTH,
                    pretty_bytes.len().to_string().parse().unwrap(),
                );
                return Response::from_parts(parts, axum::body::Body::from(pretty_bytes));
            }
        }
        parts.headers.insert(
            axum::http::header::CONTENT_LENGTH,
            nbytes.to_string().parse().unwrap(),
        );
        return Response::from_parts(parts, axum::body::Body::from(bytes));
    }
    if log_plain {
        eprintln!(
            "[http] {} {} -> {} ({:.1}ms)",
            method,
            uri,
            resp.status().as_u16(),
            dt.as_secs_f64() * 1000.0
        );
    }
    if !pretty { return resp; }

    // Only reformat JSON bodies. Collect body, re-serialize with indentation.
    let (parts, body) = resp.into_parts();
    let is_json = parts.headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map_or(false, |v| v.contains("json"));
    if !is_json {
        return Response::from_parts(parts, body);
    }
    let bytes = match axum::body::to_bytes(body, usize::MAX).await {
        Ok(b) => b,
        Err(_) => return (parts.status, "").into_response(),
    };
    let pretty_bytes: Vec<u8> = match serde_json::from_slice::<J>(&bytes) {
        Ok(v) => serde_json::to_vec_pretty(&v).unwrap_or_else(|_| bytes.to_vec()),
        Err(_) => bytes.to_vec(),
    };
    let mut resp = Response::from_parts(parts, axum::body::Body::from(pretty_bytes.clone()));
    resp.headers_mut().insert(
        axum::http::header::CONTENT_LENGTH,
        pretty_bytes.len().to_string().parse().unwrap(),
    );
    resp
}

// -------- root / meta ----------------------------------------------------

/// Run a synchronous, potentially blocking closure on tokio's blocking pool.
/// This prevents long-running work (searches, bulk writes, redb transactions)
/// from occupying the tokio runtime's worker threads and starving other
/// requests — which manifests as the server appearing "stalled" when many
/// concurrent clients send heavy requests.
async fn blocking<F, T>(f: F) -> T
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f).await.expect("blocking task panicked")
}

async fn root(State(s): State<Arc<AppState>>) -> Response {
    let body = Json(json!({
        "name": "arkimedb",
        "cluster_name": "arkimedb",
        "cluster_uuid": "arkimedb-single-node",
        "version": {
            // Report an Elasticsearch 7.x version string so Arkime's version
            // probes accept us. Our own build version is surfaced via `tagline`.
            "number": "7.10.10",
            "build_flavor": "default",
            "build_type": "rust",
            "build_hash": "HEAD",
            "build_date": "",
            "build_snapshot": false,
            "lucene_version": "8.7.0",
            "minimum_wire_compatibility_version": "6.8.0",
            "minimum_index_compatibility_version": "6.0.0"
        },
        "tagline": "You Know, for Search",
        "arkimedb_version": env!("CARGO_PKG_VERSION"),
        "_uptime_s": s.start.elapsed().as_secs(),
    }));
    let mut resp = body.into_response();
    // The @elastic/elasticsearch 7.14+ JS client refuses to talk to a server
    // that does not advertise this header; without it every request throws
    // ProductNotSupportedError and the multies viewer's clients become
    // permanently broken (which presents as multies repeatedly exiting).
    resp.headers_mut().insert(
        "X-Elastic-Product",
        axum::http::HeaderValue::from_static("Elasticsearch"),
    );
    resp
}

async fn cluster_health(State(s): State<Arc<AppState>>) -> Response {
    let n = s.engine.list_collections().len();
    Json(json!({
        "cluster_name": "arkimedb",
        "status": "green",
        "timed_out": false,
        "number_of_nodes": 1,
        "number_of_data_nodes": 1,
        "active_primary_shards": n,
        "active_shards": n,
        "relocating_shards": 0,
        "initializing_shards": 0,
        "unassigned_shards": 0,
    })).into_response()
}
async fn cluster_health_scoped(Path(_t): Path<String>, State(s): State<Arc<AppState>>) -> Response {
    cluster_health(State(s)).await
}

async fn cluster_state(State(s): State<Arc<AppState>>) -> Response {
    Json(json!({
        "cluster_name": "arkimedb",
        "cluster_uuid": "arkimedb-single-node",
        "master_node": "arkimedb-0",
        "nodes": { "arkimedb-0": { "name": "arkimedb-0", "roles": ["master", "data"] } },
        "indices_count": s.engine.list_collections().len(),
    })).into_response()
}

async fn nodes(State(_s): State<Arc<AppState>>) -> Response {
    Json(json!({
        "cluster_name": "arkimedb",
        "nodes": { "arkimedb-0": {
            "name": "arkimedb-0",
            "version": "7.10.10",
            "host": "127.0.0.1",
            "hostname": "127.0.0.1",
            "ip": "127.0.0.1",
            "transport_address": "127.0.0.1:9300",
            "roles": ["master","data","ingest"],
            "attributes": {},
            // db.pl dbCheck reads settings.config to print which file a
            // warning applies to; also checks a few legacy settings.
            "settings": {
                "config": "arkimedb (embedded)",
                "cluster.name": "arkimedb",
                "node.name": "arkimedb-0"
            },
            "thread_pool": {
                "bulk":   { "queue_size": 200, "type": "fixed", "size": 1 },
                "write":  { "queue_size": 200, "type": "fixed", "size": 1 },
                "search": { "queue_size": 1000, "type": "fixed_auto_queue_size", "size": 1 }
            }
        }}
    })).into_response()
}

async fn cluster_stats(State(s): State<Arc<AppState>>) -> Response {
    let mut docs = 0u64; let mut size = 0u64;
    for c in s.engine.list_collections() {
        if let Some(col) = s.engine.get_collection(&c) {
            docs += col.row_count().unwrap_or(0);
            size += std::fs::metadata(&col.path).map(|m| m.len()).unwrap_or(0);
        }
    }
    Json(json!({
        "cluster_name": "arkimedb",
        "indices": { "count": s.engine.list_collections().len(), "docs": { "count": docs }, "store": { "size_in_bytes": size } },
        "nodes":   { "count": { "total": 1, "data": 1, "master": 1 } }
    })).into_response()
}

#[derive(Deserialize, Default)]
struct CatQ {
    format: Option<String>,
    active_only: Option<String>,
    #[serde(default, deserialize_with = "de_flag")]
    v: bool,
}

// Accept ?v, ?v=, ?v=true, ?v=1 — ES-style flag param.
fn de_flag<'de, D: serde::Deserializer<'de>>(d: D) -> std::result::Result<bool, D::Error> {
    let s: Option<String> = Option::deserialize(d)?;
    Ok(match s.as_deref() {
        Some("") | Some("true") | Some("1") | Some("yes") | None => true,
        _ => false,
    })
}

async fn cat_index() -> Response {
    // Matches Elasticsearch's /_cat help text.
    let body = "=^.^=\n\
/_cat/allocation\n\
/_cat/shards\n\
/_cat/shards/{index}\n\
/_cat/master\n\
/_cat/nodes\n\
/_cat/tasks\n\
/_cat/indices\n\
/_cat/indices/{index}\n\
/_cat/segments\n\
/_cat/segments/{index}\n\
/_cat/count\n\
/_cat/count/{index}\n\
/_cat/recovery\n\
/_cat/recovery/{index}\n\
/_cat/health\n\
/_cat/pending_tasks\n\
/_cat/aliases\n\
/_cat/aliases/{alias}\n\
/_cat/thread_pool\n\
/_cat/thread_pool/{thread_pools}\n\
/_cat/plugins\n\
/_cat/fielddata\n\
/_cat/fielddata/{fields}\n\
/_cat/nodeattrs\n\
/_cat/repositories\n\
/_cat/snapshots/{repository}\n\
/_cat/templates\n\
/_cat/templates/{name}\n";
    ([(axum::http::header::CONTENT_TYPE, "text/plain; charset=UTF-8")], body).into_response()
}

async fn cat_indices_all(State(s): State<Arc<AppState>>, headers: HeaderMap, Query(q): Query<CatQ>) -> Response {
    cat_indices_impl(None, s, headers, q).await
}
async fn cat_indices_scoped(Path(t): Path<String>, State(s): State<Arc<AppState>>, headers: HeaderMap, Query(q): Query<CatQ>) -> Response {
    cat_indices_impl(Some(t), s, headers, q).await
}
async fn cat_indices_impl(target: Option<String>, s: Arc<AppState>, headers: HeaderMap, q: CatQ) -> Response {
    let names: Vec<String> = match target {
        Some(ref t) if t == "_all" || t == "*" => s.engine.list_collections(),
        Some(t) => s.engine.resolve(&t).unwrap_or_default().into_iter().map(|c| c.name.clone()).collect(),
        None => s.engine.list_collections(),
    };
    let want_json = q.format.as_deref() == Some("json") || wants_json(&headers);
    if want_json {
        let arr: Vec<J> = names.into_iter().filter_map(|n| {
            let c = s.engine.get_collection(&n)?;
            let docs = c.row_count().unwrap_or(0);
            let size = std::fs::metadata(&c.path).map(|m| m.len()).unwrap_or(0);
            Some(json!({
                "health": "green", "status": "open",
                "index": n, "uuid": &n, "pri": "1", "rep": "0",
                "docs.count": docs.to_string(), "docs.deleted": "0",
                "store.size": size.to_string(), "pri.store.size": size.to_string(),
            }))
        }).collect();
        return Json(arr).into_response();
    }
    let mut rows: Vec<Vec<String>> = Vec::new();
    for n in names {
        let c = match s.engine.get_collection(&n) { Some(c) => c, None => continue };
        let docs = c.row_count().unwrap_or(0);
        let size = std::fs::metadata(&c.path).map(|m| m.len()).unwrap_or(0);
        rows.push(vec![
            "green".into(), "open".into(), n.clone(), n,
            "1".into(), "0".into(),
            docs.to_string(), "0".into(), size.to_string(),
        ]);
    }
    let headers_row = ["health","status","index","uuid","pri","rep","docs.count","docs.deleted","store.size"];
    let right_just  = [false,false,false,false,true,true,true,true,true];
    let text = render_cat_table(&headers_row, &right_just, &rows, q.v);
    (StatusCode::OK, text).into_response()
}

fn render_cat_table(
    headers: &[&str],
    right_just: &[bool],
    rows: &[Vec<String>],
    show_header: bool,
) -> String {
    let n = headers.len();
    let mut widths = vec![0usize; n];
    if show_header {
        for i in 0..n { widths[i] = headers[i].len(); }
    }
    for r in rows {
        for i in 0..n.min(r.len()) { if r[i].len() > widths[i] { widths[i] = r[i].len(); } }
    }
    let mut out = String::new();
    let emit_row = |out: &mut String, cells: &[&str]| {
        for i in 0..n {
            let cell = cells.get(i).copied().unwrap_or("");
            let pad = widths[i].saturating_sub(cell.len());
            if right_just[i] {
                for _ in 0..pad { out.push(' '); }
                out.push_str(cell);
            } else {
                out.push_str(cell);
                if i + 1 < n { for _ in 0..pad { out.push(' '); } }
            }
            if i + 1 < n { out.push(' '); }
        }
        out.push('\n');
    };
    if show_header { emit_row(&mut out, headers); }
    for r in rows {
        let refs: Vec<&str> = r.iter().map(|s| s.as_str()).collect();
        emit_row(&mut out, &refs);
    }
    out
}

async fn cat_aliases(State(s): State<Arc<AppState>>, headers: HeaderMap, Query(q): Query<CatQ>) -> Response {
    let list = s.engine.catalog.list_aliases().unwrap_or_default();
    if q.format.as_deref() == Some("json") || wants_json(&headers) {
        let arr: Vec<J> = list.into_iter().flat_map(|(a, ts)| ts.into_iter().map(move |t| json!({ "alias": a, "index": t }))).collect();
        return Json(arr).into_response();
    }
    let rows: Vec<Vec<String>> = list.into_iter()
        .flat_map(|(a, ts)| ts.into_iter().map(move |t| vec![a.clone(), t]))
        .collect();
    let headers_row = ["alias","index"];
    let right_just = [false,false];
    (StatusCode::OK, render_cat_table(&headers_row, &right_just, &rows, q.v)).into_response()
}

/// Returns `{}` as JSON. Used for endpoints we don't implement (ILM/ISM)
/// but which db.pl calls with its "dontcheck" flag + unconditional
/// from_json() — an empty body would crash the parser.
async fn empty_object() -> Response {
    Json(json!({})).into_response()
}

fn wants_json(h: &HeaderMap) -> bool {
    h.get("accept").and_then(|v| v.to_str().ok()).map_or(false, |v| v.contains("json"))
        || h.get("content-type").and_then(|v| v.to_str().ok()).map_or(false, |v| v.contains("json"))
}

// -------- index lifecycle -----------------------------------------------

async fn index_exists(Path(idx): Path<String>, State(s): State<Arc<AppState>>) -> Response {
    if s.engine.has_collection(&idx) { StatusCode::OK.into_response() }
    else { StatusCode::NOT_FOUND.into_response() }
}
async fn create_index(Path(idx): Path<String>, State(s): State<Arc<AppState>>, body: Option<Json<J>>) -> Response {
    let col = match s.engine.ensure_collection(&idx) { Ok(c) => c, Err(e) => return internal(e) };
    apply_matching_templates(&s, &idx, &col);
    if let Some(Json(b)) = body {
        // Accept {mappings: {...}} or {mappings: {properties: {...}}}
        let m = b.get("mappings").cloned().unwrap_or(J::Null);
        if !m.is_null() {
            if let Err(e) = apply_mapping_to_col(&s, &col, &m) { return internal(e); }
        }
    }
    Json(json!({ "acknowledged": true, "index": idx })).into_response()
}
#[derive(Deserialize, Default)]
struct DeleteIdxQ {
    ignore_unavailable: Option<String>,
    #[allow(dead_code)] allow_no_indices: Option<String>,
    #[allow(dead_code)] expand_wildcards: Option<String>,
}

async fn delete_index(Path(idx): Path<String>, State(s): State<Arc<AppState>>, Query(q): Query<DeleteIdxQ>) -> Response {
    let ignore = matches!(q.ignore_unavailable.as_deref(), Some("true"));
    // Expand comma-separated patterns and wildcards.
    let mut any = false;
    let mut last_err: Option<Error> = None;
    for part in idx.split(',').map(|x| x.trim()).filter(|x| !x.is_empty()) {
        let targets: Vec<String> = if part.contains('*') || part.contains('?') {
            s.engine.resolve(part).unwrap_or_default().into_iter().map(|c| c.name.clone()).collect()
        } else {
            vec![part.to_string()]
        };
        for name in targets {
            match s.engine.delete_collection(&name) {
                Ok(true) => { any = true; }
                Ok(false) => {}
                Err(e) => { last_err = Some(e); }
            }
        }
    }
    if let Some(e) = last_err { if !ignore { return internal(e); } }
    if !any && !ignore {
        // ES returns 404 for non-existent indices unless ignore_unavailable
        return err(StatusCode::NOT_FOUND, &format!("no such index: {idx}"));
    }
    Json(json!({ "acknowledged": true })).into_response()
}
async fn get_index(Path(idx): Path<String>, State(s): State<Arc<AppState>>) -> Response {
    let Some(col) = s.engine.get_collection(&idx) else { return err(StatusCode::NOT_FOUND, "not found"); };
    let schema = col.schema.read().clone();
    let mapping = schema_to_mapping(&schema);
    Json(json!({ idx.clone(): { "aliases": {}, "mappings": mapping, "settings": {} } })).into_response()
}

async fn get_mapping(Path(idx): Path<String>, State(s): State<Arc<AppState>>) -> Response {
    let cols = match s.engine.resolve(&idx) {
        Ok(v) if !v.is_empty() => v,
        _ => return err(StatusCode::NOT_FOUND, "not found"),
    };
    let mut out = serde_json::Map::new();
    for col in cols {
        let schema = col.schema.read().clone();
        let m = schema_to_mapping(&schema);
        out.insert(col.name.clone(), json!({ "mappings": m }));
    }
    Json(J::Object(out)).into_response()
}
async fn put_mapping(Path(idx): Path<String>, State(s): State<Arc<AppState>>, Json(body): Json<J>) -> Response {
    let col = match s.engine.ensure_collection(&idx) { Ok(c) => c, Err(e) => return internal(e) };
    if let Err(e) = apply_mapping_to_col(&s, &col, &body) { return internal(e); }
    Json(json!({ "acknowledged": true })).into_response()
}

/// Merge fields from an ES mapping JSON into the collection's schema. Accepts:
///   {"properties": {...}}  (the inner form)
///   {"mappings": {"properties": {...}}}  (the PUT /:idx outer form — unwrapped by caller)
///   Legacy ES 6 type wrapper {"_doc": {"properties": {...}}}
/// Existing (inferred) field types are kept; new explicit types are added.
fn apply_mapping_to_col(s: &Arc<AppState>, col: &Arc<arkimedb_storage::Collection>, m: &J) -> Result<()> {
    let mut schema = col.schema.write();
    let sm = &mut *schema;
    if let Some(props) = find_properties(m) {
        extract_props_recursive("", props, &mut sm.fields, &mut sm.copy_to);
    }
    // Look for dynamic_templates either at top level or nested under mappings.
    let dts = find_dynamic_templates(m);
    if !dts.is_empty() {
        // Append, dedup by (match_glob, path_match, match_mapping_type, field_type).
        for dt in dts {
            let dup = sm.dynamic_templates.iter().any(|x|
                x.match_glob == dt.match_glob
                && x.path_match == dt.path_match
                && x.match_mapping_type == dt.match_mapping_type
                && x.field_type == dt.field_type
            );
            if !dup { sm.dynamic_templates.push(dt); }
        }
    }
    if let Some(meta) = m.get("_meta").and_then(|v| v.as_object()) {
        for (k, v) in meta { schema.metadata.insert(k.clone(), v.clone()); }
    }
    s.engine.catalog.register_collection(&col.name, &schema)?;
    s.engine.field_catalog.replace(&col.name, schema.clone());
    Ok(())
}

fn find_dynamic_templates(m: &J) -> Vec<arkimedb_core::DynamicTemplate> {
    fn collect(v: &J, out: &mut Vec<arkimedb_core::DynamicTemplate>) {
        if let Some(arr) = v.get("dynamic_templates").and_then(|x| x.as_array()) {
            for entry in arr {
                // Each entry is { "name": { match: ..., mapping: { type: ... } } }
                if let Some(o) = entry.as_object() {
                    for (_name, body) in o {
                        let dt = parse_dyn_template(body);
                        if dt.field_type != arkimedb_core::FieldType::Keyword
                            || dt.match_glob.is_some()
                            || dt.path_match.is_some()
                            || dt.match_mapping_type.is_some()
                        {
                            out.push(dt);
                        }
                    }
                }
            }
        }
        if let Some(inner) = v.get("mappings") { collect(inner, out); }
    }
    let mut out = Vec::new();
    collect(m, &mut out);
    out
}

fn parse_dyn_template(body: &J) -> arkimedb_core::DynamicTemplate {
    use arkimedb_core::FieldType;
    let mapping = body.get("mapping").cloned().unwrap_or(J::Null);
    let t = mapping.get("type").and_then(|v| v.as_str()).unwrap_or("keyword");
    let ft = match t {
        "text" => FieldType::Text,
        "keyword" | "constant_keyword" => FieldType::Keyword,
        "ip" => FieldType::Ip,
        "long" | "integer" | "short" | "byte" => FieldType::I64,
        "unsigned_long" => FieldType::U64,
        "float" | "double" | "half_float" | "scaled_float" => FieldType::F64,
        "boolean" => FieldType::Bool,
        "date" | "date_nanos" => FieldType::Timestamp,
        _ => FieldType::Keyword,
    };
    let mut copy_to = Vec::new();
    if let Some(ct) = mapping.get("copy_to") {
        match ct {
            J::String(s) => copy_to.push(s.clone()),
            J::Array(a) => for v in a { if let Some(s) = v.as_str() { copy_to.push(s.to_string()); } },
            _ => {}
        }
    }
    arkimedb_core::DynamicTemplate {
        match_glob: body.get("match").and_then(|v| v.as_str()).map(String::from),
        path_match: body.get("path_match").and_then(|v| v.as_str()).map(String::from),
        match_mapping_type: body.get("match_mapping_type").and_then(|v| v.as_str()).map(String::from),
        field_type: ft,
        copy_to,
    }
}

/// Pre-flight: ensure the named collection exists and that any matching
/// stored templates' mappings/dynamic_templates have been merged into its
/// schema. Safe to call repeatedly (idempotent on already-initialized cols).
fn ensure_with_templates(s: &Arc<AppState>, idx_name: &str) {
    if idx_name.is_empty() || idx_name.contains(',') || idx_name.contains('*') || idx_name.contains('?') { return; }
    let col = match s.engine.ensure_collection(idx_name) { Ok(c) => c, Err(_) => return };
    apply_matching_templates(s, idx_name, &col);
}

/// On a brand-new (or schema-empty) collection, find every stored template
/// whose `index_patterns` matches `idx_name` and merge its mappings into the
/// collection's schema. Idempotent when the schema already has fields.
fn apply_matching_templates(s: &Arc<AppState>, idx_name: &str, col: &Arc<arkimedb_storage::Collection>) {
    let already_initialized = {
        let r = col.schema.read();
        !r.fields.is_empty() || !r.dynamic_templates.is_empty() || !r.copy_to.is_empty()
    };
    if already_initialized { return; }
    let templates = match s.engine.catalog.list_templates() {
        Ok(v) => v,
        Err(_) => return,
    };
    // Parse, filter by index_patterns, then sort by `order` DESCENDING so
    // higher-priority templates win for both static properties (first-wins via
    // `or_insert` in extract_props_recursive) and dynamic_templates (earlier in
    // the list = matched first by `match_dynamic_template`).
    let mut matched: Vec<(i64, J)> = Vec::new();
    for (_name, body) in templates {
        let parsed: J = match serde_json::from_slice(&body) { Ok(v) => v, Err(_) => continue };
        let patterns: Vec<String> = parsed.get("index_patterns").and_then(|v| v.as_array())
            .map(|a| a.iter().filter_map(|x| x.as_str().map(String::from)).collect())
            .or_else(|| parsed.get("index_patterns").and_then(|v| v.as_str()).map(|s| vec![s.to_string()]))
            .unwrap_or_default();
        if !patterns.iter().any(|p| arkimedb_core::schema_glob_match(p, idx_name)) { continue; }
        let order = parsed.get("order").and_then(|v| v.as_i64()).unwrap_or(0);
        matched.push((order, parsed));
    }
    matched.sort_by(|a, b| b.0.cmp(&a.0));
    for (_order, parsed) in matched {
        // ES `_index_template` puts mappings under {template:{mappings:...}}, legacy under {mappings:...}.
        let body_for_mapping = parsed.get("template").cloned().unwrap_or(parsed);
        let _ = apply_mapping_to_col(s, col, &body_for_mapping);
    }
}

fn find_properties(m: &J) -> Option<&serde_json::Map<String, J>> {
    if let Some(p) = m.get("properties").and_then(|v| v.as_object()) { return Some(p); }
    if let Some(o) = m.get("mappings") { return find_properties(o); }
    // ES6-style type wrapper: {"_doc": {"properties": ...}} or arbitrary single key
    if let Some(obj) = m.as_object() {
        for v in obj.values() {
            if let Some(p) = v.get("properties").and_then(|x| x.as_object()) { return Some(p); }
        }
    }
    None
}

fn extract_props_recursive(
    prefix: &str,
    props: &serde_json::Map<String, J>,
    out: &mut std::collections::BTreeMap<String, arkimedb_core::FieldType>,
    copy_to: &mut std::collections::BTreeMap<String, Vec<String>>,
) {
    for (name, spec) in props {
        let full = if prefix.is_empty() { name.clone() } else { format!("{prefix}.{name}") };
        // Nested object: recurse.
        if let Some(inner) = spec.get("properties").and_then(|v| v.as_object()) {
            extract_props_recursive(&full, inner, out, copy_to);
            continue;
        }
        let t = spec.get("type").and_then(|v| v.as_str()).unwrap_or("keyword");
        let ft = match t {
            "text" => arkimedb_core::FieldType::Text,
            "keyword" | "constant_keyword" => arkimedb_core::FieldType::Keyword,
            "ip" => arkimedb_core::FieldType::Ip,
            "long" | "integer" | "short" | "byte" => arkimedb_core::FieldType::I64,
            "unsigned_long" => arkimedb_core::FieldType::U64,
            "float" | "double" | "half_float" | "scaled_float" => arkimedb_core::FieldType::F64,
            "boolean" => arkimedb_core::FieldType::Bool,
            "date" | "date_nanos" => arkimedb_core::FieldType::Timestamp,
            "object" | "nested" | "flattened" => arkimedb_core::FieldType::Json,
            _ => arkimedb_core::FieldType::Keyword,
        };
        out.entry(full.clone()).or_insert(ft);
        // Honor `copy_to: "x"` or `copy_to: ["x","y"]`.
        if let Some(ct) = spec.get("copy_to") {
            let mut targets: Vec<String> = Vec::new();
            match ct {
                J::String(s) => targets.push(s.clone()),
                J::Array(a) => for v in a { if let Some(s) = v.as_str() { targets.push(s.to_string()); } },
                _ => {}
            }
            if !targets.is_empty() {
                copy_to.entry(full).or_default().extend(targets);
            }
        }
    }
}
fn schema_to_mapping(sch: &arkimedb_core::CollectionSchema) -> J {
    let mut props = serde_json::Map::new();
    for (k, t) in &sch.fields {
        let tn = match t {
            arkimedb_core::FieldType::Keyword => "keyword",
            arkimedb_core::FieldType::Text => "text",
            arkimedb_core::FieldType::Ip => "ip",
            arkimedb_core::FieldType::I64 => "long",
            arkimedb_core::FieldType::U64 => "long",
            arkimedb_core::FieldType::F64 => "double",
            arkimedb_core::FieldType::Bool => "boolean",
            arkimedb_core::FieldType::Timestamp => "date",
            arkimedb_core::FieldType::Json => "object",
        };
        let mut spec = serde_json::Map::new();
        spec.insert("type".into(), json!(tn));
        if let Some(targets) = sch.copy_to.get(k) {
            if targets.len() == 1 {
                spec.insert("copy_to".into(), json!(targets[0]));
            } else {
                spec.insert("copy_to".into(), json!(targets));
            }
        }
        props.insert(k.clone(), J::Object(spec));
    }
    let mut out = serde_json::Map::new();
    if !sch.metadata.is_empty() {
        out.insert("_meta".into(), J::Object(sch.metadata.clone()));
    }
    out.insert("properties".into(), J::Object(props));
    J::Object(out)
}

// -------- document CRUD --------------------------------------------------

#[derive(Deserialize, Default)]
struct WriteQ { op_type: Option<String>, refresh: Option<String> }

fn is_refresh_true(v: &Option<String>) -> bool {
    matches!(v.as_deref(), Some("true") | Some("wait_for") | Some("1") | Some(""))
}

async fn index_doc_auto_id(Path(idx): Path<String>, State(s): State<Arc<AppState>>, Query(q): Query<WriteQ>, Json(body): Json<J>) -> Response {
    ensure_with_templates(&s, &idx);
    let op = BulkOp { collection: Some(idx.clone()), kind: BulkKind::Index { id: None, source: body } };
    let engine = s.engine.clone();
    let refresh = is_refresh_true(&q.refresh);
    let idx_c = idx.clone();
    match blocking(move || {
        let r = engine.bulk_write(Some(&idx_c), vec![op]);
        if refresh { let _ = engine.refresh(Some(&idx_c)); }
        r
    }).await {
        Ok(mut v) => outcome_to_response(v.pop().unwrap()),
        Err(e) => internal(e),
    }
}
async fn index_doc(Path((idx, id)): Path<(String, String)>, State(s): State<Arc<AppState>>, Query(q): Query<WriteQ>, Json(body): Json<J>) -> Response {
    ensure_with_templates(&s, &idx);
    let kind = if q.op_type.as_deref() == Some("create") {
        BulkKind::Create { id, source: body }
    } else {
        BulkKind::Index { id: Some(id), source: body }
    };
    let op = BulkOp { collection: Some(idx.clone()), kind };
    let engine = s.engine.clone();
    let refresh = is_refresh_true(&q.refresh);
    let idx_c = idx.clone();
    match blocking(move || {
        let r = engine.bulk_write(Some(&idx_c), vec![op]);
        if refresh { let _ = engine.refresh(Some(&idx_c)); }
        r
    }).await {
        Ok(mut v) => outcome_to_response(v.pop().unwrap()),
        Err(e) => internal(e),
    }
}
async fn create_doc(Path((idx, id)): Path<(String, String)>, State(s): State<Arc<AppState>>, Query(q): Query<WriteQ>, Json(body): Json<J>) -> Response {
    ensure_with_templates(&s, &idx);
    let op = BulkOp { collection: Some(idx.clone()), kind: BulkKind::Create { id, source: body } };
    let engine = s.engine.clone();
    let refresh = is_refresh_true(&q.refresh);
    let idx_c = idx.clone();
    match blocking(move || {
        let r = engine.bulk_write(Some(&idx_c), vec![op]);
        if refresh { let _ = engine.refresh(Some(&idx_c)); }
        r
    }).await {
        Ok(mut v) => outcome_to_response(v.pop().unwrap()),
        Err(e) => internal(e),
    }
}
async fn update_doc(Path((idx, id)): Path<(String, String)>, State(s): State<Arc<AppState>>, Query(q): Query<WriteQ>, Json(body): Json<J>) -> Response {
    ensure_with_templates(&s, &idx);
    // Script path: interpret known Arkime painless scripts (tags, hunt).
    if let Some(script) = body.get("script") {
        return apply_script_update(&s, &idx, &id, script).await;
    }
    // Elasticsearch _update semantics:
    //   { "doc": {...} }                        -> merge doc into existing
    //   { "doc": {...}, "doc_as_upsert": true } -> merge; if missing, insert doc
    //   { "doc": {...}, "upsert": {...} }       -> merge; if missing, insert the upsert body
    //   { "upsert": {...} }                     -> insert upsert body if missing, else noop
    let doc          = body.get("doc").cloned();
    let upsert       = body.get("upsert").cloned();
    let doc_as_upsert = body.get("doc_as_upsert").and_then(|v| v.as_bool()).unwrap_or(false);

    // Try a partial update first when we have a doc body.
    if let Some(d) = doc.clone() {
        let op = BulkOp { collection: Some(idx.clone()), kind: BulkKind::Update { id: id.clone(), doc: d } };
        let engine = s.engine.clone();
        let idx_c = idx.clone();
        let refresh = is_refresh_true(&q.refresh);
        match blocking(move || {
            let r = engine.bulk_write(Some(&idx_c), vec![op]);
            if refresh { let _ = engine.refresh(Some(&idx_c)); }
            r
        }).await {
            Ok(mut v) => {
                let o = v.pop().unwrap();
                if o.ok.is_some() { return outcome_to_response(o); }
                // Fall through to upsert path if the failure was "not found".
                let missing = o.error.as_deref().map_or(false, |e| e.contains("not found"));
                if !missing { return outcome_to_response(o); }
            }
            Err(e) => return internal(e),
        }
    }

    // Upsert path: insert either the explicit upsert body, or the doc itself if doc_as_upsert.
    let insert_body = upsert.or_else(|| if doc_as_upsert { doc } else { None });
    let Some(src) = insert_body else {
        return err(StatusCode::NOT_FOUND, &format!("document_missing: {idx}/{id}"));
    };
    let op = BulkOp { collection: Some(idx.clone()), kind: BulkKind::Index { id: Some(id), source: src } };
    let engine = s.engine.clone();
    let refresh = is_refresh_true(&q.refresh);
    let idx_c = idx.clone();
    match blocking(move || {
        let r = engine.bulk_write(Some(&idx_c), vec![op]);
        if refresh { let _ = engine.refresh(Some(&idx_c)); }
        r
    }).await {
        Ok(mut v) => outcome_to_response(v.pop().unwrap()),
        Err(e) => internal(e),
    }
}
async fn delete_doc(Path((idx, id)): Path<(String, String)>, State(s): State<Arc<AppState>>, Query(q): Query<WriteQ>) -> Response {
    let op = BulkOp { collection: Some(idx.clone()), kind: BulkKind::Delete { id } };
    let engine = s.engine.clone();
    let refresh = is_refresh_true(&q.refresh);
    let idx_c = idx.clone();
    match blocking(move || {
        let r = engine.bulk_write(Some(&idx_c), vec![op]);
        if refresh { let _ = engine.refresh(Some(&idx_c)); }
        r
    }).await {
        Ok(mut v) => outcome_to_response(v.pop().unwrap()),
        Err(e) => internal(e),
    }
}

fn apply_script_mutation(src: &mut J, script_src: &str, params: &J) -> Result<()> {
    // Pattern match on known Arkime painless scripts; interpret in Rust.
    let ps = script_src;
    let obj = match src {
        J::Object(m) => m,
        _ => return Err(Error::BadRequest("source is not an object".into())),
    };
    let param_tags = params.get("tags").and_then(|v| v.as_array()).cloned();

    // addTagsToSession
    if ps.contains("ctx._source.tags.indexOf") && ps.contains("ctx._source.tags.add") && !ps.contains("remove(idx)") {
        if let Some(tags) = param_tags {
            let mut cur: Vec<J> = obj.get("tags").and_then(|v| v.as_array()).cloned().unwrap_or_default();
            for t in &tags {
                if !cur.iter().any(|x| x == t) { cur.push(t.clone()); }
            }
            let len = cur.len();
            obj.insert("tags".into(), J::Array(cur));
            obj.insert("tagsCnt".into(), J::Number(serde_json::Number::from(len)));
            return Ok(());
        }
    }
    // removeTagsFromSession
    if ps.contains("ctx._source.tags.indexOf") && ps.contains("remove(idx)") {
        if let Some(tags) = param_tags {
            let mut cur: Vec<J> = obj.get("tags").and_then(|v| v.as_array()).cloned().unwrap_or_default();
            cur.retain(|x| !tags.iter().any(|t| t == x));
            if cur.is_empty() {
                obj.remove("tags");
                obj.remove("tagsCnt");
            } else {
                let len = cur.len();
                obj.insert("tags".into(), J::Array(cur));
                obj.insert("tagsCnt".into(), J::Number(serde_json::Number::from(len)));
            }
            return Ok(());
        }
    }
    // addHuntToSession
    if ps.contains("ctx._source.huntId") && ps.contains("ctx._source.huntName") {
        let hunt_id = params.get("huntId").cloned();
        let hunt_name = params.get("huntName").cloned();
        if let (Some(hid), Some(hname)) = (hunt_id, hunt_name) {
            let mut ids: Vec<J> = obj.get("huntId").and_then(|v| v.as_array()).cloned().unwrap_or_default();
            ids.push(hid);
            obj.insert("huntId".into(), J::Array(ids));
            let mut names: Vec<J> = obj.get("huntName").and_then(|v| v.as_array()).cloned().unwrap_or_default();
            names.push(hname);
            obj.insert("huntName".into(), J::Array(names));
            return Ok(());
        }
    }
    Err(Error::BadRequest(format!("unsupported painless script: {}", ps.chars().take(80).collect::<String>())))
}

async fn apply_script_update(s: &Arc<AppState>, idx: &str, id: &str, script: &J) -> Response {
    let src_text = script.get("source").or_else(|| script.get("inline")).and_then(|v| v.as_str()).unwrap_or("").to_string();
    let params = script.get("params").cloned().unwrap_or(J::Null);
    // Find the collection containing the doc.
    let cols = match s.engine.resolve(idx) { Ok(v) => v, Err(e) => return internal(e) };
    let mut found: Option<(Arc<arkimedb_storage::Collection>, Vec<u8>)> = None;
    let mut found_col_name: Option<String> = None;
    for c in cols {
        if let Ok(Some((_rid, _ver, raw))) = c.get_by_id(id) {
            found_col_name = Some(c.name.clone());
            found = Some((c, raw));
            break;
        }
    }
    let (col, _raw_first) = match found {
        Some(x) => x,
        None => return err(StatusCode::NOT_FOUND, &format!("document_missing: {idx}/{id}")),
    };
    // Acquire the per-doc update lock so concurrent script updates to the
    // same doc serialize their read-modify-write cycles.
    let lock_key = format!("{}/{}", found_col_name.as_deref().unwrap_or(""), id);
    let lock = {
        let mut map = s.update_locks.lock();
        map.entry(lock_key.clone()).or_insert_with(|| Arc::new(tokio::sync::Mutex::new(()))).clone()
    };
    let _guard = lock.lock().await;
    // Re-read inside the lock — another concurrent update may have just
    // committed a newer version.
    let raw = match col.get_by_id(id) {
        Ok(Some((_rid, _ver, raw))) => raw,
        Ok(None) => return err(StatusCode::NOT_FOUND, &format!("document_missing: {idx}/{id}")),
        Err(e) => return internal(e),
    };
    let mut cur: J = match serde_json::from_slice(&raw) { Ok(v) => v, Err(e) => return internal(Error::BadRequest(e.to_string())) };
    if let Err(e) = apply_script_mutation(&mut cur, &src_text, &params) {
        return internal(e);
    }
    let op = BulkOp { collection: Some(col.name.clone()), kind: BulkKind::Index { id: Some(id.to_string()), source: cur } };
    let res = s.engine.bulk_write(Some(&col.name), vec![op]);
    match res {
        Ok(mut v) => {
            let mut o = v.pop().unwrap();
            if let Some(ref mut r) = o.ok { r.action = "update"; }
            outcome_to_response(o)
        }
        Err(e) => internal(e),
    }
}

async fn get_doc(Path((idx, id)): Path<(String, String)>, State(s): State<Arc<AppState>>) -> Response {
    let col = match s.engine.resolve(&idx).ok().and_then(|v| v.into_iter().next()) {
        Some(c) => c,
        None => return not_found(&idx, &id),
    };
    match col.get_by_id(&id) {
        Ok(Some((_, version, raw))) => {
            let src: J = serde_json::from_slice(&raw).unwrap_or(J::Null);
            Json(json!({
                "_index": &col.name, "_id": id,
                "_version": version, "found": true, "_source": src
            })).into_response()
        }
        Ok(None) => not_found(&col.name, &id),
        Err(e) => internal(e),
    }
}

async fn get_source(Path((idx, id)): Path<(String, String)>, State(s): State<Arc<AppState>>) -> Response {
    let col = match s.engine.resolve(&idx).ok().and_then(|v| v.into_iter().next()) {
        Some(c) => c,
        None => return not_found(&idx, &id),
    };
    match col.get_by_id(&id) {
        Ok(Some((_, _version, raw))) => {
            let src: J = serde_json::from_slice(&raw).unwrap_or(J::Null);
            Json(src).into_response()
        }
        Ok(None) => not_found(&col.name, &id),
        Err(e) => internal(e),
    }
}

async fn head_source(Path((idx, id)): Path<(String, String)>, State(s): State<Arc<AppState>>) -> Response {
    let col = match s.engine.resolve(&idx).ok().and_then(|v| v.into_iter().next()) {
        Some(c) => c,
        None => return StatusCode::NOT_FOUND.into_response(),
    };
    match col.get_by_id(&id) {
        Ok(Some(_)) => StatusCode::OK.into_response(),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

async fn mget(State(s): State<Arc<AppState>>, body: String) -> Response {
    do_mget(&s, None, body).await
}
async fn mget_scoped(Path(idx): Path<String>, State(s): State<Arc<AppState>>, body: String) -> Response {
    do_mget(&s, Some(idx), body).await
}

/// ES `_mget`: batch-fetch documents by `_id`. Accepts both shapes:
///   { "docs": [{"_index":"i","_id":"1"}, ...] }
///   { "ids":  ["1","2"] }   (when scoped to a single index in the URL)
/// Each result mirrors a `GET /:idx/_doc/:id` payload (with `found:false`
/// for misses). This is what the Arkime viewer uses to load session detail
/// in batches.
async fn do_mget(s: &Arc<AppState>, default_idx: Option<String>, body: String) -> Response {
    let req: J = match serde_json::from_str(&body) {
        Ok(v) => v,
        Err(e) => return err(StatusCode::BAD_REQUEST, &e.to_string()),
    };

    let mut items: Vec<(String, String)> = Vec::new();
    if let Some(arr) = req.get("docs").and_then(|v| v.as_array()) {
        for d in arr {
            let idx = d.get("_index").and_then(|v| v.as_str()).map(String::from)
                .or_else(|| default_idx.clone());
            let id  = d.get("_id").and_then(|v| v.as_str()).map(String::from);
            if let (Some(i), Some(x)) = (idx, id) { items.push((i, x)); }
        }
    } else if let Some(arr) = req.get("ids").and_then(|v| v.as_array()) {
        let Some(idx) = default_idx.clone() else {
            return err(StatusCode::BAD_REQUEST, "_mget {\"ids\":...} requires an index in the URL");
        };
        for v in arr {
            if let Some(id) = v.as_str() { items.push((idx.clone(), id.into())); }
        }
    } else {
        return err(StatusCode::BAD_REQUEST, "_mget body needs `docs` or `ids`");
    }

    let mut out = Vec::with_capacity(items.len());
    for (idx, id) in items {
        let col = s.engine.resolve(&idx).ok().and_then(|v| v.into_iter().next());
        let entry = match col {
            None => json!({ "_index": idx, "_id": id, "found": false }),
            Some(col) => match col.get_by_id(&id) {
                Ok(Some((_, version, raw))) => {
                    let src: J = serde_json::from_slice(&raw).unwrap_or(J::Null);
                    json!({
                        "_index": &col.name, "_id": id,
                        "_version": version, "found": true, "_source": src
                    })
                }
                Ok(None) => json!({ "_index": &col.name, "_id": id, "found": false }),
                Err(e) => json!({ "_index": &col.name, "_id": id, "found": false,
                                   "error": { "reason": e.to_string() } }),
            },
        };
        out.push(entry);
    }
    Json(json!({ "docs": out })).into_response()
}

// -------- refresh / count / search --------------------------------------

async fn refresh_all(State(s): State<Arc<AppState>>) -> Response {
    let engine = s.engine.clone();
    match blocking(move || engine.refresh(None)).await {
        Ok(_) => Json(json!({ "_shards": { "total": 1, "successful": 1, "failed": 0 } })).into_response(),
        Err(e) => internal(e),
    }
}
async fn refresh_one(Path(idx): Path<String>, State(s): State<Arc<AppState>>) -> Response {
    let engine = s.engine.clone();
    match blocking(move || engine.refresh(Some(&idx))).await {
        Ok(_) => Json(json!({ "_shards": { "total": 1, "successful": 1, "failed": 0 } })).into_response(),
        Err(e) => internal(e),
    }
}

#[derive(Deserialize, Default)]
#[allow(dead_code)]
struct CountQ { q: Option<String> }
async fn count(Path(idx): Path<String>, Query(qs): Query<CountQ>, State(s): State<Arc<AppState>>, body: Option<Json<J>>) -> Response {
    let cols = match s.engine.resolve(&idx) { Ok(v) => v, Err(e) => return internal(e) };
    count_cols(cols, body, qs.q.as_deref()).await
}

async fn count_all(Query(qs): Query<CountQ>, State(s): State<Arc<AppState>>, body: Option<Json<J>>) -> Response {
    let names = s.engine.list_collections();
    let cols: Vec<_> = names.iter().filter_map(|n| s.engine.get_collection(n)).collect();
    count_cols(cols, body, qs.q.as_deref()).await
}

async fn count_cols(cols: Vec<std::sync::Arc<arkimedb_storage::Collection>>, body: Option<Json<J>>, q_param: Option<&str>) -> Response {
    let mut query_json = body.map(|b| b.0.get("query").cloned().unwrap_or_else(|| json!({"match_all": {}}))).unwrap_or(json!({"match_all": {}}));
    if let Some(qs) = q_param {
        if matches!(&query_json, J::Object(m) if m.contains_key("match_all")) {
            query_json = lucene_q_to_query(qs);
        }
    }
    // Fast path: match_all => row_count - tombstones for each collection
    // avoids compiling/evaluating the query (big win on /_count and
    // /_cat/indices/*/count style endpoints the perl tests hit often).
    let is_match_all = matches!(&query_json, J::Object(m) if m.len() == 1 && m.contains_key("match_all"));
    let mut total = 0u64;
    if is_match_all {
        for c in &cols {
            let n = c.row_count().unwrap_or(0);
            total += n;
        }
    } else {
        let cols_c = cols.clone();
        let qj = query_json.clone();
        let r: std::result::Result<u64, arkimedb_core::Error> = blocking(move || {
            let mut t = 0u64;
            for c in &cols_c {
                let q = arkimedb_query::compile_es_query(&qj, c)?;
                let bm = q.eval(c)?;
                t += bm.len();
            }
            Ok(t)
        }).await;
        match r { Ok(t) => total = t, Err(e) => return internal(e) }
    }
    Json(json!({ "count": total, "_shards": { "total": cols.len(), "successful": cols.len(), "failed": 0 } })).into_response()
}

#[derive(Deserialize, Default)]
#[allow(dead_code)]
struct ByQueryQ {
    conflicts: Option<String>,
    refresh: Option<String>,
    wait_for_completion: Option<String>,
    scroll: Option<String>,
    scroll_size: Option<usize>,
}

async fn delete_by_query(
    Path(idx): Path<String>,
    Query(qs): Query<ByQueryQ>,
    State(s): State<Arc<AppState>>,
    body: Option<Json<J>>,
) -> Response {
    let cols = match s.engine.resolve(&idx) { Ok(v) => v, Err(e) => return internal(e) };
    let query_json = body
        .map(|b| b.0.get("query").cloned().unwrap_or_else(|| json!({"match_all": {}})))
        .unwrap_or(json!({"match_all": {}}));

    let t0 = std::time::Instant::now();
    let mut ops: Vec<BulkOp> = Vec::new();
    let mut total: u64 = 0;

    for c in &cols {
        let q = match arkimedb_query::compile_es_query(&query_json, c) { Ok(v) => v, Err(e) => return internal(e) };
        let bm = match q.eval(c) { Ok(v) => v, Err(e) => return internal(e) };
        total += bm.len();
        for row_id in bm.iter() {
            match c.doc_id_of(row_id) {
                Ok(Some(id)) => ops.push(BulkOp {
                    collection: Some(c.name.clone()),
                    kind: BulkKind::Delete { id },
                }),
                Ok(None) => {}
                Err(e) => return internal(e),
            }
        }
    }

    let outcomes = match s.engine.bulk_write(Some(&idx), ops) {
        Ok(v) => v, Err(e) => return internal(e),
    };

    let mut deleted = 0u64;
    let mut failures: Vec<J> = Vec::new();
    for o in &outcomes {
        match &o.ok {
            Some(r) if r.action == "delete" => deleted += 1,
            None => failures.push(json!({
                "index": o.collection,
                "cause": { "type": "error", "reason": o.error.clone().unwrap_or_default() }
            })),
            _ => {}
        }
    }

    if matches!(qs.refresh.as_deref(), Some("true") | Some("wait_for") | Some("1")) {
        let _ = s.engine.refresh(Some(&idx));
    }

    Json(json!({
        "took": t0.elapsed().as_millis() as u64,
        "timed_out": false,
        "total": total,
        "deleted": deleted,
        "batches": 1,
        "version_conflicts": 0,
        "noops": 0,
        "retries": { "bulk": 0, "search": 0 },
        "throttled_millis": 0,
        "requests_per_second": -1.0,
        "throttled_until_millis": 0,
        "failures": failures,
    })).into_response()
}

async fn update_by_query(
    Path(idx): Path<String>,
    Query(qs): Query<ByQueryQ>,
    State(s): State<Arc<AppState>>,
    body: Option<Json<J>>,
) -> Response {
    // Minimal implementation: supports body with optional "query"; no "script" support.
    // Effectively a reindex-in-place noop with refresh — used by Arkime init in a few spots
    // just to force reindex/refresh. Returns the updated count but makes no doc changes
    // unless a script is provided (not yet implemented).
    let cols = match s.engine.resolve(&idx) { Ok(v) => v, Err(e) => return internal(e) };
    let body_j = body.map(|b| b.0).unwrap_or_else(|| json!({}));
    let query_json = body_j.get("query").cloned().unwrap_or_else(|| json!({"match_all": {}}));

    if body_j.get("script").is_some() {
        return err(StatusCode::NOT_IMPLEMENTED, "_update_by_query with script not supported");
    }

    let t0 = std::time::Instant::now();
    let mut total = 0u64;
    for c in &cols {
        let q = match arkimedb_query::compile_es_query(&query_json, c) { Ok(v) => v, Err(e) => return internal(e) };
        let bm = match q.eval(c) { Ok(v) => v, Err(e) => return internal(e) };
        total += bm.len();
    }

    if matches!(qs.refresh.as_deref(), Some("true") | Some("wait_for") | Some("1")) {
        let _ = s.engine.refresh(Some(&idx));
    }

    Json(json!({
        "took": t0.elapsed().as_millis() as u64,
        "timed_out": false,
        "total": total,
        "updated": 0,
        "deleted": 0,
        "batches": 1,
        "version_conflicts": 0,
        "noops": total,
        "retries": { "bulk": 0, "search": 0 },
        "throttled_millis": 0,
        "requests_per_second": -1.0,
        "throttled_until_millis": 0,
        "failures": [],
    })).into_response()
}

#[derive(Deserialize, Default)]
#[allow(dead_code)]
struct SearchQ {
    scroll: Option<String>,
    size: Option<usize>,
    from: Option<usize>,
    rest_total_hits_as_int: Option<bool>,
    q: Option<String>,
    sort: Option<String>,
}

/// Convert a tiny subset of Lucene query-string syntax (`field:value`) into an
/// ES JSON query. Used when callers pass `q=` on the URL (e.g. `db.pl rm`).
/// Strips backslash escapes from the value (db.pl escapes `/` as `\/`).
fn lucene_q_to_query(q: &str) -> J {
    let q = q.trim();
    if q.is_empty() { return json!({"match_all": {}}); }
    // Only support `field:value` (no AND/OR/grouping); fall back to match_all.
    let Some(colon) = q.find(':') else { return json!({"match_all": {}}); };
    let field = &q[..colon];
    let raw   = &q[colon+1..];
    // Unescape backslashes (db.pl escapes `/`, `:`, `.`, etc.)
    let mut val = String::with_capacity(raw.len());
    let mut it = raw.chars();
    while let Some(c) = it.next() {
        if c == '\\' { if let Some(n) = it.next() { val.push(n); } }
        else { val.push(c); }
    }
    // Strip optional quotes.
    let v = val.trim_matches('"');
    if v.contains('*') || v.contains('?') {
        json!({"wildcard": { field: v }})
    } else {
        json!({"term": { field: v }})
    }
}

/// ES "terms lookup": a `terms` clause whose value is an object with
/// `{index, id, path}` is resolved by fetching the referenced doc and
/// replacing the clause's value with the array found at `path`.
/// Walks the query tree in-place.
fn resolve_terms_lookups(j: &mut J, engine: &Arc<arkimedb_storage::Engine>) -> std::result::Result<(), String> {
    fn extract_path(doc: &J, path: &str) -> Option<J> {
        let mut cur = doc;
        for p in path.split('.') {
            cur = cur.get(p)?;
        }
        Some(cur.clone())
    }
    fn walk(j: &mut J, engine: &Arc<arkimedb_storage::Engine>) -> std::result::Result<(), String> {
        match j {
            J::Object(m) => {
                // Resolve a terms-lookup sibling first (so the replacement
                // doesn't get re-walked as an object).
                if let Some(terms) = m.get_mut("terms") {
                    if let J::Object(tm) = terms {
                        // Single key: {<field>: <spec>} — where <spec> may be an array OR a lookup object.
                        let keys: Vec<String> = tm.keys().cloned().collect();
                        for k in keys {
                            if let Some(spec) = tm.get(&k).cloned() {
                                if let J::Object(so) = &spec {
                                    let index = so.get("index").and_then(|v| v.as_str());
                                    let id    = so.get("id").and_then(|v| v.as_str());
                                    let path  = so.get("path").and_then(|v| v.as_str());
                                    if let (Some(i), Some(d), Some(p)) = (index, id, path) {
                                        let cols = engine.resolve(i).map_err(|e| e.to_string())?;
                                        let mut found: Option<J> = None;
                                        for c in cols {
                                            if let Some((_, _, raw)) = c.get_by_id(d).map_err(|e| e.to_string())? {
                                                let doc: J = serde_json::from_slice(&raw).map_err(|e| e.to_string())?;
                                                found = extract_path(&doc, p);
                                                break;
                                            }
                                        }
                                        let arr = match found {
                                            Some(J::Array(a)) => J::Array(a),
                                            Some(other) => J::Array(vec![other]),
                                            None => J::Array(vec![]),
                                        };
                                        tm.insert(k, arr);
                                    }
                                }
                            }
                        }
                    }
                }
                for (_, v) in m.iter_mut() { walk(v, engine)?; }
            }
            J::Array(a) => { for v in a { walk(v, engine)?; } }
            _ => {}
        }
        Ok(())
    }
    walk(j, engine)
}

fn apply_total_hits_as_int(v: &mut J, as_int: bool) {
    if !as_int { return; }
    if let Some(hits) = v.get_mut("hits").and_then(|h| h.as_object_mut()) {
        if let Some(total) = hits.get("total").and_then(|t| t.get("value")).and_then(|n| n.as_i64()) {
            hits.insert("total".into(), json!(total));
        }
    }
}

async fn search(
    Path(idx): Path<String>,
    Query(q): Query<SearchQ>,
    State(s): State<Arc<AppState>>,
    body: bytes::Bytes,
) -> Response {
    search_impl(Some(idx), q, s, body).await
}

async fn search_all(
    Query(q): Query<SearchQ>,
    State(s): State<Arc<AppState>>,
    body: bytes::Bytes,
) -> Response {
    search_impl(None, q, s, body).await
}

async fn search_impl(
    idx: Option<String>,
    q: SearchQ,
    s: Arc<AppState>,
    body: bytes::Bytes,
) -> Response {
    let idx_label = idx.clone().unwrap_or_else(|| "_all".to_string());
    let cols = match &idx {
        Some(t) => match s.engine.resolve(t) { Ok(v) => v, Err(e) => return internal(e) },
        None => s.engine.list_collections().into_iter()
            .filter_map(|n| s.engine.resolve(&n).ok().and_then(|v| v.into_iter().next()))
            .collect(),
    };
    let mut body_j: J = if body.is_empty() {
        json!({})
    } else {
        match serde_json::from_slice(&body) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("[http] _search {} 400 invalid JSON: {}\n[http]   body: {}",
                    idx_label, e, String::from_utf8_lossy(&body));
                return err(StatusCode::BAD_REQUEST, &format!("invalid JSON: {e}"));
            }
        }
    };
    if let Err(e) = resolve_terms_lookups(&mut body_j, &s.engine) {
        eprintln!("[http] _search {} 400 terms lookup failed: {}", idx_label, e);
        return err(StatusCode::BAD_REQUEST, &e.to_string());
    }
    // URL-level `q=` parameter (Lucene query_string shorthand) — inject as the
    // request `query` if the body didn't already specify one.
    if let Some(qs) = q.q.as_deref() {
        let has_query = body_j.get("query").is_some();
        if !has_query {
            if let J::Object(m) = &mut body_j {
                m.insert("query".into(), lucene_q_to_query(qs));
            } else {
                body_j = json!({ "query": lucene_q_to_query(qs) });
            }
        }
    }
    // URL-level `sort=` parameter (comma-separated `field:direction`) — inject
    // as the request `sort` if the body didn't already specify one.
    if let Some(sort_s) = q.sort.as_deref() {
        if body_j.get("sort").is_none() {
            let arr: Vec<J> = sort_s.split(',').filter(|s| !s.is_empty()).map(|spec| {
                let (f, dir) = match spec.split_once(':') {
                    Some((f, d)) => (f, d),
                    None => (spec, "asc"),
                };
                json!({ f: { "order": dir } })
            }).collect();
            if let J::Object(m) = &mut body_j {
                m.insert("sort".into(), J::Array(arr));
            }
        }
    }
    let mut req: SearchRequest = match serde_json::from_value(body_j.clone()) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("[http] _search {} 400 body not a SearchRequest: {}\n[http]   body: {}",
                idx_label, e, serde_json::to_string(&body_j).unwrap_or_default());
            return err(StatusCode::BAD_REQUEST, &e.to_string());
        }
    };
    if let Some(sz) = q.size { req.size = sz; }
    if let Some(fr) = q.from { req.from = fr; }

    let no_indices = cols.is_empty();
    let cols_for_search = cols.clone();
    let req_for_search = req.clone();
    let resp = match blocking(move || run_search(&cols_for_search, &req_for_search)).await {
        Ok(r) => r,
        Err(e) => {
            eprintln!("[http] _search {} 500: {}\n[http]   body: {}",
                idx_label, e, serde_json::to_string(&body_j).unwrap_or_default());
            return internal(e);
        }
    };

    // Non-scroll path: return result as-is.
    let Some(scroll_spec) = q.scroll.as_deref() else {
        let mut v = serde_json::to_value(resp).unwrap();
        apply_total_hits_as_int(&mut v, q.rest_total_hits_as_int.unwrap_or(false));
        if no_indices {
            if let Some(obj) = v.as_object_mut() { obj.remove("aggregations"); }
        }
        return Json(v).into_response();
    };
    let ttl = parse_es_duration(scroll_spec).unwrap_or(std::time::Duration::from_secs(60));

    let scroll_id = new_scroll_id();
    let hits_len = resp.hits.hits.len();
    {
        let mut g = s.scrolls.write();
        prune_expired_scrolls(&mut g);
        g.insert(scroll_id.clone(), ScrollState {
            targets: vec![idx.clone().unwrap_or_else(|| "_all".to_string())],
            req: req.clone(),
            next_from: req.from + hits_len,
            ttl_until: std::time::Instant::now() + ttl,
        });
    }
    let mut v = serde_json::to_value(resp).unwrap();
    if let Some(obj) = v.as_object_mut() {
        obj.insert("_scroll_id".into(), json!(scroll_id));
    }
    apply_total_hits_as_int(&mut v, q.rest_total_hits_as_int.unwrap_or(false));
    if no_indices {
        if let Some(obj) = v.as_object_mut() { obj.remove("aggregations"); }
    }
    Json(v).into_response()
}

async fn scroll_continue(State(s): State<Arc<AppState>>, Query(qs): Query<ScrollQ>, body: Option<Json<J>>) -> Response {
    let as_int = qs.rest_total_hits_as_int.unwrap_or(false);
    let (scroll_id, ttl_override) = extract_scroll_args(&qs, body.as_ref().map(|b| &b.0));
    let scroll_id = match scroll_id {
        Some(v) => v,
        None => return err(StatusCode::BAD_REQUEST, "missing scroll_id"),
    };
    scroll_continue_impl(s, scroll_id, ttl_override, as_int).await
}

async fn scroll_continue_get(State(s): State<Arc<AppState>>, Query(qs): Query<ScrollQ>) -> Response {
    let as_int = qs.rest_total_hits_as_int.unwrap_or(false);
    let (scroll_id, ttl_override) = extract_scroll_args(&qs, None);
    let scroll_id = match scroll_id {
        Some(v) => v,
        None => return err(StatusCode::BAD_REQUEST, "missing scroll_id"),
    };
    scroll_continue_impl(s, scroll_id, ttl_override, as_int).await
}

#[derive(Deserialize, Default)]
#[allow(dead_code)]
struct ScrollQ { scroll: Option<String>, scroll_id: Option<String>, rest_total_hits_as_int: Option<bool> }

fn extract_scroll_args(qs: &ScrollQ, body: Option<&J>) -> (Option<String>, Option<std::time::Duration>) {
    let id = qs.scroll_id.clone().or_else(|| {
        body.and_then(|b| b.get("scroll_id").and_then(|v| v.as_str()).map(|s| s.to_string()))
    });
    let ttl = qs.scroll.as_deref().and_then(parse_es_duration).or_else(|| {
        body.and_then(|b| b.get("scroll").and_then(|v| v.as_str())).and_then(parse_es_duration)
    });
    (id, ttl)
}

async fn scroll_continue_impl(s: Arc<AppState>, scroll_id: String, ttl_override: Option<std::time::Duration>, as_int: bool) -> Response {
    let (targets, mut req, next_from) = {
        let g = s.scrolls.read();
        match g.get(&scroll_id) {
            Some(st) if st.ttl_until > std::time::Instant::now() =>
                (st.targets.clone(), st.req.clone(), st.next_from),
            _ => return err(StatusCode::NOT_FOUND, "scroll expired or not found"),
        }
    };
    let target = targets.first().cloned().unwrap_or_default();
    let cols = match s.engine.resolve(&target) { Ok(v) => v, Err(e) => return internal(e) };

    req.from = next_from;
    let resp = match run_search(&cols, &req) { Ok(r) => r, Err(e) => return internal(e) };
    let hits_len = resp.hits.hits.len();

    if hits_len == 0 {
        s.scrolls.write().remove(&scroll_id);
    } else {
        let mut g = s.scrolls.write();
        if let Some(st) = g.get_mut(&scroll_id) {
            st.next_from = next_from + hits_len;
            if let Some(t) = ttl_override {
                st.ttl_until = std::time::Instant::now() + t;
            }
        }
    }

    let mut v = serde_json::to_value(resp).unwrap();
    if let Some(obj) = v.as_object_mut() {
        obj.insert("_scroll_id".into(), json!(scroll_id));
    }
    apply_total_hits_as_int(&mut v, as_int);
    Json(v).into_response()
}

async fn scroll_delete(State(s): State<Arc<AppState>>, Query(qs): Query<ScrollQ>, body: Option<Json<J>>) -> Response {
    let mut ids: Vec<String> = Vec::new();
    if let Some(id) = &qs.scroll_id { ids.push(id.clone()); }
    if let Some(Json(b)) = body {
        if let Some(arr) = b.get("scroll_id").and_then(|v| v.as_array()) {
            for x in arr { if let Some(s) = x.as_str() { ids.push(s.to_string()); } }
        } else if let Some(s) = b.get("scroll_id").and_then(|v| v.as_str()) {
            ids.push(s.to_string());
        }
    }
    let mut g = s.scrolls.write();
    let mut n = 0usize;
    if ids.is_empty() {
        n = g.len();
        g.clear();
    } else {
        for id in ids { if g.remove(&id).is_some() { n += 1; } }
    }
    Json(json!({ "succeeded": true, "num_freed": n })).into_response()
}

fn new_scroll_id() -> String {
    // Opaque token; clients must treat it as opaque so any unique string works.
    let t = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap();
    format!("sc_{:x}_{:x}", t.as_nanos(), rand_u64())
}

fn rand_u64() -> u64 {
    use std::hash::{BuildHasher, Hasher};
    let mut h = ahash::RandomState::new().build_hasher();
    h.write_u128(std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos());
    h.finish()
}

fn parse_es_duration(s: &str) -> Option<std::time::Duration> {
    // Accept: "10m", "30s", "1h", "500ms". Defaults to seconds if bare number.
    let s = s.trim();
    if s.is_empty() { return None; }
    let (num_end, suffix) = s.char_indices()
        .find(|(_, c)| !c.is_ascii_digit())
        .map(|(i, _)| (i, &s[i..]))
        .unwrap_or((s.len(), ""));
    let n: u64 = s[..num_end].parse().ok()?;
    let d = match suffix {
        "" | "s"  => std::time::Duration::from_secs(n),
        "ms"      => std::time::Duration::from_millis(n),
        "m"       => std::time::Duration::from_secs(n * 60),
        "h"       => std::time::Duration::from_secs(n * 3600),
        "d"       => std::time::Duration::from_secs(n * 86400),
        _         => return None,
    };
    Some(d)
}

fn prune_expired_scrolls(g: &mut ahash::AHashMap<String, ScrollState>) {
    let now = std::time::Instant::now();
    g.retain(|_, s| s.ttl_until > now);
}

#[derive(Deserialize, Default)]
#[allow(dead_code)]
struct MSearchQ { rest_total_hits_as_int: Option<bool> }

async fn msearch(headers: HeaderMap, Query(q): Query<MSearchQ>, State(s): State<Arc<AppState>>, body: String) -> Response {
    msearch_impl(headers, None, q, s, body).await
}
async fn msearch_scoped(headers: HeaderMap, Path(idx): Path<String>, Query(q): Query<MSearchQ>, State(s): State<Arc<AppState>>, body: String) -> Response {
    msearch_impl(headers, Some(idx), q, s, body).await
}
async fn msearch_impl(headers: HeaderMap, idx: Option<String>, q: MSearchQ, s: Arc<AppState>, body: String) -> Response {
    let _ = headers;
    let as_int = q.rest_total_hits_as_int.unwrap_or(false);
    let default_idx = idx.unwrap_or_default();
    let mut responses: Vec<J> = Vec::new();
    if body.trim().is_empty() {
        return err(StatusCode::BAD_REQUEST, "no requests added");
    }
    let mut lines = body.lines();
    while let Some(hdr) = lines.next() {
        let hdr_v: J = match serde_json::from_str(hdr) { Ok(v) => v, Err(e) => return err(StatusCode::BAD_REQUEST, &e.to_string()) };
        let body = match lines.next() { Some(b) => b, None => break };
        let body_v: J = match serde_json::from_str(body) { Ok(v) => v, Err(e) => return err(StatusCode::BAD_REQUEST, &e.to_string()) };
        let tgt = hdr_v.get("index").and_then(|v| v.as_str()).map(String::from)
            .unwrap_or_else(|| default_idx.clone());
        let cols = match s.engine.resolve(&tgt) { Ok(v) => v, Err(e) => return internal(e) };
        let req: SearchRequest = match serde_json::from_value(body_v) {
            Ok(r) => r, Err(e) => return err(StatusCode::BAD_REQUEST, &e.to_string()),
        };
        match run_search(&cols, &req) {
            Ok(resp) => {
                let mut v = serde_json::to_value(resp).unwrap();
                apply_total_hits_as_int(&mut v, as_int);
                responses.push(v);
            }
            Err(e) => responses.push(json!({ "error": { "reason": e.to_string() } })),
        }
    }
    Json(json!({ "responses": responses })).into_response()
}

// -------- bulk -----------------------------------------------------------

#[derive(Deserialize, Default)]
#[allow(dead_code)]
struct BulkQ { refresh: Option<String> }

async fn bulk(State(s): State<Arc<AppState>>, Query(q): Query<BulkQ>, body: String) -> Response {
    do_bulk(&s, None, q, body).await
}
async fn bulk_scoped(Path(idx): Path<String>, State(s): State<Arc<AppState>>, Query(q): Query<BulkQ>, body: String) -> Response {
    do_bulk(&s, Some(idx), q, body).await
}

async fn do_bulk(s: &Arc<AppState>, default_idx: Option<String>, q: BulkQ, body: String) -> Response {
    let t0 = std::time::Instant::now();
    let mut ops = Vec::new();
    let mut lines = body.lines().peekable();
    while let Some(hdr) = lines.next() {
        if hdr.trim().is_empty() { continue; }
        let hdr_v: J = match serde_json::from_str(hdr) { Ok(v) => v, Err(e) => return err(StatusCode::BAD_REQUEST, &e.to_string()) };
        let (action, meta) = hdr_v.as_object().and_then(|o| o.iter().next().map(|(k,v)| (k.clone(), v.clone())))
            .unwrap_or(("index".into(), J::Null));
        let coll = meta.get("_index").and_then(|v| v.as_str()).map(String::from)
            .or(default_idx.clone());
        let id   = meta.get("_id").and_then(|v| v.as_str()).map(String::from);
        let kind = match action.as_str() {
            "index" | "" => {
                let src = match lines.next() { Some(s) => s, None => break };
                let src_j: J = serde_json::from_str(src).unwrap_or(J::Null);
                let op_type = meta.get("op_type").and_then(|v| v.as_str()).unwrap_or("");
                if op_type == "create" {
                    BulkKind::Create { id: id.unwrap_or_default(), source: src_j }
                } else {
                    BulkKind::Index { id, source: src_j }
                }
            }
            "create" => {
                let src = match lines.next() { Some(s) => s, None => break };
                let src_j: J = serde_json::from_str(src).unwrap_or(J::Null);
                BulkKind::Create { id: id.unwrap_or_default(), source: src_j }
            }
            "update" => {
                let src = match lines.next() { Some(s) => s, None => break };
                let src_j: J = serde_json::from_str(src).unwrap_or(J::Null);
                let doc = src_j.get("doc").cloned().unwrap_or(src_j);
                BulkKind::Update { id: id.unwrap_or_default(), doc }
            }
            "delete" => BulkKind::Delete { id: id.unwrap_or_default() },
            other => return err(StatusCode::BAD_REQUEST, &format!("unknown bulk action: {other}")),
        };
        ops.push(BulkOp { collection: coll, kind });
    }
    let mut seen_idx: ahash::AHashSet<String> = Default::default();
    for op in &ops {
        if let Some(c) = op.collection.as_deref() {
            if seen_idx.insert(c.to_string()) { ensure_with_templates(s, c); }
        }
    }
    // Run the sync, potentially-long bulk_write on the blocking thread
    // pool. Previously this ran on the tokio worker directly; once all
    // sessions indices share one redb writer mutex, concurrent capture
    // bulks serialize and would block every tokio worker, starving even
    // trivial endpoints like /_cluster/health.
    let engine = s.engine.clone();
    let default_idx_clone = default_idx.clone();
    let results = match tokio::task::spawn_blocking(move || {
        engine.bulk_write(default_idx_clone.as_deref(), ops)
    }).await {
        Ok(Ok(r)) => r,
        Ok(Err(e)) => return internal(e),
        Err(e) => return internal(arkimedb_core::Error::Io(std::io::Error::other(e.to_string()))),
    };
    let errors = results.iter().any(|r| r.error.is_some());
    let items: Vec<J> = results.into_iter().map(outcome_to_item).collect();

    // Honor ?refresh={true,wait_for}. Both map to "make writes visible before
    // responding"; in our single-node store writes are durable on commit so a
    // refresh is effectively immediate. We still call engine.refresh to flush
    // any pending hydration state and match ES semantics.
    if matches!(q.refresh.as_deref(), Some("true") | Some("wait_for") | Some("")) {
        let _ = s.engine.refresh(default_idx.as_deref());
    }

    Json(json!({
        "took": t0.elapsed().as_millis() as u64,
        "errors": errors,
        "items": items,
    })).into_response()
}

// -------- alias management ----------------------------------------------

async fn update_aliases(State(s): State<Arc<AppState>>, Json(body): Json<J>) -> Response {
    // body: { "actions": [ { "add": {"index":"...","alias":"..."}} ... ] }
    let actions = body.get("actions").and_then(|v| v.as_array()).cloned().unwrap_or_default();
    for act in actions {
        let (k, v) = match act.as_object().and_then(|o| o.iter().next()) {
            Some((k, v)) => (k.clone(), v.clone()),
            None => continue,
        };
        let idx = v.get("index").and_then(|x| x.as_str()).unwrap_or("").to_string();
        let alias = v.get("alias").and_then(|x| x.as_str()).unwrap_or("").to_string();
        if alias.is_empty() { continue; }
        let mut current = s.engine.catalog.get_alias(&alias).unwrap_or(None).unwrap_or_default();
        match k.as_str() {
            "add"    => { if !current.contains(&idx) { current.push(idx); } }
            "remove" => { current.retain(|x| x != &idx); }
            _ => {}
        }
        if let Err(e) = s.engine.catalog.set_alias(&alias, &current) { return internal(e); }
    }
    Json(json!({ "acknowledged": true })).into_response()
}
async fn put_alias(Path((idx, alias)): Path<(String, String)>, State(s): State<Arc<AppState>>) -> Response {
    let mut current = s.engine.catalog.get_alias(&alias).unwrap_or(None).unwrap_or_default();
    if !current.contains(&idx) { current.push(idx); }
    handle(s.engine.catalog.set_alias(&alias, &current).map(|_| json!({"acknowledged": true})))
}
async fn delete_alias(Path((idx, alias)): Path<(String, String)>, State(s): State<Arc<AppState>>) -> Response {
    let mut current = s.engine.catalog.get_alias(&alias).unwrap_or(None).unwrap_or_default();
    current.retain(|x| x != &idx);
    handle(s.engine.catalog.set_alias(&alias, &current).map(|_| json!({"acknowledged": true})))
}

// -------- stubs & stats --------------------------------------------------

// flush in ES means "persist to disk". arkimedb commits every write inside
// its transaction (with Durability::Eventual on the hot path), so by the
// time a write response is returned the data is in the kernel page cache
// and queued for fsync. Clients don't need an explicit fsync here; returning
// success immediately is correct for Arkime's monitoring-data use case and
// avoids tens of seconds of fsync stalls across hundreds of daily indices.
async fn flush_all(State(_s): State<Arc<AppState>>) -> Response {
    Json(json!({ "_shards": { "total": 1, "successful": 1, "failed": 0 } })).into_response()
}
async fn flush_one(Path(_idx): Path<String>, State(_s): State<Arc<AppState>>) -> Response {
    Json(json!({ "_shards": { "total": 1, "successful": 1, "failed": 0 } })).into_response()
}

async fn cluster_stats_docs(State(s): State<Arc<AppState>>) -> Response {
    let mut indices = serde_json::Map::new();
    let mut total_docs: u64 = 0;
    let mut total_size: u64 = 0;
    for n in s.engine.list_collections() {
        if let Some(c) = s.engine.get_collection(&n) {
            let docs = c.row_count().unwrap_or(0);
            let size = std::fs::metadata(&c.path).map(|m| m.len()).unwrap_or(0);
            total_docs += docs; total_size += size;
            indices.insert(n, json!({
                "primaries": { "docs": { "count": docs, "deleted": 0 }, "store": { "size_in_bytes": size } },
                "total":     { "docs": { "count": docs, "deleted": 0 }, "store": { "size_in_bytes": size } }
            }));
        }
    }
    Json(json!({
        "_shards": { "total": 1, "successful": 1, "failed": 0 },
        "_all": {
            "primaries": { "docs": { "count": total_docs }, "store": { "size_in_bytes": total_size } },
            "total":     { "docs": { "count": total_docs }, "store": { "size_in_bytes": total_size } }
        },
        "indices": indices
    })).into_response()
}

async fn nodes_stats(State(s): State<Arc<AppState>>) -> Response {
    let total_docs: u64 = s.engine.list_collections().iter()
        .filter_map(|n| s.engine.get_collection(n))
        .map(|c| c.row_count().unwrap_or(0)).sum();
    let uptime_ms = s.start.elapsed().as_millis() as u64;
    Json(json!({
        "cluster_name": "arkimedb",
        "nodes": { "arkimedb-0": {
            "name": "arkimedb-0",
            "timestamp": std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0),
            "host": "127.0.0.1",
            "ip":   "127.0.0.1:9200",
            "transport_address": "127.0.0.1:9300",
            "roles": ["master","data","ingest"],
            "attributes": {},
            "indices": {
                "docs":  { "count": total_docs, "deleted": 0 },
                "store": { "size_in_bytes": 0, "reserved_in_bytes": 0 },
                "search": {
                    "open_contexts": 0,
                    "query_total": 0, "query_time_in_millis": 0, "query_current": 0,
                    "fetch_total": 0, "fetch_time_in_millis": 0, "fetch_current": 0,
                    "scroll_total": 0, "scroll_time_in_millis": 0, "scroll_current": 0
                },
                "segments": { "count": 0, "memory_in_bytes": 0 }
            },
            "os":  { "cpu": { "percent": 0, "load_average": { "1m": 0.0, "5m": 0.0, "15m": 0.0 } },
                     "mem": { "total_in_bytes": 0, "free_in_bytes": 0, "used_in_bytes": 0 } },
            "process": { "cpu": { "percent": 0, "total_in_millis": 0 },
                         "mem": { "total_virtual_in_bytes": 0 },
                         "open_file_descriptors": 0,
                         "max_file_descriptors": 128000 },
            "jvm":    { "uptime_in_millis": uptime_ms,
                        "mem": { "heap_used_in_bytes": 0, "heap_used_percent": 0, "heap_committed_in_bytes": 0,
                                 "heap_max_in_bytes": 0, "non_heap_used_in_bytes": 0, "non_heap_committed_in_bytes": 0 } },
            "fs":     { "total": { "total_in_bytes": 0, "free_in_bytes": 0, "available_in_bytes": 0 },
                        "io_stats": { "total": { "read_kilobytes": 0, "write_kilobytes": 0 } } },
            "thread_pool": {
                "bulk":   { "queue_size": 0, "active": 0, "rejected": 0, "completed": 0, "largest": 0, "threads": 0 },
                "write":  { "queue_size": 0, "active": 0, "rejected": 0, "completed": 0, "largest": 0, "threads": 0 },
                "search": { "queue_size": 0, "active": 0, "rejected": 0, "completed": 0, "largest": 0, "threads": 0 }
            }
        }}
    })).into_response()
}

async fn index_stats(
    Path(params): Path<Vec<(String, String)>>,
    State(s): State<Arc<AppState>>,
) -> Response {
    let idx = params.iter().find(|(k, _)| k == "idx").map(|(_, v)| v.clone()).unwrap_or_default();
    let names: Vec<String> = s.engine.resolve(&idx)
        .unwrap_or_default()
        .into_iter()
        .map(|c| c.name.clone())
        .collect();
    let mut indices = serde_json::Map::new();
    let mut total_docs: u64 = 0;
    let mut total_size: u64 = 0;
    for n in &names {
        if let Some(c) = s.engine.get_collection(n) {
            let docs = c.row_count().unwrap_or(0);
            let size = std::fs::metadata(&c.path).map(|m| m.len()).unwrap_or(0);
            total_docs += docs;
            total_size += size;
            indices.insert(n.clone(), json!({
                "uuid": n,
                "primaries": {
                    "docs":  { "count": docs, "deleted": 0 },
                    "store": { "size_in_bytes": size, "reserved_in_bytes": 0 }
                },
                "total": {
                    "docs":  { "count": docs, "deleted": 0 },
                    "store": { "size_in_bytes": size, "reserved_in_bytes": 0 }
                }
            }));
        }
    }
    Json(json!({
        "_shards": { "total": names.len(), "successful": names.len(), "failed": 0 },
        "_all": {
            "primaries": {
                "docs":  { "count": total_docs, "deleted": 0 },
                "store": { "size_in_bytes": total_size, "reserved_in_bytes": 0 }
            },
            "total": {
                "docs":  { "count": total_docs, "deleted": 0 },
                "store": { "size_in_bytes": total_size, "reserved_in_bytes": 0 }
            }
        },
        "indices": indices
    })).into_response()
}

#[derive(Deserialize, Default)]
struct ClusterSettingsQ {
    flat_settings: Option<String>,
    include_defaults: Option<String>,
}

fn truthy(v: &Option<String>) -> bool {
    v.as_deref().map_or(false, |s| !matches!(s, "false" | "0" | "no"))
}

fn nest_settings(flat: &serde_json::Map<String, J>) -> J {
    let mut out = serde_json::Map::new();
    for (k, v) in flat {
        let parts: Vec<&str> = k.split('.').collect();
        let mut cur = &mut out;
        for i in 0..parts.len() - 1 {
            let e = cur.entry(parts[i].to_string()).or_insert_with(|| J::Object(serde_json::Map::new()));
            if !e.is_object() { *e = J::Object(serde_json::Map::new()); }
            cur = e.as_object_mut().unwrap();
        }
        cur.insert(parts[parts.len() - 1].to_string(), v.clone());
    }
    J::Object(out)
}

fn default_cluster_settings() -> serde_json::Map<String, J> {
    let mut m = serde_json::Map::new();
    let add = |m: &mut serde_json::Map<String, J>, k: &str, v: J| { m.insert(k.into(), v); };
    add(&mut m, "search.max_buckets", json!("10000"));
    add(&mut m, "search.max_open_scroll_context", json!("500"));
    add(&mut m, "cluster.routing.allocation.enable", json!("all"));
    add(&mut m, "cluster.routing.allocation.cluster_concurrent_rebalance", json!("2"));
    add(&mut m, "cluster.routing.allocation.node_concurrent_recoveries", json!("2"));
    add(&mut m, "cluster.routing.allocation.node_initial_primaries_recoveries", json!("4"));
    add(&mut m, "cluster.max_shards_per_node", json!("1000"));
    add(&mut m, "indices.recovery.max_bytes_per_sec", json!("40mb"));
    add(&mut m, "cluster.routing.allocation.awareness.attributes", json!(""));
    add(&mut m, "indices.breaker.total.limit", json!("95%"));
    add(&mut m, "indices.breaker.fielddata.limit", json!("40%"));
    add(&mut m, "cluster.routing.allocation.disk.watermark.low", json!("85%"));
    add(&mut m, "cluster.routing.allocation.disk.watermark.high", json!("90%"));
    add(&mut m, "cluster.routing.allocation.disk.watermark.flood_stage", json!("95%"));
    m
}

async fn cluster_settings(State(s): State<Arc<AppState>>, Query(q): Query<ClusterSettingsQ>) -> Response {
    let cs = s.cluster_settings.read();
    let flat = truthy(&q.flat_settings);
    let want_defaults = truthy(&q.include_defaults);
    let defaults = if want_defaults { default_cluster_settings() } else { serde_json::Map::new() };
    if flat {
        let mut out = serde_json::Map::new();
        out.insert("persistent".into(), J::Object(cs.persistent.clone()));
        out.insert("transient".into(), J::Object(cs.transient.clone()));
        if want_defaults { out.insert("defaults".into(), J::Object(defaults)); }
        Json(J::Object(out)).into_response()
    } else {
        let mut out = serde_json::Map::new();
        out.insert("persistent".into(), nest_settings(&cs.persistent));
        out.insert("transient".into(), nest_settings(&cs.transient));
        if want_defaults { out.insert("defaults".into(), nest_settings(&defaults)); }
        Json(J::Object(out)).into_response()
    }
}

fn is_valid_cluster_setting(k: &str) -> bool {
    if default_cluster_settings().contains_key(k) { return true; }
    k.starts_with("cluster.routing.allocation.exclude.")
        || k.starts_with("cluster.routing.allocation.include.")
        || k.starts_with("cluster.routing.allocation.require.")
        || k.starts_with("logger.")
}

async fn cluster_settings_put(State(s): State<Arc<AppState>>, body: Option<Json<J>>) -> Response {
    let body = body.map(|j| j.0).unwrap_or(J::Null);
    // Flatten once so we can validate.
    fn flatten_into(body: &J, out: &mut Vec<(String, J)>) {
        fn walk(prefix: &str, v: &J, out: &mut Vec<(String, J)>) {
            match v {
                J::Object(m) => for (k, cv) in m {
                    let nk = if prefix.is_empty() { k.clone() } else { format!("{}.{}", prefix, k) };
                    walk(&nk, cv, out);
                },
                _ => out.push((prefix.to_string(), v.clone())),
            }
        }
        walk("", body, out);
    }
    let mut p_leaves = Vec::new();
    let mut t_leaves = Vec::new();
    if let Some(p) = body.get("persistent") { flatten_into(p, &mut p_leaves); }
    if let Some(t) = body.get("transient")  { flatten_into(t, &mut t_leaves); }
    for (k, _) in p_leaves.iter().chain(t_leaves.iter()) {
        if !is_valid_cluster_setting(k) {
            return err(StatusCode::BAD_REQUEST, &format!("unknown setting [{}]", k));
        }
    }
    {
        let mut cs = s.cluster_settings.write();
        for (k, v) in p_leaves { if v.is_null() { cs.persistent.remove(&k); } else { cs.persistent.insert(k, v); } }
        for (k, v) in t_leaves { if v.is_null() { cs.transient.remove(&k);  } else { cs.transient.insert(k, v);  } }
    }
    let cs = s.cluster_settings.read();
    Json(json!({
        "acknowledged": true,
        "persistent": cs.persistent,
        "transient":  cs.transient
    })).into_response()
}

async fn cluster_reroute(State(s): State<Arc<AppState>>, body: Option<Json<J>>) -> Response {
    if let Some(Json(b)) = body.as_ref() {
        if let Some(cmds) = b.get("commands").and_then(|c| c.as_array()) {
            let cols: std::collections::HashSet<String> =
                s.engine.list_collections().into_iter().collect();
            for cmd in cmds {
                if let Some(obj) = cmd.as_object() {
                    for (_name, args) in obj.iter() {
                        if let Some(idx) = args.get("index").and_then(|v| v.as_str()) {
                            if !cols.contains(idx) {
                                return (
                                    StatusCode::BAD_REQUEST,
                                    Json(json!({
                                        "error": {
                                            "type": "illegal_argument_exception",
                                            "reason": format!("[no_such_index] no such index [{}]", idx)
                                        },
                                        "status": 400
                                    }))
                                ).into_response();
                            }
                        }
                    }
                }
            }
        }
    }
    Json(json!({
        "acknowledged": true,
        "state": { "cluster_name": "arkimedb", "cluster_uuid": "arkimedb-0", "routing_table": { "indices": {} } }
    })).into_response()
}

async fn cat_recovery(State(s): State<Arc<AppState>>, headers: HeaderMap, Query(q): Query<CatQ>) -> Response {
    let want_json = q.format.as_deref() == Some("json") || wants_json(&headers);
    let active_only = q.active_only.as_deref() == Some("true");
    let rows: Vec<J> = if active_only {
        vec![]
    } else {
        s.engine.list_collections().into_iter().map(|idx| {
            json!({
                "index": idx,
                "shard": "0",
                "time": "0ms",
                "type": "existing_store",
                "stage": "done",
                "source_host": "n/a",
                "source_node": "n/a",
                "target_host": "127.0.0.1",
                "target_node": "arkimedb-0",
                "repository": "n/a",
                "snapshot": "n/a",
                "files": "0",
                "files_recovered": "0",
                "files_percent": "100.0%",
                "files_total": "0",
                "bytes": "0",
                "bytes_recovered": "0",
                "bytes_percent": "100.0%",
                "bytes_total": "0",
                "translog_ops": "0",
                "translog_ops_recovered": "0",
                "translog_ops_percent": "100.0%"
            })
        }).collect()
    };
    if want_json {
        return Json(J::Array(rows)).into_response();
    }
    let table_rows: Vec<Vec<String>> = rows.iter().map(|r| vec![
        r.get("index").and_then(|v| v.as_str()).unwrap_or("").to_string(),
        "0".into(), "0ms".into(), "existing_store".into(), "done".into(),
        "n/a".into(), "n/a".into(), "127.0.0.1".into(), "arkimedb-0".into(),
        "n/a".into(), "n/a".into(),
        "0".into(), "0".into(), "100.0%".into(), "0".into(),
        "0".into(), "0".into(), "100.0%".into(), "0".into(),
        "0".into(), "0".into(), "100.0%".into(),
    ]).collect();
    let headers_row = ["index","shard","time","type","stage","source_host","source_node","target_host","target_node","repository","snapshot","files","files_recovered","files_percent","files_total","bytes","bytes_recovered","bytes_percent","bytes_total","translog_ops","translog_ops_recovered","translog_ops_percent"];
    let right_just = [false,true,true,false,false,false,false,false,false,false,false,true,true,true,true,true,true,true,true,true,true,true];
    (StatusCode::OK, render_cat_table(&headers_row, &right_just, &table_rows, q.v)).into_response()
}

async fn tasks_list(State(_s): State<Arc<AppState>>, Query(_q): Query<CatQ>) -> Response {
    let task = json!({
        "node": "arkimedb-0",
        "id": 1,
        "type": "direct",
        "action": "indices:monitor/tasks/lists",
        "start_time_in_millis": 0,
        "running_time_in_nanos": 0,
        "cancellable": false,
        "headers": {}
    });
    Json(json!({
        "tasks": {
            "arkimedb-0:1": task
        }
    })).into_response()
}

async fn allocation_explain(State(_s): State<Arc<AppState>>, body: Option<Json<J>>) -> Response {
    let _ = body;
    Json(json!({
        "index": "",
        "shard": 0,
        "primary": true,
        "current_state": "started",
        "allocate_explanation": "single-node arkimedb; nothing to allocate"
    })).into_response()
}

async fn cat_nodes(State(_s): State<Arc<AppState>>, headers: HeaderMap, Query(q): Query<CatQ>) -> Response {
    let want_json = q.format.as_deref() == Some("json") || wants_json(&headers);
    if want_json {
        return Json(json!([{
            "name": "arkimedb-0",
            "diskTotal": "0b",
            "role": "mdi",
            "ip": "127.0.0.1",
            "heap.percent": "0",
            "ram.percent": "0",
            "cpu": "0",
            "master": "*"
        }])).into_response();
    }
    let rows = vec![vec![
        "127.0.0.1".into(), "0".into(), "0".into(), "0".into(),
        "mdi".into(), "*".into(), "arkimedb-0".into(),
    ]];
    let headers_row = ["ip","heap.percent","ram.percent","cpu","node.role","master","name"];
    let right_just  = [false,true,true,true,false,false,false];
    (StatusCode::OK, render_cat_table(&headers_row, &right_just, &rows, q.v)).into_response()
}

async fn cat_allocation(State(s): State<Arc<AppState>>, headers: HeaderMap, Query(q): Query<CatQ>) -> Response {
    let want_json = q.format.as_deref() == Some("json") || wants_json(&headers);
    let n = s.engine.list_collections().len();
    if want_json {
        return Json(json!([{
            "shards": n.to_string(),
            "disk.indices": "0",
            "disk.used": "0",
            "disk.avail": "0",
            "disk.total": "0",
            "disk.percent": "0",
            "host": "127.0.0.1",
            "ip": "127.0.0.1",
            "node": "arkimedb-0"
        }])).into_response();
    }
    let rows = vec![vec![
        n.to_string(), "0".into(), "0".into(), "0".into(), "0".into(), "0".into(),
        "127.0.0.1".into(), "127.0.0.1".into(), "arkimedb-0".into(),
    ]];
    let headers_row = ["shards","disk.indices","disk.used","disk.avail","disk.total","disk.percent","host","ip","node"];
    let right_just  = [true,true,true,true,true,true,false,false,false];
    (StatusCode::OK, render_cat_table(&headers_row, &right_just, &rows, q.v)).into_response()
}

async fn cat_health(State(s): State<Arc<AppState>>, headers: HeaderMap, Query(q): Query<CatQ>) -> Response {
    let want_json = q.format.as_deref() == Some("json") || wants_json(&headers);
    let n = s.engine.list_collections().len();
    let ts_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0);
    let ts_str = {
        let dt = time::OffsetDateTime::from_unix_timestamp(ts_secs as i64).ok();
        dt.and_then(|d| d.format(&time::format_description::well_known::Rfc3339).ok())
            .unwrap_or_default()
    };
    if want_json {
        return Json(json!([{
            "epoch": ts_secs.to_string(),
            "timestamp": ts_str,
            "cluster": "arkimedb",
            "status": "green",
            "node.total": "1",
            "node.data": "1",
            "shards": n.to_string(),
            "pri": n.to_string(),
            "relo": "0",
            "init": "0",
            "unassign": "0",
            "pending_tasks": "0",
            "max_task_wait_time": "-",
            "active_shards_percent": "100.0%"
        }])).into_response();
    }
    let rows = vec![vec![
        ts_secs.to_string(), ts_str, "arkimedb".into(), "green".into(),
        "1".into(), "1".into(), n.to_string(), n.to_string(),
        "0".into(), "0".into(), "0".into(), "0".into(), "-".into(), "100.0%".into(),
    ]];
    let headers_row = ["epoch","timestamp","cluster","status","node.total","node.data","shards","pri","relo","init","unassign","pending_tasks","max_task_wait_time","active_shards_percent"];
    let right_just  = [true,false,false,false,true,true,true,true,true,true,true,true,false,true];
    (StatusCode::OK, render_cat_table(&headers_row, &right_just, &rows, q.v)).into_response()
}

async fn cat_master(State(_s): State<Arc<AppState>>, headers: HeaderMap, Query(q): Query<CatQ>) -> Response {
    let want_json = q.format.as_deref() == Some("json") || wants_json(&headers);
    if want_json {
        return Json(json!([{
            "id": "arkimedb-0",
            "host": "127.0.0.1",
            "ip": "127.0.0.1",
            "node": "arkimedb-0"
        }])).into_response();
    }
    let rows = vec![vec![
        "arkimedb-0".into(), "127.0.0.1".into(), "127.0.0.1".into(), "arkimedb-0".into(),
    ]];
    let headers_row = ["id","host","ip","node"];
    let right_just  = [false,false,false,false];
    (StatusCode::OK, render_cat_table(&headers_row, &right_just, &rows, q.v)).into_response()
}

async fn cat_count_all(State(s): State<Arc<AppState>>, headers: HeaderMap, Query(q): Query<CatQ>) -> Response {
    cat_count_impl(None, s, headers, q).await
}
async fn cat_count_scoped(Path(t): Path<String>, State(s): State<Arc<AppState>>, headers: HeaderMap, Query(q): Query<CatQ>) -> Response {
    cat_count_impl(Some(t), s, headers, q).await
}
async fn cat_count_impl(target: Option<String>, s: Arc<AppState>, headers: HeaderMap, q: CatQ) -> Response {
    let want_json = q.format.as_deref() == Some("json") || wants_json(&headers);
    let cols = match &target {
        Some(t) => s.engine.resolve(t).unwrap_or_default(),
        None    => s.engine.list_collections().into_iter()
            .filter_map(|n| s.engine.resolve(&n).ok().and_then(|v| v.into_iter().next()))
            .collect(),
    };
    let mut total: u64 = 0;
    for c in &cols { total = total.saturating_add(c.row_count().unwrap_or(0)); }
    let ts_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0);
    if want_json {
        return Json(json!([{
            "epoch": ts_secs.to_string(),
            "timestamp": "",
            "count": total.to_string()
        }])).into_response();
    }
    let rows = vec![vec![ts_secs.to_string(), "-".into(), total.to_string()]];
    let headers_row = ["epoch","timestamp","count"];
    let right_just  = [true,false,true];
    (StatusCode::OK, render_cat_table(&headers_row, &right_just, &rows, q.v)).into_response()
}

async fn cat_templates(State(s): State<Arc<AppState>>, headers: HeaderMap, Query(q): Query<CatQ>) -> Response {
    cat_templates_impl(None, s, headers, q).await
}
async fn cat_templates_scoped(Path(p): Path<String>, State(s): State<Arc<AppState>>, headers: HeaderMap, Query(q): Query<CatQ>) -> Response {
    cat_templates_impl(Some(p), s, headers, q).await
}
async fn cat_templates_impl(pat: Option<String>, s: Arc<AppState>, headers: HeaderMap, q: CatQ) -> Response {
    let want_json = q.format.as_deref() == Some("json") || wants_json(&headers);
    let all = s.engine.catalog.list_templates().unwrap_or_default();
    let filtered: Vec<String> = match pat {
        Some(p) => {
            let names: Vec<String> = all.iter().map(|(k,_)| k.clone()).collect();
            let mut out = Vec::new();
            for part in p.split(',').map(|x| x.trim()).filter(|x| !x.is_empty()) {
                if part.contains('*') || part.contains('?') {
                    let m = Glob::compile(part);
                    for n in &names { if m.is_match(n) && !out.contains(n) { out.push(n.clone()); } }
                } else if !out.iter().any(|x| x == part) {
                    out.push(part.to_string());
                }
            }
            out
        }
        None => all.iter().map(|(k,_)| k.clone()).collect(),
    };
    if want_json {
        let arr: Vec<J> = filtered.iter().map(|n| json!({
            "name": n, "index_patterns": "[]", "order": "0", "version": ""
        })).collect();
        return Json(arr).into_response();
    }
    let rows: Vec<Vec<String>> = filtered.iter().map(|n| vec![
        n.clone(), "[]".into(), "0".into(), String::new(),
    ]).collect();
    let headers_row = ["name","index_patterns","order","version"];
    let right_just  = [false,false,true,true];
    (StatusCode::OK, render_cat_table(&headers_row, &right_just, &rows, q.v)).into_response()
}

async fn cat_shards_all(State(s): State<Arc<AppState>>, headers: HeaderMap, Query(q): Query<CatQ>) -> Response {
    cat_shards_impl(None, s, headers, q).await
}
async fn cat_shards_scoped(Path(t): Path<String>, State(s): State<Arc<AppState>>, headers: HeaderMap, Query(q): Query<CatQ>) -> Response {
    cat_shards_impl(Some(t), s, headers, q).await
}
async fn cat_shards_impl(target: Option<String>, s: Arc<AppState>, headers: HeaderMap, q: CatQ) -> Response {
    let want_json = q.format.as_deref() == Some("json") || wants_json(&headers);
    let names: Vec<String> = match target {
        Some(t) => s.engine.resolve(&t).unwrap_or_default().into_iter().map(|c| c.name.clone()).collect(),
        None => s.engine.list_collections(),
    };
    if want_json {
        let arr: Vec<J> = names.iter().map(|n| {
            let docs = s.engine.get_collection(n).and_then(|c| c.row_count().ok()).unwrap_or(0);
            json!({ "index": n, "i": n, "shard": "0", "prirep": "p", "state": "STARTED",
                    "docs": docs.to_string(), "sc": docs.to_string(), "store": "0",
                    "ip": "127.0.0.1", "node": "arkimedb-0" })
        }).collect();
        return Json(arr).into_response();
    }
    let mut rows: Vec<Vec<String>> = Vec::new();
    for n in names {
        let docs = s.engine.get_collection(&n).and_then(|c| c.row_count().ok()).unwrap_or(0);
        rows.push(vec![
            n, "0".into(), "p".into(), "STARTED".into(),
            docs.to_string(), "0".into(), "127.0.0.1".into(), "arkimedb-0".into(),
        ]);
    }
    let headers_row = ["index","shard","prirep","state","docs","store","ip","node"];
    let right_just  = [false,true,false,false,true,true,false,false];
    (StatusCode::OK, render_cat_table(&headers_row, &right_just, &rows, q.v)).into_response()
}

async fn cat_aliases_scoped(Path(_pat): Path<String>, State(s): State<Arc<AppState>>, headers: HeaderMap, Query(q): Query<CatQ>) -> Response {
    cat_aliases(State(s), headers, Query(q)).await
}

async fn alias_list_all(State(s): State<Arc<AppState>>) -> Response {
    let all = s.engine.catalog.list_aliases().unwrap_or_default();
    let mut out = serde_json::Map::new();
    // ES format: { "index": { "aliases": { "aliasA": {}, ... } } }
    for (alias, targets) in all {
        for t in targets {
            let entry = out.entry(t).or_insert_with(|| json!({ "aliases": {} }));
            if let Some(a) = entry.get_mut("aliases").and_then(|x| x.as_object_mut()) {
                a.insert(alias.clone(), json!({}));
            }
        }
    }
    Json(J::Object(out)).into_response()
}
async fn alias_list_by(Path(alias): Path<String>, State(s): State<Arc<AppState>>) -> Response {
    let targets = s.engine.catalog.get_alias(&alias).unwrap_or(None).unwrap_or_default();
    let mut out = serde_json::Map::new();
    for t in targets {
        out.insert(t, json!({ "aliases": { alias.clone(): {} } }));
    }
    Json(J::Object(out)).into_response()
}
async fn alias_of_index(Path(idx): Path<String>, State(s): State<Arc<AppState>>) -> Response {
    // Resolve index pattern → matching collections; return their aliases.
    let cols = s.engine.resolve(&idx).unwrap_or_default();
    if cols.is_empty() { return Json(json!({})).into_response(); }
    let all = s.engine.catalog.list_aliases().unwrap_or_default();
    let mut out = serde_json::Map::new();
    for c in &cols {
        let mut aliases = serde_json::Map::new();
        for (alias, targets) in &all {
            if targets.contains(&c.name) { aliases.insert(alias.clone(), json!({})); }
        }
        out.insert(c.name.clone(), json!({ "aliases": aliases }));
    }
    Json(J::Object(out)).into_response()
}
async fn alias_of_index_name(Path((idx, alias)): Path<(String, String)>, State(s): State<Arc<AppState>>) -> Response {
    let cols = s.engine.resolve(&idx).unwrap_or_default();
    let mut out = serde_json::Map::new();
    let targets = s.engine.catalog.get_alias(&alias).unwrap_or(None).unwrap_or_default();
    for c in cols {
        if targets.contains(&c.name) {
            out.insert(c.name.clone(), json!({ "aliases": { alias.clone(): {} } }));
        }
    }
    Json(J::Object(out)).into_response()
}

async fn get_settings(Path(idx): Path<String>, State(s): State<Arc<AppState>>) -> Response {
    let cols: Vec<_> = if idx == "_all" || idx == "*" {
        s.engine.list_collections().into_iter()
            .filter_map(|n| s.engine.resolve(&n).ok().and_then(|v| v.into_iter().next()))
            .collect()
    } else {
        s.engine.resolve(&idx).unwrap_or_default()
    };
    if cols.is_empty() { return Json(json!({})).into_response(); }
    let mut out = serde_json::Map::new();
    for c in cols {
        out.insert(c.name.clone(), json!({
            "settings": {
                "index": {
                    "number_of_shards": "1",
                    "number_of_replicas": "0",
                    "refresh_interval": "60s",
                    "uuid": c.name,
                    "provided_name": c.name,
                    "creation_date": "0",
                    "version": { "created": "7100010" }
                }
            }
        }));
    }
    Json(J::Object(out)).into_response()
}
async fn put_settings(Path(idx): Path<String>, State(s): State<Arc<AppState>>, body: Option<Json<J>>) -> Response {
    let _ = body;
    // Accept comma-separated patterns + wildcards; resolve to existing indices and
    // do NOT create phantom collections from the pattern string.
    for existing in s.engine.resolve(&idx).unwrap_or_default() {
        let _ = existing;
    }
    Json(json!({ "acknowledged": true })).into_response()
}

// -------- helpers --------------------------------------------------------

fn outcome_to_response(o: BulkOutcome) -> Response {
    match (o.ok, o.error) {
        (Some(r), _) => {
            let status = if r.created { StatusCode::CREATED } else { StatusCode::OK };
            (status, Json(json!({
                "_index": o.collection, "_id": r.id, "_version": r.version,
                "result": if r.action == "delete" { "deleted" } else if r.created { "created" } else { "updated" },
                "_shards": { "total": 1, "successful": 1, "failed": 0 }
            }))).into_response()
        }
        (_, Some(e)) => {
            let code = if e.starts_with("conflict:") || e.contains("already exists") {
                StatusCode::CONFLICT
            } else if e.contains("not found") {
                StatusCode::NOT_FOUND
            } else {
                StatusCode::BAD_REQUEST
            };
            // ES returns a richer error envelope for `_create` conflicts so
            // clients (e.g. Arkime parliament) can distinguish it from a
            // generic bad request. Match the shape closely enough that
            // `error.type == "version_conflict_engine_exception"` is detectable.
            let etype = if code == StatusCode::CONFLICT {
                "version_conflict_engine_exception"
            } else {
                "arkimedb_error"
            };
            (code, Json(json!({
                "error": { "reason": e, "type": etype },
                "status": code.as_u16()
            }))).into_response()
        }
        _ => err(StatusCode::INTERNAL_SERVER_ERROR, "no outcome"),
    }
}
fn outcome_to_item(o: BulkOutcome) -> J {
    if let Some(r) = o.ok {
        let action = r.action;
        let status = if r.created { 201 } else { 200 };
        json!({ action: {
            "_index": o.collection, "_id": r.id, "_version": r.version,
            "result": if action == "delete" { "deleted" } else if r.created { "created" } else { "updated" },
            "status": status,
        }})
    } else {
        let reason = o.error.unwrap_or_default();
        let (status, etype) = if reason.starts_with("conflict:") || reason.contains("already exists") {
            (409u16, "version_conflict_engine_exception")
        } else if reason.contains("not found") {
            (404u16, "document_missing_exception")
        } else {
            (400u16, "arkimedb_error")
        };
        json!({ "error": { "reason": reason, "type": etype },
                 "_index": o.collection, "status": status })
    }
}
fn handle(r: Result<J>) -> Response { match r { Ok(v) => Json(v).into_response(), Err(e) => internal(e) } }
fn internal(e: Error) -> Response { err(match &e {
    Error::NotFound(_) => StatusCode::NOT_FOUND,
    Error::Conflict(_) => StatusCode::CONFLICT,
    Error::BadRequest(_) => StatusCode::BAD_REQUEST,
    _ => StatusCode::INTERNAL_SERVER_ERROR,
}, &e.to_string()) }
fn err(code: StatusCode, msg: &str) -> Response {
    (code, Json(json!({ "error": { "reason": msg, "type": "arkimedb_error" }, "status": code.as_u16() }))).into_response()
}
fn not_found(idx: &str, id: &str) -> Response {
    (StatusCode::NOT_FOUND, Json(json!({ "_index": idx, "_id": id, "found": false }))).into_response()
}

// -------- index templates -----------------------------------------------

#[derive(Deserialize, Default)]
struct TemplateQ { filter_path: Option<String>, flat_settings: Option<String> }

fn flatten_settings_in_template(v: &mut J) {
    fn flatten(prefix: &str, v: &J, out: &mut serde_json::Map<String, J>) {
        if let Some(obj) = v.as_object() {
            for (k, vv) in obj {
                let key = if prefix.is_empty() { k.clone() } else { format!("{prefix}.{k}") };
                if vv.is_object() {
                    flatten(&key, vv, out);
                } else {
                    out.insert(key, vv.clone());
                }
            }
        } else {
            out.insert(prefix.to_string(), v.clone());
        }
    }
    if let Some(obj) = v.as_object_mut() {
        if let Some(settings) = obj.get("settings").cloned() {
            let mut flat = serde_json::Map::new();
            flatten("", &settings, &mut flat);
            obj.insert("settings".into(), J::Object(flat));
        }
    }
}

async fn template_list(State(s): State<Arc<AppState>>, Query(q): Query<TemplateQ>) -> Response {
    let all = match s.engine.catalog.list_templates() { Ok(v) => v, Err(e) => return internal(e) };
    let mut m = serde_json::Map::new();
    for (name, body) in all {
        let mut v: J = serde_json::from_slice(&body).unwrap_or(J::Null);
        if q.flat_settings.as_deref().map_or(false, |v| !matches!(v, "false" | "0")) { flatten_settings_in_template(&mut v); }
        m.insert(name, v);
    }
    let resp = J::Object(m);
    apply_filter_path(resp, q.filter_path.as_deref())
}

async fn template_get(Path(names): Path<String>, State(s): State<Arc<AppState>>, Query(q): Query<TemplateQ>) -> Response {
    let wanted = expand_template_names(&s, &names).unwrap_or_default();
    let mut m = serde_json::Map::new();
    for name in &wanted {
        match s.engine.catalog.get_template(name) {
            Ok(Some(body)) => {
                let mut v: J = serde_json::from_slice(&body).unwrap_or(J::Null);
                if q.flat_settings.as_deref().map_or(false, |v| !matches!(v, "false" | "0")) { flatten_settings_in_template(&mut v); }
                m.insert(name.clone(), v);
            }
            Ok(None) => {}
            Err(e) => return internal(e),
        }
    }
    if m.is_empty() {
        return err(StatusCode::NOT_FOUND, &format!("no such template(s): {names}"));
    }
    apply_filter_path(J::Object(m), q.filter_path.as_deref())
}

async fn template_head(Path(names): Path<String>, State(s): State<Arc<AppState>>) -> Response {
    let wanted = expand_template_names(&s, &names).unwrap_or_default();
    for name in &wanted {
        if matches!(s.engine.catalog.get_template(name), Ok(Some(_))) {
            return StatusCode::OK.into_response();
        }
    }
    StatusCode::NOT_FOUND.into_response()
}

async fn template_put(Path(name): Path<String>, State(s): State<Arc<AppState>>, body: String) -> Response {
    // Accept any JSON body; store verbatim.
    let trimmed = body.trim();
    let bytes = if trimmed.is_empty() { b"{}".to_vec() } else { trimmed.as_bytes().to_vec() };
    if serde_json::from_slice::<J>(&bytes).is_err() {
        return err(StatusCode::BAD_REQUEST, "template body is not valid JSON");
    }
    // Template name path may be a single name (not a pattern on PUT).
    let name = name.split(',').next().unwrap_or("").to_string();
    if name.is_empty() { return err(StatusCode::BAD_REQUEST, "empty template name"); }
    match s.engine.catalog.put_template(&name, &bytes) {
        Ok(()) => Json(json!({ "acknowledged": true })).into_response(),
        Err(e) => internal(e),
    }
}

async fn template_delete(Path(names): Path<String>, State(s): State<Arc<AppState>>) -> Response {
    let wanted = expand_template_names(&s, &names).unwrap_or_default();
    let mut any = false;
    for n in wanted {
        if s.engine.catalog.delete_template(&n).unwrap_or(false) { any = true; }
    }
    if any { Json(json!({ "acknowledged": true })).into_response() }
    else { err(StatusCode::NOT_FOUND, "no such template") }
}

fn expand_template_names(s: &AppState, spec: &str) -> Result<Vec<String>> {
    let all: Vec<String> = s.engine.catalog.list_templates()?.into_iter().map(|(k, _)| k).collect();
    let mut out = Vec::new();
    for part in spec.split(',').map(|x| x.trim()).filter(|x| !x.is_empty()) {
        if part.contains('*') || part.contains('?') {
            let m = Glob::compile(part);
            for n in &all { if m.is_match(n) && !out.contains(n) { out.push(n.clone()); } }
        } else if !out.iter().any(|x| x == part) {
            out.push(part.to_string());
        }
    }
    Ok(out)
}

// Minimal glob matcher (supports * and ?).
struct Glob { parts: Vec<GlobPart> }
enum GlobPart { Lit(String), Any, One }
impl Glob {
    fn compile(pat: &str) -> Self {
        let mut parts = Vec::new();
        let mut buf = String::new();
        for c in pat.chars() {
            match c {
                '*' => { if !buf.is_empty() { parts.push(GlobPart::Lit(std::mem::take(&mut buf))); } parts.push(GlobPart::Any); }
                '?' => { if !buf.is_empty() { parts.push(GlobPart::Lit(std::mem::take(&mut buf))); } parts.push(GlobPart::One); }
                x => buf.push(x),
            }
        }
        if !buf.is_empty() { parts.push(GlobPart::Lit(buf)); }
        Self { parts }
    }
    fn is_match(&self, s: &str) -> bool { gm(&self.parts, s) }
}
fn gm(parts: &[GlobPart], s: &str) -> bool {
    match parts.first() {
        None => s.is_empty(),
        Some(GlobPart::Lit(l)) => s.strip_prefix(l.as_str()).map_or(false, |rest| gm(&parts[1..], rest)),
        Some(GlobPart::One) => { let mut it = s.chars(); it.next().is_some() && gm(&parts[1..], it.as_str()) }
        Some(GlobPart::Any) => {
            if gm(&parts[1..], s) { return true; }
            let mut it = s.chars();
            while it.next().is_some() { if gm(parts, it.as_str()) { return true; } }
            false
        }
    }
}

// -------- filter_path ---------------------------------------------------

#[derive(Clone, Debug)]
enum FpSeg { Lit(String), Star, DStar }

fn parse_fp(expr: &str) -> Vec<Vec<FpSeg>> {
    expr.split(',').map(|p| p.trim()).filter(|p| !p.is_empty()).map(|p| {
        p.split('.').map(|seg| match seg {
            "*" => FpSeg::Star,
            "**" => FpSeg::DStar,
            x => FpSeg::Lit(x.to_string()),
        }).collect()
    }).collect()
}

fn apply_filter_path(v: J, expr: Option<&str>) -> Response {
    let Some(expr) = expr else { return Json(v).into_response(); };
    let pats = parse_fp(expr);
    if pats.is_empty() { return Json(v).into_response(); }
    let positions: Vec<(usize, usize)> = (0..pats.len()).map(|i| (i, 0)).collect();
    let filtered = fp_filter(v, &positions, &pats).unwrap_or(J::Object(serde_json::Map::new()));
    Json(filtered).into_response()
}

fn fp_advance(positions: &[(usize, usize)], pats: &[Vec<FpSeg>], key: &str) -> (bool, Vec<(usize, usize)>) {
    let mut complete = false;
    let mut next = Vec::new();
    for &(pi, si) in positions {
        fp_advance_from(pi, si, pats, key, &mut next, &mut complete);
    }
    next.sort_unstable();
    next.dedup();
    (complete, next)
}

fn fp_advance_from(pi: usize, si: usize, pats: &[Vec<FpSeg>], key: &str,
                   next: &mut Vec<(usize, usize)>, complete: &mut bool) {
    let p = &pats[pi];
    if si >= p.len() { return; }
    match &p[si] {
        FpSeg::Lit(s) => {
            if s == key { fp_after(pi, si + 1, pats, next, complete); }
        }
        FpSeg::Star => {
            fp_after(pi, si + 1, pats, next, complete);
        }
        FpSeg::DStar => {
            // DStar consumes this key and stays.
            fp_after(pi, si, pats, next, complete);
            // Or DStar matches zero — try next seg on this key.
            fp_advance_from(pi, si + 1, pats, key, next, complete);
        }
    }
}

fn fp_after(pi: usize, si: usize, pats: &[Vec<FpSeg>],
            next: &mut Vec<(usize, usize)>, complete: &mut bool) {
    let p = &pats[pi];
    if si == p.len() { *complete = true; return; }
    // Trailing DStar matches anything, including nothing.
    if matches!(p[si], FpSeg::DStar) && si + 1 == p.len() { *complete = true; }
    next.push((pi, si));
}

fn fp_filter(v: J, positions: &[(usize, usize)], pats: &[Vec<FpSeg>]) -> Option<J> {
    match v {
        J::Object(m) => {
            let mut out = serde_json::Map::new();
            for (k, child) in m {
                let (complete, next) = fp_advance(positions, pats, &k);
                if complete {
                    out.insert(k, child);
                } else if !next.is_empty() {
                    if let Some(sub) = fp_filter(child, &next, pats) {
                        out.insert(k, sub);
                    }
                }
            }
            if out.is_empty() { None } else { Some(J::Object(out)) }
        }
        J::Array(arr) => {
            let mut out = Vec::new();
            for child in arr {
                if let Some(sub) = fp_filter(child, positions, pats) { out.push(sub); }
            }
            if out.is_empty() { None } else { Some(J::Array(out)) }
        }
        _ => None,
    }
}
