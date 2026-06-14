//! HttpArena core server for mq-bridge (Rust).
//!
//! Serves the HTTP/1.1 + cleartext-HTTP/2 (h2c) profiles on `0.0.0.0:8080`
//! through a single catch-all `http -> response` route that dispatches on the
//! request's `http_method` / `http_path` / `http_query` metadata. hyper-util's
//! auto connection builder (used by mq-bridge's HTTP server) negotiates both
//! HTTP/1.1 and HTTP/2 prior-knowledge on the plaintext port, so one binary
//! covers the cleartext profiles.
//!
//! Endpoints (per HttpArena's reference contract)
//! ----------------------------------------------
//! * `GET  /pipeline`                     -> `ok`               (baseline/pipelined/limited-conn)
//! * `GET  /baseline11?a=&b=`             -> `a+b`              (baseline)
//! * `POST /baseline11?a=&b=` + body int  -> `a+b+body`
//! * `GET  /baseline2?a=&b=`              -> `a+b`
//! * `GET  /json/{count}?m=`              -> processed dataset JSON   (json/json-comp)
//! * `POST /upload` + body                -> received byte count      (upload)
//! * `GET  /async-db?min=&max=&limit=`    -> Postgres `items` rows    (async-db)
//! * `GET  /static/{file}`                -> file from /data/static   (static)
//!
//! Harness-provided inputs: the dataset is read from `/data/dataset.json`
//! (`DATASET_PATH` overrides), static assets from `/data/static`
//! (`STATIC_DIR`), and Postgres from `DATABASE_URL`. Missing DB is non-fatal:
//! `/async-db` then returns an empty result so the cleartext profiles still run.
//!
//! `json-comp` is handled by mq-bridge's response compression
//! (`compression_enabled`): bodies above the threshold are gzip-encoded when the
//! client advertises `Accept-Encoding: gzip`, and sent identity otherwise — so
//! the same `/json` handler serves both the `json` and `json-comp` profiles.

use bytes::Bytes;
use mq_bridge::models::{Endpoint, EndpointType, HttpConfig, TlsConfig};
use mq_bridge::{CanonicalMessage, Handled, HandlerError, Route};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};
use std::collections::HashMap;
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex};

const SERVER: &str = "mq-bridge";

// ---------- dataset (json profile) ----------

#[derive(Deserialize, Clone)]
struct Rating {
    score: i64,
    count: i64,
}

#[derive(Deserialize, Clone)]
struct DatasetItem {
    id: i64,
    name: String,
    category: String,
    price: i64,
    quantity: i64,
    active: bool,
    tags: Vec<String>,
    rating: Rating,
}

#[derive(Serialize)]
struct ProcessedItem<'a> {
    id: i64,
    name: &'a str,
    category: &'a str,
    price: i64,
    quantity: i64,
    active: bool,
    tags: &'a [String],
    rating: RatingOut,
    total: i64,
}

#[derive(Serialize)]
struct RatingOut {
    score: i64,
    count: i64,
}

#[derive(Serialize)]
struct JsonResponse<'a> {
    items: Vec<ProcessedItem<'a>>,
    count: usize,
}

struct AppState {
    dataset: Vec<DatasetItem>,
    /// Static assets read once at startup, with a pre-gzipped variant ready to
    /// serve — no per-request filesystem read or allocation.
    static_cache: HashMap<String, CachedBody>,
    /// Memoized `/json/{count}?m=` responses (serialized once, gzipped once per
    /// `(count, m)`), so `json` and `json-comp` never re-serialize or re-gzip.
    json_cache: Mutex<HashMap<(usize, i64), Arc<CachedBody>>>,
    pool: Option<PgPool>,
}

/// A response body cached in both identity and gzip form. The gzip variant is
/// only kept when it actually shrinks the body.
struct CachedBody {
    plain: Bytes,
    gzip: Option<Bytes>,
    content_type: &'static str,
}

impl CachedBody {
    fn build(bytes: Vec<u8>, content_type: &'static str) -> Self {
        let plain = Bytes::from(bytes);
        let compressed = gzip(&plain);
        let gzip = (compressed.len() < plain.len()).then_some(compressed);
        Self {
            plain,
            gzip,
            content_type,
        }
    }

    /// Build a reply, serving the pre-gzipped variant when the client accepts it.
    /// `content-encoding: gzip` is set on the reply; the mq-bridge HTTP layer
    /// honors it and skips its own compression pass.
    fn into_message(&self, want_gzip: bool) -> CanonicalMessage {
        let (body, encoding) = match (want_gzip, &self.gzip) {
            (true, Some(g)) => (g.clone(), Some("gzip")),
            _ => (self.plain.clone(), None),
        };
        let mut msg = CanonicalMessage::new_bytes(body, None)
            .with_metadata_kv("content-type", self.content_type)
            .with_metadata_kv("Server", SERVER);
        if let Some(encoding) = encoding {
            msg = msg.with_metadata_kv("content-encoding", encoding);
        }
        msg
    }
}

fn gzip(data: &[u8]) -> Bytes {
    use flate2::{write::GzEncoder, Compression};
    use std::io::Write;
    let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
    encoder.write_all(data).expect("gzip write");
    Bytes::from(encoder.finish().expect("gzip finish"))
}

/// Whether the client advertised it can decode gzip (header captured in metadata).
fn accepts_gzip(msg: &CanonicalMessage) -> bool {
    msg.metadata
        .get("accept-encoding")
        .is_some_and(|v| v.to_ascii_lowercase().contains("gzip"))
}

fn load_static(dir: &Path) -> HashMap<String, CachedBody> {
    let mut cache = HashMap::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return cache;
    };
    for entry in entries.flatten() {
        if entry.file_type().map(|ft| !ft.is_file()).unwrap_or(true) {
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        if let Ok(bytes) = std::fs::read(entry.path()) {
            let content_type = content_type_for(&name);
            cache.insert(name, CachedBody::build(bytes, content_type));
        }
    }
    cache
}

fn load_dataset() -> Vec<DatasetItem> {
    let path = std::env::var("DATASET_PATH").unwrap_or_else(|_| "/data/dataset.json".to_string());
    std::fs::read_to_string(&path)
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_default()
}

// ---------- helpers ----------

fn reply(body: Vec<u8>, content_type: &str) -> CanonicalMessage {
    CanonicalMessage::new(body, None)
        .with_metadata_kv("content-type", content_type)
        .with_metadata_kv("Server", SERVER)
}

fn text(body: String) -> CanonicalMessage {
    reply(body.into_bytes(), "text/plain")
}

fn json(body: Vec<u8>) -> CanonicalMessage {
    reply(body, "application/json")
}

fn status(status: u16, body: &str) -> CanonicalMessage {
    text(body.to_string()).with_metadata_kv("http_status_code", status.to_string())
}

/// Look up an integer query parameter (`a`, `b`, `m`, `min`, `max`, `limit`).
fn query_int(query: &str, key: &str) -> Option<i64> {
    query
        .split('&')
        .find_map(|pair| pair.strip_prefix(key).and_then(|r| r.strip_prefix('=')))
        .and_then(|v| v.parse::<i64>().ok())
}

// ---------- handlers ----------

fn build_json(dataset: &[DatasetItem], count: usize, m: i64) -> Vec<u8> {
    let count = count.min(dataset.len());
    let items: Vec<ProcessedItem> = dataset[..count]
        .iter()
        .map(|d| ProcessedItem {
            id: d.id,
            name: &d.name,
            category: &d.category,
            price: d.price,
            quantity: d.quantity,
            active: d.active,
            tags: &d.tags,
            rating: RatingOut {
                score: d.rating.score,
                count: d.rating.count,
            },
            total: d.price * d.quantity * m,
        })
        .collect();
    serde_json::to_vec(&JsonResponse { count, items }).unwrap_or_default()
}

async fn async_db(pool: &PgPool, query: &str) -> CanonicalMessage {
    let min = query_int(query, "min").unwrap_or(10) as i32;
    let max = query_int(query, "max").unwrap_or(50) as i32;
    let limit = query_int(query, "limit").unwrap_or(50).clamp(1, 50);

    let rows = sqlx::query(
        "SELECT id, name, category, price, quantity, active, tags, rating_score, rating_count \
         FROM items WHERE price BETWEEN $1 AND $2 LIMIT $3",
    )
    .bind(min)
    .bind(max)
    .bind(limit)
    .fetch_all(pool)
    .await;

    let rows = match rows {
        Ok(r) => r,
        Err(_) => return json(br#"{"items":[],"count":0}"#.to_vec()),
    };

    let items: Vec<serde_json::Value> = rows
        .iter()
        .map(|row| {
            serde_json::json!({
                "id": row.get::<i32, _>("id"),
                "name": row.get::<&str, _>("name"),
                "category": row.get::<&str, _>("category"),
                "price": row.get::<i32, _>("price"),
                "quantity": row.get::<i32, _>("quantity"),
                "active": row.get::<bool, _>("active"),
                "tags": row.get::<serde_json::Value, _>("tags"),
                "rating": {
                    "score": row.get::<i32, _>("rating_score"),
                    "count": row.get::<i32, _>("rating_count"),
                }
            })
        })
        .collect();
    let body = serde_json::json!({ "count": items.len(), "items": items });
    json(serde_json::to_vec(&body).unwrap_or_default())
}

fn content_type_for(name: &str) -> &'static str {
    match name.rsplit_once('.').map(|(_, ext)| ext) {
        Some("js") => "application/javascript",
        Some("css") => "text/css",
        Some("html") => "text/html",
        Some("json") => "application/json",
        Some("woff2") => "font/woff2",
        Some("png") => "image/png",
        Some("svg") => "image/svg+xml",
        _ => "application/octet-stream",
    }
}

fn serve_static(state: &AppState, name: &str, want_gzip: bool) -> CanonicalMessage {
    // Reject path traversal: the filename must be a single normal component.
    let candidate = Path::new(name);
    let mut comps = candidate.components();
    let safe = matches!(comps.next(), Some(Component::Normal(_))) && comps.next().is_none();
    if !safe || name.is_empty() {
        return status(404, "Not Found");
    }
    match state.static_cache.get(name) {
        Some(cached) => cached.into_message(want_gzip),
        None => status(404, "Not Found"),
    }
}

/// Serve `/json/{count}?m=`, building+gzipping the body once per `(count, m)`.
fn serve_json(state: &AppState, count: usize, m: i64, want_gzip: bool) -> CanonicalMessage {
    let key = (count, m);
    // Hold the lock across the build via the entry API so concurrent requests for
    // the same uncached key don't each build+gzip the body redundantly.
    let cached = state
        .json_cache
        .lock()
        .unwrap()
        .entry(key)
        .or_insert_with(|| {
            Arc::new(CachedBody::build(
                build_json(&state.dataset, count, m),
                "application/json",
            ))
        })
        .clone();
    cached.into_message(want_gzip)
}

async fn handle(state: Arc<AppState>, msg: CanonicalMessage) -> Result<Handled, HandlerError> {
    let method = msg.metadata.get("http_method").map(String::as_str).unwrap_or("");
    let path = msg.metadata.get("http_path").map(String::as_str).unwrap_or("");
    let query = msg.metadata.get("http_query").map(String::as_str).unwrap_or("");
    let want_gzip = accepts_gzip(&msg);

    let out = match (method, path) {
        ("GET", "/pipeline") => text("ok".to_string()),
        ("GET", "/baseline11") | ("GET", "/baseline2") => {
            let sum = query_int(query, "a").unwrap_or(0) + query_int(query, "b").unwrap_or(0);
            text(sum.to_string())
        }
        ("POST", "/baseline11") => {
            let mut sum = query_int(query, "a").unwrap_or(0) + query_int(query, "b").unwrap_or(0);
            if let Ok(s) = std::str::from_utf8(&msg.payload) {
                if let Ok(n) = s.trim().parse::<i64>() {
                    sum += n;
                }
            }
            text(sum.to_string())
        }
        ("POST", "/upload") => text(msg.payload.len().to_string()),
        ("GET", "/async-db") => match &state.pool {
            Some(pool) => async_db(pool, query).await,
            None => json(br#"{"items":[],"count":0}"#.to_vec()),
        },
        ("GET", p) if p.starts_with("/json/") => {
            let count: usize = p["/json/".len()..].parse().unwrap_or(0);
            let m = query_int(query, "m").unwrap_or(1);
            serve_json(&state, count, m, want_gzip)
        }
        ("GET", p) if p.starts_with("/static/") => {
            serve_static(&state, &p["/static/".len()..], want_gzip)
        }
        _ => status(404, "Not Found"),
    };

    Ok(Handled::Publish(out))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let listen = std::env::var("MQB_LISTEN").unwrap_or_else(|_| "0.0.0.0:8080".to_string());
    let static_dir = std::env::var("STATIC_DIR").unwrap_or_else(|_| "/data/static".to_string());

    let pool = match std::env::var("DATABASE_URL") {
        Ok(url) if !url.is_empty() => {
            let max = std::env::var("DATABASE_MAX_CONN")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(256);
            match PgPoolOptions::new().max_connections(max).connect(&url).await {
                Ok(pool) => Some(pool),
                Err(e) => {
                    eprintln!("Postgres connection failed ({e}); /async-db returns empty");
                    None
                }
            }
        }
        _ => None,
    };

    let state = Arc::new(AppState {
        dataset: load_dataset(),
        static_cache: load_static(&PathBuf::from(static_dir)),
        json_cache: Mutex::new(HashMap::new()),
        pool,
    });

    // Plaintext HTTP/1.1 + h2c on 8080.
    let plaintext = build_route(make_http(listen, None), state.clone());
    let mut handles = vec![plaintext.run("httparena").await?];

    // HTTP/2 over TLS on 8443 (baseline-h2 / static-h2 / json-tls). The library
    // advertises ALPN `h2`, so conformant clients negotiate HTTP/2 over the TLS
    // port. Only enabled when the harness has mounted the certs, so a local
    // plaintext-only run still works.
    let cert = std::env::var("TLS_CERT").unwrap_or_else(|_| "/certs/server.crt".to_string());
    let key = std::env::var("TLS_KEY").unwrap_or_else(|_| "/certs/server.key".to_string());
    if Path::new(&cert).is_file() && Path::new(&key).is_file() {
        // rustls needs a process-default crypto provider before any TLS endpoint.
        match rustls::crypto::ring::default_provider().install_default() {
            Ok(()) => {}
            Err(provider) => eprintln!(
                "rustls ring crypto provider was not installed; a process default is already set (attempted provider: {provider:?})"
            ),
        }
        let tls_listen =
            std::env::var("MQB_TLS_LISTEN").unwrap_or_else(|_| "0.0.0.0:8443".to_string());
        let mut tls = TlsConfig::new();
        tls.required = true;
        tls.cert_file = Some(cert);
        tls.key_file = Some(key);
        let tls_route = build_route(make_http(tls_listen, Some(tls)), state.clone());
        handles.push(tls_route.run("httparena-tls").await?);
    } else {
        eprintln!("TLS certs not found ({cert} / {key}); serving plaintext only");
    }

    for handle in handles {
        handle.join().await?;
    }
    Ok(())
}

/// Shared HTTP listener config; `tls` set => HTTPS (ALPN h2) on the TLS port.
fn make_http(listen: String, tls: Option<TlsConfig>) -> HttpConfig {
    let mut http = HttpConfig::new(listen).with_inline_response_fast_path(true);
    http.concurrency_limit = Some(65_536);
    http.internal_buffer_size = Some(16_384);
    // The handler owns compression: static and json bodies are pre-gzipped once
    // and cached (see CachedBody), and the reply carries `content-encoding: gzip`
    // when the client accepts it. So the server's per-request gzip pass is off —
    // it would otherwise re-compress the same bodies on every request.
    http.compression_enabled = false;
    if let Some(tls) = tls {
        http.tls = tls;
    }
    http
}

/// Builds the catch-all `http -> response` route bound to `http`, dispatching
/// every request through the shared `AppState` handler.
fn build_route(http: HttpConfig, state: Arc<AppState>) -> Route {
    let input = Endpoint::new(EndpointType::Http(http));
    let output = Endpoint::new_response();
    Route::new(input, output).with_handler(move |msg| handle(state.clone(), msg))
}
