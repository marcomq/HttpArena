//! HttpArena h2c server for mq-bridge (Rust) — cleartext HTTP/2 on `0.0.0.0:8082`.
//!
//! mq-bridge's HTTP server uses hyper-util's auto connection builder, which
//! negotiates HTTP/2 prior-knowledge (h2c) on a plaintext port. This entry binds
//! 8082 and serves the baseline + JSON endpoints the `baseline-h2c` / `json-h2c`
//! profiles drive; it shares the route model of the core `mq-bridge` entry.
//!
//! * `GET  /pipeline`            -> `ok`
//! * `GET  /baseline11?a=&b=`    -> `a+b`
//! * `POST /baseline11?a=&b=`    -> `a+b+body`
//! * `GET  /baseline2?a=&b=`     -> `a+b`
//! * `GET  /json/{count}?m=`     -> processed dataset JSON (from /data/dataset.json)

use mq_bridge::models::{Endpoint, EndpointType, HttpConfig};
use mq_bridge::{CanonicalMessage, Handled, HandlerError, Route};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

const SERVER: &str = "mq-bridge";

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
struct RatingOut {
    score: i64,
    count: i64,
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
struct JsonResponse<'a> {
    items: Vec<ProcessedItem<'a>>,
    count: usize,
}

fn load_dataset() -> Vec<DatasetItem> {
    let path = std::env::var("DATASET_PATH").unwrap_or_else(|_| "/data/dataset.json".to_string());
    std::fs::read_to_string(&path)
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_default()
}

fn reply(body: Vec<u8>, content_type: &str) -> CanonicalMessage {
    CanonicalMessage::new(body, None)
        .with_metadata_kv("content-type", content_type)
        .with_metadata_kv("Server", SERVER)
}

fn query_int(query: &str, key: &str) -> Option<i64> {
    query
        .split('&')
        .find_map(|pair| pair.strip_prefix(key).and_then(|r| r.strip_prefix('=')))
        .and_then(|v| v.parse::<i64>().ok())
}

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

async fn handle(dataset: Arc<Vec<DatasetItem>>, msg: CanonicalMessage) -> Result<Handled, HandlerError> {
    let method = msg.metadata.get("http_method").map(String::as_str).unwrap_or("");
    let path = msg.metadata.get("http_path").map(String::as_str).unwrap_or("");
    let query = msg.metadata.get("http_query").map(String::as_str).unwrap_or("");

    let out = match (method, path) {
        ("GET", "/pipeline") => reply(b"ok".to_vec(), "text/plain; charset=utf-8"),
        ("GET", "/baseline11") | ("GET", "/baseline2") => {
            let sum = query_int(query, "a").unwrap_or(0) + query_int(query, "b").unwrap_or(0);
            reply(sum.to_string().into_bytes(), "text/plain; charset=utf-8")
        }
        ("POST", "/baseline11") => {
            let mut sum = query_int(query, "a").unwrap_or(0) + query_int(query, "b").unwrap_or(0);
            if let Ok(s) = std::str::from_utf8(&msg.payload) {
                if let Ok(n) = s.trim().parse::<i64>() {
                    sum += n;
                }
            }
            reply(sum.to_string().into_bytes(), "text/plain; charset=utf-8")
        }
        ("GET", p) if p.starts_with("/json/") => {
            let count: usize = p["/json/".len()..].parse().unwrap_or(0);
            let m = query_int(query, "m").unwrap_or(1);
            reply(build_json(&dataset, count, m), "application/json")
        }
        _ => reply(b"Not Found".to_vec(), "text/plain; charset=utf-8")
            .with_metadata_kv("http_status_code", "404"),
    };

    Ok(Handled::Publish(out))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let listen = std::env::var("MQB_LISTEN").unwrap_or_else(|_| "0.0.0.0:8082".to_string());
    let dataset = Arc::new(load_dataset());

    let mut http = HttpConfig::new(listen).with_inline_response_fast_path(true);
    http.concurrency_limit = Some(65_536);
    http.internal_buffer_size = Some(16_384);

    let input = Endpoint::new(EndpointType::Http(http));
    let output = Endpoint::new_response();

    let route = Route::new(input, output).with_handler(move |msg| handle(dataset.clone(), msg));
    let handle = route.run("httparena-h2c").await?;
    handle.join().await?;
    Ok(())
}
