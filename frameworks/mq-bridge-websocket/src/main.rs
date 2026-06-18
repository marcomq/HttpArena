//! HttpArena WebSocket echo server for mq-bridge (Rust).
//!
//! Accepts WebSocket upgrades on `0.0.0.0:8080` at path `/ws` and echoes each
//! inbound frame back on the same connection (the `echo-ws` profile).
//!
//! The route is `websocket -> response`: mq-bridge's WebSocket consumer turns
//! each inbound frame into a `CanonicalMessage` (tagging text/binary in the
//! `ws_message_type` metadata), the handler returns that payload unchanged, and
//! the Response output sends it back as a Reply on the originating socket. The
//! reply honours `ws_message_type`, so text stays text and binary stays binary.

use mq_bridge::models::{BufferMiddleware, Endpoint, EndpointType, Middleware, WebSocketConfig};
use mq_bridge::{CanonicalMessage, Handled, HandlerError, Route};

async fn echo(msg: CanonicalMessage) -> Result<Handled, HandlerError> {
    // The inbound message already carries the original payload and the
    // `ws_message_type` metadata (set by the WS consumer), and the reply path
    // reads `ws_message_type` straight off it — so echo it back as-is. This
    // avoids a payload copy, a String clone, a fresh CanonicalMessage, and a
    // metadata insert on every frame.
    Ok(Handled::Publish(msg))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let listen = std::env::var("MQB_LISTEN").unwrap_or_else(|_| "0.0.0.0:8080".to_string());
    let workers = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(8);

    let mut ws = WebSocketConfig::new(listen).with_path("/ws");
    ws.internal_buffer_size = Some(65_536);
    let mut input = Endpoint::new(EndpointType::WebSocket(ws));

    // Input buffer middleware: coalesce inbound frames from all connections so
    // the route consumer dispatches them in larger batches. Tunable via env so
    // the benchmark can sweep values:
    //   MQB_BUF_MAX_MSGS    - flush once this many frames are buffered (default 512)
    //   MQB_BUF_MAX_DELAY_MS - flush a partial buffer after this long (default 1ms)
    let buf_max_messages = std::env::var("MQB_BUF_MAX_MSGS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(512usize);
    let buf_max_delay_ms = std::env::var("MQB_BUF_MAX_DELAY_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1u64);
    input.middlewares = vec![Middleware::Buffer(BufferMiddleware {
        max_messages: buf_max_messages,
        max_delay_ms: buf_max_delay_ms,
    })];

    let output = Endpoint::new_response();

    // Unlike the HTTP entries (inline fast path bypasses the route consumer),
    // the WS echo path DOES run through the consumer/batch pipeline, so
    // batch_size applies here. Raised from the default 1 to coalesce frames per
    // consumer poll — unverified; confirm with an echo-ws run before trusting it.
    let route = Route::new(input, output)
        .with_concurrency(workers)
        .with_batch_size(1024)
        .with_handler(echo);
    let handle = route.run("httparena-ws").await?;
    handle.join().await?;
    Ok(())
}
