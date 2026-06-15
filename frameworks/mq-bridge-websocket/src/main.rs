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

use mq_bridge::models::{Endpoint, EndpointType, WebSocketConfig};
use mq_bridge::{CanonicalMessage, Handled, HandlerError, Route};

async fn echo(msg: CanonicalMessage) -> Result<Handled, HandlerError> {
    let ws_type = msg
        .metadata
        .get("ws_message_type")
        .cloned()
        .unwrap_or_else(|| "text".to_string());
    let reply = CanonicalMessage::new(msg.payload.to_vec(), None)
        .with_metadata_kv("ws_message_type", ws_type);
    Ok(Handled::Publish(reply))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let listen = std::env::var("MQB_LISTEN").unwrap_or_else(|_| "0.0.0.0:8080".to_string());
    let workers = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(8);

    let ws = WebSocketConfig::new(listen).with_path("/ws");
    let input = Endpoint::new(EndpointType::WebSocket(ws));
    let output = Endpoint::new_response();

    // batch_size defaults to 1, which funnels every frame through one
    // dispatch; raise it so the echo path can coalesce frames per consumer
    // poll (the HTTP entry uses 1024 for the same reason).
    let route = Route::new(input, output)
        .with_concurrency(workers)
        .with_batch_size(1024)
        .with_handler(echo);
    let handle = route.run("httparena-ws").await?;
    handle.join().await?;
    Ok(())
}
