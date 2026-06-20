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

use mq_bridge::models::{Endpoint, EndpointType, WebSocketConfig, WebSocketExecutionMode};
use mq_bridge::{CanonicalMessage, Handled, HandlerError, Route};

async fn echo(msg: CanonicalMessage) -> Result<Handled, HandlerError> {
    Ok(Handled::Publish(msg))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let listen = std::env::var("MQB_LISTEN").unwrap_or_else(|_| "0.0.0.0:8080".to_string());

    let ws = WebSocketConfig::new(listen)
        .with_path("/ws")
        .with_execution_mode(WebSocketExecutionMode::DirectOnly);
    let input = Endpoint::new(EndpointType::WebSocket(ws));

    let output = Endpoint::new_response();

    let route = Route::new(input, output).with_handler(echo);
    let handle = route.run("httparena-ws").await?;
    handle.join().await?;
    Ok(())
}
