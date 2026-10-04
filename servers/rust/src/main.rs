// Rust + axum (Tokio) echo server (/ws on PORT) and room server (/room on
// PORT+1 .. PORT+CORES, room.rs).
// CORES sets the Tokio multi-thread runtime's worker_threads.
mod room;

use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::response::Response;
use axum::routing::{any, get};
use axum::serve::ListenerExt;
use axum::Router;

const MAX_PAYLOAD: usize = 1 << 20;

async fn ws(upgrade: WebSocketUpgrade) -> Response {
    upgrade
        .max_message_size(MAX_PAYLOAD)
        .max_frame_size(MAX_PAYLOAD)
        .on_upgrade(echo)
}

async fn echo(mut socket: WebSocket) {
    while let Some(Ok(msg)) = socket.recv().await {
        match msg {
            Message::Binary(_) | Message::Text(_) => {
                if socket.send(msg).await.is_err() {
                    return;
                }
            }
            Message::Close(_) => return,
            _ => {}
        }
    }
}

fn main() {
    let cores: usize = std::env::var("CORES").ok().and_then(|v| v.parse().ok()).unwrap_or(1);
    let port: u16 = std::env::var("PORT").ok().and_then(|v| v.parse().ok()).unwrap_or(8080);

    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(cores)
        .enable_io()
        .build()
        .expect("tokio runtime")
        .block_on(async move {
            let app = Router::new()
                .route("/health", get(|| async { "ok" }))
                .route("/ws", any(ws))
                .route("/room", any(room::handler))
                .with_state(Arc::new(room::Rooms::default()));
            // One process owns every shard port, so all ports share the room registry.
            for p in port + 1..=port + cores as u16 {
                tokio::spawn(serve_on(p, app.clone()));
            }
            serve_on(port, app).await;
        });
}

async fn serve_on(port: u16, app: Router) {
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port))
        .await
        .expect("bind")
        .tap_io(|tcp| {
            let _ = tcp.set_nodelay(true);
        });
    axum::serve(listener, app).await.expect("serve");
}
