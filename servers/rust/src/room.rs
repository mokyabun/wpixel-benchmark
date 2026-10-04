// Room endpoint; the protocol is in loader/room.go.
// One process serves every shard port from one room registry. A room's mutex
// orders its broadcasts; each member has an unbounded channel drained by its
// own writer task, so a slow member never blocks the room.
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use axum::extract::ws::{Message, Utf8Bytes, WebSocket, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::response::Response;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

const HISTORY_SIZE: usize = 20;
const MAX_BODY: usize = 1024;
const ERR_INVALID: &str = r#"{"t":"err","code":"invalid"}"#;

#[derive(Default)]
pub struct Rooms(Mutex<HashMap<String, Arc<Mutex<Room>>>>);

#[derive(Default)]
pub struct Room {
    v: u64,
    last_user: u64,
    members: HashMap<u64, mpsc::UnboundedSender<Utf8Bytes>>,
    recent: VecDeque<Utf8Bytes>,
}

#[derive(Deserialize)]
pub struct RoomQuery {
    id: String,
}

#[derive(Deserialize)]
struct Inbound {
    t: Option<String>,
    s: Option<u64>,
    body: Option<String>,
}

#[derive(Serialize)]
struct Broadcast<'a> {
    t: &'static str,
    u: u64,
    s: u64,
    v: u64,
    body: &'a str,
}

impl Rooms {
    fn get(&self, id: String) -> Arc<Mutex<Room>> {
        self.0.lock().unwrap().entry(id).or_default().clone()
    }
}

impl Room {
    fn join(&mut self, tx: mpsc::UnboundedSender<Utf8Bytes>) -> u64 {
        self.last_user += 1;
        let recent: Vec<&str> = self.recent.iter().map(|m| m.as_str()).collect();
        let hello = format!(r#"{{"t":"hello","u":{},"v":{},"recent":[{}]}}"#, self.last_user, self.v, recent.join(","));
        let _ = tx.send(hello.into());
        self.members.insert(self.last_user, tx);
        self.last_user
    }

    fn broadcast(&mut self, u: u64, s: u64, body: &str) {
        self.v += 1;
        let out: Utf8Bytes = serde_json::to_string(&Broadcast { t: "msg", u, s, v: self.v, body }).unwrap().into();
        if self.recent.len() == HISTORY_SIZE {
            self.recent.pop_front();
        }
        self.recent.push_back(out.clone());
        for tx in self.members.values() {
            let _ = tx.send(out.clone());
        }
    }
}

/// The message's (seq, body) if it passes validation.
fn parse(text: &str) -> Option<(u64, String)> {
    let m: Inbound = serde_json::from_str(text).ok()?;
    let body = m.body?;
    let n = body.chars().count();
    (m.t.as_deref() == Some("msg") && (1..=MAX_BODY).contains(&n)).then_some((m.s?, body))
}

pub async fn handler(ws: WebSocketUpgrade, Query(q): Query<RoomQuery>, State(rooms): State<Arc<Rooms>>) -> Response {
    ws.max_message_size(64 << 10).on_upgrade(move |socket| serve(socket, rooms.get(q.id)))
}

async fn serve(socket: WebSocket, room: Arc<Mutex<Room>>) {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let u = room.lock().unwrap().join(tx.clone());
    let (mut sink, mut stream) = socket.split();
    let writer = tokio::spawn(async move {
        while let Some(m) = rx.recv().await {
            if sink.send(Message::Text(m)).await.is_err() {
                return;
            }
        }
    });

    while let Some(Ok(msg)) = stream.next().await {
        match msg {
            Message::Text(text) => match parse(&text) {
                Some((s, body)) => room.lock().unwrap().broadcast(u, s, &body),
                None => {
                    let _ = tx.send(Utf8Bytes::from_static(ERR_INVALID));
                }
            },
            Message::Binary(_) => {
                let _ = tx.send(Utf8Bytes::from_static(ERR_INVALID));
            }
            Message::Close(_) => break,
            _ => {}
        }
    }
    room.lock().unwrap().members.remove(&u);
    writer.abort();
}
