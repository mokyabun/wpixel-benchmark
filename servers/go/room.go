// Room endpoint; the protocol is in loader/room.go.
// One process serves every shard port from one room registry. A room's mutex
// orders its broadcasts; each member has an unbounded queue drained by its
// own writer goroutine, so a slow member never blocks the room.
package main

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"unicode/utf8"

	"github.com/coder/websocket"
)

const (
	historySize = 20
	maxBody     = 1024
)

type inbound struct {
	T    string  `json:"t"`
	S    *uint64 `json:"s"`
	Body *string `json:"body"`
}

type broadcast struct {
	T    string `json:"t"`
	U    uint64 `json:"u"`
	S    uint64 `json:"s"`
	V    uint64 `json:"v"`
	Body string `json:"body"`
}

type hello struct {
	T      string            `json:"t"`
	U      uint64            `json:"u"`
	V      uint64            `json:"v"`
	Recent []json.RawMessage `json:"recent"`
}

var errInvalid = []byte(`{"t":"err","code":"invalid"}`)

type member struct {
	mu    sync.Mutex
	queue [][]byte
	wake  chan struct{}
}

func (m *member) push(b []byte) {
	m.mu.Lock()
	m.queue = append(m.queue, b)
	m.mu.Unlock()
	select {
	case m.wake <- struct{}{}:
	default:
	}
}

type room struct {
	mu       sync.Mutex
	v        uint64
	lastUser uint64
	members  map[uint64]*member
	recent   []json.RawMessage
}

var rooms = struct {
	sync.Mutex
	m map[string]*room
}{m: map[string]*room{}}

func getRoom(id string) *room {
	rooms.Lock()
	defer rooms.Unlock()
	r := rooms.m[id]
	if r == nil {
		r = &room{members: map[uint64]*member{}}
		rooms.m[id] = r
	}
	return r
}

func (r *room) join(m *member) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lastUser++
	r.members[r.lastUser] = m
	b, _ := json.Marshal(hello{T: "hello", U: r.lastUser, V: r.v, Recent: append([]json.RawMessage{}, r.recent...)})
	m.push(b)
	return r.lastUser
}

func (r *room) leave(u uint64) {
	r.mu.Lock()
	delete(r.members, u)
	r.mu.Unlock()
}

func (r *room) broadcast(u, s uint64, body string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.v++
	b, _ := json.Marshal(broadcast{T: "msg", U: u, S: s, V: r.v, Body: body})
	if len(r.recent) == historySize {
		copy(r.recent, r.recent[1:])
		r.recent[historySize-1] = b
	} else {
		r.recent = append(r.recent, b)
	}
	for _, m := range r.members {
		m.push(b)
	}
}

func roomHandler(w http.ResponseWriter, req *http.Request) {
	id := req.URL.Query().Get("id")
	if id == "" {
		http.Error(w, "missing id", http.StatusBadRequest)
		return
	}
	c, err := websocket.Accept(w, req, &websocket.AcceptOptions{
		CompressionMode:    websocket.CompressionDisabled,
		InsecureSkipVerify: true,
	})
	if err != nil {
		return
	}
	defer c.CloseNow()
	c.SetReadLimit(64 << 10)

	ctx, cancel := context.WithCancel(req.Context())
	defer cancel()
	r := getRoom(id)
	me := &member{wake: make(chan struct{}, 1)}
	u := r.join(me)
	defer r.leave(u)
	go writeLoop(ctx, c, me)

	for {
		typ, data, err := c.Read(ctx)
		if err != nil {
			return
		}
		var in inbound
		if typ != websocket.MessageText || json.Unmarshal(data, &in) != nil || in.T != "msg" || in.S == nil ||
			in.Body == nil || !validBody(*in.Body) {
			me.push(errInvalid)
			continue
		}
		r.broadcast(u, *in.S, *in.Body)
	}
}

func validBody(s string) bool {
	n := utf8.RuneCountInString(s)
	return n >= 1 && n <= maxBody
}

func writeLoop(ctx context.Context, c *websocket.Conn, me *member) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-me.wake:
		}
		me.mu.Lock()
		batch := me.queue
		me.queue = nil
		me.mu.Unlock()
		for _, b := range batch {
			if err := c.Write(ctx, websocket.MessageText, b); err != nil {
				c.CloseNow()
				return
			}
		}
	}
}
