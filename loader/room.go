// Room mode: users sit in rooms of -room-size and send JSON chat messages at
// -user-rate each; the server broadcasts every message to the whole room at
// once, sender included. Protocol (every server implements it the same way):
//
//	GET /room?id=<room>  WebSocket upgrade on port PORT+1+(room % CORES)
//	server -> on join    {"t":"hello","u":<user id>,"v":<room version>,"recent":[<last 20 broadcasts>]}
//	client -> server     {"t":"msg","s":<uint seq>,"body":"<1..1024 chars>"}
//	server -> room       {"t":"msg","u":<sender>,"s":<seq>,"v":<version+1>,"body":"..."}
//	server -> sender     {"t":"err","code":"invalid"} for a message that fails validation
//
// User ids are unique within a room; versions count the room's broadcasts and
// reach every member in order. One shard (port) owns a room for its lifetime.
//
// Delivery latency runs from the message's scheduled send time to its arrival
// at each member, all on this process's monotonic clock: the broadcast carries
// the sender's id (u) and sequence (s), which index the sender's stamp ring.
//
// Every member checks that room versions (v) arrive as v+1, v+2, ...; a skip
// counts the missing deliveries, a repeat or step back counts as out of order.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/gorilla/websocket"
)

// Loopback source addresses connections are spread over. One source address
// can open only ~28k connections to one destination (the ephemeral port range).
const sourceAddrs = 16

type member struct {
	id, room int
	ws       *websocket.Conn
	u        uint64         // server-assigned user id, unique within the room
	ring     []atomic.Int64 // scheduled send stamp per seq, indexed by seq & mask
	mask     uint64
	seq      uint64 // writer side
	lastV    uint64 // reader side: last room version seen
}

type roomStats struct {
	*stats
	gaps, disorder, serverErrors, unknown, disconnects atomic.Int64
}

func runRoom(c config) error {
	if c.roomSize < 1 || c.userRate <= 0 || c.shards < 1 {
		return fmt.Errorf("room mode needs -room-size >= 1, -user-rate > 0, -shards >= 1")
	}
	base, err := url.Parse(c.url)
	if err != nil {
		return err
	}
	basePort, err := strconv.Atoi(base.Port())
	if err != nil {
		return fmt.Errorf("-url needs an explicit port: %v", err)
	}
	rate := int(math.Round(float64(c.conns) * c.userRate))
	if rate < 1 {
		return fmt.Errorf("total send rate rounds to 0")
	}
	// A stamp must survive until every member has the message: 60 s at this rate.
	ringSize := nextPow2(max(256, int(c.userRate*60)))

	members, connectP99, failed := connectRoom(c, base, basePort, ringSize)
	log.Printf("connected %d/%d users in %d rooms (connect p99 %.1f ms)",
		len(members), c.conns, (c.conns+c.roomSize-1)/c.roomSize, connectP99)
	if len(members) == 0 {
		return fmt.Errorf("no connection succeeded")
	}

	// room -> user id -> member; read-only from here on.
	rooms := make([]map[uint64]*member, (c.conns+c.roomSize-1)/c.roomSize)
	for i := range rooms {
		rooms[i] = map[uint64]*member{}
	}
	for _, m := range members {
		if _, dup := rooms[m.room][m.u]; dup {
			return fmt.Errorf("room %d: duplicate user id %d", m.room, m.u)
		}
		rooms[m.room][m.u] = m
	}

	st := &roomStats{stats: newStats()}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	queues := make([]chan int64, len(members))
	body := bytes.Repeat([]byte("abcdefghijklmnopqrstuvwxyz"), c.payload/26+1)[:c.payload]
	for i, m := range members {
		queues[i] = make(chan int64, 16)
		wg.Add(2)
		go func(m *member) { defer wg.Done(); roomReader(m, rooms[m.room], st) }(m)
		go func(m *member, q chan int64) { defer wg.Done(); roomWriter(m, q, body, st) }(m, queues[i])
	}
	wg.Add(1)
	go func() { defer wg.Done(); schedule(ctx, rate, queues, st.stats) }()

	log.Printf("warm-up %s (%d msg/s sent, ~%d deliveries/s)", c.warmup, rate, rate*min(c.roomSize, len(members)))
	time.Sleep(c.warmup)

	var ru0, ru1 syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru0)
	wallStart := time.Now()
	phase.Store(measuring)
	log.Printf("measuring %s", c.duration)
	time.Sleep(c.duration)
	phase.Store(stopping)
	elapsed := time.Since(wallStart)
	wallEnd := time.Now()
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru1)

	cancel()
	time.Sleep(500 * time.Millisecond)
	for _, m := range members {
		_ = m.ws.WriteControl(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), time.Now().Add(time.Second))
		_ = m.ws.Close()
	}
	wg.Wait()

	secs := elapsed.Seconds()
	breakdown := map[string]int64{
		"missed":        st.gaps.Load(),
		"out_of_order":  st.disorder.Load(),
		"server_errors": st.serverErrors.Load(),
		"unknown":       st.unknown.Load(),
		"disconnects":   st.disconnects.Load(),
		"send_backlog":  st.errors.Load(), // scheduler could not hand out a send in time
	}
	var errs int64
	for _, n := range breakdown {
		errs += n
	}
	res := map[string]any{
		"mode":          c.mode,
		"payload_bytes": c.payload,
		"rate_target":   rate,
		"window": map[string]any{
			"start_unix_ns": wallStart.UnixNano(),
			"end_unix_ns":   wallEnd.UnixNano(),
			"seconds":       round(secs, 3),
		},
		"connections": map[string]any{
			"target":         c.conns,
			"successful":     len(members),
			"failed":         failed,
			"connect_p99_ms": round(connectP99, 2),
		},
		"messages": map[string]any{
			"sent":     st.sent.Load(),
			"received": st.received.Load(),
			"errors":   errs,
		},
		"throughput": map[string]any{
			"messages_per_second": round(float64(st.received.Load())/secs, 1),
		},
		"room": map[string]any{
			"rooms":                 len(rooms),
			"room_size":             c.roomSize,
			"user_rate":             c.userRate,
			"shards":                c.shards,
			"sends_per_second":      round(float64(st.sent.Load())/secs, 1),
			"deliveries_per_second": round(float64(st.received.Load())/secs, 1),
			"errors":                breakdown,
		},
		"loader_cpu_cores": round((cpuSeconds(ru1)-cpuSeconds(ru0))/secs, 3),
	}
	if h := st.merged(); h.TotalCount() > 0 {
		res["latency_ms"] = latencySummary(h)
	}
	return writeResult(res)
}

// connectRoom dials every user, user i into room i/roomSize, and reads the
// hello that carries the user's id and the room version.
func connectRoom(c config, base *url.URL, basePort, ringSize int) ([]*member, float64, int) {
	lat := hdrhistogram.New(1, 60_000_000, 3)
	var mu sync.Mutex
	var out []*member
	failed := 0
	loopback := net.ParseIP(base.Hostname()).IsLoopback()

	ids := make(chan int)
	var wg sync.WaitGroup
	for w := 0; w < c.dialWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for id := range ids {
				room := id / c.roomSize
				target := *base
				target.Host = net.JoinHostPort(base.Hostname(), strconv.Itoa(basePort+room%c.shards))
				target.RawQuery = "id=" + strconv.Itoa(room)

				dialer := &net.Dialer{Timeout: 30 * time.Second}
				if loopback {
					dialer.LocalAddr = &net.TCPAddr{IP: net.IPv4(127, 0, 0, byte(1+id%sourceAddrs))}
				}
				wsd := websocket.Dialer{
					HandshakeTimeout: 30 * time.Second,
					NetDialContext:   dialer.DialContext,
					ReadBufferSize:   4096,
					WriteBufferSize:  4096,
				}
				t0 := time.Now()
				m, err := join(wsd, target.String(), id, room, ringSize)
				mu.Lock()
				if err != nil {
					failed++
					if failed <= 5 {
						log.Printf("user %d: %v", id, err)
					}
				} else {
					_ = lat.RecordValue(max(time.Since(t0).Microseconds(), 1))
					out = append(out, m)
				}
				mu.Unlock()
			}
		}()
	}
	for i := 0; i < c.conns; i++ {
		ids <- i
	}
	close(ids)
	wg.Wait()
	return out, float64(lat.ValueAtQuantile(99)) / 1000, failed
}

func join(d websocket.Dialer, target string, id, room, ringSize int) (*member, error) {
	ws, _, err := d.Dial(target, http.Header{})
	if err != nil {
		return nil, err
	}
	ws.SetReadLimit(1 << 20)
	_ = ws.SetReadDeadline(time.Now().Add(30 * time.Second))
	_, data, err := ws.ReadMessage()
	if err != nil {
		ws.Close()
		return nil, fmt.Errorf("hello: %w", err)
	}
	_ = ws.SetReadDeadline(time.Time{})
	var hello struct {
		T      string            `json:"t"`
		U      *uint64           `json:"u"`
		V      *uint64           `json:"v"`
		Recent []json.RawMessage `json:"recent"`
	}
	if err := json.Unmarshal(data, &hello); err != nil || hello.T != "hello" || hello.U == nil || hello.V == nil || hello.Recent == nil {
		ws.Close()
		return nil, fmt.Errorf("bad hello: %.120s", data)
	}
	return &member{id: id, room: room, ws: ws, u: *hello.U, lastV: *hello.V,
		ring: make([]atomic.Int64, ringSize), mask: uint64(ringSize - 1)}, nil
}

func roomWriter(m *member, q <-chan int64, body []byte, st *roomStats) {
	buf := make([]byte, 0, 64+len(body))
	for stamp := range q {
		m.seq++
		m.ring[m.seq&m.mask].Store(stamp)
		buf = append(buf[:0], `{"t":"msg","s":`...)
		buf = strconv.AppendUint(buf, m.seq, 10)
		buf = append(buf, `,"body":"`...)
		buf = append(buf, body...)
		buf = append(buf, `"}`...)
		if err := m.ws.WriteMessage(websocket.TextMessage, buf); err != nil {
			for range q { // drain so the scheduler never blocks
			}
			return
		}
		if phase.Load() == measuring {
			st.sent.Add(1)
		}
	}
}

func roomReader(m *member, room map[uint64]*member, st *roomStats) {
	for {
		_, data, err := m.ws.ReadMessage()
		t := now()
		measured := phase.Load() == measuring
		if err != nil {
			if measured {
				st.disconnects.Add(1)
			}
			return
		}
		u, s, v, ok := parseBroadcast(data)
		if !ok {
			if measured {
				if bytes.Contains(data, []byte(`"err"`)) {
					st.serverErrors.Add(1)
				} else {
					st.unknown.Add(1)
				}
			}
			continue
		}
		switch {
		case v == m.lastV+1:
		case v > m.lastV+1:
			if measured {
				st.gaps.Add(int64(v - m.lastV - 1))
			}
		default:
			if measured {
				st.disorder.Add(1)
			}
			continue
		}
		m.lastV = v
		if !measured {
			continue
		}
		sender, found := room[u]
		var stamp int64
		if found {
			stamp = sender.ring[s&sender.mask].Load()
		}
		if stamp == 0 {
			st.unknown.Add(1)
			continue
		}
		st.received.Add(1)
		st.record(m.id, t-stamp)
	}
}

// parseBroadcast reads u, s and v from {"t":"msg","u":..,"s":..,"v":..,"body":..}
// in any key order. The body the loader sends is lowercase letters only, so
// these keys cannot occur inside it.
func parseBroadcast(b []byte) (u, s, v uint64, ok bool) {
	if !bytes.Contains(b, []byte(`"msg"`)) {
		return 0, 0, 0, false
	}
	var okU, okS, okV bool
	u, okU = uintField(b, `"u"`)
	s, okS = uintField(b, `"s"`)
	v, okV = uintField(b, `"v"`)
	return u, s, v, okU && okS && okV
}

func uintField(b []byte, key string) (uint64, bool) {
	i := bytes.Index(b, []byte(key))
	if i < 0 {
		return 0, false
	}
	i += len(key)
	for i < len(b) && (b[i] == ' ' || b[i] == ':') {
		i++
	}
	j := i
	for j < len(b) && b[j] >= '0' && b[j] <= '9' {
		j++
	}
	n, err := strconv.ParseUint(string(b[i:j]), 10, 64)
	return n, err == nil
}
