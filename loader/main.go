// loader — WebSocket echo load generator.
//
// Every message is [8-byte little-endian sequence][payload]. RTT is measured
// on the loader's monotonic clock only; the server's clock is never used.
//
// Modes:
//
//	throughput  closed loop: each connection keeps -pipeline messages in flight
//	rate        open loop: -rate msg/s in total, spread round-robin over the
//	            connections. RTT is measured from the scheduled send time, so a
//	            backlog on the loader side is charged to latency instead of
//	            silently lowering the offered load (no coordinated omission).
//	idle        connect and hold; no messages
//	room        users in rooms exchange JSON messages, broadcast by the server
//	            to the whole room (room.go)
//
// The result is one JSON object on stdout. Progress goes to stderr.
package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/gorilla/websocket"
)

const headerSize = 8

type config struct {
	url         string
	mode        string
	conns       int
	payload     int
	rate        int
	pipeline    int
	warmup      time.Duration
	duration    time.Duration
	dialWorkers int
	roomSize    int
	userRate    float64
	shards      int
}

// Phases. Samples and counters are kept only while phase == measuring.
const (
	warming int32 = iota
	measuring
	stopping
)

var (
	start = time.Now() // anchor for monotonic nanosecond stamps
	phase atomic.Int32
)

func now() int64 { return int64(time.Since(start)) }

// stats is shared by every connection. Histograms are sharded to keep lock
// contention negligible when the loader runs on more than one core.
type stats struct {
	shards [32]struct {
		sync.Mutex
		h *hdrhistogram.Histogram
	}
	sent, received, errors atomic.Int64
}

func newStats() *stats {
	s := &stats{}
	for i := range s.shards {
		// 1 µs .. 60 s, 3 significant digits.
		s.shards[i].h = hdrhistogram.New(1, 60_000_000, 3)
	}
	return s
}

func (s *stats) record(shard int, rttNs int64) {
	us := rttNs / 1000
	if us < 1 {
		us = 1
	}
	us = min(us, 60_000_000)
	sh := &s.shards[shard%len(s.shards)]
	sh.Lock()
	_ = sh.h.RecordValue(us)
	sh.Unlock()
}

func (s *stats) merged() *hdrhistogram.Histogram {
	h := hdrhistogram.New(1, 60_000_000, 3)
	for i := range s.shards {
		h.Merge(s.shards[i].h)
	}
	return h
}

type conn struct {
	id   int
	ws   *websocket.Conn
	ring []atomic.Int64 // send stamp per in-flight seq, indexed by seq & mask
	mask uint64
	free chan struct{} // throughput mode: one token per in-flight slot

	sendSeq  uint64       // writer side
	recvSeq  uint64       // reader side; echoes arrive in order
	inFlight atomic.Int64 // sent - received
}

func main() {
	var c config
	flag.StringVar(&c.url, "url", "ws://127.0.0.1:8080/ws", "server WebSocket URL")
	flag.StringVar(&c.mode, "mode", "throughput", "throughput | rate | idle | room")
	flag.IntVar(&c.conns, "connections", 100, "concurrent connections")
	flag.IntVar(&c.payload, "payload", 64, "payload bytes per message (excluding the 8-byte header)")
	flag.IntVar(&c.rate, "rate", 10000, "rate mode: total messages per second")
	flag.IntVar(&c.pipeline, "pipeline", 1, "throughput mode: messages in flight per connection")
	flag.DurationVar(&c.warmup, "warmup", 15*time.Second, "warm-up duration (not measured)")
	flag.DurationVar(&c.duration, "duration", 30*time.Second, "measurement duration")
	flag.IntVar(&c.dialWorkers, "dial-workers", 64, "parallel connection attempts")
	flag.IntVar(&c.roomSize, "room-size", 20, "room mode: users per room")
	flag.Float64Var(&c.userRate, "user-rate", 0.2, "room mode: messages per second per user")
	flag.IntVar(&c.shards, "shards", 1, "room mode: server ports; room r goes to the -url port + r % shards")
	flag.Parse()

	if err := run(c); err != nil {
		log.Fatalf("loader: %v", err)
	}
}

func run(c config) error {
	switch c.mode {
	case "room":
		return runRoom(c)
	case "throughput", "rate", "idle":
	default:
		return fmt.Errorf("unknown mode %q", c.mode)
	}
	if c.pipeline < 1 {
		c.pipeline = 1
	}

	ringSize := c.pipeline
	if c.mode == "rate" {
		// Room for ~2 s of backlog per connection; beyond that a send is
		// skipped and counted as an error rather than overwriting a stamp.
		ringSize = c.rate*2/c.conns + 64
	}
	ringSize = nextPow2(ringSize)

	conns, connectP99, failed := connectAll(c, ringSize)
	log.Printf("connected %d/%d (connect p99 %.1f ms)", len(conns), c.conns, connectP99)
	if len(conns) == 0 {
		return fmt.Errorf("no connection succeeded")
	}

	st := newStats()
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	frameLen := headerSize + c.payload

	// Readers run in every mode; in idle mode they only notice disconnects.
	for _, cn := range conns {
		if c.mode == "throughput" {
			cn.free = make(chan struct{}, c.pipeline)
			for i := 0; i < c.pipeline; i++ {
				cn.free <- struct{}{}
			}
		}
		wg.Add(1)
		go func(cn *conn) { defer wg.Done(); readLoop(cn, st, frameLen) }(cn)
	}

	switch c.mode {
	case "throughput":
		for _, cn := range conns {
			wg.Add(1)
			go func(cn *conn) { defer wg.Done(); closedLoop(ctx, cn, st, frameLen, c.pipeline) }(cn)
		}
	case "rate":
		queues := make([]chan int64, len(conns))
		for i, cn := range conns {
			queues[i] = make(chan int64, 256)
			wg.Add(1)
			go func(cn *conn, q chan int64) { defer wg.Done(); openLoopWriter(cn, st, q, frameLen) }(cn, queues[i])
		}
		wg.Add(1)
		go func() { defer wg.Done(); schedule(ctx, c.rate, queues, st) }()
	}

	log.Printf("warm-up %s", c.warmup)
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

	// Stop sending, give in-flight echoes a moment, then tear down.
	cancel()
	time.Sleep(500 * time.Millisecond)
	for _, cn := range conns {
		_ = cn.ws.WriteControl(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), time.Now().Add(time.Second))
		_ = cn.ws.Close()
	}
	wg.Wait()

	h := st.merged()
	secs := elapsed.Seconds()
	res := map[string]any{
		"mode":          c.mode,
		"payload_bytes": c.payload,
		"rate_target":   c.rate,
		"pipeline":      c.pipeline,
		"window": map[string]any{
			"start_unix_ns": wallStart.UnixNano(),
			"end_unix_ns":   wallEnd.UnixNano(),
			"seconds":       round(secs, 3),
		},
		"connections": map[string]any{
			"target":         c.conns,
			"successful":     len(conns),
			"failed":         failed,
			"connect_p99_ms": round(connectP99, 2),
		},
		"messages": map[string]any{
			"sent":     st.sent.Load(),
			"received": st.received.Load(),
			"errors":   st.errors.Load(),
		},
		"throughput": map[string]any{
			"messages_per_second": round(float64(st.received.Load())/secs, 1),
		},
		"loader_cpu_cores": round((cpuSeconds(ru1)-cpuSeconds(ru0))/secs, 3),
	}
	if h.TotalCount() > 0 {
		res["latency_ms"] = latencySummary(h)
	}
	return writeResult(res)
}

func latencySummary(h *hdrhistogram.Histogram) map[string]any {
	ms := func(us int64) float64 { return round(float64(us)/1000, 3) }
	return map[string]any{
		"p50":     ms(h.ValueAtQuantile(50)),
		"p95":     ms(h.ValueAtQuantile(95)),
		"p99":     ms(h.ValueAtQuantile(99)),
		"max":     ms(h.Max()),
		"mean":    round(h.Mean()/1000, 3),
		"samples": h.TotalCount(),
	}
}

func writeResult(res map[string]any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(res)
}

func connectAll(c config, ringSize int) ([]*conn, float64, int) {
	dialer := websocket.Dialer{
		HandshakeTimeout:  30 * time.Second,
		EnableCompression: false,
		ReadBufferSize:    4096,
		WriteBufferSize:   4096,
	}
	lat := hdrhistogram.New(1, 60_000_000, 3)
	var mu sync.Mutex
	var out []*conn
	failed := 0

	ids := make(chan int)
	var wg sync.WaitGroup
	for w := 0; w < c.dialWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for id := range ids {
				t0 := time.Now()
				ws, _, err := dialer.Dial(c.url, http.Header{})
				mu.Lock()
				if err != nil {
					failed++
					if failed <= 5 {
						log.Printf("dial %d: %v", id, err)
					}
				} else {
					_ = lat.RecordValue(max(time.Since(t0).Microseconds(), 1))
					ws.SetReadLimit(int64(headerSize + c.payload))
					out = append(out, &conn{id: id, ws: ws, ring: make([]atomic.Int64, ringSize), mask: uint64(ringSize - 1)})
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

func newFrame(size int) []byte {
	f := make([]byte, size)
	for i := headerSize; i < size; i++ {
		f[i] = byte(i)
	}
	return f
}

// send stamps and writes one message. stamp is the time the message counts
// as sent: now for closed loop, the scheduled time for open loop.
func send(cn *conn, st *stats, frame []byte, stamp int64) error {
	cn.sendSeq++
	binary.LittleEndian.PutUint64(frame, cn.sendSeq)
	cn.ring[cn.sendSeq&cn.mask].Store(stamp)
	cn.inFlight.Add(1)
	if err := cn.ws.WriteMessage(websocket.BinaryMessage, frame); err != nil {
		return err
	}
	if phase.Load() == measuring {
		st.sent.Add(1)
	}
	return nil
}

func closedLoop(ctx context.Context, cn *conn, st *stats, frameLen, pipeline int) {
	frame := newFrame(frameLen)
	for {
		select {
		case <-ctx.Done():
			return
		case <-cn.free:
		}
		if err := send(cn, st, frame, now()); err != nil {
			if ctx.Err() == nil {
				st.errors.Add(1)
			}
			return
		}
	}
}

func openLoopWriter(cn *conn, st *stats, q <-chan int64, frameLen int) {
	frame := newFrame(frameLen)
	for stamp := range q {
		if cn.inFlight.Load() >= int64(len(cn.ring)) {
			if phase.Load() == measuring {
				st.errors.Add(1) // backlog beyond the ring: the server is not keeping up
			}
			continue
		}
		if err := send(cn, st, frame, stamp); err != nil {
			if phase.Load() != stopping {
				st.errors.Add(1)
			}
			for range q { // drain so the scheduler never blocks
			}
			return
		}
	}
}

// schedule hands out send slots on an absolute timeline: message i is due at
// base + i/rate, however slow the server is. It sleeps until the next due time
// and then releases everything that is due, so at high rates messages go out
// in small bursts while each one still carries its own scheduled stamp.
func schedule(ctx context.Context, rate int, queues []chan int64, st *stats) {
	defer func() {
		for _, q := range queues {
			close(q)
		}
	}()
	period := float64(time.Second) / float64(rate)
	base := now()
	for i := int64(0); ctx.Err() == nil; {
		if d := base + int64(float64(i)*period) - now(); d > 0 {
			time.Sleep(time.Duration(d))
		}
		due := int64(float64(now()-base)/period) + 1
		for ; i < due; i++ {
			stamp := base + int64(float64(i)*period)
			select {
			case queues[i%int64(len(queues))] <- stamp:
			default:
				if phase.Load() == measuring {
					st.errors.Add(1) // writer queue full
				}
			}
		}
	}
}

func readLoop(cn *conn, st *stats, frameLen int) {
	for {
		_, data, err := cn.ws.ReadMessage()
		t := now()
		if err != nil {
			if phase.Load() == measuring {
				st.errors.Add(1)
			}
			return
		}
		cn.recvSeq++
		cn.inFlight.Add(-1)
		if cn.free != nil {
			select {
			case cn.free <- struct{}{}:
			default:
			}
		}
		if phase.Load() != measuring {
			continue
		}
		if len(data) != frameLen || binary.LittleEndian.Uint64(data) != cn.recvSeq {
			st.errors.Add(1)
			continue
		}
		st.received.Add(1)
		st.record(cn.id, t-cn.ring[cn.recvSeq&cn.mask].Load())
	}
}

func nextPow2(n int) int {
	p := 1
	for p < n {
		p <<= 1
	}
	return p
}

func cpuSeconds(r syscall.Rusage) float64 {
	tv := func(t syscall.Timeval) float64 { return float64(t.Sec) + float64(t.Usec)/1e6 }
	return tv(r.Utime) + tv(r.Stime)
}

func round(v float64, places int) float64 {
	p := 1.0
	for i := 0; i < places; i++ {
		p *= 10
	}
	return float64(int64(v*p+0.5)) / p
}
