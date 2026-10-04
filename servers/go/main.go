// Go + coder/websocket echo server (/ws on PORT) and room server (/room on
// PORT+1 .. PORT+CORES, room.go). Concurrency is goroutine-per-connection;
// CORES sets GOMAXPROCS.
package main

import (
	"log"
	"net/http"
	"os"
	"runtime"
	"strconv"

	"github.com/coder/websocket"
)

const maxPayload = 1 << 20

func echo(w http.ResponseWriter, r *http.Request) {
	c, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		CompressionMode:    websocket.CompressionDisabled,
		InsecureSkipVerify: true,
	})
	if err != nil {
		return
	}
	defer c.CloseNow()
	c.SetReadLimit(maxPayload)

	ctx := r.Context()
	for {
		typ, data, err := c.Read(ctx)
		if err != nil {
			return
		}
		if err := c.Write(ctx, typ, data); err != nil {
			return
		}
	}
}

func main() {
	if n, err := strconv.Atoi(os.Getenv("CORES")); err == nil && n > 0 {
		runtime.GOMAXPROCS(n)
	}
	cores := max(runtime.GOMAXPROCS(0), 1)
	port, err := strconv.Atoi(os.Getenv("PORT"))
	if err != nil {
		port = 8080
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/ws", echo)
	mux.HandleFunc("/room", roomHandler)
	// net/http sets TCP_NODELAY on accepted connections by default.
	// One process owns every shard port, so all ports share the room registry.
	for i := 1; i <= cores; i++ {
		go func(p int) { log.Fatal(http.ListenAndServe(":"+strconv.Itoa(p), mux)) }(port + i)
	}
	log.Fatal(http.ListenAndServe(":"+strconv.Itoa(port), mux))
}
