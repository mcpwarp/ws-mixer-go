// Command goserver is a tiny ws-mixer.v1 test server used only by
// js/test/interop.test.ts to exercise the JS client SDK against the real Go
// implementation. It is driven over stdin/stdout with one JSON object per
// line (never touches go/, per the JS SDK task's constraints): stdin carries
// commands, stdout carries events, so the Node test can orchestrate a
// request/response round trip, an `app` exchange, and a `Drain` without any
// networked control plane of its own.
//
// Commands (stdin, one JSON object per line):
//
//	{"cmd":"open_streams","count":3,"prefix":"req"}   open N streams, write
//	    "<prefix>-<i>", CloseWrite, read the full response, report each.
//	{"cmd":"send_app","body":{...}}                    SendApp(body)
//	{"cmd":"drain"}                                     Drain(rollout, 2s)
//	{"cmd":"quit"}                                      exit
//
// Events (stdout, one JSON object per line):
//
//	{"event":"listening","addr":"127.0.0.1:PORT"}
//	{"event":"connected","session":"..."}
//	{"event":"stream_result","id":1,"ok":true,"response":"..."}
//	{"event":"stream_result","id":1,"ok":false,"error":"..."}
//	{"event":"app_received","body":{...}}
//	{"event":"drain_sent"}
//	{"event":"disconnected"}
//	{"event":"error","message":"..."}
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/mcpwarp/ws-mixer/go/wsmixer"
	"github.com/mcpwarp/ws-mixer/go/wsmixerserver"
)

type event map[string]any

var (
	stdout   = bufio.NewWriter(os.Stdout)
	emitLock sync.Mutex
)

// emit is called concurrently from many goroutines (one per open_streams
// request, the OnConn "disconnected" watcher, the drain goroutine, the
// server's own error path): without a lock, two goroutines racing to Write+
// WriteByte+Flush can interleave their bytes into one corrupted, unparseable
// line, which the JS side used to just silently drop (see interop.test.ts).
func emit(e event) {
	b, _ := json.Marshal(e)
	emitLock.Lock()
	defer emitLock.Unlock()
	stdout.Write(b)
	stdout.WriteByte('\n')
	stdout.Flush()
}

func main() {
	log.SetOutput(os.Stderr)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		emit(event{"event": "error", "message": err.Error()})
		os.Exit(1)
	}

	connCh := make(chan *wsmixer.Conn, 4)
	appCh := make(chan json.RawMessage, 16)

	listener := wsmixerserver.NewListener(wsmixerserver.ServerOptions{
		Options: wsmixer.Options{
			Window:       262144,
			MaxStreams:   64,
			PingInterval: 30 * time.Second,
			PingTimeout:  90 * time.Second,
		},
		Authenticate: func(_ context.Context, h *wsmixer.Hello) (wsmixer.WelcomeMeta, error) {
			if h.Token != "test-token" {
				return wsmixer.WelcomeMeta{}, wsmixer.Unauthorized("bad token")
			}
			return wsmixer.WelcomeMeta{}, nil
		},
		OnConn: func(c *wsmixer.Conn) {
			c.OnApp(func(body json.RawMessage) { appCh <- body })
			connCh <- c
			emit(event{"event": "connected", "session": c.Session()})
			go func() {
				<-c.Done()
				emit(event{"event": "disconnected"})
			}()
		},
	})

	mux := http.NewServeMux()
	mux.Handle("/v1/tunnel", listener)
	server := &http.Server{Handler: mux}
	go func() {
		if err := server.Serve(ln); err != nil && err != http.ErrServerClosed {
			emit(event{"event": "error", "message": err.Error()})
		}
	}()

	emit(event{"event": "listening", "addr": ln.Addr().String()})

	var current *wsmixer.Conn
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for scanner.Scan() {
		line := scanner.Bytes()
		var cmd struct {
			Cmd    string          `json:"cmd"`
			Count  int             `json:"count"`
			Prefix string          `json:"prefix"`
			Body   json.RawMessage `json:"body"`
		}
		if err := json.Unmarshal(line, &cmd); err != nil {
			emit(event{"event": "error", "message": "bad command: " + err.Error()})
			continue
		}

		switch cmd.Cmd {
		case "wait_conn":
			select {
			case current = <-connCh:
			case <-time.After(10 * time.Second):
				emit(event{"event": "error", "message": "timed out waiting for a connection"})
			}

		case "open_streams":
			if current == nil {
				emit(event{"event": "error", "message": "open_streams before wait_conn"})
				continue
			}
			for i := 0; i < cmd.Count; i++ {
				go runOneStream(current, i, cmd.Prefix)
			}

		case "send_app":
			if current == nil {
				emit(event{"event": "error", "message": "send_app before wait_conn"})
				continue
			}
			var body any
			_ = json.Unmarshal(cmd.Body, &body)
			if err := current.SendApp(context.Background(), body); err != nil {
				emit(event{"event": "error", "message": err.Error()})
			}

		case "wait_app":
			select {
			case body := <-appCh:
				emit(event{"event": "app_received", "body": json.RawMessage(body)})
			case <-time.After(10 * time.Second):
				emit(event{"event": "error", "message": "timed out waiting for an app message"})
			}

		case "drain":
			if current == nil {
				emit(event{"event": "error", "message": "drain before wait_conn"})
				continue
			}
			go func(c *wsmixer.Conn) {
				_ = c.Drain(context.Background(), "rollout", wsmixer.DrainOptions{Deadline: 2 * time.Second})
				emit(event{"event": "drain_sent"})
			}(current)

		case "quit":
			stdout.Flush()
			os.Exit(0)

		default:
			emit(event{"event": "error", "message": "unknown command " + cmd.Cmd})
		}
	}
}

func runOneStream(c *wsmixer.Conn, i int, prefix string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	st, err := c.OpenStream(ctx)
	if err != nil {
		emit(event{"event": "stream_result", "id": i, "ok": false, "error": err.Error()})
		return
	}
	req := fmt.Sprintf("%s-%d", prefix, i)
	if _, err := st.Write([]byte(req)); err != nil {
		emit(event{"event": "stream_result", "id": i, "ok": false, "error": err.Error()})
		return
	}
	if err := st.CloseWrite(); err != nil {
		emit(event{"event": "stream_result", "id": i, "ok": false, "error": err.Error()})
		return
	}
	resp, err := io.ReadAll(st)
	if err != nil {
		emit(event{"event": "stream_result", "id": i, "ok": false, "error": err.Error()})
		return
	}
	emit(event{"event": "stream_result", "id": st.ID(), "ok": true, "response": string(resp)})
}
