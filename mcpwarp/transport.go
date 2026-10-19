package mcpwarp

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"

	"github.com/mcpwarp/ws-mixer-go/internal/version"
	"github.com/mcpwarp/ws-mixer-go/wsmixer"
)

// transportEvents is what a transport reports to the agent. Every callback
// may run on any goroutine; none is called with a transport lock held.
type transportEvents struct {
	connect    func(c appConn)                  // c received welcome and is now the active connection
	connEnd    func(c appConn)                  // c, connected earlier, ended
	app        func(c appConn, body []byte)     // an app message; c is nil when not attributed yet
	drain      func(reason string)              // a drain arrived; runs before any replacement is scheduled
	disconnect func(r wsmixer.DisconnectReason) // every disconnect, recoverable or fatal
	retry      func(info wsmixer.ReconnectInfo) // a scheduled redial, before it starts
	stream     func(st *wsmixer.Stream)         // an inbound stream
}

// transportConfig is one agent's connection identity (§3.1).
type transportConfig struct {
	url         string
	token       wsmixer.TokenProvider
	refreshable bool   // token is a caller's provider; wsmixer.Client tells that apart itself, fakes use this
	integration string // lowercased
	runtime, os string
	options     wsmixer.Options
	reconnect   wsmixer.ReconnectOptions
	httpClient  *http.Client
	name        string // the service; fakes use it to answer for the agent
	events      transportEvents
}

// transport is the ws-mixer client under one agent: wsmixer.Client in
// production, a scripted fake in the tunnel fixture runner.
type transport interface {
	start()
	// active is the connection whose welcome came last, or nil.
	active() appConn
	// close ends the transport for good and runs done (if non-nil) once it
	// has. now skips the graceful part only where the transport can. It is
	// synchronous enough that no redial starts after it returns.
	close(now bool, done func())
	// client is the underlying wsmixer.Client, nil for fakes.
	client() *wsmixer.Client
}

// wsTransport is transport over wsmixer.Client.
type wsTransport struct {
	cl *wsmixer.Client

	mu      sync.Mutex
	closing bool
	ended   bool
	dones   []func()
}

func newWSTransport(cfg transportConfig) transport {
	t := &wsTransport{}
	ev := cfg.events
	header := http.Header{}
	if cfg.integration != "" {
		header.Set("X-Mcpwarp-Integration", cfg.integration)
	}
	// Authorization: Bearer is set by wsmixer.Dial itself on every dial.
	header.Set("User-Agent", sdkName+"/"+version.SDK+" ("+cfg.runtime+" "+cfg.os+")")
	t.cl = wsmixer.NewClient(cfg.url, cfg.token, wsmixer.ClientConfig{
		Options:    cfg.options,
		Agent:      wsmixer.AgentInfo{SDK: sdkName, SDKVersion: version.SDK, Runtime: cfg.runtime, OS: cfg.os},
		Meta:       helloMeta{Client: clientID, Version: version.SDK, OS: cfg.os, Integration: cfg.integration},
		HTTPHeader: header,
		HTTPClient: cfg.httpClient,
		Reconnect:  cfg.reconnect,
		OnConnect: func(c *wsmixer.Conn, _ *wsmixer.WelcomeMsg) {
			// connect first: a message bound to c before the session knows c
			// would be dropped as superseded; until the bind, c's messages
			// take the unattributed path below.
			ev.connect(c)
			c.OnApp(func(body json.RawMessage) { ev.app(c, body) })
			go func() {
				<-c.Done()
				ev.connEnd(c)
			}()
		},
		OnDisconnect: ev.disconnect,
		OnReconnect:  ev.retry,
		OnStream:     ev.stream,
		// Until OnConnect binds a connection's own handler its messages
		// arrive here, unattributed (it is always the newest connection).
		OnApp:   func(body json.RawMessage) { ev.app(nil, body) },
		OnDrain: func(d *wsmixer.DrainMsg) { ev.drain(d.Reason) },
	})
	return t
}

func (t *wsTransport) start() {
	go func() { _ = t.cl.Connect(context.Background()) }()
}

func (t *wsTransport) active() appConn {
	if c := t.cl.Conn(); c != nil {
		return c
	}
	return nil
}

func (t *wsTransport) client() *wsmixer.Client { return t.cl }

// close: wsmixer.Client.Close (drain{client_requested}, grace ≤ 5 s,
// NO_ERROR/1000), or with now Client.CloseImmediately (NO_ERROR/1000 at
// once, no drain frame, no grace) on the caller's goroutine, so that called
// from OnDrain it marks the client closed before the client could schedule
// a replacement dial.
func (t *wsTransport) close(now bool, done func()) {
	t.mu.Lock()
	if t.ended {
		t.mu.Unlock()
		if done != nil {
			done()
		}
		return
	}
	if done != nil {
		t.dones = append(t.dones, done)
	}
	first := !t.closing
	t.closing = true
	t.mu.Unlock()
	if !first && !now {
		return
	}
	run := func() {
		if now {
			_ = t.cl.CloseImmediately(context.Background())
		} else {
			_ = t.cl.Close(context.Background())
		}
		t.mu.Lock()
		t.ended = true
		dones := t.dones
		t.dones = nil
		t.mu.Unlock()
		for _, d := range dones {
			d()
		}
	}
	if now {
		run()
	} else {
		go run()
	}
}
