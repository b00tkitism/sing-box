package wsc

import (
	"context"
	"io"
	"net"
	"sync"
	"time"

	"github.com/coder/websocket"
)

type wsStreamConn struct {
	ctx    context.Context
	cancel context.CancelFunc
	conn   *websocket.Conn
	mu     sync.Mutex // protects write
	server bool
}

func newWSStreamConn(conn *websocket.Conn, server bool) net.Conn {
	ctx, cancel := context.WithCancel(context.Background())
	return &wsStreamConn{
		ctx:    ctx,
		cancel: cancel,
		conn:   conn,
		server: server,
	}
}

func (wsConn *wsStreamConn) Read(p []byte) (int, error) {
	wsConn.conn.SetReadLimit(65536)
	typ, msg, err := wsConn.conn.Read(wsConn.ctx)
	if err != nil {
		if websocket.CloseStatus(err) != -1 {
			return 0, io.EOF
		}
		return 0, err
	}
	if typ != websocket.MessageBinary {
		return 0, nil
	}
	return copy(p, msg), nil
}

func (wsConn *wsStreamConn) Write(p []byte) (int, error) {
	wsConn.mu.Lock()
	defer wsConn.mu.Unlock()

	writeCtx, cancel := context.WithTimeout(wsConn.ctx, 5*time.Second)
	defer cancel()

	err := wsConn.conn.Write(writeCtx, websocket.MessageBinary, p)
	if err != nil {
		return 0, err
	}
	return len(p), nil
}

func (wsConn *wsStreamConn) Close() error {
	wsConn.cancel()
	_, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return wsConn.conn.Close(websocket.StatusNormalClosure, "")
}

func (wsConn *wsStreamConn) LocalAddr() net.Addr {
	return nil
}

func (wsConn *wsStreamConn) RemoteAddr() net.Addr {
	return nil
}

func (wsConn *wsStreamConn) SetDeadline(t time.Time) error {
	return nil
}

func (wsConn *wsStreamConn) SetReadDeadline(t time.Time) error {
	return nil
}

func (wsConn *wsStreamConn) SetWriteDeadline(t time.Time) error {
	return nil
}
