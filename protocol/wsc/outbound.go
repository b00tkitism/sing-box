package wsc

import (
	"context"
	"crypto/ecdsa"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"

	"github.com/coder/websocket"
	zcrypto "github.com/crypto4people/zoro/crypto"
	"github.com/ethereum/go-ethereum/crypto"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/common/dialer"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

func RegisterOutbound(registry *outbound.Registry) {
	outbound.Register(registry, C.TypeWSC, NewOutbound)
}

type Outbound struct {
	outbound.Adapter

	account    string
	privKey    *ecdsa.PrivateKey
	path       string
	logger     logger.ContextLogger
	serverAddr M.Socksaddr
	dialer     N.Dialer
	tlsCfg     *tls.Config
	useTLS     bool
}

func NewOutbound(ctx context.Context, router adapter.Router, lg log.ContextLogger, tag string, opts option.WSCOutboundOptions) (adapter.Outbound, error) {
	dialer, err := dialer.New(ctx, opts.DialerOptions, opts.ServerIsDomain())
	if err != nil {
		return nil, err
	}

	priv, err := crypto.HexToECDSA(opts.Auth)
	if err != nil {
		return nil, err
	}

	outbound := &Outbound{
		Adapter: outbound.NewAdapterWithDialerOptions(
			C.TypeWSC, tag, []string{N.NetworkTCP}, opts.DialerOptions,
		),
		account: crypto.PubkeyToAddress(priv.PublicKey).Hex(),
		privKey: priv,
		path:    opts.Path,
		logger:  lg,
		dialer:  dialer,
		useTLS:  opts.TLS.Enabled,
	}

	if outbound.path == "" {
		outbound.path = "/"
	}

	if opts.TLS.Enabled {
		outbound.tlsCfg = &tls.Config{
			MinVersion: tls.VersionTLS12,
		}
	}

	outbound.serverAddr = opts.ServerOptions.Build()
	if outbound.serverAddr.Port == 0 {
		return nil, errors.New("port is not specified")
	}
	return outbound, nil
}

func (out *Outbound) Type() string {
	return C.TypeWSC
}

func (out *Outbound) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	if network != N.NetworkTCP {
		return nil, errors.New("wsc: only TCP is supported")
	}

	host := out.serverAddr.Fqdn
	port := out.serverAddr.Port

	if host == "" && out.serverAddr.Fqdn == "" {
		host = out.serverAddr.Addr.String()
	}

	scheme := "ws"
	if out.useTLS {
		scheme = "wss"
	}

	uri := url.URL{
		Scheme: scheme,
		Host:   net.JoinHostPort(host, fmt.Sprint(port)),
		Path:   "/wsc",
	}

	query := uri.Query()
	query.Set("user", out.account)
	query.Set("net", "tcp")
	query.Set("addr", destination.String())
	uri.RawQuery = query.Encode()

	transport := &http.Transport{
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			host, portStr, err := net.SplitHostPort(address)
			if err != nil {
				return nil, err
			}
			p64, err := strconv.ParseUint(portStr, 10, 16)
			if err != nil {
				return nil, err
			}
			port := uint16(p64)

			sa := M.Socksaddr{Port: port}

			if ip, ok := netip.ParseAddr(host); ok == nil { // Go 1.22: returns (Addr, error)
				sa.Addr = ip
			} else {
				if ipaddr, err := netip.ParseAddr(host); err == nil {
					sa.Addr = ipaddr
				} else {
					sa.Fqdn = host
				}
			}

			return out.dialer.DialContext(ctx, N.NetworkTCP, sa)
		},
	}

	if out.useTLS && out.tlsCfg != nil {
		if out.tlsCfg.ServerName == "" {
			out.tlsCfg = out.tlsCfg.Clone()
			out.tlsCfg.ServerName = host
		}
		transport.TLSClientConfig = out.tlsCfg
	} else if transport.TLSClientConfig == nil {
		transport.TLSClientConfig = &tls.Config{}
	}

	httpClient := &http.Client{Transport: transport}

	opts := &websocket.DialOptions{
		HTTPClient:      httpClient,
		CompressionMode: websocket.CompressionDisabled,
	}

	wsConn, _, err := websocket.Dial(ctx, uri.String(), opts)
	if err != nil {
		return nil, err
	}

	_, challenge, err := wsConn.Read(ctx)
	if err != nil {
		_ = wsConn.Close(websocket.StatusInternalError, "challenge read error")
		return nil, err
	}

	sig := zcrypto.SignChallenge(out.privKey, challenge, "tcp", destination.String())

	err = wsConn.Write(ctx, websocket.MessageBinary, sig)
	if err != nil {
		_ = wsConn.Close(websocket.StatusInternalError, "challenge write error")
		return nil, err
	}

	return newWSStreamConn(wsConn, false), nil
}

func (out *Outbound) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	return nil, errors.New("wsc: UDP is not supported")
}
