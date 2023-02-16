package cliutil

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/http"
	"os"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
)

func addRootCA(certPool *x509.CertPool, caCertPath string) error {
	caCertRaw, err := os.ReadFile(caCertPath)
	if err != nil {
		return err
	}
	if ok := certPool.AppendCertsFromPEM(caCertRaw); !ok {
		return fmt.Errorf("Could not add root ceritificate to pool.")
	}

	return nil
}

func NewHttp3Client(pconn net.PacketConn, insecureSkipVerify bool, caCertPath string) *http.Client {
	pool, err := x509.SystemCertPool()
	if err != nil {
		log.Fatal(err)
	}

	if caCertPath != "" {
		addRootCA(pool, caCertPath)
	}

	dail := func(ctx context.Context, addr string, tlsCfg *tls.Config, cfg *quic.Config) (quic.EarlyConnection, error) {
		remoteAddr, err := net.ResolveUDPAddr("udp", addr)
		if err != nil {
			return nil, err
		}
		return quic.DialEarlyContext(ctx, pconn, remoteAddr, "localhost", tlsCfg, cfg)
	}

	roundTripper := &http3.RoundTripper{
		TLSClientConfig: &tls.Config{
			RootCAs:            pool,
			InsecureSkipVerify: insecureSkipVerify,
		},
		QuicConfig: &quic.Config{},
		Dial:       dail,
	}

	return &http.Client{
		Transport: roundTripper,
	}
}
