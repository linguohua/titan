package gateway

import (
	"context"
	"crypto/rsa"
	"fmt"
	"net/http"
	"strings"

	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/carfile/store"
	titanrsa "github.com/linguohua/titan/node/rsa"
)

var log = logging.Logger("gateway")

const (
	path = "/ipfs"
)

type Handler struct {
	handler http.Handler
	gw      *Gateway
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case strings.HasPrefix(r.URL.Path, path):
		h.gw.handler(w, r)
	default:
		h.handler.ServeHTTP(w, r)
	}

}

type Gateway struct {
	cs                 *store.CarfileStore
	scheduler          api.Scheduler
	privateKey         *rsa.PrivateKey
	schedulerPublicKey *rsa.PublicKey
}

func NewGateway(cs *store.CarfileStore, scheduler api.Scheduler) *Gateway {
	privateKey, err := titanrsa.GeneratePrivateKey(1024)
	if err != nil {
		log.Panicf("generate private key error:%s", err.Error())
	}

	publicKey := titanrsa.PublicKey2Pem(&privateKey.PublicKey)
	if publicKey == nil {
		log.Panicf("can not convert public key to pem")
	}

	publicPem, err := scheduler.ExchangePublicKey(context.Background(), publicKey)
	if err != nil {
		log.Errorf("exchange public key from scheduler error %s", err.Error())
	}

	schedulerPublicKey, err := titanrsa.Pem2PublicKey(publicPem)
	if err != nil {
		log.Errorf("can not convert pem to public key %s", err.Error())
	}

	gw := &Gateway{cs: cs, scheduler: scheduler, privateKey: privateKey, schedulerPublicKey: schedulerPublicKey}

	return gw
}

func (gw *Gateway) AppendHandler(handler http.Handler) http.Handler {
	return &Handler{handler, gw}
}

func (gw *Gateway) handler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodHead:
		gw.headHandler(w, r)
	case http.MethodGet:
		gw.getHandler(w, r)
	default:
		http.Error(w, fmt.Sprintf("method %s not allowed", r.Method), http.StatusBadRequest)
	}
}
