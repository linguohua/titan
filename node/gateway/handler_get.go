package gateway

import (
	"bytes"
	"crypto"
	"encoding/gob"
	"fmt"
	"io/ioutil"
	"mime"
	"net/http"
	"strings"

	"github.com/linguohua/titan/api/types"
	titanrsa "github.com/linguohua/titan/node/rsa"
)

const (
	formatRaw     = "application/vnd.ipld.raw"
	formatCar     = "application/vnd.ipld.car"
	formatTar     = "application/x-tar"
	formatDagJson = "application/vnd.ipld.dag-json"
	formatDagCbor = "application/vnd.ipld.dag-cbor"
	formatJson    = "application/json"
	formatCbor    = "application/cbor"
)

func (gw *Gateway) getHandler(w http.ResponseWriter, r *http.Request) {
	ticket, err := gw.verifyCredentials(w, r)
	if err != nil {
		http.Error(w, fmt.Sprintf("verify ticket error : %s", err.Error()), http.StatusUnauthorized)
		return
	}

	respFormat, formatParams, err := customResponseFormat(r)
	if err != nil {
		http.Error(w, fmt.Sprintf("processing the Accept header error: %s", err.Error()), http.StatusBadRequest)
		return
	}

	switch respFormat {
	case "", formatJson, formatCbor: // The implicit response format is UnixFS
		gw.serveUnixFS(w, r, ticket)
	case formatRaw:
		gw.serveRawBlock(w, r, ticket)
	case formatCar:
		gw.serveCar(w, r, ticket, formatParams["version"])
	case formatTar:
		gw.serveTAR(w, r, ticket)
	case formatDagJson, formatDagCbor:
		gw.serveCodec(w, r, ticket)
	default: // catch-all for unsuported application/vnd.*
		http.Error(w, fmt.Sprintf("unsupported format %s", respFormat), http.StatusBadRequest)
		return
	}
}

func (gw *Gateway) verifyCredentials(w http.ResponseWriter, r *http.Request) (*types.Credentials, error) {
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}

	buffer := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buffer)

	gwCredentials := &types.GatewayCredentials{}
	err = dec.Decode(gwCredentials)
	if err != nil {
		return nil, err
	}

	rsa := titanrsa.New(crypto.SHA256, crypto.SHA256.New())
	err = rsa.VerifySign(gw.schedulerPublicKey, []byte(gwCredentials.Sign), []byte(gwCredentials.Ciphertext))
	if err != nil {
		return nil, err
	}

	mgs, err := rsa.Decrypt([]byte(gwCredentials.Ciphertext), gw.privateKey)
	if err != nil {
		return nil, err
	}

	buffer = bytes.NewBuffer(mgs)
	dec = gob.NewDecoder(buffer)

	credentials := &types.Credentials{}
	err = dec.Decode(credentials)
	if err != nil {
		return nil, err
	}
	return credentials, nil
}

func customResponseFormat(r *http.Request) (mediaType string, params map[string]string, err error) {
	if formatParam := r.URL.Query().Get("format"); formatParam != "" {
		// translate query param to a content type
		switch formatParam {
		case "raw":
			return formatRaw, nil, nil
		case "car":
			return formatCar, nil, nil
		case "tar":
			return formatTar, nil, nil
		case "json":
			return formatJson, nil, nil
		case "cbor":
			return formatCbor, nil, nil
		case "dag-json":
			return formatDagJson, nil, nil
		case "dag-cbor":
			return formatDagCbor, nil, nil
		}
	}
	// Browsers and other user agents will send Accept header with generic types like:
	// Accept:text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8
	// We only care about explciit, vendor-specific content-types.
	for _, accept := range r.Header.Values("Accept") {
		// respond to the very first ipld content type
		if strings.HasPrefix(accept, "application/vnd.ipld") ||
			strings.HasPrefix(accept, "application/x-tar") ||
			strings.HasPrefix(accept, "application/json") ||
			strings.HasPrefix(accept, "application/cbor") ||
			strings.HasPrefix(accept, "application/vnd.ipfs") {
			mediatype, params, err := mime.ParseMediaType(accept)
			if err != nil {
				return "", nil, err
			}
			return mediatype, params, nil
		}
	}
	return "", nil, nil
}
