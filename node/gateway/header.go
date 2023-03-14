package gateway

import (
	"net/http"

	"github.com/ipfs/go-cid"
)

func setContentDispositionHeader(w http.ResponseWriter, r *http.Request, c cid.Cid) {
	filename := r.URL.Query().Get("filename")
	if filename == "" {
		filename = c.String() + ".bin"
	}

	// utf8Name := url.PathEscape(filename)
	// asciiName := url.PathEscape(onlyAscii.ReplaceAllLiteralString(filename, "_"))
	// w.Header().Set("Content-Disposition", fmt.Sprintf("%s; filename=\"%s\"; filename*=UTF-8''%s", disposition, asciiName, utf8Name))
}
