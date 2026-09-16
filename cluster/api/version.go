package api

import (
	"net/http"
	"strconv"
)

// COWSQLVersion is the current cowsql protocol version.
const COWSQLVersion = 1

// SetCOWSQLVersionHeader sets the cowsql version header.
func SetCOWSQLVersionHeader(request *http.Request) {
	request.Header.Set("X-Dqlite-Version", strconv.Itoa(COWSQLVersion))
}
