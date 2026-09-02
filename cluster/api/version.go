package api

import (
	"fmt"
	"net/http"
)

// Current cowsql protocol version.
const COWSQLVersion = 1

// Set the cowsql version header.
func SetCOWSQLVersionHeader(request *http.Request) {
	request.Header.Set("X-Dqlite-Version", fmt.Sprintf("%d", COWSQLVersion))
}
