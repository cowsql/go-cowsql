package bindings

/*
#cgo linux LDFLAGS: -lcowsql
*/
import "C"

// required cowsql version
var (
	cowsqlMajorVersion int = 1
	cowsqlMinorVersion int = 14
)
