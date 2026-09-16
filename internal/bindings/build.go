package bindings

/*
#cgo linux LDFLAGS: -lcowsql
*/
import "C"

// required cowsql version.
var (
	cowsqlMajorVersion = 1
	cowsqlMinorVersion = 14
)
