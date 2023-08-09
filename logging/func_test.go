package logging_test

import (
	"testing"

	"github.com/cowsql/go-cowsql/logging"
)

func Test_TestFunc(t *testing.T) {
	f := logging.Test(t)
	f(logging.Info, "hello")
}
