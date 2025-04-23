package logging_test

import (
	"reflect"
	"testing"

	"github.com/cowsql/go-cowsql/logging"
)

func assertEqual(t *testing.T, expected, actual any) {
	t.Helper()
	if expected == nil || actual == nil {
		if expected != actual {
			t.Fatal(expected, actual)
		}
	}

	if !reflect.DeepEqual(expected, actual) {
		t.Fatal(expected, actual)
	}
}

func TestLevel_String(t *testing.T) {
	assertEqual(t, "DEBUG", logging.Debug.String())
	assertEqual(t, "INFO", logging.Info.String())
	assertEqual(t, "WARN", logging.Warn.String())
	assertEqual(t, "ERROR", logging.Error.String())

	unknown := logging.Level(666)
	assertEqual(t, "UNKNOWN", unknown.String())
}
