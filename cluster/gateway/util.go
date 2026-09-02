package gateway

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
)

// writeJSON encodes the body as JSON and sends it back to the client
// Accepts optional debugLogger that activates debug logging if non-nil.
func writeJSON(w http.ResponseWriter, body any, debugLogger *slog.Logger) error {
	var output io.Writer
	var captured *bytes.Buffer

	output = w
	if debugLogger != nil {
		captured = &bytes.Buffer{}
		output = io.MultiWriter(w, captured)
	}

	enc := json.NewEncoder(output)
	enc.SetEscapeHTML(false)
	err := enc.Encode(body)

	if captured != nil {
		debugJSON("WriteJSON", captured, debugLogger)
	}

	return err
}

// debugJSON helper to log JSON.
// Accepts a title to prefix the JSON log with, a *bytes.Buffer containing the JSON and a logger to use for
// logging the JSON (allowing for custom context to be added to the log).
func debugJSON(title string, r *bytes.Buffer, l *slog.Logger) {
	pretty := &bytes.Buffer{}
	err := json.Indent(pretty, r.Bytes(), "\t", "\t")
	if err != nil {
		l.Debug("Error indenting JSON", "err", err)
		return
	}

	// Print the JSON without the last "\n"
	str := pretty.String()
	l.Debug(fmt.Sprintf("%s\n\t%s", title, str[0:len(str)-1]))
}
