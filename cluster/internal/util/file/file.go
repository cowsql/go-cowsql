package file

import (
	"errors"
	"io"
	"io/fs"
	"os"
)

// PathExists checks if the provided path exists.
func PathExists(name string) bool {
	_, err := os.Lstat(name)
	if err != nil && errors.Is(err, fs.ErrNotExist) {
		return false
	}

	return true
}

// SafeCopy behaves like io.Copy but performs the copy through a loop of
// io.CopyN calls using a fixed 4MiB chunk size.
func SafeCopy(dst io.Writer, src io.Reader) (int64, error) {
	const chunkSize = 4 * 1024 * 1024

	var written int64

	for {
		n, err := io.CopyN(dst, src, chunkSize)
		written += n

		if err != nil {
			if errors.Is(err, io.EOF) {
				return written, nil
			}

			return written, err
		}
	}
}
