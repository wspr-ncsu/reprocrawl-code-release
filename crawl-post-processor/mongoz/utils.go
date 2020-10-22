package mongoz

import (
	"io"
	"log"
	"os"
)

// ClosingReader attempts to close the underlying reader on EOF
type ClosingReader struct {
	reader io.Reader
}

func (cr *ClosingReader) Read(p []byte) (int, error) {
	n, err := cr.reader.Read(p)
	if err != nil {
		closer, ok := cr.reader.(io.Closer)
		if ok {
			cerr := closer.Close()
			if cerr != nil {
				// Log the failure but do not die/panic--continue back to caller
				log.Print(cerr)
			}
		}
	}
	return n, err
}

// NewClosingReader wraps an existing io.Reader to auto-close on EOF
func NewClosingReader(rawReader io.Reader) *ClosingReader {
	return &ClosingReader{reader: rawReader}
}

// GetEnvDefault looks up the environmental variable with key and if not found returns the default
func GetEnvDefault(key, def string) string {
	val, ok := os.LookupEnv(key)
	if !ok {
		val = def
	}
	return val
}
