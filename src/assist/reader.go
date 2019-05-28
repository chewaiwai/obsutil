package assist

import (
	"io"
)

type MultiWritersReader struct {
	reader  io.Reader
	writers []io.Writer
}

func (mwr *MultiWritersReader) Read(p []byte) (n int, err error) {
	n, err = mwr.reader.Read(p)
	if mwr.writers != nil {
		if n > 0 {
			for _, writer := range mwr.writers {
				if writer != nil {
					writer.Write(p[:n])
				}
			}
		}
	}
	return
}

func Wrap(reader io.Reader, writers ...io.Writer) io.Reader {
	length := len(writers)
	if length == 0 {
		return reader
	}

	_writers := make([]io.Writer, 0, length)
	for _, writer := range writers {
		_writers = append(_writers, writer)
	}

	return &MultiWritersReader{reader: reader, writers: _writers}
}
