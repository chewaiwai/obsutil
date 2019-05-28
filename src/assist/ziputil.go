package assist

import (
	"archive/zip"
	"io"
	"os"
)

func DoCompress(file *os.File, prefix string, zw *zip.Writer) (int64, error) {
	info, err := file.Stat()
	if err != nil {
		return 0, err
	}

	header, err := zip.FileInfoHeader(info)
	if err != nil {
		return 0, err
	}
	header.Method = zip.Deflate
	if prefix != "" {
		header.Name = prefix + "/" + header.Name
	}
	fw, err := zw.CreateHeader(header)
	if err != nil {
		return 0, err
	}
	wcnt, err := io.Copy(fw, file)
	if err != nil {
		return 0, err
	}
	return wcnt, nil
}
