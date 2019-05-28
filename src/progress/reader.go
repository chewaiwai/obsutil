package progress

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"hash"
	"io"
)

type ReaderWrapper struct {
	Reader      io.Reader
	TotalCount  int64
	readedCount int64
}

func (rw *ReaderWrapper) Read(p []byte) (n int, err error) {
	n, err = rw.Reader.Read(p)
	if rw.TotalCount <= 0 {
		return
	}
	readedOnce := int64(n)
	if remainCount := rw.TotalCount - rw.readedCount; remainCount > readedOnce {
		rw.readedCount += readedOnce
		return n, err
	} else {
		rw.readedCount += remainCount
		return int(remainCount), io.EOF
	}
}

type CheckSumReader struct {
	ReaderWrapper
	EnableChecksum bool
	checksum       []byte
	md5Hash        hash.Hash
}

func (csr *CheckSumReader) Base64Md5() string {
	if csr.EnableChecksum {
		if csr.checksum == nil {
			csr.checksum = csr.md5Hash.Sum(nil)
		}
		return base64.StdEncoding.EncodeToString(csr.checksum)
	}
	return ""
}

func (csr *CheckSumReader) HexMd5() string {
	if csr.EnableChecksum {
		if csr.checksum == nil {
			csr.checksum = csr.md5Hash.Sum(nil)
		}
		return hex.EncodeToString(csr.checksum)
	}
	return ""
}

func (csr *CheckSumReader) Read(p []byte) (n int, err error) {
	n, err = csr.ReaderWrapper.Read(p)
	if !csr.EnableChecksum {
		return
	}
	if n > 0 {
		csr.md5Hash.Write(p[:n])
	}
	return
}

type SingleProgressReader struct {
	CheckSumReader
	BarCh SingleBarChan
}

func (spr *SingleProgressReader) Read(p []byte) (n int, err error) {
	n, err = spr.CheckSumReader.Read(p)
	if n > 0 {
		ctx.AddEffectiveStream(int64(n))
		ctx.AddFinishedStream(int64(n))
		if spr.BarCh != nil {
			spr.BarCh.Send(n)
		}
	}
	return
}

func NewSingleProgressReader(reader io.Reader, totalCount int64, enableChecksum bool, barCh SingleBarChan) *SingleProgressReader {
	spr := &SingleProgressReader{}
	spr.Reader = reader
	spr.TotalCount = totalCount
	spr.EnableChecksum = enableChecksum
	if spr.EnableChecksum {
		spr.md5Hash = md5.New()
	}
	spr.BarCh = barCh
	return spr
}
