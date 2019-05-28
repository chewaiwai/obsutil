package command

import (
	"assist"
	"bufio"
	"concurrent"
	"encoding/xml"
	"fmt"
	"io"
	"obs"
	"os"
	"path/filepath"
	"progress"
	"ratelimit"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type TempFileInfo struct {
	XMLName     xml.Name `xml:"TempFileInfo"`
	TempFileUrl string   `xml:"TempFileUrl"`
	Size        int64    `xml:"Size"`
}

type DownloadPart struct {
	XMLName     xml.Name `xml:"DownloadPart"`
	PartNumber  int      `xml:"PartNumber"`
	RangeStart  int64    `xml:"RangeStart"`
	RangeEnd    int64    `xml:"RangeEnd"`
	IsCompleted bool     `xml:"IsCompleted"`
}

type DownloadFileCheckpoint struct {
	XMLName       xml.Name       `xml:"DownloadFileCheckpoint"`
	Bucket        string         `xml:"Bucket"`
	Key           string         `xml:"Key"`
	VersionId     string         `xml:"VersionId"`
	FileUrl       string         `xml:"FileUrl"`
	ObjectInfo    ObjectInfo     `xml:"ObjectInfo"`
	TempFileInfo  TempFileInfo   `xml:"TempFileInfo"`
	DownloadParts []DownloadPart `xml:"DownloadParts>DownloadPart"`
}

type downloadPartTask struct {
	bucket      string
	key         string
	versionId   string
	tempFileUrl string
	partNumber  int
	rangeStart  int64
	rangeEnd    int64
	abort       *int32
	barCh       progress.SingleBarChan
	limiter     *ratelimit.RateLimiter
	objectInfo  ObjectInfo
}

type downloadPartResult struct {
	partNumber int
	metadata   map[string]string
	requestId  string
	status     int
}

func (c *transferCommand) getRange() (int64, int64, bool) {
	if c.drange != "" {
		ranges := strings.Split(c.drange, "-")
		if len(ranges) >= 3 {
			printf("Error: Invalid range [%s], valid format is x-y", c.drange)
			return -1, -1, false
		}
		if len(ranges) == 1 {
			rangeStart, err := strconv.ParseInt(ranges[0], 10, 64)
			if err != nil {
				printf("Error: Invalid range [%s], x is not a valid number", c.drange)
				return -1, -1, false
			}
			return rangeStart, -1, true
		}
		rangeStart, err := strconv.ParseInt(ranges[0], 10, 64)
		if err != nil {
			printf("Error: Invalid range [%s], x is not a valid number", c.drange)
			return -1, -1, false
		}
		rangeEnd, err := strconv.ParseInt(ranges[1], 10, 64)
		if err != nil {
			printf("Error: Invalid range [%s], y is not a valid number", c.drange)
			return -1, -1, false
		}
		return rangeStart, rangeEnd, true
	}
	return -1, -1, true
}

func (t *downloadPartTask) Run() interface{} {
	if atomic.LoadInt32(t.abort) == 1 {
		return abortError
	}

	input := &obs.GetObjectInput{}
	input.Bucket = t.bucket
	input.Key = t.key
	input.VersionId = t.versionId
	input.RangeStart = t.rangeStart
	input.RangeEnd = t.rangeEnd

	output, err := obsClient.GetObject(input)
	if err == nil {
		defer output.Body.Close()

		_readBufferIoSize, transErr := assist.TranslateToInt64(config["readBufferIoSize"])
		if transErr != nil {
			_readBufferIoSize = readBufferIoSize
		}
		if _readBufferIoSize < minReadBufferIoSize {
			_readBufferIoSize = minReadBufferIoSize
		}
		_range := input.RangeEnd - input.RangeStart + 1
		if _range < _readBufferIoSize {
			_readBufferIoSize = _range
		}

		var reader io.Reader = progress.NewSingleProgressReader(bufio.NewReaderSize(output.Body, int(_readBufferIoSize)), -1, false, t.barCh)
		if t.limiter != nil {
			reader = ratelimit.NewRateLimitReaderWithLimiter(reader, t.limiter)
		}

		fd, err := assist.OpenFile(t.tempFileUrl, os.O_WRONLY, 0666)
		if err != nil {
			return err
		}

		if fd != nil && err == nil {
			defer fd.Close()
			if _, err := fd.Seek(t.rangeStart, 0); err != nil {
				return err
			}

			_writeBufferIoSize, transErr := assist.TranslateToInt64(config["writeBufferIoSize"])
			if transErr != nil {
				_writeBufferIoSize = writeBufferIoSize
			}

			bufWriter := bufio.NewWriterSize(fd, int(_writeBufferIoSize))
			p := make([]byte, defaultChunkSize)
			for {
				n, err := reader.Read(p)
				if n > 0 {
					slice := p[0:n]
					wcnt, werr := bufWriter.Write(slice)
					if werr != nil {
						return werr
					}

					if wcnt != n {
						return fmt.Errorf("Write to file [%s] failed, expect [%d], actual [%d]", t.tempFileUrl, n, wcnt)
					}
				}

				if err != nil {
					if err != io.EOF {
						return err
					}
					break
				}
			}

			if ferr := bufWriter.Flush(); ferr != nil {
				return ferr
			}

			if config["fsyncForDownload"] == "true" {
				if err := fd.Sync(); err != nil {
					return err
				}
			}
		}

		if changedErr := checkSourceChangedForDownload(t.bucket, t.key, t.versionId, t.objectInfo.LastModified, t.abort); changedErr != nil {
			return changedErr
		}

		return downloadPartResult{
			partNumber: t.partNumber,
			metadata:   output.Metadata,
			status:     output.StatusCode,
			requestId:  output.RequestId,
		}
	}

	if obsError, ok := err.(obs.ObsError); ok && obsError.StatusCode >= 400 && obsError.StatusCode < 500 {
		atomic.CompareAndSwapInt32(t.abort, 0, 1)
	}

	doLogError(err, LEVEL_ERROR, fmt.Sprintf("Bucket [%s], Key [%s], VersionId [%s], PartNumber [%d]", t.bucket, t.key, t.versionId, t.partNumber))
	return err
}

func checkSourceChangedForDownload(bucket, key, versionId string, originLastModified int64, abort *int32) error {
	if config["checkSourceChange"] == "true" {
		if metaContext, err := getObjectMetadata(bucket, key, versionId); err != nil {
			if obsError, ok := err.(obs.ObsError); ok && obsError.StatusCode == 404 {
				if abort != nil {
					atomic.CompareAndSwapInt32(abort, 0, 1)
				}
				return fmt.Errorf("Source object [%s] in the bucket [%s] doesnot exist", key, bucket)
			}
		} else if originLastModified != metaContext.LastModified.Unix() {
			if abort != nil {
				atomic.CompareAndSwapInt32(abort, 0, 1)
			}
			return fmt.Errorf("Source object [%s] in the bucket [%s] changed", key, bucket)
		}
	}
	return nil
}

func (c *transferCommand) ensureKeyForDownload(metaContext *MetaContext, metaErr error, fileStat os.FileInfo, batchFlag int, key string) (bool, error) {
	var changed bool
	if metaErr == nil {
		if fileStat == nil || metaContext == nil {
			changed = true
		} else {
			if fileStat.IsDir() {
				if batchFlag == 2 && isObsFolder(key) {
					changed = false
				} else {
					changed = true
				}
			} else {
				changed = metaContext.Size != fileStat.Size() || metaContext.LastModified.After(fileStat.ModTime())
			}
		}
	} else if obsError, ok := metaErr.(obs.ObsError); ok && obsError.StatusCode >= 300 && obsError.StatusCode < 500 {
		changed = true
	} else {
		changed = false
	}
	return changed, metaErr
}

func (c *transferCommand) createFile(key, fileUrl string, fileStat os.FileInfo) (*os.File, error) {
	var fd *os.File
	var err error
	if fileStat == nil {
		parentDir := filepath.Dir(fileUrl)
		stat, statErr := os.Stat(parentDir)
		if statErr != nil {
			err = assist.MkdirAll(parentDir, os.ModePerm)
		} else if !stat.IsDir() {
			err = fmt.Errorf("Cannot create the parent folder [%s] due to a same file exits!", parentDir)
		}
		if err == nil {
			fd, err = assist.OpenFile(fileUrl, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
		}
	} else if fileStat.IsDir() {
		fileName := key
		if index := strings.LastIndex(key, "/"); index >= 0 {
			fileName = key[index+1:]
		}
		_fileUrl := fileUrl + "/" + fileName
		fd, err = assist.OpenFile(_fileUrl, os.O_CREATE|os.O_WRONLY, 0666)
		doLog(LEVEL_TRACE, "Change file path: %s->%s", fileUrl, _fileUrl)
		if _err := assist.Chown(_fileUrl); err != nil {
			doLog(LEVEL_WARN, "Change own for file [%s] failed, %s", _fileUrl, _err.Error())
		}
	} else {
		fd, err = assist.OpenFile(fileUrl, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
		if _err := assist.Chown(fileUrl); err != nil {
			doLog(LEVEL_WARN, "Change own for file [%s] failed, %s", fileUrl, _err.Error())
		}
	}

	return fd, err
}

func (c *transferCommand) downloadSmallFile(bucket, key, versionId, fileUrl string, fileStat os.FileInfo, metaContext *MetaContext,
	barCh progress.SingleBarChan, limiter *ratelimit.RateLimiter) (requestId string, status int, md5Value string, downloadFileError error) {
	input := &obs.GetObjectInput{}
	input.Bucket = bucket
	input.Key = key
	input.VersionId = versionId

	output, err := obsClient.GetObject(input)
	if err != nil {
		downloadFileError = err
		return
	}

	objectSize := metaContext.Size

	barChFlag := false
	if barCh == nil && objectSize > 0 {
		barCh = newSingleBarChan()
		barCh.SetBytes(true)
		barCh.SetTemplate(progress.SpeedOnly)
		barCh.SetTotalCount(objectSize)
		progress.SetTotalStream(objectSize)
		barCh.Start()
		barChFlag = true
	}

	_readBufferIoSize, transErr := assist.TranslateToInt64(config["readBufferIoSize"])
	if transErr != nil {
		_readBufferIoSize = readBufferIoSize
	}
	if _readBufferIoSize < minReadBufferIoSize {
		_readBufferIoSize = minReadBufferIoSize
	}
	if output.ContentLength < _readBufferIoSize {
		_readBufferIoSize = output.ContentLength
	}

	defer output.Body.Close()
	var reader io.Reader = progress.NewSingleProgressReader(bufio.NewReaderSize(output.Body, int(_readBufferIoSize)), -1, false, barCh)

	if limiter == nil {
		limiter = c.createRateLimiter()
	}

	if limiter != nil {
		reader = ratelimit.NewRateLimitReaderWithLimiter(reader, limiter)
	}

	fd, err := c.createFile(key, fileUrl, fileStat)
	if err != nil {
		downloadFileError = err
		return
	}

	var md5Writer io.Writer = nilWriter
	if c.verifyMd5 {
		if _md5Value, ok := output.Metadata[checkSumKey]; ok && _md5Value != "" {
			md5Value = _md5Value
			md5Writer = assist.GetMd5Writer()
		} else {
			var _versionId string
			if versionId != "" {
				_versionId = "?versionId=" + versionId
			}
			warnMessage := fmt.Sprintf("Cannot get the valid md5 value of key [%s] in bucket [%s] to check", key, bucket)
			warnLoggerMessage := fmt.Sprintf("%s, obs://%s/%s%s --> %s, warn message [%s]",
				assist.NormalizeBytes(objectSize), bucket, key, _versionId, fileUrl, warnMessage)

			c.recordWarnMessage(warnMessage, warnLoggerMessage)
		}
	}

	if fd != nil && err == nil {
		defer fd.Close()

		_writeBufferIoSize, transErr := assist.TranslateToInt64(config["writeBufferIoSize"])
		if transErr != nil {
			_writeBufferIoSize = writeBufferIoSize
		}

		bufWriter := bufio.NewWriterSize(fd, int(_writeBufferIoSize))
		p := make([]byte, defaultChunkSize)
		for {
			n, err := reader.Read(p)
			if n > 0 {
				slice := p[0:n]
				wcnt, werr := bufWriter.Write(slice)
				md5Writer.Write(slice)
				if werr != nil {
					downloadFileError = werr
					return
				}

				if wcnt != n {
					downloadFileError = fmt.Errorf("Write to file [%s] failed, expect [%d], actual [%d]", fileUrl, n, wcnt)
					return
				}
			}

			if err != nil {
				if err != io.EOF {
					downloadFileError = err
					return
				}
				break
			}
		}

		if err := bufWriter.Flush(); err != nil {
			downloadFileError = err
			return
		}

		if config["fsyncForDownload"] == "true" {
			if err := fd.Sync(); err != nil {
				downloadFileError = err
				return
			}
		}

	}
	if barChFlag {
		barCh.WaitToFinished()
	}

	if changedErr := checkSourceChangedForDownload(bucket, key, versionId, metaContext.LastModified.Unix(), nil); changedErr != nil {
		downloadFileError = changedErr
		return
	}

	if md5Value != "" {
		localMd5 := assist.GetHexMd5(md5Writer)
		if localMd5 != md5Value {
			doLog(LEVEL_ERROR, "Verify md5 failed after downloading file [%s], local md5 [%s] remote md5 [%s], will try to delete downloaded file", fileUrl, localMd5, md5Value)
			downloadFileError = &verifyMd5Error{msg: fmt.Sprintf("Verify md5 failed after downloading file [%s], local md5 [%s] remote md5 [%s]", fileUrl, localMd5, md5Value)}
			return
		}
	}

	requestId = output.RequestId
	status = output.StatusCode
	return
}

func (c *transferCommand) prepareDownloadFileCheckpoint(bucket, key, versionId, fileUrl string,
	metaContext *MetaContext, dfc *DownloadFileCheckpoint) error {

	var tempFileUrl string

	if dir := c.tempFileDir; dir != "" {
		tempFileUrl = assist.NormalizeFilePath(dir + "/" + fmt.Sprintf("%s", assist.HexMd5(assist.StringToBytes(fileUrl))))
	} else if dir := filepath.Dir(fileUrl); dir != "" {
		tempFileUrl = assist.NormalizeFilePath(dir + "/" + fmt.Sprintf("%s", assist.HexMd5(assist.StringToBytes(fileUrl))))
	} else {
		tempFileUrl = assist.NormalizeFilePath(fmt.Sprintf("%s", assist.HexMd5(assist.StringToBytes(fileUrl))))
	}

	tempFileStat, _ := os.Stat(tempFileUrl)
	fd, err := c.createFile(key, tempFileUrl, tempFileStat)
	if err != nil {
		return err
	}
	defer fd.Close()

	objectSize := metaContext.Size

	if _, err := fd.WriteAt(assist.StringToBytes("a"), objectSize-1); err != nil {
		return err
	}
	doLog(LEVEL_DEBUG, "Create a temp file [%s] to download key [%s] in the bucket [%s] successfully", tempFileUrl, key, bucket)

	dfc.Bucket = bucket
	dfc.Key = key
	dfc.VersionId = versionId
	dfc.FileUrl = fileUrl
	dfc.TempFileInfo = TempFileInfo{
		TempFileUrl: tempFileUrl,
		Size:        objectSize,
	}
	dfc.ObjectInfo = ObjectInfo{
		Size:         objectSize,
		LastModified: metaContext.LastModified.Unix(),
		ETag:         metaContext.ETag,
	}
	partSize := c.partSize
	count := dfc.ObjectInfo.Size / partSize
	if count >= 10000 {
		partSize = dfc.ObjectInfo.Size / 10000
		if dfc.ObjectInfo.Size%10000 != 0 {
			partSize += 1
		}
		count = dfc.ObjectInfo.Size / partSize
	}

	if dfc.ObjectInfo.Size%partSize != 0 {
		count += 1
	}

	downloadParts := make([]DownloadPart, 0, count)
	var i int64
	for i = 0; i < count; i++ {
		downloadPart := DownloadPart{
			RangeStart: i * partSize,
			RangeEnd:   (i+1)*partSize - 1,
			PartNumber: int(i) + 1,
		}
		downloadParts = append(downloadParts, downloadPart)
	}
	if lastPartSize := dfc.ObjectInfo.Size % partSize; lastPartSize != 0 {
		downloadParts[count-1].RangeEnd = dfc.ObjectInfo.Size - 1
	}
	dfc.DownloadParts = downloadParts
	return nil
}

func (c *transferCommand) handleDownloadPartResult(dfc *DownloadFileCheckpoint, checkpointFile string, result interface{}, lock *sync.Mutex) (status int, requestId string, metadata map[string]string, downloadFileError error) {
	if ret, ok := result.(downloadPartResult); ok {
		lock.Lock()
		defer lock.Unlock()
		dfc.DownloadParts[ret.partNumber-1].IsCompleted = true
		metadata = ret.metadata
		status = ret.status
		requestId = ret.requestId
		downloadFileError = c.recordCheckpointFile(checkpointFile, dfc)
	} else if result != abortError {
		if resultError, ok := result.(error); ok {
			downloadFileError = resultError
		}
	}
	return
}

func (c *transferCommand) downloadBigFileConcurrent(dfc *DownloadFileCheckpoint, checkpointFile string, barChFlag bool,
	barCh progress.SingleBarChan, limiter *ratelimit.RateLimiter) (int32, int, string, map[string]string, error) {
	pool := concurrent.NewRoutinePool(c.parallel, defaultParallelsCacheCount)

	var downloadFileError atomic.Value
	var downloadFileErrorFlag int32 = 0
	var metadata atomic.Value
	var metadataFlag int32 = 0
	var abort int32 = 0
	var status atomic.Value
	var requestId atomic.Value
	var lock *sync.Mutex = new(sync.Mutex)

	for _, downloadPart := range dfc.DownloadParts {
		if atomic.LoadInt32(&abort) == 1 {
			break
		}
		if !downloadPart.IsCompleted {
			task := &downloadPartTask{
				bucket:      dfc.Bucket,
				key:         dfc.Key,
				versionId:   dfc.VersionId,
				tempFileUrl: dfc.TempFileInfo.TempFileUrl,
				partNumber:  downloadPart.PartNumber,
				rangeStart:  downloadPart.RangeStart,
				rangeEnd:    downloadPart.RangeEnd,
				abort:       &abort,
				barCh:       barCh,
				limiter:     limiter,
				objectInfo:  dfc.ObjectInfo,
			}
			pool.ExecuteFunc(func() interface{} {
				ret := task.Run()
				if _status, _requestId, _metadata, _downloadFileError := c.handleDownloadPartResult(dfc, checkpointFile, ret, lock); _downloadFileError != nil {
					if atomic.CompareAndSwapInt32(&downloadFileErrorFlag, 0, 1) {
						downloadFileError.Store(_downloadFileError)
					}
				} else if _metadata != nil {
					if atomic.CompareAndSwapInt32(&metadataFlag, 0, 1) {
						metadata.Store(_metadata)
						status.Store(_status)
						requestId.Store(_requestId)
					}
				}
				return nil
			})
		} else {
			completed := downloadPart.RangeEnd - downloadPart.RangeStart + 1
			barCh.Send64(completed)
			progress.AddFinishedStream(completed)
		}
	}
	if barChFlag {
		barCh.Start()
	}
	pool.ShutDown()

	var s int = 0
	var r string
	var m map[string]string
	var e error

	if _s, ok := status.Load().(int); ok {
		s = _s
	}

	if _r, ok := requestId.Load().(string); ok {
		r = _r
	}

	if _e, ok := downloadFileError.Load().(error); ok {
		e = _e
	}

	if _m, ok := metadata.Load().(map[string]string); ok {
		m = _m
	}

	return abort, s, r, m, e
}

func (dfc *DownloadFileCheckpoint) isValid(bucket, key, versionId, fileUrl string, metaContext *MetaContext) bool {
	if dfc.Bucket != bucket || dfc.Key != key || dfc.VersionId != versionId || dfc.FileUrl != fileUrl {
		return false
	}

	if dfc.ObjectInfo.Size != metaContext.Size || dfc.ObjectInfo.ETag != metaContext.ETag ||
		dfc.ObjectInfo.LastModified != metaContext.LastModified.Unix() {
		return false
	}

	if dfc.ObjectInfo.Size != dfc.TempFileInfo.Size {
		return false
	}

	if stat, err := os.Stat(dfc.TempFileInfo.TempFileUrl); err != nil || stat.Size() != dfc.ObjectInfo.Size {
		return false
	}

	return true
}

func (c *transferCommand) downloadBigFile(bucket, key, versionId, fileUrl string, fileStat os.FileInfo, metaContext *MetaContext,
	barCh progress.SingleBarChan, limiter *ratelimit.RateLimiter, batchFlag int) (requestId string, status int, md5Value string, downloadFileError error) {

	if metaContext.Size == 0 {
		return c.downloadSmallFile(bucket, key, versionId, fileUrl, fileStat, metaContext, barCh, limiter)
	}

	checkpointFile := c.getCheckpointFile(bucket, key, versionId, dm)
	dfc := &DownloadFileCheckpoint{}
	stat, err := os.Stat(checkpointFile)
	needPrepare := true
	if err == nil {
		if stat.IsDir() {
			downloadFileError = fmt.Errorf("Checkpoint file for downloading [%s]-[%s] is a folder", bucket, key)
			return
		}
		err = c.loadCheckpoint(checkpointFile, dfc)
		if err != nil {
			if err = os.Remove(checkpointFile); err != nil {
				downloadFileError = err
				return
			}
		} else if !dfc.isValid(bucket, key, versionId, fileUrl, metaContext) {
			if dfc.TempFileInfo.TempFileUrl != "" {
				os.Remove(dfc.TempFileInfo.TempFileUrl)
			}

			if err = os.Remove(checkpointFile); err != nil {
				downloadFileError = err
				return
			}
		} else {
			needPrepare = false
		}
	}
	if needPrepare {
		err = c.prepareDownloadFileCheckpoint(bucket, key, versionId, fileUrl, metaContext, dfc)
		if err != nil {
			os.Remove(dfc.TempFileInfo.TempFileUrl)
			downloadFileError = err
			return
		}
		err = c.recordCheckpointFile(checkpointFile, dfc)
		if err != nil {
			os.Remove(dfc.TempFileInfo.TempFileUrl)
			downloadFileError = err
			return
		}
	}

	barChFlag := false
	if barCh == nil {
		barCh = newSingleBarChan()
		barCh.SetBytes(true)
		barCh.SetTemplate(progress.SpeedOnly)
		barCh.SetTotalCount(dfc.ObjectInfo.Size)
		progress.SetTotalStream(dfc.ObjectInfo.Size)
		barChFlag = true
	}

	if limiter == nil {
		limiter = c.createRateLimiter()
	}

	var abort int32
	var metadata map[string]string
	abort, status, requestId, metadata, downloadFileError = c.downloadBigFileConcurrent(dfc, checkpointFile, barChFlag, barCh, limiter)

	if barChFlag {
		barCh.WaitToFinished()
	}
	if abort == 1 {
		os.Remove(dfc.TempFileInfo.TempFileUrl)
		if err = os.Remove(checkpointFile); err != nil {
			downloadFileError = err
			return
		}
	}
	if downloadFileError != nil {
		return
	}
	start := assist.GetUtcNow()
	if batchFlag <= 1 {
		printf("Waiting to rename temporary file...")
	}
	if err = assist.RenameFile(dfc.TempFileInfo.TempFileUrl, dfc.FileUrl); err != nil {
		downloadFileError = err
	} else {
		cost := (assist.GetUtcNow().UnixNano() - start.UnixNano()) / 1000000
		doLog(LEVEL_DEBUG, "Rename checkpoint file [%s] to [%s] successfully, cost [%d]",
			checkpointFile, dfc.FileUrl, cost)
		if err = os.Remove(checkpointFile); err != nil {
			doLog(LEVEL_WARN, "Download big file [%s] from key [%s] in the bucket [%s] successfully, but remove checkpoint file [%s] failed",
				dfc.FileUrl, dfc.Key, dfc.Bucket, checkpointFile)
		}
	}

	if c.verifyMd5 {
		if _md5Value, ok := metadata[checkSumKey]; ok && _md5Value != "" {
			md5Value = _md5Value
			if barChFlag {
				h := &assist.Hint{}
				h.Message = "Waiting to caculate the md5 value"
				h.Start()
				if etag, err := md5File(fileUrl); err != nil {
					downloadFileError = &verifyMd5Error{msg: fmt.Sprintf("Verify md5 failed after downloading file [%s], %s", fileUrl, err.Error())}
				} else {
					localMd5 := assist.Hex(etag)
					if localMd5 != md5Value {
						doLog(LEVEL_ERROR, "Verify md5 failed after downloading file [%s], local md5 [%s] remote md5 [%s], will try to delete downloaded file", fileUrl, localMd5, md5Value)
						downloadFileError = &verifyMd5Error{msg: fmt.Sprintf("Verify md5 failed after downloading file [%s], local md5 [%s] remote md5 [%s]", fileUrl, localMd5, md5Value)}
					}
				}
				h.End()
			} else {
				if etag, err := md5File(fileUrl); err != nil {
					downloadFileError = &verifyMd5Error{msg: fmt.Sprintf("Verify md5 failed after downloading file [%s], %s", fileUrl, err.Error())}
				} else {
					localMd5 := assist.Hex(etag)
					if localMd5 != md5Value {
						doLog(LEVEL_ERROR, "Verify md5 failed after downloading file [%s], local md5 [%s] remote md5 [%s], will try to delete downloaded file", fileUrl, localMd5, md5Value)
						downloadFileError = &verifyMd5Error{msg: fmt.Sprintf("Verify md5 failed after downloading file [%s], local md5 [%s] remote md5 [%s]", fileUrl, localMd5, md5Value)}
					}
				}
			}
		} else {
			var _versionId string
			if versionId != "" {
				_versionId = "?versionId=" + versionId
			}
			objectSizeStr := "n/a"
			if metaContext != nil {
				objectSizeStr = assist.NormalizeBytes(metaContext.Size)
			}

			warnMessage := fmt.Sprintf("Cannot get the valid md5 value of key [%s] in bucket [%s] to check", key, bucket)
			warnLoggerMessage := fmt.Sprintf("%s, obs://%s/%s%s --> %s, warn message [%s]",
				objectSizeStr, bucket, key, _versionId, fileUrl, warnMessage)
			c.recordWarnMessage(warnMessage, warnLoggerMessage)
		}
	}
	return
}

func (c *transferCommand) downloadFileWithMetaContext(bucket, key, versionId string, metaContext *MetaContext, metaErr error, fileUrl string, fileStat os.FileInfo, barCh progress.SingleBarChan,
	limiter *ratelimit.RateLimiter, batchFlag int, fastFailed error) int {
	var _versionId string
	if versionId != "" {
		_versionId = "?versionId=" + versionId
	}

	objectSizeStr := "n/a"
	if metaContext != nil {
		objectSizeStr = assist.NormalizeBytes(metaContext.Size)
	}

	if fastFailed != nil {
		c.failedLogger.doRecord("%s, obs://%s/%s%s --> %s, n/a, n/a, n/a, error message [%s], n/a", objectSizeStr, bucket, key, _versionId, fileUrl, fastFailed.Error())
		return 0
	}

	if batchFlag == 2 && atomic.LoadInt32(&c.abort) == 1 {
		c.failedLogger.doRecord("%s, obs://%s/%s%s --> %s, n/a, n/a, error code [%s], error message [%s], n/a", objectSizeStr, bucket, key, _versionId, fileUrl,
			"AbortError", "Task is aborted")
		return 0
	}

	if c.update {
		changed, err := c.ensureKeyForDownload(metaContext, metaErr, fileStat, batchFlag, key)
		if !changed {
			if err == nil {
				if barCh != nil {
					if metaContext.Size <= 0 {
						barCh.Send64(1)
					} else {
						barCh.Send64(metaContext.Size)
					}
				}
				progress.AddFinishedStream(metaContext.Size)
				if batchFlag >= 1 {
					c.succeedLogger.doRecord("%s, n/a, obs://%s/%s%s --> %s, n/a, n/a, success message [skip since the source is not changed], n/a", objectSizeStr, bucket, key, _versionId, fileUrl)
				}
				if batchFlag != 2 {
					printf("%s, obs://%s/%s%s --> %s, skip since the source is not changed", objectSizeStr, bucket, key, _versionId, fileUrl)
				}
				return 2
			}
			if batchFlag >= 1 {
				c.failedLogger.doRecord("%s, obs://%s/%s%s --> %s, n/a, n/a, n/a, error message [skip since the status of source is unknown], n/a", objectSizeStr, bucket, key, _versionId, fileUrl)
			}
			if batchFlag != 2 {
				printf("obs://%s/%s%s --> %s, skip since the status of source is unknown", bucket, key, _versionId, fileUrl)
			}
			return 0
		}
	}

	var downloadFileError error = metaErr
	if c.dryRun {
		if downloadFileError == nil {
			if barCh != nil {
				if metaContext.Size <= 0 {
					barCh.Send64(1)
				} else {
					barCh.Send64(metaContext.Size)
				}
			}
			progress.AddFinishedStream(metaContext.Size)
			if batchFlag >= 1 {
				c.succeedLogger.doRecord("%s, n/a, obs://%s/%s%s --> %s, n/a, n/a, success message [dry run done], n/a", objectSizeStr, bucket, key, _versionId, fileUrl)
			}
			if batchFlag != 2 {
				printf("\nDownload dry run successfully, %s, obs://%s/%s%s --> %s", objectSizeStr, bucket, key, _versionId, fileUrl)
			}
			return 1
		}
		if batchFlag >= 1 {
			c.failedLogger.doRecord("%s, obs://%s/%s%s --> %s, n/a, n/a, n/a, error message [dry run done with error - %s], n/a", objectSizeStr, bucket, key, _versionId, fileUrl, downloadFileError.Error())
		}
		if batchFlag != 2 {
			logError(downloadFileError, LEVEL_INFO, fmt.Sprintf("\nDownload dry run failed, obs://%s/%s%s --> %s", bucket, key, _versionId, fileUrl))
		}
		return 0
	}

	var requestId string
	var status int
	var md5Value string
	start := assist.GetUtcNow()
	addCostFlag := false
	if downloadFileError == nil {
		if isObsFolder(key) {
			if fileStat == nil {
				downloadFileError = assist.MkdirAll(fileUrl, os.ModePerm)
			} else if !fileStat.IsDir() {
				downloadFileError = fmt.Errorf("Cannot create the folder [%s] due to a same file exits!", fileUrl)
			}
			if downloadFileError == nil && barCh != nil {
				barCh.Send64(1)
			}
		} else {
			objectSize := metaContext.Size
			addCostFlag = true
			if objectSize >= c.bigfileThreshold {
				requestId, status, md5Value, downloadFileError = c.downloadBigFile(bucket, key, versionId, fileUrl, fileStat, metaContext, barCh, limiter, batchFlag)
			} else {
				requestId, status, md5Value, downloadFileError = c.downloadSmallFile(bucket, key, versionId, fileUrl, fileStat, metaContext, barCh, limiter)
			}

			if _, ok := downloadFileError.(*verifyMd5Error); ok {
				if err := os.Remove(fileUrl); err == nil {
					doLog(LEVEL_INFO, "Delete file [%s] successfully", fileUrl)
				} else {
					warnMessage := fmt.Sprintf("Delete file [%s] failed - %s", fileUrl, err.Error())
					warnLoggerMessage := fmt.Sprintf("%s, obs://%s/%s%s --> %s, warn message [%s]",
						objectSizeStr, bucket, key, _versionId, fileUrl, warnMessage)
					c.recordWarnMessage(warnMessage, warnLoggerMessage)
				}
			}

			if downloadFileError == nil {
				if barCh != nil && objectSize <= 0 {
					barCh.Send64(1)
				}

				if (!c.verifyMd5 || md5Value == "") && c.verifyLength {
					if stat, err := os.Stat(fileUrl); err == nil {
						if !stat.IsDir() && stat.Size() != metaContext.Size {
							doLog(LEVEL_ERROR, "Verify length failed after downloading file [%s], local length [%d] remote length [%d], will try to delete downloaded file", fileUrl, stat.Size(), metaContext.Size)
							if err := os.Remove(fileUrl); err == nil {
								doLog(LEVEL_INFO, "Delete local file [%s] successfully", fileUrl)
							} else {
								warnMessage := fmt.Sprintf("Delete local file [%s] failed - %s", fileUrl, err.Error())
								warnLoggerMessage := fmt.Sprintf("%s, obs://%s/%s%s --> %s, warn message [%s]",
									objectSizeStr, bucket, key, _versionId, fileUrl, warnMessage)
								c.recordWarnMessage(warnMessage, warnLoggerMessage)
							}
							downloadFileError = &verifyLengthError{msg: fmt.Sprintf("Verify length failed after downloading file [%s], local length [%d] remote length [%d]", fileUrl, stat.Size(), metaContext.Size)}
						}
					} else {
						warnMessage := fmt.Sprintf("Download file [%s] from key [%s] in the bucket [%s] successfully - but can not verify length - %s",
							fileUrl, key, bucket, err.Error())
						warnLoggerMessage := fmt.Sprintf("%s, obs://%s/%s%s --> %s, warn message [%s]",
							objectSizeStr, bucket, key, _versionId, fileUrl, warnMessage)
						c.recordWarnMessage(warnMessage, warnLoggerMessage)
					}
				}
			}
		}
	}

	if md5Value == "" {
		md5Value = "n/a"
	}

	cost := (assist.GetUtcNow().UnixNano() - start.UnixNano()) / 1000000

	if batchFlag >= 1 {
		if downloadFileError == nil {
			c.succeedLogger.doRecord("%s, %s, obs://%s/%s%s --> %s, cost [%d], status [%d], success message [download succeed], request id [%s]", objectSizeStr, md5Value, bucket, key, _versionId, fileUrl, cost, status, requestId)
		} else {
			status, code, message, requestId := c.checkAbort(downloadFileError, 401, 405)
			c.failedLogger.doRecord("%s, obs://%s/%s%s --> %s, cost [%d], status [%d], error code [%s], error message [%s], request id [%s]", objectSizeStr, bucket, key, _versionId, fileUrl, cost,
				status, code, message, requestId)
		}
	}

	if batchFlag == 2 {
		if addCostFlag {
			c.ensureMaxCostAndMinCost(cost)
		}
		atomic.AddInt64(&c.totalCost, cost)
	} else {
		if downloadFileError == nil {
			printf("\nDownload successfully, %s, %s, obs://%s/%s%s --> %s, cost [%d], status [%d], request id [%s]", objectSizeStr, md5Value, bucket, key, _versionId, fileUrl, cost, status, requestId)
			doLog(LEVEL_DEBUG, "Download successfully, %s, %s, obs://%s/%s%s --> %s, cost [%d], status [%d], request id [%s]", objectSizeStr, md5Value, bucket, key, _versionId, fileUrl, cost, status, requestId)
		} else {
			logError(downloadFileError, LEVEL_INFO, fmt.Sprintf("\nDownload failed, obs://%s/%s%s --> %s, cost [%d]", bucket, key, _versionId, fileUrl, cost))
		}
	}
	if downloadFileError == nil {
		return 1
	}
	return 0
}

func (c *transferCommand) submitDownloadTask(bucket, dir, folder, relativePrefix string, barCh progress.SingleBarChan,
	limiter *ratelimit.RateLimiter, pool concurrent.Pool) (totalBytes int64, totalBytesForProgress int64, totalObjects int64, hasListError error) {
	input := &obs.ListObjectsInput{}
	input.Bucket = bucket
	input.Prefix = dir
	input.MaxKeys = defaultListMaxKeys
	for {
		start := assist.GetUtcNow()
		output, err := obsClient.ListObjects(input)
		if err != nil {
			hasListError = err
			break
		} else {
			cost := (assist.GetUtcNow().UnixNano() - start.UnixNano()) / 1000000
			doLog(LEVEL_INFO, "List objects from bucket [%s] to download successfully, cost [%d], request id [%s]", bucket, cost, output.RequestId)
		}

		for _, content := range output.Contents {
			key := content.Key
			fileName := key
			if relativePrefix != "" {
				if index := strings.Index(fileName, relativePrefix); index >= 0 {
					fileName = fileName[len(relativePrefix):]
				}
			}

			var fastFailed error = nil
			if checkEmptyFolder(bucket, key, dm) {
				fastFailed = fmt.Errorf("Cannot download the specified key [%s] in the bucket [%s]", key, bucket)
			}

			fileUrl := assist.NormalizeFilePath(folder + "/" + fileName)
			fileStat, _ := os.Stat(fileUrl)
			if isObsFolder(key) {

				atomic.AddInt64(&totalBytesForProgress, 1)
				atomic.AddInt64(&totalObjects, 1)

				metaContext := &MetaContext{
					ETag:         content.ETag,
					LastModified: content.LastModified,
					Size:         0,
				}
				pool.ExecuteFunc(func() interface{} {
					return c.handleExecResultTransAction(c.downloadFileWithMetaContext(bucket, key, "",
						metaContext, nil, fileUrl, fileStat, barCh, limiter, 2, fastFailed), 0, false)
				})
			} else {
				if c.matchExclude(key) {
					continue
				}

				if !c.matchInclude(key) {
					continue
				}

				if !c.matchLastModifiedTime(content.LastModified) {
					continue
				}

				if !c.force && !confirm(fmt.Sprintf("Do you want download key [%s] in the bucket [%s] to [%s] ? Please input (y/n) to confirm:", key, bucket, fileUrl)) {
					continue
				}

				metaContext := &MetaContext{
					ETag:         content.ETag,
					LastModified: content.LastModified,
					Size:         content.Size,
				}
				atomic.AddInt64(&totalBytes, metaContext.Size)
				atomic.AddInt64(&totalObjects, 1)
				if metaContext.Size == 0 {
					atomic.AddInt64(&totalBytesForProgress, 1)
				} else {
					atomic.AddInt64(&totalBytesForProgress, metaContext.Size)
				}
				pool.ExecuteFunc(func() interface{} {
					return c.handleExecResult(c.downloadFileWithMetaContext(bucket, key, "", metaContext, nil, fileUrl, fileStat, barCh, limiter, 2, fastFailed), metaContext.Size)
				})
			}

		}

		if !output.IsTruncated {
			doLog(LEVEL_INFO, "List objects to download finished, bucket [%s], prefix [%s], marker [%s]", bucket, input.Prefix, input.Marker)
			break
		}
		input.Marker = output.NextMarker
	}

	return
}

func (c *transferCommand) recordStartFuncForDownload() time.Time {
	start := c.recordStart()
	c.succeedLogger.doRecord("[%s, %s, %s, %s, %s, %s, %s]", "object size", "md5 value", "src --> dst", "cost(ms)", "status code", "success message", "request id")
	c.failedLogger.doRecord("[%s, %s, %s, %s, %s, %s, %s]", "object size", "src --> dst", "cost(ms)", "status code", "error code", "error message", "request id")
	c.warningLogger.doRecord("[%s, %s, %s]", "object size", "src --> dst", "warn message")
	return start
}

func (c *transferCommand) downloadDir(bucket, dir, folder string, folderStat os.FileInfo) error {

	start := c.recordStartFuncForDownload()
	poolCacheCount := assist.StringToInt(config["defaultJobsCacheCount"], defaultJobsCacheCount)
	pool := concurrent.NewRoutinePool(c.jobs, poolCacheCount)

	barCh := newSingleBarChan()
	barCh.SetBytes(true)
	barCh.SetTemplate(progress.TpsAndSpeed)
	if c.force {
		barCh.Start()
	}

	limiter := c.createRateLimiter()

	var relativePrefix string
	if c.flat {
		if dir != "" && !isObsFolder(dir) {
			dir += "/"
		}
		relativePrefix = dir
	} else {
		relativePrefix = dir
		if isObsFolder(relativePrefix) {
			relativePrefix = relativePrefix[:len(relativePrefix)-1]
		}
		if index := strings.LastIndex(relativePrefix, "/"); index >= 0 {
			relativePrefix = relativePrefix[:index+1]
		} else {
			relativePrefix = ""
		}
	}

	totalBytes, totalBytesForProgress, totalObjects, hasListError := c.submitDownloadTask(bucket, dir, folder, relativePrefix, barCh, limiter, pool)

	doLog(LEVEL_INFO, "Number of objects to download [%d], total size to download [%d(B)]", totalObjects, totalBytes)
	progress.SetTotalCount(totalObjects)
	progress.SetTotalStream(totalBytes)
	barCh.SetTotalCount(totalBytesForProgress)
	if !c.force {
		barCh.Start()
	}

	pool.ShutDown()
	barCh.WaitToFinished()
	c.recordEndWithMetricsV2(start, totalObjects, progress.GetSucceedStream(), progress.GetTotalStream())
	if hasListError != nil {
		logError(hasListError, LEVEL_ERROR, fmt.Sprintf("List objects from bucket [%s] to download failed", bucket))
		return assist.UncompeletedError
	}
	if progress.GetFailedCount() > 0 {
		return assist.UncompeletedError
	}
	return nil
}

func (c *transferCommand) downloadFile(bucket, key, versionId, fileUrl string, fileStat os.FileInfo, barCh progress.SingleBarChan, limiter *ratelimit.RateLimiter, batchFlag int, fastFailed error) int {
	metaContext, metaErr := getObjectMetadata(bucket, key, versionId)
	return c.downloadFileWithMetaContext(bucket, key, versionId, metaContext, metaErr, fileUrl, fileStat, barCh, limiter, batchFlag, fastFailed)
}
