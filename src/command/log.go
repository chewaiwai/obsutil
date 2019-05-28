package command

import (
	"assist"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
)

type Level int

const (
	LEVEL_OFF   Level = 500
	LEVEL_ERROR Level = 400
	LEVEL_WARN  Level = 300
	LEVEL_INFO  Level = 200
	LEVEL_DEBUG Level = 100
	LEVEL_TRACE Level = 50
)

var logLevelMap = map[Level]string{
	LEVEL_OFF:   "[OFF]: ",
	LEVEL_ERROR: "[ERROR]: ",
	LEVEL_WARN:  "[WARN]: ",
	LEVEL_INFO:  "[INFO]: ",
	LEVEL_DEBUG: "[DEBUG]: ",
	LEVEL_TRACE: "[TRACE]: ",
}

type recorder interface {
	doRecord(format string, v ...interface{})
	doClose()
}

type NilRecorder struct {
}

func (*NilRecorder) doRecord(format string, v ...interface{}) {

}

func (*NilRecorder) doClose() {

}

var nilRecorder = &NilRecorder{}

type loggerWrapper struct {
	fullPath   string
	level      Level
	maxLogSize int64
	backups    int
	fd         *os.File
	ch         chan string
	wg         sync.WaitGroup
	queue      []string
	logger     *log.Logger
	index      int
	cacheCount int
	closed     int32
}

func (lw *loggerWrapper) doInit() {
	lw.queue = make([]string, 0, lw.cacheCount)
	lw.logger = log.New(lw.fd, "", 0)
	lw.ch = make(chan string, lw.cacheCount)
	lw.wg.Add(1)
	go lw.doWrite()
}

func (lw *loggerWrapper) rotate() {
	stat, err := lw.fd.Stat()
	if err != nil {
		lw.fd.Close()
		panic(err)
	}
	if stat.Size() >= lw.maxLogSize {
		lw.fd.Close()
		if lw.index > lw.backups {
			lw.index = 1
		}
		if err := assist.RenameFile(lw.fullPath, lw.fullPath+"."+assist.IntToString(lw.index)); err != nil {
			panic(err)
		}
		lw.index += 1

		fd, err := assist.OpenFile(lw.fullPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			panic(err)
		}
		lw.fd = fd
		lw.logger.SetOutput(lw.fd)
	}
}

func (lw *loggerWrapper) doFlush() {
	lw.rotate()
	if len(lw.queue) > 0 {
		lw.logger.Println(strings.Join(lw.queue, "\n"))
	}
}

func (lw *loggerWrapper) doClose() {
	if atomic.CompareAndSwapInt32(&lw.closed, 0, 1) {
		close(lw.ch)
		lw.wg.Wait()
	}
}

func (lw *loggerWrapper) doWrite() {
	defer lw.wg.Done()
	for {
		msg, ok := <-lw.ch
		if !ok {
			lw.doFlush()
			lw.fd.Close()
			break
		}
		if len(lw.queue) >= lw.cacheCount {
			lw.doFlush()
			lw.queue = lw.queue[:0]
		}
		lw.queue = append(lw.queue, msg)
	}

}

func (lw *loggerWrapper) doPrint(msg string) {
	defer func() {
		recover()
	}()
	lw.ch <- msg
}

func (lw *loggerWrapper) Printf(format string, v ...interface{}) {
	if atomic.LoadInt32(&lw.closed) == 0 {
		msg := fmt.Sprintf(format, v...)
		lw.doPrint(msg)
	}
}

var fileLogger *loggerWrapper
var lock *sync.RWMutex = new(sync.RWMutex)

func (lw *loggerWrapper) isTraceLogEnabled() bool {
	return lw.level <= LEVEL_TRACE
}

func (lw *loggerWrapper) isDebugLogEnabled() bool {
	return lw.level <= LEVEL_DEBUG
}

func (lw *loggerWrapper) isErrorLogEnabled() bool {
	return lw.level <= LEVEL_ERROR
}

func (lw *loggerWrapper) isWarnLogEnabled() bool {
	return lw.level <= LEVEL_WARN
}

func (lw *loggerWrapper) isInfoLogEnabled() bool {
	return lw.level <= LEVEL_INFO
}

func (lw *loggerWrapper) doLog(level Level, format string, v ...interface{}) {
	if lw.level <= level {
		msg := fmt.Sprintf(format, v...)
		if _, file, line, ok := runtime.Caller(1); ok {
			index := strings.LastIndex(file, "/")
			if index >= 0 {
				file = file[index+1:]
			}
			msg = fmt.Sprintf("%s:%d|%s", file, line, msg)
		}
		lw.Printf("%s %s%s", assist.FormatUtcNow("2006-01-02T15:04:05Z"), logLevelMap[level], msg)
	}
}

func (lw *loggerWrapper) doRecord(format string, v ...interface{}) {
	if len(v) > 0 {
		lw.Printf("%s %s", assist.FormatUtcNow("2006-01-02T15:04:05Z"), fmt.Sprintf(format, v...))
	} else {
		lw.Printf("%s %s", assist.FormatUtcNow("2006-01-02T15:04:05Z"), format)
	}
}

func newLogger(logFullPath string, maxLogSize int64, backups int, level Level, cacheCnt int, suffix string) (*loggerWrapper, error) {
	if fullPath := strings.TrimSpace(logFullPath); fullPath != "" {
		if cacheCnt <= 0 {
			cacheCnt = 50
		}
		_fullPath, err := filepath.Abs(fullPath)
		if err != nil {
			return nil, err
		}

		if !strings.HasSuffix(_fullPath, suffix) {
			_fullPath += suffix
		}

		stat, err := os.Stat(_fullPath)
		if err == nil && stat.IsDir() {
			return nil, fmt.Errorf("logFullPath:[%s] is a directory", _fullPath)
		} else if err := assist.MkdirAll(filepath.Dir(_fullPath), os.ModePerm); err != nil {
			return nil, err
		}

		fd, err := assist.OpenFile(_fullPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			return nil, err
		}

		if stat == nil {
			stat, err = os.Stat(_fullPath)
			if err != nil {
				fd.Close()
				return nil, err
			}
		}

		prefix := stat.Name() + "."
		index := 1
		walkFunc := func(path string, info os.FileInfo, err error) error {
			if err == nil {
				if name := info.Name(); strings.HasPrefix(name, prefix) {
					if i := assist.StringToInt(name[len(prefix):], 0); i >= index {
						index = i + 1
					}
				}
			}
			return nil
		}

		if err = filepath.Walk(filepath.Dir(_fullPath), walkFunc); err != nil {
			fd.Close()
			return nil, err
		}

		lw := &loggerWrapper{fullPath: _fullPath, fd: fd, index: index,
			cacheCount: cacheCnt, maxLogSize: maxLogSize, backups: backups, level: level}
		lw.doInit()
		return lw, nil
	}
	return nil, fmt.Errorf("The logFullPath [%s] is not valid", logFullPath)
}

func reset() {
	if fileLogger != nil {
		fileLogger.doClose()
		fileLogger = nil
	}
}

func initLog(logFullPath string, maxLogSize int64, backups int, level Level, cacheCnt int) (err error) {
	lock.Lock()
	defer lock.Unlock()
	reset()
	fileLogger, err = newLogger(logFullPath, maxLogSize, backups, level, cacheCnt, ".log")
	return
}

func closeLog() {
	lock.Lock()
	defer lock.Unlock()
	reset()
}

func doLog(level Level, format string, v ...interface{}) {
	if fileLogger != nil {
		fileLogger.doLog(level, format, v...)
	}
}
