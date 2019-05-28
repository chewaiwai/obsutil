package command

import (
	"archive/zip"
	"assist"
	"command/i18n"
	"concurrent"
	"obs"
	"os"
	"path/filepath"
	"progress"
	"strings"
	"sync/atomic"
)

type archiveCommand struct {
	cloudUrlCommand
}

type logRecord struct {
	logType     string
	logDir      string
	filePattern string
}

func (c *archiveCommand) getLogRecords() []logRecord {
	logRecords := make([]logRecord, 0, 2)
	helper := assist.MapHelper(config)

	getLogDirFunc := func(key string) {
		logPath := helper.Get(key)
		if logPath == "" {
			return
		}

		_logPath := assist.NormalizeFilePath(logPath)

		logDir := filepath.Dir(_logPath)
		if stat, statErr := os.Lstat(logDir); statErr == nil && stat.IsDir() {
			logRecord := logRecord{
				logType:     key,
				logDir:      logDir,
				filePattern: filepath.Base(_logPath),
			}
			logRecords = append(logRecords, logRecord)
		}
	}

	getLogDirFunc("sdkLogPath")
	getLogDirFunc("utilLogPath")

	return logRecords
}

func (c *archiveCommand) doCompress(filePath string, prefix string,
	zw *zip.Writer, filePattern string, pool concurrent.Pool, ch progress.SingleBarChan, totalCnt *int64) {

	fd, err := os.Open(filePath)
	if err != nil {
		return
	}
	info, _ := fd.Stat()
	if info == nil {
		fd.Close()
		return
	}

	if info.IsDir() {
		defer fd.Close()
		atomic.AddInt64(totalCnt, 1)
		c.doCompressFolder(fd, info, prefix, zw, filePattern, pool, ch, totalCnt)
		ch.Send64(1)
		progress.AddSucceedCount(1)
		progress.AddFinishedCount(1)
		return
	}

	if index := strings.LastIndex(fd.Name(), filePattern); index < 0 {
		fd.Close()
		return
	}

	atomic.AddInt64(totalCnt, 1)

	pool.ExecuteFunc(func() interface{} {
		defer fd.Close()
		_, err = assist.DoCompress(fd, prefix, zw)
		if err != nil {
			progress.AddFailedCount(1)
			return err
		}
		ch.Send64(1)
		progress.AddSucceedCount(1)
		progress.AddFinishedCount(1)
		return nil
	})

}

func (c *archiveCommand) doCompressFolder(fd *os.File, info os.FileInfo, prefix string,
	zw *zip.Writer, filePattern string, pool concurrent.Pool, ch progress.SingleBarChan, totalCnt *int64) {

	if prefix == "" {
		prefix = info.Name()
	} else {
		prefix = prefix + "/" + info.Name()
	}
	subFiles, err := fd.Readdir(-1)
	if err != nil {
		return
	}
	for _, subFile := range subFiles {
		if !subFile.IsDir() {
			if index := strings.LastIndex(subFile.Name(), filePattern); index < 0 {
				continue
			}
		}
		c.doCompress(fd.Name()+"/"+subFile.Name(), prefix, zw, filePattern, pool, ch, totalCnt)
	}

}

func (c *archiveCommand) doArchive(dst string, succeedFunc func(dst string) error) error {
	dst = assist.NormalizeFilePath(dst)
	if !strings.HasSuffix(dst, ".zip") {
		dst += ".zip"
	}

	if err := assist.MkdirAll(filepath.Dir(dst), os.ModePerm); err != nil {
		printError(err)
		return assist.ExecutingError
	}

	logRecords := c.getLogRecords()
	if len(logRecords) == 0 {
		printf("Error: Cannot find any log directory to be archived")
		return assist.TaskNotFoundError
	}
	obs.CloseLog()
	closeLog()

	dstFile, err := assist.OpenFile(dst, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, os.ModePerm)
	if err != nil {
		printError(err)
		return assist.ExecutingError
	}
	defer dstFile.Close()
	zw := zip.NewWriter(dstFile)
	defer zw.Close()

	c.printStart()

	pool := concurrent.NewRoutinePool(1, assist.StringToInt(config["defaultJobsCacheCount"], defaultJobsCacheCount))
	ch := newSingleBarChan()
	ch.SetTemplate(progress.Simple)
	ch.Start()

	var totalCnt int64
	for _, logRecord := range logRecords {
		c.doCompress(logRecord.logDir, "", zw, logRecord.filePattern, pool, ch, &totalCnt)
	}

	ch.SetTotalCount(totalCnt)
	progress.SetTotalCount(totalCnt)
	pool.ShutDown()
	ch.WaitToFinished()

	if progress.GetSucceedCount() == totalCnt {
		printf("Succeed to archive log files to [%s]", dst)
		zw.Close()
		dstFile.Close()
		if succeedFunc != nil {
			return succeedFunc(dst)
		}
		return nil
	}
	printf("Finished to archive log files to [%s], but archive some of log files failed", dst)
	return assist.UncompeletedError
}

func initArchive() command {
	c := &archiveCommand{}
	c.key = "archive"
	c.usage = "[archive_url]"
	c.description = "archive log files to local file system or OBS"
	c.additional = true

	c.emptyArgsAction = func() error {
		dir := getCurrentDir()
		return c.doArchive(dir+"/obsutil_log", nil)
	}

	c.action = func() error {
		cloudUrl, err := c.prepareCloudUrl()
		if err == emptyArgsError {
			return c.emptyArgsAction()
		}

		if err != nil {
			if cloudUrl == "" {
				printError(err)
				return assist.InvalidArgsError
			}

			var ret error
			stat, err := os.Lstat(cloudUrl)
			if err != nil {
				if isObsFolder(cloudUrl) || strings.HasSuffix(cloudUrl, "\\") {
					cloudUrl += "obsutil_log"
				}
				ret = c.doArchive(cloudUrl, nil)
			} else if stat.IsDir() {
				ret = c.doArchive(cloudUrl+"/obsutil_log", nil)
			} else {
				ret = c.doArchive(cloudUrl, nil)
			}
			return ret
		}
		dir := getCurrentDir()
		succeedFunc := func(dst string) error {
			stat, err := os.Lstat(dst)
			if err != nil {
				printError(err)
				return assist.FileNotFoundError
			}
			progress.ResetContext()
			cp := &cpCommand{}
			if cp.prepareOptions() {

				bucket, keyOrDir, err := c.splitCloudUrl(cloudUrl)
				if err != nil {
					printError(err)
					return assist.InvalidArgsError
				}
				key := keyOrDir
				if key == "" || isObsFolder(key) {
					key += stat.Name()
				}

				cp.printParams(false, false, false, false)
				if cp.uploadFile(bucket, key, "/", dst, stat, nil, "", "", nil, nil, 0, nil) == 1 {
					if err := os.Remove(dst); err != nil {
						doLog(LEVEL_WARN, "Archive log files to OBS successfully, but remove the zip package of log files failed, %s", err.Error())
					}
					return nil
				}
				return assist.ExecutingError
			}
			return assist.InvalidArgsError
		}
		return c.doArchive(dir+"/obsutil_log", succeedFunc)
	}

	c.help = func() {
		p := i18n.GetCurrentPrinter()
		p.Printf("Summary:")
		printf("%2s%s", "", p.Sprintf("archive log files to local file system or OBS"))
		printf("")
		p.Printf("Syntax 1:")
		printf("%2s%s", "", "obsutil archive [file_url|folder_url] [-config=xxx]")
		printf("")
		p.Printf("Syntax 2:")
		printf("%2s%s", "", "obsutil archive obs://bucket[/prefix] [-config=xxx]")
		printf("")

		p.Printf("Options:")
		printf("%2s%s", "", "-config=xxx")
		printf("%4s%s", "", p.Sprintf("the path to the custom config file when running this command"))
		printf("")
	}

	return c
}
