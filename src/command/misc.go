package command

import (
	"assist"
	"bufio"
	"command/i18n"
	"concurrent"
	"errors"
	"flag"
	"fmt"
	"github.com/satori/go.uuid"
	"obs"
	"os"
	"os/signal"
	"path/filepath"
	"progress"
	"regexp"
	"strings"
	"sync/atomic"
	"time"
)

const (
	kb                         = 1024
	mb                         = 1024 * kb
	gb                         = 1024 * mb
	tb                         = 1024 * gb
	ISO8601_DATE_FORMAT        = "2006-01-02T15:04:05Z"
	timeSuffixDateFormat       = "20060102150405"
	defaultConfigFileName      = ".obsutilconfig"
	defaultOutputDirectory     = ".obsutil_output"
	defaultCheckpointDirectory = ".obsutil_checkpoint"
	defaultTempFileDirectory   = ".obsutil_tempfile"
	defaultLogDirectory        = ".obsutil_log"
	defaultChunkSize           = 8192
	defaultMaxLogSize          = 30 * mb
	defaultRecordMaxLogSize    = 30 * mb
	defaultRecordBackups       = 1024
	defaultLogBackups          = 10
	defaultLogCacheCnt         = 50
	defaultScanCacheCount      = 100
	defaultJobsCacheCount      = 1000000
	defaultParallelsCacheCount = 10000
	defaultMaxConnections      = 1000
	defaultBigfileThreshold    = 10 * mb
	defaultPartSize            = 5 * mb
	defaultJobs                = 5
	defaultParallels           = 5
	defaultBrustRate           = 10
	defaultRateLimitThreshold  = 0
	defaultSdkLogLevel         = "WARN"
	defaultUtilLogLevel        = "INFO"
	defaultConnectTimeout      = 30
	defaultSocketTimeout       = 310
	defaultMaxRetryCount       = 1
	defaultMinCost             = 9999999
	defaultMaxCost             = -1
	emptyString                = ""
	defaultListMaxKeys         = 1000

	readBufferIoSize         = 8192
	minReadBufferIoSize      = 4096
	writeBufferIoSize        = 65536
	defaultEndpoint          = "http://your-endpoint"
	defaultAcessKey          = "*** Provide your Access Key ***"
	defaultSecurityKey       = "*** Provide your Secret Key ***"
	serverBigFileThreshold   = 5 * gb
	serialVerifyMd5Threshold = 1 * tb
	checkSumKey              = "md5chksum"
	OBS_VERSION_UNKNOWN      = "unknown"
)

var cloudUrlRegex = regexp.MustCompile("^obs://[a-z0-9-.]+?$")
var bucketRegex = regexp.MustCompile("^[a-z0-9-.]+?$")
var cleanUpS3Regex = regexp.MustCompile("(?i:(aws|amz|amazon))")
var permRegex = regexp.MustCompile("(?i:-rw.rw.rw.)")
var invalidFileUrlRegex = regexp.MustCompile("/\\s*/")
var cleanUpRegex = regexp.MustCompile("(aws|amz|amazon)")

var cleanUpAkRegex1 = regexp.MustCompile("-i=.*?\\s+")
var cleanUpAkRegex2 = regexp.MustCompile("-i\\s+.*?\\s+")
var cleanUpSkRegex1 = regexp.MustCompile("-k=.*?\\s+")
var cleanUpSkRegex2 = regexp.MustCompile("-k\\s+.*?\\s+")
var cleanUpTokenRegex1 = regexp.MustCompile("-t=.*?\\s+")
var cleanUpTokenRegex2 = regexp.MustCompile("-t\\s+.*?\\s+")

var hmacSaltKey = assist.StringToBytes("obsutil")
var aclEveryOne = "Everyone"

var defaultConfigMap = map[string]interface{}{
	"endpoint":                defaultEndpoint,
	"ak":                      defaultAcessKey,
	"sk":                      defaultSecurityKey,
	"token":                   emptyString,
	"endpointCrr":             defaultEndpoint,
	"akCrr":                   defaultAcessKey,
	"skCrr":                   defaultSecurityKey,
	"tokenCrr":                emptyString,
	"connectTimeout":          defaultConnectTimeout,
	"socketTimeout":           defaultSocketTimeout,
	"maxRetryCount":           defaultMaxRetryCount,
	"maxConnections":          defaultMaxConnections,
	"utilLogPath":             emptyString,
	"utilMaxLogSize":          defaultMaxLogSize,
	"utilLogBackups":          defaultLogBackups,
	"utilLogLevel":            defaultUtilLogLevel,
	"sdkLogPath":              emptyString,
	"sdkMaxLogSize":           defaultMaxLogSize,
	"sdkLogBackups":           defaultLogBackups,
	"sdkLogLevel":             defaultSdkLogLevel,
	"rateLimitThreshold":      defaultRateLimitThreshold,
	"writeBufferIoSize":       writeBufferIoSize,
	"readBufferIoSize":        readBufferIoSize,
	"defaultJobsCacheCount":   defaultJobsCacheCount,
	"defaultBigfileThreshold": defaultBigfileThreshold,
	"defaultPartSize":         defaultPartSize,
	"defaultJobs":             defaultJobs,
	"defaultParallels":        defaultParallels,
	"recordMaxLogSize":        defaultRecordMaxLogSize,
	"recordBackups":           defaultRecordBackups,
	"defaultTempFileDir":      emptyString,
	"showProgressBar":         true,
	"showStartTime":           true,
	"checkSourceChange":       false,
	"skipCheckEmptyFolder":    false,
	"fsyncForDownload":        false,
}

var defaultConfigSlice = []string{
	"endpoint",
	"ak",
	"sk",
	"token",
	"endpointCrr",
	"akCrr",
	"skCrr",
	"tokenCrr",
	"connectTimeout",
	"socketTimeout",
	"maxRetryCount",
	"maxConnections",
	"defaultBigfileThreshold",
	"defaultPartSize",
	"defaultParallels",
	"defaultJobs",
	"defaultJobsCacheCount",
	"rateLimitThreshold",
	"sdkLogBackups",
	"sdkLogLevel",
	"sdkLogPath",
	"sdkMaxLogSize",
	"utilLogBackups",
	"utilLogLevel",
	"utilLogPath",
	"utilMaxLogSize",
	"writeBufferIoSize",
	"readBufferIoSize",
	"recordMaxLogSize",
	"recordBackups",
	"showProgressBar",
	"showStartTime",
	"defaultTempFileDir",
	"checkSourceChange",
	"skipCheckEmptyFolder",
	"fsyncForDownload",
}

const (
	c_private         = "private"
	c_publicRead      = "public-read"
	c_publicReadWrite = "public-read-write"
	c_standard        = "standard"
	c_warm            = "warm"
	c_cold            = "cold"
	c_enabled         = "enabled"
	c_disabled        = "disabled"
	c_unknown         = "unknown"
	c_multiAz         = "multi-az"
	c_expedited       = "expedited"
)

var bucketAclType = map[string]obs.AclType{
	c_private:         obs.AclPrivate,
	c_publicRead:      obs.AclPublicRead,
	c_publicReadWrite: obs.AclPublicReadWrite,
}

var storageClassType = map[string]obs.StorageClassType{
	c_standard: obs.StorageClassStandard,
	c_warm:     obs.StorageClassWarm,
	c_cold:     obs.StorageClassCold,
}

var fsStatusType = map[string]obs.FSStatusType{
	c_enabled:  obs.FSStatusEnabled,
	c_disabled: obs.FSStatusDisabled,
}

var availableZoneType = map[string]obs.AvailableZoneType{
	c_multiAz: obs.AvailableZoneMultiAz,
}

var restoreTierType = map[string]obs.RestoreTierType{
	c_standard:  obs.RestoreTierStandard,
	c_expedited: obs.RestoreTierExpedited,
}

var abortError = errors.New("AbortError")
var skipError = errors.New("SkipError")
var emptyArgsError = errors.New("EmptyArgsError")
var currentDir string

type verifyLengthError struct {
	msg string
}

func (err *verifyLengthError) Error() string {
	return "VerifyLengthError, detail:" + err.msg
}

type verifyMd5Error struct {
	msg string
}

func (err *verifyMd5Error) Error() string {
	return "VerifyMd5Error, detail:" + err.msg
}

type command interface {
	getKey() string
	getUsage() interface{}
	getDescription(p *i18n.PrinterWrapper) string
	getAdditional() bool
	getDefine() func()
	getAction() func() error
	getHelp() func()
	setFlagSet(fs *flag.FlagSet)
	parse(args []string) error
}

type taskRecorder interface {
	getTaskId() string
	printTaskId()
}

type scanContext struct {
	scanPool      concurrent.Pool
	scanError     atomic.Value
	scanErrorFlag int32
}

func (c *scanContext) init() {
	c.scanPool = nil
	c.scanError = atomic.Value{}
	c.scanErrorFlag = 0
}

type defaultCommand struct {
	key         string
	usage       interface{}
	description string
	additional  bool
	define      func()
	action      func() error
	help        func()
	flagSet     *flag.FlagSet
	configUrl   string
}

func (c *defaultCommand) getKey() string {
	return c.key
}

func (c *defaultCommand) getUsage() interface{} {
	return c.usage
}

func (c *defaultCommand) getDescription(p *i18n.PrinterWrapper) string {
	if p == nil {
		return c.description
	}
	return p.Sprintf(c.description)
}

func (c *defaultCommand) getAdditional() bool {
	return c.additional
}

func (c *defaultCommand) getDefine() func() {
	return c.define
}

func (c *defaultCommand) getAction() func() error {
	return c.action
}

func (c *defaultCommand) getHelp() func() {
	return c.help
}

func (c *defaultCommand) setFlagSet(flagSet *flag.FlagSet) {
	c.flagSet = flagSet
}

func (c *defaultCommand) parse(args []string) error {
	c.setFlagSet(initFlagSet())

	if c.getDefine() != nil {
		c.getDefine()()
	}

	c.flagSet.StringVar(&c.configUrl, "config", "", "")

	if err := c.flagSet.Parse(args); err != nil {
		c.showHelp()
		printError(err)
		return assist.InvalidArgsError
	}

	if c.configUrl == "" {
		for index, v := range c.flagSet.Args() {
			v := strings.TrimSpace(v)
			if v == "-config" {
				if len(c.flagSet.Args())-1 <= index {
					printf("Error: The config url is not set correctly!")
					return assist.InvalidArgsError
				}

				configUrl := c.flagSet.Arg(index + 1)

				if strings.HasPrefix(configUrl, "-") {
					printf("Error: The config url is not set correctly!")
					return assist.InvalidArgsError
				}

				c.configUrl = configUrl
				break
			}
			if inx := strings.Index(v, "-config="); inx == 0 {
				c.configUrl = v[len("-config="):]
				break
			}
		}

	}

	if c.getAction() != nil {
		needReload := false

		if c.configUrl != "" {
			// reload config file if set a new config url
			if c.configUrl != configFile {
				needReload = true
				configFile = c.configUrl
			}
		} else if configFile != originConfigFile { // reset and reload config file if not set a new config url
			needReload = true
			configFile = originConfigFile
		}

		// reload config file if config or obsClient is not initialized
		if !needReload && (config == nil || obsClient == nil) {
			needReload = true
		}

		// reload config file if changed
		if !needReload && configFileStat != nil && !configFileStat.IsDir() {
			newStat, _ := os.Stat(configFile)
			if newStat != nil && !newStat.IsDir() && newStat.ModTime() != configFileStat.ModTime() {
				needReload = true
			}
		}

		if needReload {
			doClean()
			var err error
			config, err = readConfigFile()
			if err != nil {
				printError(err)
				return assist.InitializingError
			}
			configFileStat, _ = os.Stat(configFile)
			setCurrentLanguage()
		}

		if createDefaultConfigFile && !c.getAdditional() {
			printf("Warn: Please set ak, sk and endpoint in the configuration file!")
			return assist.InvalidArgsError
		}

		if (config["ak"] == defaultAcessKey || config["sk"] == defaultSecurityKey || config["endpoint"] == defaultEndpoint || config["endpoint"] == "") && !c.getAdditional() {
			printf("Warn: Please set ak, sk and endpoint in the configuration file!")
			return assist.InvalidArgsError
		}

		if needReload && !initClientAndLog() {
			return assist.InitializingError
		}

		return c.getAction()()
	}
	return nil
}

func (c *defaultCommand) showHelp() {
	if c.getHelp() != nil {
		c.getHelp()()
	} else {
		usage()
	}
}

func (c *defaultCommand) printStart() (start time.Time) {
	if config["showStartTime"] == "true" {
		start = assist.GetUtcNow()
		printf("Start at %s\n", start)
	}
	return
}

type cloudUrlCommand struct {
	defaultCommand
	emptyArgsAction    func() error
	additionalValidate func(cloudUrl string) bool
}

func (c *cloudUrlCommand) checkArgs(args []string) error {
	if err := c.flagSet.Parse(args); err != nil {
		c.showHelp()
		return err
	}

	if len(c.flagSet.Args()) >= 1 {
		c.showHelp()
		return fmt.Errorf("Invalid args [%v], please refer to help doc", c.flagSet.Args())
	}
	return nil
}

func (c *cloudUrlCommand) prepareCloudUrl() (cloudUrl string, err error) {
	args := c.flagSet.Args()
	if len(args) <= 0 {
		if c.emptyArgsAction != nil {
			err = emptyArgsError
			return
		}
		c.showHelp()
		err = fmt.Errorf("Invalid args, please refer to help doc")
		return
	}

	cloudUrl = args[0]
	if !strings.HasPrefix(cloudUrl, "obs://") {
		err = fmt.Errorf("cloud_url [%s] is not in well format", cloudUrl)
		return
	}

	if len(cloudUrl[6:]) == 0 {
		err = fmt.Errorf("cloud_url [%s] is not in well format", cloudUrl)
		return
	}

	if c.additionalValidate != nil && !c.additionalValidate(cloudUrl) {
		err = fmt.Errorf("cloud_url [%s] is not in well format", cloudUrl)
		return
	}

	if _err := c.checkArgs(args[1:]); _err != nil {
		err = _err
		return
	}

	return
}

func (c *cloudUrlCommand) splitCloudUrl(cloudUrl string) (bucket string, key string, err error) {
	if len(cloudUrl) < 6 {
		err = fmt.Errorf("cloud_url [%s] is not in well format", cloudUrl)
		return
	}
	_cloudUrl := cloudUrl[6:]
	if index := strings.Index(_cloudUrl, "/"); index > 0 {
		bucket = _cloudUrl[:index]
		key = _cloudUrl[index+1:]
	} else {
		bucket = _cloudUrl
	}

	if bucketLength := len(bucket); bucketLength < 3 || bucketLength > 63 {
		err = fmt.Errorf("bucket [%s] in cloud_url [%s] is not in well format", bucket, cloudUrl)
		return
	}

	if strings.HasPrefix(bucket, "-") || strings.HasPrefix(bucket, ".") ||
		strings.HasSuffix(bucket, "-") || strings.HasSuffix(bucket, ".") {
		err = fmt.Errorf("bucket [%s] in cloud_url [%s] is not in well format", bucket, cloudUrl)
		return
	}

	if strings.Contains(bucket, "..") || strings.Contains(bucket, "-.") || strings.Contains(bucket, ".-") {
		err = fmt.Errorf("bucket [%s] in cloud_url [%s] is not in well format", bucket, cloudUrl)
		return
	}

	if !bucketRegex.MatchString(bucket) {
		err = fmt.Errorf("bucket [%s] in cloud_url [%s] is not in well format", bucket, cloudUrl)
		return
	}

	return
}

func (c *cloudUrlCommand) ensureBucket(bucket string) error {
	if isAnonymousUser() {
		return nil
	}

	return c.ensureBucketByClient(bucket, obsClient)
}

func (c *cloudUrlCommand) ensureBucketByClient(bucket string, client *obs.ObsClient) error {
	if _, err := client.GetBucketQuota(bucket); err != nil {
		if obsError, ok := err.(obs.ObsError); ok {
			if status := obsError.StatusCode; status >= 300 && status < 500 && status != 404 {
				return nil
			}
		}
		return fmt.Errorf("Check the status of bucket [%s] failed, %s", bucket, err.Error())
	}
	return nil
}

func (c *cloudUrlCommand) checkBucketFSStatus(bucket string) (string, error) {
	input := &obs.GetBucketFSStatusInput{}
	input.Bucket = bucket
	if output, err := obsClient.GetBucketFSStatus(input); err != nil {
		if obsError, ok := err.(obs.ObsError); ok {
			if status := obsError.StatusCode; status >= 300 && status < 500 && status != 404 {
				return c_unknown, nil
			}
		}
		return c_unknown, fmt.Errorf("Check the fs status of bucket [%s] failed, %s", bucket, err.Error())
	} else {
		return transFSStatusType(output.FSStatus), nil
	}
}

type reportCommand struct {
	cloudUrlCommand
	succeedReportFile string
	failedReportFile  string
	warningReportFile string
	succeedLogger     recorder
	failedLogger      recorder
	warningLogger     recorder
	taskId            string
	outDir            string
	maxCost           int64
	minCost           int64
	totalCost         int64
	abort             int32
	forceRecord       bool
	autoCleanRecord   bool
}

func (c *reportCommand) getTaskId() string {
	return c.taskId
}

func (c *reportCommand) printTaskId() {
	if c.taskId != "" && (progress.GetSucceedCount()+progress.GetFailedCount()) > 0 {
		printf("\nTask id is: %s", c.taskId)
	}
}

func (c *reportCommand) init() {
	c.minCost = defaultMinCost
	c.maxCost = defaultMaxCost
	c.succeedLogger = nilRecorder
	c.failedLogger = nilRecorder
	c.warningLogger = nilRecorder
	c.totalCost = 0
	c.abort = 0
	c.taskId = ""
	c.succeedReportFile = ""
	c.failedReportFile = ""
	c.warningReportFile = ""
	c.autoCleanRecord = true
}

func (c *reportCommand) checkAbort(err error, abortStatus ...int) (status int, code string, message string, requestId string) {
	status, code, message, requestId = getErrorInfo(err)
	for _, stat := range abortStatus {
		if status == stat {
			atomic.CompareAndSwapInt32(&c.abort, 0, 1)
			return
		}
	}
	return
}

func (c *reportCommand) ensureOutputDirectory() error {
	c.outDir = strings.TrimSpace(c.outDir)
	if c.outDir == "" {
		outDir, err := getOutputDirectory()
		if err != nil {
			return err
		}
		c.outDir = outDir
	}

	stat, err := os.Stat(c.outDir)
	if err == nil && !stat.IsDir() {
		return fmt.Errorf("output directory [%s] is a file!", c.outDir)
	}

	if err = assist.MkdirAll(c.outDir, os.ModePerm); err != nil {
		return err
	}

	taskId, err := uuid.NewV4()
	if err == nil {
		c.taskId = fmt.Sprintf("%s", taskId)
	}

	timeSuffix := assist.FormatUtcNow(timeSuffixDateFormat)
	failedReportFile := fmt.Sprintf("%s/%s_failed_report_%s_%s.txt", c.outDir, c.getKey(), timeSuffix, c.taskId)
	failedfd, err := assist.OpenFile(failedReportFile, os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	defer failedfd.Close()

	succeedReportFile := fmt.Sprintf("%s/%s_succeed_report_%s_%s.txt", c.outDir, c.getKey(), timeSuffix, c.taskId)
	succeedfd, err := assist.OpenFile(succeedReportFile, os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	defer succeedfd.Close()

	warningReportFile := fmt.Sprintf("%s/%s_warning_report_%s_%s.txt", c.outDir, c.getKey(), timeSuffix, c.taskId)
	warningfd, err := assist.OpenFile(warningReportFile, os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	defer warningfd.Close()

	c.succeedReportFile = succeedReportFile
	c.failedReportFile = failedReportFile
	c.warningReportFile = warningReportFile
	return nil
}

func (c *reportCommand) startLogger(warningFlag bool) (err error) {
	recordBackups := assist.StringToInt(config["recordBackups"], defaultRecordBackups)
	var recordMaxLogSize int64
	if _recordMaxLogSize, err := assist.TranslateToInt64(config["recordMaxLogSize"]); err == nil && _recordMaxLogSize > 0 {
		recordMaxLogSize = _recordMaxLogSize
	} else {
		recordMaxLogSize = defaultRecordMaxLogSize
	}
	c.succeedLogger, err = newLogger(c.succeedReportFile, recordMaxLogSize, recordBackups,
		LEVEL_DEBUG, defaultLogCacheCnt, ".txt")

	if err != nil {
		return
	}
	c.failedLogger, err = newLogger(c.failedReportFile, recordMaxLogSize, recordBackups,
		LEVEL_DEBUG, defaultLogCacheCnt, ".txt")

	if err != nil {
		return
	}

	if warningFlag {
		c.warningLogger, err = newLogger(c.warningReportFile, recordMaxLogSize, recordBackups,
			LEVEL_DEBUG, defaultLogCacheCnt, ".txt")
		if err != nil {
			return
		}
	}

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, os.Kill)
	go func() {
		<-ch
		if c.succeedLogger != nil {
			c.succeedLogger.doClose()
		}

		if c.failedLogger != nil {
			c.failedLogger.doClose()
		}

		if c.warningLogger != nil {
			c.warningLogger.doClose()
		}
	}()

	return
}

func (c *reportCommand) endLogger() {
	if c.succeedLogger != nil {
		c.succeedLogger.doClose()
		if c.autoCleanRecord && progress.GetSucceedCount() == 0 {
			if err := os.Remove(c.succeedReportFile); err != nil {
				doLog(LEVEL_WARN, "Delete succeed report file [%s] failed, %s", c.succeedReportFile, err.Error())
			}
		}
	}
	if c.failedLogger != nil {
		c.failedLogger.doClose()
		if c.autoCleanRecord && progress.GetFailedCount() == 0 {
			if err := os.Remove(c.failedReportFile); err != nil {
				doLog(LEVEL_WARN, "Delete failed report file [%s] failed, %s", c.failedReportFile, err.Error())
			}
		}
	}

	if c.warningLogger != nil {
		c.warningLogger.doClose()
		if c.autoCleanRecord && progress.GetWarningCount() == 0 {
			if err := os.Remove(c.warningReportFile); err != nil {
				doLog(LEVEL_WARN, "Delete warning report file [%s] failed, %s", c.warningReportFile, err.Error())
			}
		}
	}

	c.printTaskId()
}

func (c *reportCommand) recordStart() (start time.Time) {
	start = assist.GetUtcNow()
	c.failedLogger.doRecord("Start at %s\n", start)
	c.succeedLogger.doRecord("Start at %s\n", start)

	return
}

func (c *reportCommand) recordEnd(start time.Time) int64 {
	end := assist.GetUtcNow()
	cost := (end.UnixNano() - start.UnixNano()) / 1000000
	c.failedLogger.doRecord("End at %s, cost [%d]", end, cost)
	c.succeedLogger.doRecord("End at %s, cost [%d]", end, cost)
	return cost
}

func (c *reportCommand) recordEndAndCnt(start time.Time) int64 {
	c.recordCnt()
	return c.recordEnd(start)
}

func (c *reportCommand) recordEndAndCntV2(start time.Time, succeedStream, totalStream int64) int64 {
	c.recordCntV2()

	if succeedStream >= 0 && totalStream >= 0 {
		c.succeedLogger.doRecord("Succeed bytes is: %s/%s", assist.NormalizeBytes(succeedStream), assist.NormalizeBytes(totalStream))
		printf("%-20s%-10s", "Succeed bytes is:", assist.NormalizeBytes(succeedStream))
	}

	return c.recordEnd(start)
}

func (c *reportCommand) recordEndWithMetricsV2(start time.Time, totalObjects, succeedStream, totalStream int64) int64 {
	cost := c.recordEndAndCntV2(start, succeedStream, totalStream)

	if totalObjects > 0 {
		var averageCost float64
		if totalObjects > 0 {
			averageCost = float64(c.totalCost) / float64(totalObjects)
		}
		var averageTps float64
		if cost > 0 {
			averageTps = float64(progress.GetTransaction()) / float64(cost) * 1000
		}

		maxCost := "n/a"
		if c.maxCost != -1 {
			maxCost = assist.Int64ToString(c.maxCost) + " ms"
		}

		minCost := "n/a"
		if c.minCost != 9999999 {
			minCost = assist.Int64ToString(c.minCost) + " ms"
		}
		printf("Metrics [max cost:%s, min cost:%s, average cost:%.2f ms, average tps:%.2f, uploaded size :%s]",
			maxCost, minCost, averageCost, averageTps, assist.NormalizeBytes(succeedStream))
	}

	return cost
}

func (c *reportCommand) recordEndWithMetrics(start time.Time, totalObjects int64) int64 {
	cost := c.recordEndAndCnt(start)
	if totalObjects > 0 {
		var averageCost float64
		if totalObjects > 0 {
			averageCost = float64(c.totalCost) / float64(totalObjects)
		}
		var averageTps float64
		if cost > 0 {
			averageTps = float64(totalObjects) / float64(cost) * 1000
		}

		maxCost := "n/a"
		if c.maxCost != defaultMaxCost {
			maxCost = assist.Int64ToString(c.maxCost) + " ms"
		}

		minCost := "n/a"
		if c.minCost != defaultMinCost {
			minCost = assist.Int64ToString(c.minCost) + " ms"
		}

		printf("Metrics [max cost:%s, min cost:%s, average cost:%.2f ms, average tps:%.2f]",
			maxCost, minCost, averageCost, averageTps)
	}

	return cost
}

func (c *reportCommand) recordCnt() {
	succeedCnt := progress.GetSucceedCount()
	failedCnt := progress.GetFailedCount()

	if succeedCnt+failedCnt == 0 {
		printf("Warn: No task to run")
		return
	}

	c.failedLogger.doRecord("Failed count is: %d/%d", failedCnt, failedCnt+succeedCnt)
	c.succeedLogger.doRecord("Succeed count is: %d/%d", succeedCnt, failedCnt+succeedCnt)
	printf("%-20s%-10d%-20s%-10d", "Succeed count is: ", succeedCnt, "Failed count is: ", failedCnt)
	warningCount := progress.GetWarningCount()
	if warningCount > 0 {
		printf("%-20s%-10d", "Warning count is: ", warningCount)
	}
}

func (c *reportCommand) recordCntV2() {
	succeedCnt := progress.GetSucceedCount()
	failedCnt := progress.GetFailedCount()

	if succeedCnt+failedCnt == 0 {
		printf("Warn: No task to run")
		return
	}

	resumeCnt := progress.GetResumeCount()
	c.failedLogger.doRecord("Failed count is: %d/%d", failedCnt, failedCnt+succeedCnt)
	c.succeedLogger.doRecord("Succeed count is: %d/%d", succeedCnt, failedCnt+succeedCnt)
	if resumeCnt > 0 {
		printf("%-20s%-10d%-20s%-10d%-20s%-10d", "Succeed count is: ", succeedCnt, "Failed count is: ", failedCnt, "Skip count is:", resumeCnt)
	} else {
		printf("%-20s%-10d%-20s%-10d", "Succeed count is: ", succeedCnt, "Failed count is: ", failedCnt)
	}
	warningCount := progress.GetWarningCount()
	if warningCount > 0 {
		c.warningLogger.doRecord("Warning count is: %d", warningCount)
		printf("%-20s%-10d", "Warning count is: ", warningCount)
	}
}

func (c *reportCommand) ensureMaxCostAndMinCost(cost int64) {
	for {
		if old := atomic.LoadInt64(&c.maxCost); old >= cost || atomic.CompareAndSwapInt64(&c.maxCost, old, cost) {
			break
		}
	}

	for {
		if old := atomic.LoadInt64(&c.minCost); old <= cost || atomic.CompareAndSwapInt64(&c.minCost, old, cost) {
			break
		}
	}
}

func (c *reportCommand) simpleAction(batchFlag int, abortHandler func(),
	actionFunc func() (output *obs.BaseModel, err error), recordHandler func(cost int64, output *obs.BaseModel, err error), printHandler func(cost int64, output *obs.BaseModel, err error)) bool {
	if batchFlag == 2 && atomic.LoadInt32(&c.abort) == 1 {
		abortHandler()
		return false
	}

	start := assist.GetUtcNow()
	output, err := actionFunc()

	cost := (assist.GetUtcNow().UnixNano() - start.UnixNano()) / 1000000
	if batchFlag >= 1 {
		recordHandler(cost, output, err)
	}

	if batchFlag == 2 {
		c.ensureMaxCostAndMinCost(cost)
		atomic.AddInt64(&c.totalCost, cost)
	} else {
		printHandler(cost, output, err)
	}

	return err == nil
}

type recursiveCommand struct {
	reportCommand
	scanContext
	recursive         bool
	force             bool
	jobs              int
	bucketsVersionMap map[string]string
	include           string
	exclude           string
	timeRange         string

	//need to be reset in init func
	includeRegex *regexp.Regexp
	excludeRegex *regexp.Regexp
	gt           int64
	lt           int64
}

func (c *recursiveCommand) init() {
	c.reportCommand.init()
	c.scanContext.init()
	c.includeRegex = nil
	c.excludeRegex = nil
	c.gt = 0
	c.lt = 0
}

func (c *recursiveCommand) checkExclude() bool {
	if c.exclude == "" {
		return true
	}
	if re, err := assist.CompileWildcardInput(c.exclude); err != nil {
		printf("Error: The exclude pattern [%s] is not well-formed, %s", c.exclude, err.Error())
		return false
	} else {
		c.excludeRegex = re
		return true
	}
}

func (c *recursiveCommand) checkTimeRange() bool {
	if c.timeRange == "" {
		return true
	}
	timePair := strings.Split(c.timeRange, "-")
	length := len(timePair)
	if length != 2 {
		printf("Error: The timeRange pattern [%s] is not well-formed, must be time1-time2", c.timeRange)
		return false
	}

	if ts, err := assist.Str2Timestamp(timePair[0], 0); err != nil {
		printf("Error: The timeRange pattern [%s] is not well-formed, %s", c.timeRange, err.Error())
		return false
	} else {
		c.gt = ts
	}
	if ts, err := assist.Str2Timestamp(timePair[1], 1<<63-1); err != nil {
		printf("Error: The timeRange pattern [%s] is not well-formed, %s", c.timeRange, err.Error())
		return false
	} else {
		c.lt = ts
	}
	if c.gt > c.lt {
		printf("Error: The timeRange pattern [%s] is not well-formed, start time greater than end time.", c.timeRange)
		return false
	}
	return true
}

func (c *recursiveCommand) checkInclude() bool {
	if c.include == "" {
		return true
	}
	if re, err := assist.CompileWildcardInput(c.include); err != nil {
		printf("Error: The include pattern [%s] is not well-formed, %s", c.include, err.Error())
		return false
	} else {
		c.includeRegex = re
		return true
	}
}

func (c *recursiveCommand) matchExclude(fileName string) bool {
	if c.excludeRegex == nil {
		return false
	}
	return c.excludeRegex.MatchString(fileName)
}

func (c *recursiveCommand) matchInclude(fileName string) bool {
	if c.includeRegex == nil {
		return true
	}
	return c.includeRegex.MatchString(fileName)
}

func (c *recursiveCommand) matchLastModifiedTime(mt time.Time) (match bool) {
	if c.timeRange == "" {
		return true
	}
	match = mt.UTC().Unix() >= c.gt && mt.UTC().Unix() <= c.lt
	return
}

func (c *recursiveCommand) checkBucketVersion(bucket string) string {
	if c.bucketsVersionMap == nil {
		c.bucketsVersionMap = make(map[string]string)
	}

	if obsVersion, ok := c.bucketsVersionMap[bucket]; ok {
		return obsVersion
	}
	input := &obs.GetBucketMetadataInput{}
	input.Bucket = bucket
	output, err := obsClient.GetBucketMetadata(input)
	obsVersion := OBS_VERSION_UNKNOWN
	if err != nil {
		doLog(LEVEL_WARN, "Check the status of bucket [%s] failed, set bucket version to [%v], %s", bucket, OBS_VERSION_UNKNOWN, err.Error())
	} else {
		obsVersion = output.ObsVersion
	}
	c.bucketsVersionMap[bucket] = obsVersion
	return obsVersion
}

func (c *recursiveCommand) submitListObjectsTask(bucket, prefix, action string,
	pool concurrent.Pool, ch progress.SingleBarChan, actionFunc func(bucket, key string) bool, isSkipFunc func(content obs.Content) bool) (totalCnt int64, hasListError error) {
	input := &obs.ListObjectsInput{}
	input.Bucket = bucket
	input.Prefix = prefix
	input.MaxKeys = defaultListMaxKeys
	if isSkipFunc == nil {
		isSkipFunc = func(content obs.Content) bool {
			return false
		}
	}
	for {
		start := assist.GetUtcNow()
		output, err := obsClient.ListObjects(input)
		if err != nil {
			hasListError = err
			break
		} else {
			cost := (assist.GetUtcNow().UnixNano() - start.UnixNano()) / 1000000
			doLog(LEVEL_DEBUG, "List objects in the bucket [%s] to %s successfully, cost [%d], request id [%s]", bucket, action, cost, output.RequestId)
		}
		for _, content := range output.Contents {
			if isSkipFunc(content) {
				continue
			}

			key := content.Key
			if !c.force && !confirm(fmt.Sprintf("Do you want %s object [%s] ? Please input (y/n) to confirm:", action, key)) {
				continue
			}
			atomic.AddInt64(&totalCnt, 1)
			pool.ExecuteFunc(func() interface{} {
				return handleResult(actionFunc(bucket, key), ch)
			})
		}

		if !output.IsTruncated {
			doLog(LEVEL_INFO, "List objects to %s finished, bucket [%s], prefix [%s], marker [%s]", action, bucket, input.Prefix, input.Marker)
			break
		}
		input.Marker = output.NextMarker
	}
	return
}

func (c *recursiveCommand) submitListVersionsTask(bucket, prefix, action string,
	pool concurrent.Pool, ch progress.SingleBarChan, actionFunc func(bucket, key, versionId string) bool,
	isSkipFunc func(version obs.Version) bool, isSkipFunc2 func(deleteMarker obs.DeleteMarker) bool, withDeleteMarker bool) (totalCnt int64, hasListError error) {
	input := &obs.ListVersionsInput{}
	input.Bucket = bucket
	input.Prefix = prefix
	input.MaxKeys = defaultListMaxKeys
	if isSkipFunc == nil {
		isSkipFunc = func(version obs.Version) bool {
			return false
		}
	}
	if isSkipFunc2 == nil {
		isSkipFunc2 = func(deleteMarker obs.DeleteMarker) bool {
			return false
		}
	}
	for {
		start := assist.GetUtcNow()
		output, err := obsClient.ListVersions(input)
		if err != nil {
			hasListError = err
			break
		} else {
			cost := (assist.GetUtcNow().UnixNano() - start.UnixNano()) / 1000000
			doLog(LEVEL_INFO, "List versioning objects in the bucket [%s] to %s successfully, cost [%d], request id [%s]", bucket, action, cost, output.RequestId)
		}
		for _, version := range output.Versions {
			if isSkipFunc(version) {
				continue
			}

			key := version.Key
			versionId := version.VersionId
			if !c.force && !confirm(fmt.Sprintf("Do you want %s object [%s] with version id [%s] ? Please input (y/n) to confirm:", action, key, versionId)) {
				continue
			}
			atomic.AddInt64(&totalCnt, 1)
			pool.ExecuteFunc(func() interface{} {
				return handleResult(actionFunc(bucket, key, versionId), ch)
			})

		}

		if withDeleteMarker {
			for _, deleteMarker := range output.DeleteMarkers {
				if isSkipFunc2(deleteMarker) {
					continue
				}

				key := deleteMarker.Key
				versionId := deleteMarker.VersionId
				if !c.force && !confirm(fmt.Sprintf("Do you want %s object [%s] with version id [%s] ? Please input (y/n) to confirm:", action, key, versionId)) {
					continue
				}
				atomic.AddInt64(&totalCnt, 1)
				pool.ExecuteFunc(func() interface{} {
					return handleResult(actionFunc(bucket, key, versionId), ch)
				})

			}
		}

		if !output.IsTruncated {
			doLog(LEVEL_INFO, "List versioning objects to %s finished, bucket [%s], prefix [%s], marker [%s], versionIdMarker [%s]", action, bucket, input.Prefix, input.KeyMarker, input.VersionIdMarker)
			break
		}

		input.KeyMarker = output.NextKeyMarker
		input.VersionIdMarker = output.NextVersionIdMarker
	}
	return
}

func (c *recursiveCommand) recursiveAction(bucket, prefix string,
	submitFunc func(pool concurrent.Pool, ch progress.SingleBarChan) (int64, error),
	errorHandler func(hasListError error), recordStartFunc func() time.Time, withMetrics bool) error {
	start := recordStartFunc()

	if c.jobs <= 0 {
		c.jobs = assist.MaxInt(assist.StringToInt(config["defaultJobs"], defaultJobs), 1)
	}
	poolCacheCount := assist.StringToInt(config["defaultJobsCacheCount"], defaultJobsCacheCount)

	pool := concurrent.NewRoutinePool(c.jobs, poolCacheCount)

	ch := newSingleBarChan()
	ch.SetTemplate(progress.TpsOnly)
	if c.force {
		ch.Start()
	}

	totalCnt, hasListError := submitFunc(pool, ch)

	doLog(LEVEL_INFO, "Total number is [%d]", totalCnt)
	progress.SetTotalCount(totalCnt)
	ch.SetTotalCount(totalCnt)
	if !c.force {
		ch.Start()
	}

	pool.ShutDown()
	ch.WaitToFinished()
	if withMetrics {
		c.recordEndWithMetrics(start, totalCnt)
	} else {
		c.recordEndAndCnt(start)
	}
	if hasListError != nil {
		errorHandler(hasListError)
		return assist.UncompeletedError
	}

	if progress.GetFailedCount() > 0 {
		return assist.UncompeletedError
	}
	return nil
}

func (c *recursiveCommand) chooseAction(checkParamFunc func() bool, emptyPrefixFunc func(bucket string) error, confirmFunc func(bucket, prefix string) bool,
	prefixFunc func(bucket, prefix string, batchFlag int) error, recursivePrefixFunc func(bucket, prefix string) error, recordStartFunc func() time.Time) error {
	cloudUrl, err := c.prepareCloudUrl()
	if err != nil {
		printError(err)
		return assist.InvalidArgsError
	}

	bucket, prefix, err := c.splitCloudUrl(cloudUrl)
	if err != nil {
		printError(err)
		return assist.InvalidArgsError
	}

	if checkParamFunc != nil && !checkParamFunc() {
		return assist.InvalidArgsError
	}

	c.printStart()
	if !c.recursive {
		if prefix == "" && emptyPrefixFunc != nil {
			return emptyPrefixFunc(bucket)
		}

		if confirmFunc != nil && !c.force {
			if !confirmFunc(bucket, prefix) {
				return nil
			}
		}

		var ret error

		if c.forceRecord {
			if err := c.ensureBucket(bucket); err != nil {
				printError(err)
				doLog(LEVEL_ERROR, err.Error())
				return assist.CheckBucketStatusError
			}
			if err := c.ensureOutputDirectory(); err != nil {
				printError(err)
				return assist.InitializingError
			}

			if err := c.startLogger(false); err != nil {
				printError(err)
				return assist.InitializingError
			}

			start := recordStartFunc()
			defer c.endLogger()
			ret = prefixFunc(bucket, prefix, 1)
			if ret == nil {
				progress.AddSucceedCount(1)
			} else {
				progress.AddFailedCount(1)
			}
			c.recordEnd(start)
		} else {
			ret = prefixFunc(bucket, prefix, 0)
		}

		if ret != nil {
			return ret
		}

		return nil
	}

	if err := c.ensureBucket(bucket); err != nil {
		printError(err)
		doLog(LEVEL_ERROR, err.Error())
		return assist.CheckBucketStatusError
	}

	if err := c.ensureOutputDirectory(); err != nil {
		printError(err)
		return assist.InitializingError
	}

	if err := c.startLogger(false); err != nil {
		printError(err)
		return assist.InitializingError
	}

	defer c.endLogger()

	return recursivePrefixFunc(bucket, prefix)
}

func handleResult(ret bool, ch progress.SingleBarChan) bool {
	progress.AddFinishedCount(1)
	progress.AddTransaction(1)
	if ret {
		ch.Send64(1)
		progress.AddSucceedCount(1)
	} else {
		progress.AddFailedCount(1)
	}
	return ret
}

func getCurrentDir() string {
	if currentDir == "" {
		currentDir = assist.GetOsPath(os.Args[0])
		currentDir, _ = filepath.Abs(filepath.Dir(currentDir))
	}
	return currentDir
}

func getAclType(acl string) (obs.AclType, bool) {
	if acl != "" {
		if aclType, ok := bucketAclType[acl]; !ok {
			printf("Error: Invalid acl [%s], possible values are:[%s|%s|%s]", acl, c_private, c_publicRead, c_publicReadWrite)
			return "", false
		} else {
			return aclType, true
		}
	}

	return "", true
}

func getStorageClassType(sc string) (obs.StorageClassType, bool) {
	if sc != "" {
		if scType, ok := storageClassType[sc]; !ok {
			printf("Error: Invalid sc [%s], possible values are:[%s|%s|%s]", sc, c_standard, c_warm, c_cold)
			return "", false
		} else {
			return scType, true
		}
	}
	return "", true
}

func getAvailableZoneType(az string) (obs.AvailableZoneType, bool) {
	if az != "" {
		if azType, ok := availableZoneType[az]; !ok {
			printf("Error: Invalid az [%s], possible values are:[%s]", az, c_multiAz)
			return "", false
		} else {
			return azType, true
		}
	}
	return "", true
}

func transAclType(acl obs.AclType) string {
	for k, v := range bucketAclType {
		if v == acl {
			return k
		}
	}
	return ""
}

func transStorageClassType(storageClass obs.StorageClassType) string {
	for k, v := range storageClassType {
		if v == storageClass {
			return k
		}
	}
	return c_standard
}

func transFSStatusType(status obs.FSStatusType) string {
	for k, v := range fsStatusType {
		if v == status {
			return k
		}
	}
	return c_disabled
}

func transAvailableZoneType(availableZone obs.AvailableZoneType) string {
	for k, v := range availableZoneType {
		if v == availableZone {
			return k
		}
	}
	return ""
}

func transLogLevel(level string) int {
	level = strings.TrimSpace(level)
	if level == "ERROR" {
		return 400
	} else if level == "WARN" {
		return 300
	} else if level == "INFO" {
		return 200
	} else if level == "DEBUG" {
		return 100
	}
	return 500
}

func getErrorInfo(err error) (status int, code string, message string, requestId string) {
	if obsError, ok := err.(obs.ObsError); ok {
		status = obsError.StatusCode
		code = obsError.Code
		message = obsError.Message
		requestId = obsError.RequestId
	} else {
		message = err.Error()
	}
	return
}

func cleanUpMessage(message string) string {
	return cleanUpS3Regex.ReplaceAllString(message, "")
}

func doLogError(err error, level Level, msg string) (requestId string, ret string) {
	if obsError, ok := err.(obs.ObsError); ok {
		msg += fmt.Sprintf(", status [%d], error code [%s], error message [%s], request id [%s]", obsError.StatusCode,
			obsError.Code, cleanUpMessage(obsError.Message), obsError.RequestId)
		requestId = obsError.RequestId
	} else if err != nil {
		msg += fmt.Sprintf(", error [%s]", err.Error())
	}
	ret = msg
	doLog(level, msg)
	return
}

func logError(err error, level Level, msg string) string {
	requestId, ret := doLogError(err, level, msg)
	printf(ret)
	return requestId
}

func printError(err error) {
	if err == skipError {
		return
	}

	if obsError, ok := err.(obs.ObsError); ok {
		printf("Error: Status [%d], error code [%s], error message [%s], request id [%s]", obsError.StatusCode, obsError.Code, cleanUpMessage(obsError.Message),
			obsError.RequestId)
	} else if err != nil {
		printf("Error: %s", err.Error())
	}
}

func printWarn(err error) {
	printf("Warn: %s", err.Error())
}

type parser interface {
	parse(args []string)
	showHelp()
}

type RWLocker interface {
	Lock()
	Unlock()
	RLock()
	RUnlock()
}

type NilLocker struct {
}

func (*NilLocker) Lock() {

}
func (*NilLocker) Unlock() {

}
func (*NilLocker) RLock() {

}
func (*NilLocker) RUnlock() {

}

var nilLocker = &NilLocker{}

func printf(format string, a ...interface{}) {
	if len(a) > 0 {
		fmt.Printf(format+"\n", a...)
		return
	}
	fmt.Println(format)
}

func getUserInput(notice string) (string, error) {
	inputReader := bufio.NewReader(os.Stdin)
	printf(notice)
	input, err := assist.ReadLine(inputReader)
	if err == nil {
		return assist.BytesToString(input), nil
	}
	return "", err
}

func confirm(notice string) bool {
	input, err := getUserInput(notice)
	if err == nil {
		if _input := strings.TrimSpace(input); input == "" || strings.ToLower(_input) == "y" {
			return true
		}
	}
	return false
}

func md5File(fileUrl string) ([]byte, error) {
	doLog(LEVEL_INFO, "Start to caculate md5 for [%s]", fileUrl)
	start := assist.GetUtcNow()
	md5, err := assist.Md5File(fileUrl)
	doLog(LEVEL_INFO, "End to caculate md5 for [%s], cost", fileUrl, (assist.GetUtcNow().UnixNano()-start.UnixNano())/1000000)
	return md5, err
}

type MetaContext struct {
	Size                    int64
	ETag                    string
	LastModified            time.Time
	RequestId               string
	ContentType             string
	StorageClass            obs.StorageClassType
	WebsiteRedirectLocation string
	Metadata                map[string]string
}

func getObjectMetadata(bucket, key, versionId string) (*MetaContext, error) {
	return getObjectMetadataByClient(bucket, key, versionId, obsClient)
}

func getObjectMetadataByClient(bucket, key, versionId string, client *obs.ObsClient) (*MetaContext, error) {
	input := &obs.GetObjectMetadataInput{}
	input.Bucket = bucket
	input.Key = key
	input.VersionId = versionId
	output, err := client.GetObjectMetadata(input)
	if err == nil {
		return &MetaContext{
			Size:         output.ContentLength,
			LastModified: output.LastModified,
			ETag:         output.ETag,
			RequestId:    output.RequestId,
			Metadata:     output.Metadata,
		}, nil
	}

	return nil, err
}

func compareETag(hexMd5 string, etag string) bool {
	return "\""+hexMd5+"\"" == etag
}

func newSingleBarChan() progress.SingleBarChan {
	if config["showProgressBar"] == "true" {
		return progress.NewSingleBarChan()
	}
	return progress.NewNilSingleBarChan()
}

func checkEmptyFolder(bucket, key string, mode cpMode) bool {
	if config["skipCheckEmptyFolder"] == "true" {
		return false
	}

	if mode == um || mode == cm {
		return false
	}

	matchTarget := key

	if bucket != "" {
		matchTarget = bucket + "/" + matchTarget
	}
	return invalidFileUrlRegex.MatchString(matchTarget)
}

func getCheckpointDirectory() (string, error) {
	home, err := assist.Home()
	if err != nil {
		return "", err
	}
	return home + "/" + defaultCheckpointDirectory, nil
}

func getOutputDirectory() (string, error) {
	home, err := assist.Home()
	if err != nil {
		return "", err
	}
	return home + "/" + defaultOutputDirectory, nil
}

func getTempFileDirectory() (string, error) {
	if tempFileDir, ok := config["defaultTempFileDir"]; ok {
		tempFileDir = strings.TrimSpace(tempFileDir)
		if tempFileDir != "" {
			return tempFileDir, nil
		}
	}

	home, err := assist.Home()
	if err != nil {
		return "", err
	}
	return home + "/" + defaultTempFileDirectory, nil
}

func isObsFolder(key string) bool {
	return strings.HasSuffix(key, "/")
}
