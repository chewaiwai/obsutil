package command

import (
	"assist"
	"obs"
	"strings"
)

var obsClient *obs.ObsClient
var obsClientCrr *obs.ObsClient

func refreshObsClient() (err error) {
	if obsClient != nil {
		obsClient.Close()
	}
	obsClient, err = createObsClient("")
	return
}

func refreshObsClientCrr() (err error) {
	if obsClientCrr != nil {
		obsClientCrr.Close()
	}
	obsClientCrr, err = createObsClient("Crr")
	return
}

func createObsClient(suffix string) (*obs.ObsClient, error) {
	connectTimeout := assist.StringToInt(config["connectTimeout"], defaultConnectTimeout)
	socketTimeout := assist.StringToInt(config["socketTimeout"], defaultSocketTimeout)
	maxRetryCount := assist.StringToInt(config["maxRetryCount"], defaultMaxRetryCount)
	maxConnections := assist.StringToInt(config["maxConnections"], defaultMaxConnections)

	endpoint := strings.TrimSpace(config["endpoint"+suffix])
	if endpoint == "" {
		endpoint = defaultEndpoint
	}

	return obs.New(config["ak"+suffix], config["sk"+suffix], endpoint,
		obs.WithSecurityToken(config["token"+suffix]),
		obs.WithConnectTimeout(connectTimeout),
		obs.WithSocketTimeout(socketTimeout),
		obs.WithHeaderTimeout(socketTimeout),
		obs.WithMaxRetryCount(maxRetryCount),
		obs.WithMaxConnections(maxConnections),
		obs.WithProxyUrl(config["proxyUrl"]),
		obs.WithUserAgent("obsutil/"+obsUtilVersion),
		obs.WithDisableCompression(config["enableCompression"] != "true"),
	)
}

func initClientAndLog() bool {
	var err error

	err = refreshObsClient()
	if err != nil {
		printError(err)
		return false
	}

	helper := assist.MapHelper(config)

	if sdkLogPath := helper.Get("sdkLogPath"); sdkLogPath != "" {
		sdkLogBackups := assist.StringToInt(config["sdkLogBackups"], defaultLogBackups)
		var sdkMaxLogSize int64
		if _sdkMaxLogSize, err := assist.TranslateToInt64(config["sdkMaxLogSize"]); err == nil && _sdkMaxLogSize > 0 {
			sdkMaxLogSize = _sdkMaxLogSize
		} else {
			sdkMaxLogSize = defaultMaxLogSize
		}
		if err := obs.InitLog(sdkLogPath, sdkMaxLogSize, sdkLogBackups, obs.Level(transLogLevel(config["sdkLogLevel"])), false); err != nil {
			printError(err)
		}
	}

	if utilLogPath := helper.Get("utilLogPath"); utilLogPath != "" {
		utilLogBackups := assist.StringToInt(config["utilLogBackups"], defaultLogBackups)
		var utilMaxLogSize int64
		if _utilMaxLogSize, err := assist.TranslateToInt64(config["utilMaxLogSize"]); err == nil && _utilMaxLogSize > 0 {
			utilMaxLogSize = _utilMaxLogSize
		} else {
			utilMaxLogSize = defaultMaxLogSize
		}
		if err := initLog(utilLogPath, utilMaxLogSize, utilLogBackups, Level(transLogLevel(config["utilLogLevel"])), defaultLogCacheCnt); err != nil {
			printError(err)
		}
	}

	return true
}

func doClean() {
	if obsClient != nil {
		obsClient.Close()
		obsClient = nil
	}

	if obsClientCrr != nil {
		obsClientCrr.Close()
		obsClientCrr = nil
	}

	obs.CloseLog()
	closeLog()
}
