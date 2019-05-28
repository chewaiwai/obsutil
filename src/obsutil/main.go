package main

import (
	"assist"
	"command"
	"runtime"
)

var (
	AesKey    = "not set"
	AesIv     = "not set"
	CloudType = ""
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	assist.SetCloudType(CloudType)
	command.Run(AesKey, AesIv)
}
