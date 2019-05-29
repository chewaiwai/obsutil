package main

import (
	"assist"
	"command"
	"runtime"
)

var (
	AesKey    = ""
	AesIv     = ""
	CloudType = ""
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	assist.SetCloudType(CloudType)
	command.Run(AesKey, AesIv)
}
