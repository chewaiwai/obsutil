package main

import (
	"command"
	"runtime"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	command.Run()
}
