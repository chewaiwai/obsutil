package assist

import (
	"errors"
	"os"
)

var FileNotFoundError = errors.New("FileNotFoundError")
var TaskNotFoundError = errors.New("TaskNotFoundError")
var InvalidArgsError = errors.New("InvalidArgsError")
var InitializingError = errors.New("InitializingError")
var InterruptedError = errors.New("InterruptedError")
var CheckBucketStatusError = errors.New("CheckBucketStatusError")
var ExecutingError = errors.New("ExecutedError")
var UncompeletedError = errors.New("UncompeletedError")
var UnsupportedError = errors.New("UnsupportedError")

func CheckErrorAndExit(err error) {
	if err == nil {
		os.Exit(0)
		return
	}

	if err == FileNotFoundError {
		os.Exit(1)
		return
	}

	if err == TaskNotFoundError {
		os.Exit(2)
		return
	}

	if err == InvalidArgsError {
		os.Exit(3)
		return
	}

	if err == CheckBucketStatusError {
		os.Exit(4)
		return
	}

	if err == InitializingError {
		os.Exit(5)
		return
	}

	if err == ExecutingError {
		os.Exit(6)
		return
	}

	if err == UnsupportedError {
		os.Exit(7)
		return
	}

	if err == UncompeletedError {
		os.Exit(8)
		return
	}

	if err == InterruptedError {
		os.Exit(9)
		return
	}

	if err != nil {
		os.Exit(-1)
		return
	}
}
