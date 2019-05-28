package assist

import (
	"os"
)

const cloudType = "CLOUDTYPE"

func IsHec() bool {
	t := os.Getenv(cloudType)
	return t == "hec" || t == ""
}

func IsDt() bool {
	return os.Getenv(cloudType) == "dt"
}

func IsOtc() bool {
	return os.Getenv(cloudType) == "otc"
}
