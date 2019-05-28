package assist

var cloudType = "hec"

func SetCloudType(c string) {
	if c != "" {
		cloudType = c
	}
}

func IsHec() bool {
	return cloudType == "hec"
}

func IsDt() bool {
	return cloudType == "dt"
}

func IsOtc() bool {
	return cloudType == "otc"
}

