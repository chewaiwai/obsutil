package assist

import (
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"gopkg.in/cheggaaa/pb.v2/termutil"
	"hash"
	"io"
	"math"
	"os"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"
	"unsafe"
)

var chineseRegex = regexp.MustCompile("[\u4e00-\u9fa5]")
var noUnicodeRegex = regexp.MustCompile("^[\\w\\s=;:,<>!@#%&-/'~`.+*?\"$^|{}()\\[\\]\\\\]*$")

func HasChinese(val string) bool {
	return chineseRegex.MatchString(val)
}

func HasUnicode(val string) bool {
	return !noUnicodeRegex.MatchString(val)
}

func GetOS() string {
	return runtime.GOOS
}

func GetArch() string {
	return runtime.GOARCH
}

func Round(val float64) (newVal float64) {
	roundOn := 0.5
	places := 0
	var round float64
	pow := math.Pow(10, float64(places))
	digit := pow * val
	_, div := math.Modf(digit)
	if div >= roundOn {
		round = math.Ceil(digit)
	} else {
		round = math.Floor(digit)
	}
	newVal = round / pow
	return
}

func GetTerminalWidth() (int, error) {
	return termutil.TerminalWidth()
}

func IsWindows() bool {
	return runtime.GOOS == "windows"
}

func IsMac() bool {
	return runtime.GOOS == "darwin"
}

func IsLinux() bool {
	return runtime.GOOS == "linux"
}

func GetCpuNumber() int {
	return runtime.NumCPU()
}

func StringToInt(value string, def int) int {
	ret, err := strconv.Atoi(value)
	if err != nil {
		ret = def
	}
	return ret
}

func StringToInt64(value string, def int64) int64 {
	ret, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		ret = def
	}
	return ret
}

func StringToFloat64(value string, def float64) float64 {
	ret, err := strconv.ParseFloat(value, 64)
	if err != nil {
		ret = def
	}
	return ret
}

func StringToIntV2(value string) (int, error) {
	return strconv.Atoi(value)
}

func StringToInt64V2(value string) (int64, error) {
	return strconv.ParseInt(value, 10, 64)
}

func StringToFloat64V2(value string) (float64, error) {
	return strconv.ParseFloat(value, 64)
}

func ParseXml(value []byte, result interface{}) error {
	if len(value) == 0 {
		return nil
	}
	return xml.Unmarshal(value, result)
}

func TransToXml(value interface{}) ([]byte, error) {
	if value == nil {
		return []byte{}, nil
	}
	return xml.Marshal(value)
}

func HmacSha1(key, value []byte) []byte {
	mac := hmac.New(sha1.New, key)
	mac.Write(value)
	return mac.Sum(nil)
}

func HmacSha256(key, value []byte) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write(value)
	return mac.Sum(nil)
}

func Base64Encode(value []byte) string {
	return base64.StdEncoding.EncodeToString(value)
}

func Base64Decode(value string) ([]byte, error) {
	return base64.StdEncoding.DecodeString(value)
}

func Md5(value []byte) []byte {
	m := md5.New()
	m.Write(value)
	return m.Sum(nil)
}

func GetMd5Writer() io.Writer {
	return md5.New()
}

func GetBase64Md5(writer io.Writer) string {
	if md5Hash, ok := writer.(hash.Hash); ok {
		return Base64Encode(md5Hash.Sum(nil))
	}
	return ""
}

func GetHexMd5(writer io.Writer) string {
	if md5Hash, ok := writer.(hash.Hash); ok {
		return Hex(md5Hash.Sum(nil))
	}
	return ""
}

func Hex(value []byte) string {
	return hex.EncodeToString(value)
}

func HexMd5(value []byte) string {
	return Hex(Md5(value))
}

func Base64Md5(value []byte) string {
	return Base64Encode(Md5(value))
}

func IntToString(value int) string {
	return strconv.Itoa(value)
}

func Int64ToString(value int64) string {
	return strconv.FormatInt(value, 10)
}

func GetCurrentTimestamp() int64 {
	return time.Now().UnixNano() / 1000000
}

func GetUtcNow() time.Time {
	return time.Now().UTC()
}

func FormatUtcNow(format string) string {
	return time.Now().UTC().Format(format)
}

func FormatUtcToRfc1123(t time.Time) string {
	ret := t.UTC().Format(time.RFC1123)
	return ret[:strings.LastIndex(ret, "UTC")] + "GMT"
}

func MinFloat64(va, vb float64) float64 {
	if va <= vb {
		return va
	}
	return vb
}

func MinInt(va, vb int) int {
	if va <= vb {
		return va
	}
	return vb
}

func MaxFloat64(va, vb float64) float64 {
	if va <= vb {
		return vb
	}
	return va
}

func MaxInt(va, vb int) int {
	if va <= vb {
		return vb
	}
	return va
}

func Str2Timestamp(str string, defaultValue int64) (ts int64, err error) {
	basic := "00000101000000"
	str = strings.TrimSpace(str)
	if str == "*" {
		ts = defaultValue
	} else {
		str += basic[len(str):]
		if formatTime, _err := time.Parse("20060102150405", str); _err == nil {
			ts = formatTime.Unix()
		} else {
			err = _err
		}
	}
	return
}

func EnsureDirectory(dir string) (err error) {
	stat, err := os.Stat(dir)
	if err == nil && !stat.IsDir() {
		err = fmt.Errorf("path [%s] is not a directory", dir)
		return
	}
	err = MkdirAll(dir, os.ModePerm)
	return
}

func MaybeDeleteBeginningSlash(srckey string) string {
	key := srckey
	if strings.HasPrefix(srckey, "/") {
		key = srckey[1:]
	}
	return key
}

func StringToBytes(s string) []byte {
	x := (*[2]uintptr)(unsafe.Pointer(&s))
	h := [3]uintptr{x[0], x[1], x[1]}
	return *(*[]byte)(unsafe.Pointer(&h))
}

func BytesToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func MaybeDeleteTrailingSlash(srckey string) string {
	key := srckey
	if strings.HasSuffix(srckey, "/") {
		key = srckey[:len(srckey)-1]
	}
	return key
}

func MaybeAddTrailingSlash(srckey string) string {
	key := srckey
	if srckey != "" && !strings.HasSuffix(srckey, "/") {
		key = srckey + "/"
	}
	return key
}
