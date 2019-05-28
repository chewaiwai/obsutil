package assist

import (
	"bufio"
	"crypto/md5"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"unicode/utf8"
)

const (
	defaultChunkSize     = 8192
	readBufferIoSize     = 8192
	windowsMaxFileLength = 259
)

var uid = os.Getuid()
var gid = os.Getgid()

func findFirstValidPath(value []byte) (string, bool) {
	vals := strings.Split(BytesToString(value), "\n")
	for _, val := range vals {
		if _val := strings.TrimSpace(val); _val == "" {
			continue
		} else {
			return _val, true
		}
	}
	return "", false
}

func GetOsPath(path string) (ret string) {
	var c *exec.Cmd
	if IsWindows() {
		c = exec.Command("where", path)
		if value, err := c.CombinedOutput(); err == nil {
			if _value, flag := findFirstValidPath(value); flag {
				ret = _value
			}
		}
	} else {
		c = exec.Command("which", path)
		if value, err := c.CombinedOutput(); err == nil {
			if _value, flag := findFirstValidPath(value); flag {
				ret = _value
			}
		}
	}

	if ret == "" {
		ret = path
		_, err := os.Lstat(path)
		if err == nil {
			ret, _ = filepath.Abs(path)
		}
	}

	return
}

func checkLength(path string) error {
	if IsWindows() {
		path = NormalizeFilePath(path)
		if length := utf8.RuneCountInString(path); length >= windowsMaxFileLength {
			return fmt.Errorf("the length:%d of path:%s exceed the max length %d", length, path, windowsMaxFileLength)
		}
	}
	return nil
}

func ReadLine(rd *bufio.Reader) ([]byte, error) {
	line := make([]byte, 0, 4096)
	for {
		lineByte, isPrefix, err := rd.ReadLine()
		if err != nil {
			return nil, err
		}
		line = append(line, lineByte...)
		if !isPrefix {
			break
		}
	}
	return line, nil
}

func MkdirAll(path string, perm os.FileMode) error {
	if err := checkLength(path); err != nil {
		return err
	}
	return os.MkdirAll(path, perm)
}

func OpenFile(path string, flag int, perm os.FileMode) (*os.File, error) {
	if err := checkLength(path); err != nil {
		return nil, err
	}

	return os.OpenFile(path, flag, perm)
}

func Chown(path string) error {
	return os.Chown(path, uid, gid)
}

func Rename(oldpath, newpath string) error {
	if err := checkLength(newpath); err != nil {
		return err
	}
	return os.Rename(oldpath, newpath)
}

func FindFiles(folder string, pattern *regexp.Regexp, action func(fileUrl string)) error {
	walkFn := func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && pattern.MatchString(path) {
			action(path)
		}
		return nil
	}
	return filepath.Walk(folder, walkFn)
}

func FindFilesV2(folder string, pattern *regexp.Regexp) ([]string, error) {
	files := make([]string, 0)
	walkFn := func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && pattern.MatchString(path) {
			files = append(files, path)
		}
		return nil
	}
	if err := filepath.Walk(folder, walkFn); err != nil {
		return nil, err
	}
	return files, nil
}

func FindMatches(fileUrl string, pattern *regexp.Regexp, action func(groups []string)) error {
	fd, err := os.Open(fileUrl)
	if err != nil {
		return err
	}
	defer fd.Close()

	rd := bufio.NewReader(fd)
	for {
		lineByte, err := ReadLine(rd)
		if err != nil {
			break
		}
		line := strings.TrimSpace(BytesToString(lineByte))
		if line == "" {
			continue
		}
		if pattern.MatchString(line) {
			action(pattern.FindStringSubmatch(line))
		}
	}
	return nil
}

func ReadContentLineByFileUrl(fileUrl string) ([]string, error) {
	lines := []string{}
	fd, err := os.Open(fileUrl)
	if err != nil {
		return nil, err
	}
	defer fd.Close()

	rd := bufio.NewReader(fd)
	for {
		lineByte, err := ReadLine(rd)
		if err != nil {
			break
		}
		line := strings.TrimSpace(BytesToString(lineByte))
		if line == "" {
			continue
		}
		lines = append(lines, line)
	}
	return lines, nil
}

func Md5File(fileUrl string) ([]byte, error) {
	fd, err := os.Open(fileUrl)
	if err != nil {
		return nil, err
	}
	defer fd.Close()
	m := md5.New()

	p := make([]byte, defaultChunkSize)
	reader := bufio.NewReaderSize(fd, readBufferIoSize)
	for {
		n, err := reader.Read(p)
		if n > 0 {
			_, werr := m.Write(p[0:n])
			if werr != nil {
				return nil, werr
			}
		}

		if err != nil {
			if err != io.EOF {
				return nil, err
			}
			break
		}
	}
	return m.Sum(nil), nil
}

func GetRealPath(path string) (realPath string, realStat os.FileInfo, err error) {
	realPath = path
	realStat, err = os.Lstat(realPath)
	if err != nil {
		return
	}
	for realStat.Mode()&os.ModeSymlink == os.ModeSymlink {
		realPath, err = os.Readlink(realPath)
		if err != nil {
			return
		}
		realStat, err = os.Lstat(realPath)
		if err != nil {
			return
		}
	}
	return
}

func copyFile(oldpath, newpath string) error {
	fd, fdErr := os.Open(oldpath)
	if fdErr != nil {
		return fdErr
	}
	defer fd.Close()
	wfd, wfdErr := OpenFile(newpath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if wfdErr != nil {
		return wfdErr
	}

	defer wfd.Close()
	p := make([]byte, 8192)
	for {
		n, err := fd.Read(p)
		if n > 0 {
			_, werr := wfd.Write(p[0:n])
			if werr != nil {
				return werr
			}
		}

		if err != nil {
			if err != io.EOF {
				return err
			}
			break
		}
	}
	return nil
}

func CopyFile(oldpath, newpath string, checkNewpathLength bool) error {
	if checkNewpathLength {
		if err := checkLength(newpath); err != nil {
			return err
		}
	}
	oldStat, oldStatErr := os.Stat(oldpath)
	if oldStatErr != nil {
		return oldStatErr
	}
	if oldStat.IsDir() {
		return fmt.Errorf("oldpath:%s is a dir", oldpath)
	}

	newfullpath, newfullpathErr := filepath.Abs(newpath)
	if newfullpathErr != nil {
		return newfullpathErr
	}

	newStat, newStatErr := os.Stat(newfullpath)
	if newStatErr == nil && newStat.IsDir() {
		return fmt.Errorf("newpath:%s is a dir", newpath)
	}

	if err := MkdirAll(filepath.Dir(newfullpath), os.ModePerm); err != nil {
		return err
	}

	if err := copyFile(oldpath, newfullpath); err != nil {
		return err
	}

	return nil
}

func RenameFile(oldpath, newpath string) error {
	if err := Rename(oldpath, newpath); err != nil {
		if err := CopyFile(oldpath, newpath, false); err != nil {
			return err
		}

		if err := os.Remove(oldpath); err != nil {
			return err
		}
	}
	return nil
}
