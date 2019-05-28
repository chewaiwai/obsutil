package assist

import (
	"os"
	"path/filepath"
	"strings"
)

func IsObsFilePath(path string) bool {
	return strings.HasPrefix(path, OBS_PREFIX)
}

func justOneParam(args []string) bool {
	return len(args) == 1
}

func HasCommandPrex(commandArg string) bool {
	return strings.HasPrefix(commandArg, COMMAND_CONNECTOR)
}

func UseDefaultDownloadPath(args []string) bool {
	return justOneParam(args) || HasCommandPrex(args[1])
}

func FindDirsToDelete(dir string) (dirs []string, err error) {
	rmPath := NormalizeFilePath(dir)
	if _stat, _err := os.Lstat(rmPath); _err != nil {
		err = _err
		return
	} else if _stat.IsDir() {
		dirs = make([]string, 0)
		findDirsToDeleteFunc := func(path string, f os.FileInfo, err error) error {
			if err == nil {
				if f.IsDir() {
					dirs = append(dirs, path)
				} else if f.Mode()&os.ModeSymlink == os.ModeSymlink {
					if _, _stat, err := GetRealPath(path); err == nil && _stat.IsDir() {
						dirs = append(dirs, path)
					}
				}
			}
			return err
		}

		walkErr := filepath.Walk(rmPath, findDirsToDeleteFunc)
		if err != nil {
			err = walkErr
			return
		}
	}

	return
}
