package command

import (
	"assist"
	"bufio"
	"bytes"
	"command/i18n"
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const (
	aesKey = "mockkey123456789"
	aesIv  = "mockiv0123456789"
)

var configFile string
var configFileStat os.FileInfo
var originConfigFile string
var config map[string]string
var createDefaultConfigFile = false
var defaultConfig map[string]string

func GetDefaultConfig() map[string]string {
	if defaultConfig != nil {
		return defaultConfig
	}

	if home, err := assist.Home(); err == nil {
		defaultConfigMap["utilLogPath"] = assist.NormalizeFilePath(home + "/" + defaultLogDirectory + "/obsutil.log")
		defaultConfigMap["sdkLogPath"] = assist.NormalizeFilePath(home + "/" + defaultLogDirectory + "/obssdk.log")
		defaultConfigMap["defaultTempFileDir"] = assist.NormalizeFilePath(home + "/" + defaultTempFileDirectory)
	}

	kv := make(map[string]string, len(defaultConfigMap))
	for k, v := range defaultConfigMap {
		if _v, ok := v.(string); ok {
			kv[k] = _v
		} else if _v, ok := v.(int); ok {
			kv[k] = assist.IntToString(_v)
		} else if _v, ok := v.(int64); ok {
			kv[k] = assist.Int64ToString(_v)
		} else if _v, ok := v.(bool); ok {
			if _v {
				kv[k] = "true"
			} else {
				kv[k] = "false"
			}
		}
	}
	defaultConfig = kv
	return kv
}

func AesEncrypt(encodeStr string, key []byte) (string, error) {
	encodeBytes := assist.StringToBytes(encodeStr)
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}

	blockSize := block.BlockSize()
	encodeBytes = PKCS5Padding(encodeBytes, blockSize)

	blockMode := cipher.NewCBCEncrypter(block, assist.StringToBytes(aesIv))
	crypted := make([]byte, len(encodeBytes))
	blockMode.CryptBlocks(crypted, encodeBytes)

	return base64.StdEncoding.EncodeToString(crypted), nil
}

func PKCS5Padding(ciphertext []byte, blockSize int) []byte {
	padding := blockSize - len(ciphertext)%blockSize
	padtext := bytes.Repeat([]byte{byte(padding)}, padding)

	return append(ciphertext, padtext...)
}

func AesDecrypt(decodeStr string, key []byte) (retByte []byte, retErr error) {
	defer func() {
		if r := recover(); r != nil {
			retErr = errors.New("AesDecrypt Panic")
		}
	}()

	decodeBytes, retErr := base64.StdEncoding.DecodeString(decodeStr)
	if retErr != nil {
		return nil, retErr
	}
	block, retErr := aes.NewCipher(key)
	if retErr != nil {
		return nil, retErr
	}
	blockMode := cipher.NewCBCDecrypter(block, assist.StringToBytes(aesIv))
	origData := make([]byte, len(decodeBytes))

	blockMode.CryptBlocks(origData, decodeBytes)
	retByte = PKCS5UnPadding(origData)
	return
}

func PKCS5UnPadding(origData []byte) []byte {
	length := len(origData)
	unpadding := int(origData[length-1])
	return origData[:(length - unpadding)]
}

func readConfigFileKeys() map[string]bool {
	kv := make(map[string]bool, 50)
	fd, err := assist.OpenFile(configFile, os.O_RDONLY, 0666)
	if err != nil {
		return kv
	}
	defer fd.Close()

	rd := bufio.NewReader(fd)
	for {
		lineByte, err := assist.ReadLine(rd)
		if err != nil {
			break
		}
		line := strings.TrimSpace(assist.BytesToString(lineByte))
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if index := strings.Index(line, "="); index > 0 {
			key := strings.TrimSpace(line[:index])
			kv[key] = true
		}
	}
	return kv
}

func readConfigFile() (map[string]string, error) {
	fd, err := assist.OpenFile(configFile, os.O_RDONLY, 0666)
	if err != nil {
		return nil, err
	}
	defer fd.Close()

	kv := make(map[string]string, 50)
	rd := bufio.NewReader(fd)
	var aksk map[string]string
	interestedKeys := map[string]string{
		"ak":    defaultAcessKey,
		"sk":    defaultSecurityKey,
		"akCrr": defaultAcessKey,
		"skCrr": defaultSecurityKey,
	}

	for {
		lineByte, err := assist.ReadLine(rd)
		if err != nil {
			break
		}
		line := strings.TrimSpace(assist.BytesToString(lineByte))
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if index := strings.Index(line, "="); index > 0 {
			key := strings.TrimSpace(line[:index])
			val := strings.TrimSpace(line[index+1:])

			if v, ok := interestedKeys[key]; ok {
				_val, err := AesDecrypt(val, assist.StringToBytes(aesKey))
				if err == nil {
					val = assist.BytesToString(_val)
				} else if val != v {
					if aksk == nil {
						aksk = make(map[string]string, 2)
					}
					crypted, err := AesEncrypt(val, assist.StringToBytes(aesKey))
					if err != nil {
						return nil, err
					}
					aksk[key] = crypted
				}
			}
			kv[key] = val
		} else {
			return nil, fmt.Errorf("Configuration file [%s] is not well-formed", configFile)
		}
	}

	if aksk != nil {
		if err := InitConfigFile(aksk, false); err != nil {
			printf("Warn: Try to write config file failed, %s", err.Error())
		}
	}

	for k, v := range GetDefaultConfig() {
		if _, ok := kv[k]; !ok {
			kv[k] = v
		}
	}

	return kv, nil
}

func loadConfigFile(kv map[string]string, bootstrap bool) ([]string, error) {
	fd, err := assist.OpenFile(configFile, os.O_CREATE|os.O_RDONLY, 0666)
	if err != nil {
		return nil, err
	}
	defer fd.Close()
	lines := make([]string, 0, 50)
	rd := bufio.NewReader(fd)
	if kv == nil {
		kv = map[string]string{}
	}
	for {
		lineByte, err := assist.ReadLine(rd)
		if err != nil {
			break
		}
		line := assist.BytesToString(lineByte)
		_line := strings.TrimSpace(line)
		if _line == "" || strings.HasPrefix(_line, "#") {
			lines = append(lines, line)
			continue
		}

		if index := strings.Index(_line, "="); index > 0 {
			key := strings.TrimSpace(_line[:index])
			if newVal, ok := kv[key]; ok {
				if bootstrap {
					lines = append(lines, line)
				} else {
					lines = append(lines, strings.Join([]string{key, newVal}, "="))
				}
				delete(kv, key)
			} else {
				lines = append(lines, line)
			}
		} else {
			return nil, fmt.Errorf("Configuration file [%s] is not well-formed", configFile)
		}
	}

	if len(kv) > 0 {
		for _, k := range defaultConfigSlice {
			if _, ok := kv[k]; !ok {
				continue
			}
			lines = append(lines, strings.Join([]string{k, kv[k]}, "="))
		}
	}

	return lines, nil
}

func writeConfigFile(lines []string) error {
	fd, err := assist.OpenFile(configFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}
	defer fd.Close()
	wd := bufio.NewWriter(fd)
	for _, line := range lines {
		_, err := wd.WriteString(line + "\n")
		if err != nil {
			return err
		}
	}
	wd.Flush()
	return nil
}

func isAnonymousUser() bool {
	return config["ak"] == "" && config["sk"] == ""
}

func isAnonymousUserCrr() bool {
	return config["akCrr"] == "" && config["skCrr"] == ""
}

func isVerifyLengthConsistency() bool {
	return config["verifyLengthConsistency"] == "true"
}

func isVerifyMd5Integrality() bool {
	return config["verifyMd5Integrality"] == "true"
}

func InitConfigFile(kv map[string]string, bootstrap bool) error {
	if configFile == "" {
		home, err := assist.Home()
		if err != nil {
			return err
		}
		configFile = home + "/" + defaultConfigFileName
		configFile = assist.NormalizeFilePath(configFile)
	}

	if originConfigFile == "" {
		originConfigFile = configFile
	}

	stat, err := os.Stat(configFile)
	if err == nil {
		if stat.IsDir() {
			return fmt.Errorf("Error: The specified configuration file url [%s] is a folder, not a file", configFile)
		}
		createDefaultConfigFile = false
	} else {
		createDefaultConfigFile = true
		parentFolder := filepath.Dir(configFile)
		if _, err := os.Stat(parentFolder); err != nil {
			if err := os.MkdirAll(parentFolder, os.ModePerm); err != nil {
				return fmt.Errorf("Error: Cannot create parent folder for [%s], %s", configFile, err.Error())
			}
		}

	}

	if kv == nil || len(kv) <= 0 {
		return nil
	}

	lines, err := loadConfigFile(kv, bootstrap)
	if err != nil {
		return err
	}

	if bootstrap {
		needWriteConfigFile := false
		newKv := readConfigFileKeys()
		for _, k := range defaultConfigSlice {
			if _, ok := newKv[k]; !ok {
				needWriteConfigFile = true
				break
			}
		}

		if !needWriteConfigFile {
			return nil
		}
	}

	return writeConfigFile(lines)
}

type configCommand struct {
	defaultCommand
	endpoint    string
	ak          string
	sk          string
	token       string
	interactive bool
	crr         bool
}

func initConfig() command {

	c := &configCommand{}
	c.key = "config"
	c.usage = "[options...]"
	c.description = "update the configuration file"
	c.additional = true

	c.define = func() {
		c.flagSet.StringVar(&c.endpoint, "e", "n/a", "")
		if assist.IsHec() {
			c.flagSet.StringVar(&c.ak, "i", "n/a", "")
			c.flagSet.StringVar(&c.sk, "k", "n/a", "")
			c.flagSet.StringVar(&c.token, "t", "n/a", "")
		} else {
			c.ak = "n/a"
			c.sk = "n/a"
			c.token = "n/a"
		}
		c.flagSet.BoolVar(&c.interactive, "interactive", false, "")
		c.flagSet.BoolVar(&c.crr, "crr", false, "")
	}

	c.action = func() error {
		args := c.flagSet.Args()
		if len(args) > 0 {
			c.showHelp()
			printf("Error: Invalid args: %v", args)
			return assist.InvalidArgsError
		}

		suffix := ""
		if c.crr {
			suffix = "Crr"
		}

		if c.interactive {
			if ak, err := getUserInput(fmt.Sprintf("Please input your ak%s:", suffix)); err == nil && ak != "" {
				c.ak = ak
			}

			if sk, err := getUserInput(fmt.Sprintf("Please input your sk%s:", suffix)); err == nil && sk != "" {
				c.sk = sk
			}

			if endpoint, err := getUserInput(fmt.Sprintf("Please input your endpoint%s:", suffix)); err == nil && endpoint != "" {
				c.endpoint = endpoint
			}

			if token, err := getUserInput(fmt.Sprintf("Please input your token%s:", suffix)); err == nil && token != "" {
				c.token = token
			}
		}

		kv := make(map[string]string, 5)
		if endpoint := strings.TrimSpace(c.endpoint); endpoint != "n/a" && endpoint != "" {
			kv["endpoint"+suffix] = endpoint
		}

		if ak := strings.TrimSpace(c.ak); ak != "n/a" {
			cryptedAk, err := AesEncrypt(ak, assist.StringToBytes(aesKey))
			if err != nil {
				printError(err)
				return assist.ExecutingError
			}
			kv["ak"+suffix] = cryptedAk
		}

		if sk := strings.TrimSpace(c.sk); sk != "n/a" {
			cryptedSk, err := AesEncrypt(sk, assist.StringToBytes(aesKey))
			if err != nil {
				printError(err)
				return assist.ExecutingError
			}
			kv["sk"+suffix] = cryptedSk
		}

		if token := strings.TrimSpace(c.token); token != "n/a" {
			kv["token"+suffix] = token
		}

		printf("Config file url:")
		printf("%2s%s", "", configFile)
		printf("")

		if len(kv) <= 0 {
			return nil
		}

		if err := InitConfigFile(kv, false); err != nil {
			printError(err)
			return assist.ExecutingError
		}
		printf("Update config file successfully!")
		return nil
	}

	c.help = func() {
		p := i18n.GetCurrentPrinter()
		p.Printf("Summary:")
		printf("%2s%s", "", p.Sprintf("update the configuration file"))
		printf("")
		p.Printf("Syntax 1:")
		printf("%2s%s", "", "obsutil config [-config=xxx]")
		printf("")
		p.Printf("Syntax 2:")
		printf("%2s%s", "", "obsutil config -interactive [-crr] [-config=xxx]")
		printf("")
		isHec := assist.IsHec()
		if isHec {
			p.Printf("Syntax 3:")
			printf("%2s%s", "", "obsutil config [-e=xxx] [-i=xxx] [-k=xxx] [-t=xxx] [-crr] [-config=xxx]")
			printf("")
		}

		p.Printf("Options:")
		printf("%2s%s", "", "-interactive")
		printf("%4s%s", "", p.Sprintf("update the configuration file through interactive mode"))
		printf("")
		printf("%2s%s", "", "-e=xxx")
		printf("%4s%s", "", p.Sprintf("endpoint"))
		printf("")
		if isHec {
			printf("%2s%s", "", "-i=xxx")
			printf("%4s%s", "", p.Sprintf("access key ID"))
			printf("")
			printf("%2s%s", "", "-k=xxx")
			printf("%4s%s", "", p.Sprintf("security key ID"))
			printf("")
			printf("%2s%s", "", "-t=xxx")
			printf("%4s%s", "", p.Sprintf("security token"))
			printf("")
		}
		printf("%2s%s", "", "-crr")
		printf("%4s%s", "", p.Sprintf("update the configuration file for crr"))
		printf("")
		printf("%2s%s", "", "-config=xxx")
		printf("%4s%s", "", p.Sprintf("the path to the custom config file when running this command"))
		printf("")
	}

	return c
}
