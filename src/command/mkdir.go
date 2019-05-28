package command

import (
	"assist"
	"command/i18n"
	"fmt"
	"obs"
	"os"
	"strings"
)

type mkdirCommand struct {
	cloudUrlCommand
}

func initMkdir() command {

	c := &mkdirCommand{}
	c.key = "mkdir"
	c.usage = "cloud_url|folder_url"
	c.description = "create folder(s) in a specified bucket or in the local file system"

	c.action = func() error {
		cloudUrl, err := c.prepareCloudUrl()
		if err != nil {
			if cloudUrl == "" {
				printError(err)
				return assist.InvalidArgsError
			}

			folderUrl := assist.NormalizeFilePath(cloudUrl)
			if err := assist.MkdirAll(folderUrl, os.ModePerm); err != nil {
				printError(err)
				return assist.ExecutingError
			}
			printf("Create folder(s) [%s] in the local file system successfully", folderUrl)
			return nil
		}
		bucket, key, err := c.splitCloudUrl(cloudUrl)
		if err != nil {
			printError(err)
			return assist.InvalidArgsError
		}

		if key == "" {
			printf("Error: No folder(s) specified to create in the bucket [%s]", bucket)
			return assist.InvalidArgsError
		}

		fsStatus, err := c.checkBucketFSStatus(bucket)
		if err != nil {
			printError(err)
			return assist.CheckBucketStatusError
		}

		if fsStatus == c_enabled {
			printf("The bucket [%s] supports POSIX, create folder(s) directly", bucket)
			input := &obs.NewFolderInput{}
			input.Bucket = bucket
			input.Key = key
			output, err := obsClient.NewFolder(input)
			if err == nil {
				printf("Create folder [obs://%s/%s] successfully, request id [%s]", bucket, key, output.RequestId)
				doLog(LEVEL_INFO, "Create folder [obs://%s/%s] successfully, request id [%s]", bucket, key, output.RequestId)
				return nil
			} else {
				logError(err, LEVEL_INFO, fmt.Sprintf("Create folder [obs://%s/%s] failed", bucket, key))
				return assist.ExecutingError
			}
		}

		if fsStatus == c_disabled {
			printf("The bucket [%s] does not support POSIX, create folder(s) step by step", bucket)
		} else if fsStatus == c_unknown {
			printf("Can not identify whether the bucket [%s] supports POSIX, create folder(s) step by step", bucket)
		}

		if isObsFolder(key) {
			key = key[:len(key)-1]
		}
		folders := strings.Split(key, "/")
		current := ""
		input := &obs.PutObjectInput{}
		input.Bucket = bucket
		input.ContentLength = 0
		allSucceed := true
		for _, folder := range folders {
			current += folder + "/"
			input.Key = current
			output, err := obsClient.PutObject(input)
			if err == nil {
				printf("Create folder [obs://%s/%s] successfully, request id [%s]", bucket, current, output.RequestId)
				doLog(LEVEL_INFO, "Create folder [obs://%s/%s] successfully, request id [%s]", bucket, current, output.RequestId)
			} else {
				logError(err, LEVEL_INFO, fmt.Sprintf("Create folder [obs://%s/%s] failed", bucket, current))
				allSucceed = false
			}
		}

		if !allSucceed {
			return assist.UncompeletedError
		}
		return nil

	}

	c.help = func() {
		p := i18n.GetCurrentPrinter()
		p.Printf("Summary:")
		printf("%2s%s", "", p.Sprintf("create folder(s) in a specified bucket or in the local file system"))
		printf("")
		p.Printf("Syntax 1:")
		printf("%2s%s", "", "obsutil mkdir obs://bucket/folder1/folder2/folder3/ [-config=xxx]")
		printf("")
		p.Printf("Syntax 2:")
		printf("%2s%s", "", "obsutil mkdir folder_url [-config=xxx]")
		printf("")

		p.Printf("Options:")
		printf("%2s%s", "", "-config=xxx")
		printf("%4s%s", "", p.Sprintf("the path to the custom config file when running this command"))
		printf("")
	}

	return c
}
