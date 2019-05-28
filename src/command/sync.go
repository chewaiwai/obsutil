package command

import (
	"assist"
	"command/i18n"
	"os"
	"path/filepath"
	"progress"
	"strings"
)

type syncCommand struct {
	transferCommand
	del bool
}

func (c *syncCommand) doUpload(url1, url2 string) error {
	url1, err := filepath.Abs(url1)
	if err != nil {
		printError(err)
		return assist.InvalidArgsError
	}

	stat, err := os.Lstat(url1)
	if err != nil {
		printError(err)
		return assist.FileNotFoundError
	}

	linkFolder := false
	relativeFolder := ""
	if stat.Mode()&os.ModeSymlink == os.ModeSymlink {
		if c.link {
			if _url1, _stat, err := assist.GetRealPath(url1); err != nil {
				printError(err)
				return assist.FileNotFoundError
			} else {
				if _stat.IsDir() {
					if !c.flat {
						relativeFolder = c.getRelativeFolder(url1)
					}
					linkFolder = true
				}
				url1 = _url1
				stat = _stat
			}
		} else {
			if _stat, err := os.Stat(url1); err == nil && _stat.IsDir() {
				stat = _stat
			}
		}
	}

	aclType, storageClassType, metadata, succeed := c.checkParams()
	if !succeed {
		return assist.InvalidArgsError
	}

	bucket, keyOrDir, err := c.splitCloudUrl(url2)
	if err != nil {
		printError(err)
		return assist.InvalidArgsError
	}

	if checkEmptyFolder(bucket, keyOrDir, um) {
		printf("Error: Cannot upload to [%s], the url contains empty folder", url2)
		return assist.InvalidArgsError
	}

	if c.verifyMd5 {
		c.checkBucketVersion(bucket)
	}

	c.printStart()
	c.ensureParentFolder(bucket, keyOrDir)

	if !stat.IsDir() {
		key := keyOrDir
		if key == "" || isObsFolder(key) {
			key += stat.Name()
		}

		arcPath := ""
		if c.arcDir != "" {
			arcPath = c.arcDir + "/" + stat.Name()
		}

		if c.forceRecord {
			return c.ensureBucketsAndStartAction([]string{bucket}, func() error {
				c.printParams(true, true, true, false)
				c.recordStartFuncForUpload()
				ret := c.uploadFile(bucket, key, arcPath, url1, stat, metadata, aclType, storageClassType, nil, nil, 1, nil)
				if ret >= 1 {
					progress.AddSucceedCount(1)
					return nil
				}
				progress.AddFailedCount(1)
				return assist.ExecutingError
			}, true)
		}
		c.printParams(false, true, true, false)
		ret := c.uploadFile(bucket, key, arcPath, url1, stat, metadata, aclType, storageClassType, nil, nil, 0, nil)
		if warn, ok := c.warn.Load().(error); ok {
			printWarn(warn)
		}
		if ret == 0 {
			return assist.ExecutingError
		}
		return nil
	}
	return c.ensureBucketsAndStartAction([]string{bucket}, func() error {
		dir := keyOrDir
		c.printParams(true, true, true, false)
		doLog(LEVEL_INFO, "Upload objects from local folder [%s] to cloud folder [%s] in the bucket [%s]", url1, dir, bucket)
		arcDir := c.arcDir
		if linkFolder {
			if dir != "" && !isObsFolder(dir) {
				dir = dir + "/"
			}
			dir += relativeFolder

			if arcDir != "" {
				if !isObsFolder(arcDir) {
					arcDir = arcDir + "/"
				}
				arcDir += relativeFolder
			}
		}
		if c.del {
			//TODO delete
		}

		return c.uploadDir(bucket, dir, arcDir, url1, linkFolder, stat, metadata, aclType, storageClassType)
	}, false)
}

func (c *syncCommand) doDownload(url1, url2 string) error {
	url2, err := filepath.Abs(url2)
	if err != nil {
		printError(err)
		return assist.InvalidArgsError
	}
	bucket, keyOrDir, err := c.splitCloudUrl(url1)
	if err != nil {
		printError(err)
		return assist.InvalidArgsError
	}

	if checkEmptyFolder(bucket, keyOrDir, dm) {
		printf("Error: Cannot download from [%s], the url contains empty folder", url1)
		return assist.InvalidArgsError
	}

	if err := c.ensureBucket(bucket); err != nil {
		printError(err)
		doLog(LEVEL_ERROR, err.Error())
		return assist.CheckBucketStatusError
	}

	c.printStart()

	stat, err := os.Lstat(url2)
	if err != nil {
		if err = assist.MkdirAll(url2, os.ModePerm); err != nil {
			printError(err)
			return assist.InvalidArgsError
		}
		stat, err = os.Lstat(url2)
		if err != nil {
			printError(err)
			return assist.FileNotFoundError
		}
	} else if !stat.IsDir() {
		printf("Error: Cannot download to the folder [%s] due to a file with the same name exits!", url2)
		return assist.InvalidArgsError
	}

	dir := keyOrDir
	if err := c.ensureOutputDirectory(); err != nil {
		printError(err)
		return assist.InvalidArgsError
	}
	c.printParams(true, true, false, true)
	if err := c.startLogger(true); err != nil {
		printError(err)
		return assist.InvalidArgsError
	}
	doLog(LEVEL_INFO, "Download objects from cloud folder [%s] in the bucket [%s] to local folder [%s] ", dir, bucket, url2)
	defer c.endLogger()
	if c.del {
		//TODO delete
	}
	return c.downloadDir(bucket, dir, url2, stat)
}

func (c *syncCommand) doCopy(url1, url2 string) error {
	if url1 == url2 {
		printf("The source url and the destination url are same!")
		return assist.InvalidArgsError
	}

	aclType, storageClassType, metadata, succeed := c.checkParams()
	if !succeed {
		return assist.InvalidArgsError
	}

	srcBucket, srcKeyOrDir, err := c.splitCloudUrl(url1)
	if err != nil {
		printError(err)
		return assist.InvalidArgsError
	}

	dstBucket, dstKeyOrDir, err := c.splitCloudUrl(url2)
	if err != nil {
		printError(err)
		return assist.InvalidArgsError
	}

	if checkEmptyFolder(srcBucket, srcKeyOrDir, cm) {
		printf("Error: Cannot copy from [%s], the url contains empty folder", url1)
		return assist.InvalidArgsError
	}

	if checkEmptyFolder(dstBucket, dstKeyOrDir, cm) {
		printf("Error: Cannot copy to [%s], the url contains empty folder", url2)
		return assist.InvalidArgsError
	}

	if c.crr {
		if !c.createObsClientCrr() {
			return assist.InitializingError
		}
		if c.verifyMd5 {
			c.checkBucketVersion(dstBucket)
		}
	}

	if dstBucket == srcBucket {
		if index := strings.Index(url2, url1); index >= 0 {
			printf("The source url and the destination url are nested")
			return assist.InvalidArgsError
		}

		if index := strings.Index(url1, url2); index >= 0 {
			printf("The source url and the destination url are nested")
			return assist.InvalidArgsError
		}
	}

	c.printStart()
	c.ensureParentFolder(dstBucket, dstKeyOrDir)

	if c.crr {
		return c.ensureBucketsAndStartActionCrr(srcBucket, dstBucket, func() error {
			c.printParams(true, true, false, false)
			srcDir := srcKeyOrDir
			dstDir := dstKeyOrDir
			doLog(LEVEL_INFO, "Copy objects from cloud folder [%s] in the bucket [%s] to cloud folder [%s] in the bucket [%s] ",
				srcDir, srcBucket, dstDir, dstBucket)
			if c.del {
				//TODO delete
			}
			return c.copyDir(srcBucket, srcDir, dstBucket, dstDir, metadata, aclType, storageClassType)
		}, false)
	}
	if succeed := c.compareLocation(srcBucket, dstBucket); !succeed {
		return assist.InvalidArgsError
	}
	return c.ensureBucketsAndStartAction([]string{srcBucket, dstBucket}, func() error {
		c.printParams(true, false, false, false)
		srcDir := srcKeyOrDir
		dstDir := dstKeyOrDir
		doLog(LEVEL_INFO, "Copy objects from cloud folder [%s] in the bucket [%s] to cloud folder [%s] in the bucket [%s] ",
			srcDir, srcBucket, dstDir, dstBucket)
		if c.del {
			//TODO delete
		}
		return c.copyDir(srcBucket, srcDir, dstBucket, dstDir, metadata, aclType, storageClassType)
	}, false)
}

func initSync() command {
	c := &syncCommand{}
	c.key = "sync"
	c.usage = []string{
		"file_url cloud_url [options...]",
		"cloud_url file_url [options...]",
		"cloud_url cloud_url [options...]",
	}

	c.description = "synchronize objects from the source to the destination"
	c.force = true
	c.recursive = true
	c.flat = true
	c.update = true
	c.define = func() {
		c.init()
		c.defineBasic()
		c.flagSet.BoolVar(&c.del, "delete", false, "")
	}

	c.action = func() error {
		args := c.flagSet.Args()
		if len(args) == 0 {
			c.showHelp()
			printf("Error: Invalid args, please refer to help doc")
			return assist.InvalidArgsError
		}

		url1, url2, mode, err := c.prepareUrls(args)
		if err != nil {
			printError(err)
			return assist.InvalidArgsError
		}

		if !c.prepareOptions() {
			return assist.InvalidArgsError
		}

		var ret error
		if mode == um {
			ret = c.doUpload(url1, url2)
		} else if mode == dm {
			ret = c.doDownload(url1, url2)
		} else if mode == cm {
			//TODO
			if !c.crr {
				if c.jobs > 10 {
					printf("Error: The max jobs for copy is 10")
					return assist.InvalidArgsError
				}
				if c.parallel >= 10 {
					printf("Error: The max parallel for copy is 10")
					return assist.InvalidArgsError
				}
			}
			ret = c.doCopy(url1, url2)
		}
		return ret
	}

	c.help = func() {
		p := i18n.GetCurrentPrinter()
		p.Printf("Summary:")
		printf("%2s%s", "", p.Sprintf("synchronize objects from the source to the destination"))
		printf("")
		p.Printf("Syntax 1:")
		printf("%2s%s", "", "obsutil sync file_url obs://bucket[/key] [-dryRun] [-link] [-vlength] [-vmd5] [-p=1] [-threshold=52428800] [-acl=xxx] [-sc=xxx] [-meta=aaa:bbb#ccc:ddd] [-ps=5242880] [-cpd=xxx] [-fr] [-arcDir=xxx] [-o=xxx] [-config=xxx]")
		printf("")
		p.Printf("Syntax 2:")
		printf("%2s%s", "", "obsutil sync folder_url obs://bucket[/prefix] [-dryRun] [-link] [-vlength] [-vmd5] [-j=1] [-p=1] [-threshold=52428800] [-acl=xxx] [-sc=xxx] [-meta=aaa:bbb#ccc:ddd] [-ps=5242880] [-include=*.xxx] [-exclude=*.xxx] [-arcDir=xxx] [-timeRange=time1-time2] [-o=xxx] [-cpd=xxx] [-config=xxx]")
		printf("")
		p.Printf("Syntax 3:")
		printf("%2s%s", "", "obsutil sync obs://bucket[/prefix] folder_url [-dryRun] [-tempFileDir=xxx] [-vlength] [-vmd5] [-j=1] [-p=1] [-threshold=52428800] [-ps=5242880] [-include=*.xxx] [-exclude=*.xxx] [-timeRange=time1-time2] [-o=xxx] [-cpd=xxx] [-config=xxx]")
		printf("")
		p.Printf("Syntax 4:")
		printf("%2s%s", "", "obsutil sync obs://srcbucket[/src_prefix] obs://dstbucket[/dest_prefix] [-dryRun] [-crr] [-vlength] [-vmd5] [-j=1] [-p=1] [-threshold=52428800] [-acl=xxx] [-sc=xxx] [-meta=aaa:bbb#ccc:ddd] [-ps=5242880] [-include=*.xxx] [-exclude=*.xxx] [-timeRange=time1-time2] [-o=xxx] [-cpd=xxx] [-config=xxx]")
		printf("")

		p.Printf("Options:")
		printf("%2s%s", "", "-dryRun")
		printf("%4s%s", "", p.Sprintf("conduct a dry run"))
		printf("")
		printf("%2s%s", "", "-tempFileDir=xxx")
		printf("%4s%s", "", p.Sprintf("the temp file dir, used to save temporary files during the objects are downloading"))
		printf("")
		printf("%2s%s", "", "-link")
		printf("%4s%s", "", p.Sprintf("upload the actual path of the symbolic-link file/folder"))
		printf("")
		//		printf("")
		//		printf("%2s%s", "", "-delete")
		//		printf("%4s%s", "", "delete objects inconsistent with those on the source from the destination")
		printf("%2s%s", "", "-fr")
		printf("%4s%s", "", p.Sprintf("force to generate the record files"))
		printf("")
		printf("%2s%s", "", "-crr")
		printf("%4s%s", "", "copy the source object(s) through the client-side cross region replication mode")
		printf("")
		printf("%2s%s", "", "-vlength")
		printf("%4s%s", "", p.Sprintf("verify the size after the objects are uploaded or downloaded or copied through crr"))
		printf("")
		printf("%2s%s", "", "-vmd5")
		printf("%4s%s", "", p.Sprintf("verify the MD5 value after the objects are uploaded or downloaded or copied through crr"))
		printf("")
		printf("%2s%s", "", "-cpd=xxx")
		printf("%4s%s", "", p.Sprintf("the directory where the part records reside, used to record the synchronization progress"))
		printf("")
		printf("%2s%s", "", "-j=1")
		printf("%4s%s", "", p.Sprintf("the maximum number of concurrent synchronization jobs, the default value can be set in the config file"))
		printf("")
		printf("%2s%s", "", "-p=1")
		printf("%4s%s", "", p.Sprintf("the maximum number of concurrent synchronization tasks (a task is a sub-job), the default value can be set in the config file"))
		printf("")
		printf("%2s%s", "", "-ps=5242880")
		printf("%4s%s", "", p.Sprintf("the part size of each synchronization task, the default value can be set in the config file"))
		printf("")
		printf("%2s%s", "", "-threshold=52428800")
		printf("%4s%s", "", p.Sprintf("the threshold, if it is exceeded, the synchronization job will be divided into multiple tasks by the part size, the default value can be set in the config file"))
		printf("")
		printf("%2s%s", "", "-meta=aaa:bbb#ccc:ddd")
		printf("%4s%s", "", p.Sprintf("the customized metadata of each object to be uploaded or copied"))
		printf("")
		printf("%2s%s", "", "-acl=xxx")
		printf("%4s%s", "", p.Sprintf("the ACL of each object to be uploaded or copied, possible values are [private|public-read|public-read-write]"))
		printf("")
		printf("%2s%s", "", "-sc=xxx")
		printf("%4s%s", "", p.Sprintf("the storage class of each object to be uploaded or copied, possible values are [standard|warm|cold]"))
		printf("")
		printf("%2s%s", "", "-include=*.xxx")
		printf("%4s%s", "", p.Sprintf("the objects whose names match this pattern will be included"))
		printf("")
		printf("%2s%s", "", "-exclude=*.xxx")
		printf("%4s%s", "", p.Sprintf("the objects whose names match this pattern will be excluded"))
		printf("")
		printf("%2s%s", "", "-arcDir=xxx")
		printf("%4s%s", "", p.Sprintf("the archive dir, used to archive the successful uploaded file(s)"))
		printf("")
		printf("%2s%s", "", "-timeRange=time1-time2")
		printf("%4s%s", "", p.Sprintf("the time range, between which the objects will be uploaded, downloaded or copied"))
		printf("")
		printf("%2s%s", "", "-o=xxx")
		printf("%4s%s", "", p.Sprintf("the output dir, used to record the synchronization results"))
		printf("")
		printf("%2s%s", "", "-config=xxx")
		printf("%4s%s", "", p.Sprintf("the path to the custom config file when running this command"))
		printf("")
	}
	return c
}
