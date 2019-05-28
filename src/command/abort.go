package command

import (
	"assist"
	"command/i18n"
	"concurrent"
	"fmt"
	"obs"
	"progress"
	"sync/atomic"
	"time"
)

type abortCommand struct {
	recursiveCommand
	uploadId string
}

func (c *abortCommand) abortMultipartUpload(bucket, key, uploadId string, batchFlag int) bool {

	abortHandler := func() {
		c.failedLogger.doRecord("Bucket [%s], key [%s], upload id [%s], n/a, n/a, error code [%s], error message [%s], n/a", bucket, key, uploadId,
			"AbortError", "Task is aborted")
	}
	actionFunc := func() (output *obs.BaseModel, err error) {
		input := &obs.AbortMultipartUploadInput{}
		input.Bucket = bucket
		input.Key = key
		input.UploadId = uploadId
		return obsClient.AbortMultipartUpload(input)
	}
	recordHandler := func(cost int64, output *obs.BaseModel, err error) {
		if err == nil {
			c.succeedLogger.doRecord("Bucket [%s], key [%s], upload id [%s], cost [%d], status [%d], request id [%s]", bucket, key, uploadId, cost, output.StatusCode, output.RequestId)
		} else {
			status, code, message, requestId := c.checkAbort(err, 401, 405)
			c.failedLogger.doRecord("Bucket [%s], key [%s], upload id [%s], cost [%d], status [%d], error code [%s], error message [%s], request id [%s]", bucket, key, uploadId, cost,
				status, code, message, requestId)
		}
	}
	printHandler := func(cost int64, output *obs.BaseModel, err error) {
		if err == nil {
			printf("Abort multipart upload [%s] in the bucket [%s] successfully, cost [%d], request id [%s]", key, bucket, cost, output.RequestId)
			doLog(LEVEL_INFO, "Abort multipart upload [%s] in the bucket [%s] successfully, cost [%d], request id [%s]", key, bucket, cost, output.RequestId)
		} else {
			logError(err, LEVEL_INFO, fmt.Sprintf("Abort multipart upload [%s] in the bucket [%s] failed, cost [%d]", key, bucket, cost))
		}
	}

	return c.simpleAction(batchFlag, abortHandler, actionFunc, recordHandler, printHandler)
}

func (c *abortCommand) submitAbortTask(bucket, prefix string, pool concurrent.Pool, ch progress.SingleBarChan) (totalCnt int64, hasListError error) {
	input := &obs.ListMultipartUploadsInput{}
	input.Bucket = bucket
	input.Prefix = prefix
	input.MaxUploads = defaultListMaxKeys
	for {
		start := assist.GetUtcNow()
		output, err := obsClient.ListMultipartUploads(input)
		if err != nil {
			hasListError = err
			break
		} else {
			cost := (assist.GetUtcNow().UnixNano() - start.UnixNano()) / 1000000
			doLog(LEVEL_INFO, "List multipart uploads in the bucket [%s] to abort successfully, cost [%d], request id [%s]", bucket, cost, output.RequestId)
		}
		for _, upload := range output.Uploads {
			key := upload.Key
			uploadId := upload.UploadId
			if !c.force && !confirm(fmt.Sprintf("Do you want abort multipart upload [%s] ? Please input (y/n) to confirm:", key)) {
				continue
			}
			atomic.AddInt64(&totalCnt, 1)

			pool.ExecuteFunc(func() interface{} {
				return handleResult(c.abortMultipartUpload(bucket, key, uploadId, 2), ch)
			})
		}

		if !output.IsTruncated {
			doLog(LEVEL_INFO, "List multipart uploads to abort finished, bucket [%s], prefix [%s], marker [%s], uploadIdMarker [%s]", bucket, input.Prefix, input.KeyMarker, input.UploadIdMarker)
			break
		}
		input.KeyMarker = output.NextKeyMarker
		input.UploadIdMarker = output.NextUploadIdMarker
	}
	return
}

func (c *abortCommand) recordStartFunc() time.Time {
	start := c.recordStart()
	c.succeedLogger.doRecord("[%s, %s, %s, %s, %s, %s]", "bucket name", "object key", "upload id", "cost(ms)", "status code", "request id")
	c.failedLogger.doRecord("[%s, %s, %s, %s, %s, %s, %s, %s]", "bucket name", "object key", "upload id", "cost(ms)", "status code", "error code", "error message", "request id")
	return start
}

func (c *abortCommand) abortMultipartUploads(bucket, prefix string) error {
	submitFunc := func(pool concurrent.Pool, ch progress.SingleBarChan) (int64, error) {
		return c.submitAbortTask(bucket, prefix, pool, ch)
	}

	errorHandleFunc := func(hasListError error) {
		logError(hasListError, LEVEL_ERROR, fmt.Sprintf("\nList multipart uploads in the bucket [%s] to abort failed", bucket))
	}

	return c.recursiveAction(bucket, prefix, submitFunc, errorHandleFunc, c.recordStartFunc, true)
}

func initAbort() command {
	c := &abortCommand{}
	c.key = "abort"
	c.usage = "cloud_url [options...]"
	c.description = "abort multipart uploads"

	c.define = func() {
		c.init()
		c.flagSet.BoolVar(&c.recursive, "r", false, "")
		c.flagSet.BoolVar(&c.force, "f", false, "")
		c.flagSet.BoolVar(&c.forceRecord, "fr", false, "")
		c.flagSet.IntVar(&c.jobs, "j", 0, "")
		c.flagSet.StringVar(&c.uploadId, "u", "", "")
		c.flagSet.StringVar(&c.outDir, "o", "", "")
	}

	c.action = func() error {
		confirmFunc := func(bucket, prefix string) bool {
			return confirm(fmt.Sprintf("Do you want abort multipart upload [%s] in the bucket [%s] ? Please input (y/n) to confirm:", prefix, bucket))
		}
		prefixFunc := func(bucket, prefix string, batchFlag int) error {
			if c.abortMultipartUpload(bucket, prefix, c.uploadId, batchFlag) {
				return nil
			}
			return assist.ExecutingError
		}
		recursivePrefixFun := func(bucket, prefix string) error {
			return c.abortMultipartUploads(bucket, prefix)
		}

		return c.chooseAction(nil, nil, confirmFunc, prefixFunc, recursivePrefixFun, c.recordStartFunc)
	}

	c.help = func() {
		p := i18n.GetCurrentPrinter()
		p.Printf("Summary:")
		printf("%2s%s\n", "", p.Sprintf("abort multipart uploads"))
		printf("")
		p.Printf("Syntax 1:")
		printf("%2s%s", "", "obsutil abort obs://bucket/key -u=xxx [-f] [-fr] [-o=xxx] [-config=xxx]")
		printf("")
		p.Printf("Syntax 2:")
		printf("%2s%s", "", "obsutil abort obs://bucket/[prefix] -r [-f] [-o=xxx] [-j=1] [-config=xxx]")
		printf("")

		p.Printf("Options:")
		printf("%2s%s", "", "-u=xxx")
		printf("%4s%s", "", p.Sprintf("the ID of the multipart upload to be aborted"))
		printf("")
		printf("%2s%s", "", "-r")
		printf("%4s%s", "", p.Sprintf("batch abort multipart uploads by prefix"))
		printf("")
		printf("%2s%s", "", "-f")
		printf("%4s%s", "", p.Sprintf("force mode, the progress will not be suspended while a multipart upload is to be aborted"))
		printf("")
		printf("%2s%s", "", "-fr")
		printf("%4s%s", "", p.Sprintf("force to generate the record files when aborting one multipart upload"))
		printf("")
		printf("%2s%s", "", "-j=1")
		printf("%4s%s", "", p.Sprintf("the maximum number of concurrent abort jobs, the default value can be set in the config file"))
		printf("")
		printf("%2s%s", "", "-o=xxx")
		printf("%4s%s", "", p.Sprintf("the output dir, used to record the abort results"))
		printf("")
		printf("%2s%s", "", "-config=xxx")
		printf("%4s%s", "", p.Sprintf("the path to the custom config file when running this command"))
		printf("")
	}

	return c
}
