package command

import (
	"assist"
	"command/i18n"
	"concurrent"
	"fmt"
	"obs"
	"sync"
	"sync/atomic"
)

type lsCommand struct {
	cloudUrlCommand
	short           bool
	storageClass    bool
	dir             bool
	multipart       bool
	all             bool
	version         bool
	jobs            int
	limit           int64
	marker          string
	versionIdMarker string
	uploadIdMarker  string
}

type storageClassResult struct {
	storageClass string
	bucket       string
	err          error
}

func (c *lsCommand) getBucketStorageClass(bucket string) *storageClassResult {
	input := &obs.GetBucketMetadataInput{}
	input.Bucket = bucket
	output, err := obsClient.GetBucketMetadata(input)
	if err == nil {
		return &storageClassResult{bucket: bucket, storageClass: transStorageClassType(output.StorageClass)}
	}
	return &storageClassResult{err: err}
}

func (c *lsCommand) listBuckets() error {
	if c.limit == 0 {
		printf("Bucket Number is: %d", 0)
		return nil
	}

	input := &obs.ListBucketsInput{}
	input.QueryLocation = true
	output, err := obsClient.ListBuckets(input)

	if err == nil {
		count := int64(len(output.Buckets))
		if c.limit > 0 && c.limit < count {
			output.Buckets = output.Buckets[:c.limit]
		}

		if c.short {
			for _, val := range output.Buckets {
				printf("obs://%s", val.Name)
			}
		} else {
			if c.storageClass {
				storageClasses := make(map[string]string, len(output.Buckets))
				if c.jobs <= 0 {
					c.jobs = assist.MaxInt(assist.StringToInt(config["defaultJobs"], defaultJobs), 1)
				}

				poolCacheCount := assist.StringToInt(config["defaultJobsCacheCount"], defaultJobsCacheCount)
				futureChan := make(chan concurrent.Future, poolCacheCount)
				pool := concurrent.NewRoutinePool(c.jobs, poolCacheCount)
				var wg *sync.WaitGroup = new(sync.WaitGroup)
				wg.Add(1)
				go func() {
					for {
						future, ok := <-futureChan
						if !ok {
							break
						}
						result := future.Get().(*storageClassResult)
						if result.err == nil {
							storageClasses[result.bucket] = result.storageClass
						}
					}
					wg.Done()
				}()

				h := &assist.Hint{}
				h.Message = "Querying the storage classes of buckets"
				h.Start()

				for _, val := range output.Buckets {
					bucket := val.Name
					future, err := pool.SubmitFunc(func() interface{} {
						return c.getBucketStorageClass(bucket)
					})
					if err != nil {
						doLog(LEVEL_ERROR, "Submit task to pool failed bucket [%s], %s", bucket, err.Error())
					} else if future != nil {
						futureChan <- future
					}
				}

				close(futureChan)
				pool.ShutDown()
				wg.Wait()
				h.End()

				format := "%-10s%-20s%-30s%-20s"
				printf(format, "Bucket", "StorageClass", "CreationDate", "Location")
				for _, val := range output.Buckets {
					bucketName := "obs://" + val.Name
					printf("%s", bucketName)
					printf(format, "", storageClasses[val.Name], val.CreationDate.Format(ISO8601_DATE_FORMAT), val.Location)
					printf("")
				}

			} else {
				format := "%-30s%-30s%-20s"
				printf(format, "Bucket", "CreationDate", "Location")
				for _, val := range output.Buckets {
					bucketName := "obs://" + val.Name
					if len(bucketName) >= 30 {
						printf("%s", bucketName)
						printf(format, "", val.CreationDate.Format(ISO8601_DATE_FORMAT), val.Location)
					} else {
						printf(format, bucketName, val.CreationDate.Format(ISO8601_DATE_FORMAT), val.Location)
					}
					printf("")
				}
			}
		}
		printf("Bucket number is: %d", len(output.Buckets))
		return nil
	}
	logError(err, LEVEL_INFO, "List buckets failed")
	return assist.ExecutingError
}

func (c *lsCommand) getObjectsResult(output *obs.ListObjectsOutput) ([]string, []obs.Content) {
	folders := make([]string, 0, len(output.CommonPrefixes))
	objects := make([]obs.Content, 0, len(output.Contents))

	folders = append(folders, output.CommonPrefixes...)
	for _, content := range output.Contents {
		if isObsFolder(content.Key) {
			folders = append(folders, content.Key)
		} else {
			objects = append(objects, content)
		}
	}
	return folders, objects
}

func (c *lsCommand) getVersionsResult(output *obs.ListVersionsOutput) ([]string, []obs.Version, []obs.DeleteMarker) {
	folders := make([]string, 0, len(output.CommonPrefixes))
	versions := make([]obs.Version, 0, len(output.Versions))

	folders = append(folders, output.CommonPrefixes...)
	for _, version := range output.Versions {
		if isObsFolder(version.Key) {
			folders = append(folders, version.Key)
		} else {
			versions = append(versions, version)
		}
	}
	return folders, versions, output.DeleteMarkers
}

func (c *lsCommand) listVersions(bucket, prefix string) error {
	input := &obs.ListVersionsInput{}
	input.Bucket = bucket
	input.Prefix = prefix
	input.KeyMarker = c.marker
	input.VersionIdMarker = c.versionIdMarker
	if c.dir {
		input.Delimiter = "/"
		if input.Prefix != "" && !isObsFolder(input.Prefix) {
			input.Prefix += "/"
		}
	}
	limit := c.limit
	count := limit
	if count <= 0 {
		count = 1000
	}
	truncated := false
	totalFolders := make([]string, 0, count)
	totalVersions := make([]obs.Version, 0, count)
	totalDeleteMarkers := make([]obs.DeleteMarker, 0, count)
	nextKeyMarker := ""
	nextVersionIdMarker := ""
	var totalCount int64 = 0
	h := &assist.HintV2{}
	h.MessageFunc = func() string {
		count := ""
		if tc := atomic.LoadInt64(&totalCount); tc > 0 {
			count = "[" + assist.Int64ToString(tc) + "]"
		}
		return fmt.Sprintf("Listing versioning objects %s", count)
	}
	h.Start()
	var hasListError error
	for {
		if limit > 0 {
			if limit <= 1000 {
				input.MaxKeys = int(limit)
				truncated = true
			} else {
				input.MaxKeys = 1000
				limit -= 1000
			}
		}

		output, err := obsClient.ListVersions(input)
		if err != nil {
			hasListError = err
			break
		}

		atomic.AddInt64(&totalCount, int64(len(output.Versions)))
		atomic.AddInt64(&totalCount, int64(len(output.DeleteMarkers)))
		atomic.AddInt64(&totalCount, int64(len(output.CommonPrefixes)))

		folders, versions, deleteMarkers := c.getVersionsResult(output)
		totalFolders = append(totalFolders, folders...)
		totalVersions = append(totalVersions, versions...)
		totalDeleteMarkers = append(totalDeleteMarkers, deleteMarkers...)
		if !output.IsTruncated {
			break
		}

		if truncated {
			nextKeyMarker = output.NextKeyMarker
			nextVersionIdMarker = output.NextVersionIdMarker
			break
		}

		input.KeyMarker = output.NextKeyMarker
		input.VersionIdMarker = output.NextVersionIdMarker
	}

	h.End()

	if hasListError != nil {
		printError(hasListError)
		return assist.UncompeletedError
	}

	totalFoldersNumber := len(totalFolders)
	totalVersionsNumber := len(totalVersions)
	totalDeleteMarkersNumber := len(totalDeleteMarkers)
	var totalSize int64 = 0
	if c.short {
		if totalFoldersNumber > 0 {
			printf("Folder list:")
			for _, prefix := range totalFolders {
				printf("obs://%s/%s", bucket, prefix)
			}
			printf("")
		}
		if totalVersionsNumber > 0 {
			printf("Versioning Object list:")
			for _, val := range totalVersions {
				printf("obs://%s/%s", bucket, val.Key)
				totalSize += val.Size
			}
			printf("")
		}
		if totalDeleteMarkersNumber > 0 {
			printf("DeleteMarker list:")
			for _, val := range totalDeleteMarkers {
				printf("obs://%s/%s", bucket, val.Key)
			}
			printf("")
		}
	} else {
		if totalFoldersNumber > 0 {
			printf("Folder list:")
			for _, prefix := range totalFolders {
				printf("obs://%s/%s", bucket, prefix)
			}
			printf("")
		}
		if totalVersionsNumber > 0 {
			printf("Versioning Object list:")
			printf("%-30s%-40s%-30s%-10s%-20s%-20s", "Key", "VersionId", "LastModified", "Size", "StorageClass", "ETag")
			for _, val := range totalVersions {
				objectKey := "obs://" + bucket + "/" + val.Key
				printf("%s", objectKey)
				printf("%-30s%-40s%-30s%-10s%-20s%-20s", "", val.VersionId, val.LastModified.Format(ISO8601_DATE_FORMAT),
					assist.NormalizeBytes(val.Size), transStorageClassType(val.StorageClass), val.ETag)
				printf("")
				totalSize += val.Size
			}
		}
		if totalDeleteMarkersNumber > 0 {
			printf("DeleteMarker list:")
			printf("%-30s%-40s%-30s%-20s", "Key", "VersionId", "LastModified", "StorageClass")
			for _, val := range totalDeleteMarkers {
				objectKey := "obs://" + bucket + "/" + val.Key
				printf("%s", objectKey)
				printf("%-30s%-40s%-30s%-20s", "", val.VersionId, val.LastModified.Format(ISO8601_DATE_FORMAT),
					transStorageClassType(val.StorageClass))
				printf("")
			}
		}
	}

	if nextKeyMarker != "" || nextVersionIdMarker != "" {
		printf("Next key marker is: %s", nextKeyMarker)
		printf("Next version id marker is: %s", nextVersionIdMarker)
	} else if !c.dir {
		if prefix == "" {
			printf("Total size of bucket is: %s", assist.NormalizeBytes(totalSize))
		} else {
			printf("Total size of prefix [%s] is: %s", prefix, assist.NormalizeBytes(totalSize))
		}
	}

	printf("Folder number is: %d", totalFoldersNumber)
	printf("Versioning Object number is: %d", totalVersionsNumber)
	printf("DeleteMarker number is: %d", totalDeleteMarkersNumber)
	return nil
}

func (c *lsCommand) listObjects(bucket, prefix string) error {
	input := &obs.ListObjectsInput{}
	input.Bucket = bucket
	input.Prefix = prefix
	input.Marker = c.marker
	if c.dir {
		input.Delimiter = "/"
		if input.Prefix != "" && !isObsFolder(input.Prefix) {
			input.Prefix += "/"
		}
	}

	limit := c.limit
	truncated := false

	count := limit
	if count <= 0 {
		count = 1000
	}

	totalFolders := make([]string, 0, count)
	totalObjects := make([]obs.Content, 0, count)
	nextMarker := ""
	var totalCount int64 = 0
	h := &assist.HintV2{}
	h.MessageFunc = func() string {
		count := ""
		if tc := atomic.LoadInt64(&totalCount); tc > 0 {
			count = "[" + assist.Int64ToString(tc) + "]"
		}
		return fmt.Sprintf("Listing objects %s", count)
	}
	h.Start()
	var hasListError error
	for {
		if limit > 0 {
			if limit <= 1000 {
				input.MaxKeys = int(limit)
				truncated = true
			} else {
				input.MaxKeys = 1000
				limit -= 1000
			}
		}

		output, err := obsClient.ListObjects(input)
		if err != nil {
			hasListError = err
			break
		}

		atomic.AddInt64(&totalCount, int64(len(output.Contents)))
		atomic.AddInt64(&totalCount, int64(len(output.CommonPrefixes)))

		folders, objects := c.getObjectsResult(output)
		totalFolders = append(totalFolders, folders...)
		totalObjects = append(totalObjects, objects...)
		if !output.IsTruncated {
			break
		}

		if truncated {
			nextMarker = output.NextMarker
			break
		}

		input.Marker = output.NextMarker
	}

	h.End()

	if hasListError != nil {
		printError(hasListError)
		return assist.UncompeletedError
	}

	totalFolderNumber := len(totalFolders)
	totalObjectNumber := len(totalObjects)
	var totalSize int64 = 0
	if c.short {
		if totalFolderNumber > 0 {
			printf("Folder list:")
			for _, prefix := range totalFolders {
				printf("obs://%s/%s", bucket, prefix)
			}
			printf("")
		}

		if totalObjectNumber > 0 {
			printf("Object list:")
			for _, val := range totalObjects {
				printf("obs://%s/%s", bucket, val.Key)
				totalSize += val.Size
			}
			printf("")
		}
	} else {
		if totalFolderNumber > 0 {
			printf("Folder list:")
			for _, prefix := range totalFolders {
				printf("obs://%s/%s", bucket, prefix)
			}
			printf("")
		}

		if totalObjectNumber > 0 {
			printf("Object list:")
			printf("%-50s%-30s%-10s%-20s%-20s", "key", "LastModified", "Size", "StorageClass", "ETag")
			for _, val := range totalObjects {
				objectKey := "obs://" + bucket + "/" + val.Key
				if len(objectKey) >= 50 || assist.HasUnicode(objectKey) {
					printf("%s", objectKey)
					printf("%-50s%-30s%-10s%-20s%-20s", "", val.LastModified.Format(ISO8601_DATE_FORMAT),
						assist.NormalizeBytes(val.Size), transStorageClassType(val.StorageClass), val.ETag)
					printf("")
				} else {
					printf("%-50s%-30s%-10s%-20s%-20s", objectKey, val.LastModified.Format(ISO8601_DATE_FORMAT),
						assist.NormalizeBytes(val.Size), transStorageClassType(val.StorageClass), val.ETag)
					printf("")
				}
				totalSize += val.Size
			}
		}
	}

	if nextMarker != "" {
		printf("Next marker is: %s", nextMarker)
	} else if !c.dir {
		if prefix == "" {
			printf("Total size of bucket is: %s", assist.NormalizeBytes(totalSize))
		} else {
			printf("Total size of prefix [%s] is: %s", prefix, assist.NormalizeBytes(totalSize))
		}
	}

	printf("Folder number is: %d", totalFolderNumber)
	printf("Object number is: %d", totalObjectNumber)
	return nil
}

func (c *lsCommand) getUploadsResult(output *obs.ListMultipartUploadsOutput) ([]string, []obs.Upload) {
	folders := make([]string, 0, len(output.CommonPrefixes))
	uploads := make([]obs.Upload, 0, len(output.Uploads))

	folders = append(folders, output.CommonPrefixes...)
	for _, upload := range output.Uploads {
		if isObsFolder(upload.Key) {
			folders = append(folders, upload.Key)
		} else {
			uploads = append(uploads, upload)
		}
	}
	return folders, uploads

}

func (c *lsCommand) listMultipartUploads(bucket, prefix string) error {
	input := &obs.ListMultipartUploadsInput{}
	input.Bucket = bucket
	input.Prefix = prefix
	input.KeyMarker = c.marker
	input.UploadIdMarker = c.uploadIdMarker
	if c.dir {
		input.Delimiter = "/"
		if input.Prefix != "" && !isObsFolder(input.Prefix) {
			input.Prefix += "/"
		}
	}
	limit := c.limit

	count := limit
	if count <= 0 {
		count = 1000
	}

	truncated := false
	totalFolders := make([]string, 0, count)
	totalUploads := make([]obs.Upload, 0, count)
	nextKeyMarker := ""
	nextUploadIdMarker := ""
	var totalCount int64 = 0
	h := &assist.HintV2{}
	h.MessageFunc = func() string {
		return fmt.Sprintf("Listing multipart uploads [%d]", atomic.LoadInt64(&totalCount))
	}
	h.Start()
	var hasListError error
	for {
		if limit > 0 {
			if limit <= 1000 {
				input.MaxUploads = int(limit)
				truncated = true
			} else {
				input.MaxUploads = 1000
				limit -= 1000
			}
		}
		output, err := obsClient.ListMultipartUploads(input)
		if err != nil {
			hasListError = err
			break
		}

		atomic.AddInt64(&totalCount, int64(len(output.Uploads)))
		atomic.AddInt64(&totalCount, int64(len(output.CommonPrefixes)))

		folders, uploads := c.getUploadsResult(output)
		totalFolders = append(totalFolders, folders...)
		totalUploads = append(totalUploads, uploads...)
		if !output.IsTruncated {
			break
		}
		if truncated {
			nextKeyMarker = output.NextKeyMarker
			nextUploadIdMarker = output.NextUploadIdMarker
			break
		}

		input.KeyMarker = output.NextKeyMarker
		input.UploadIdMarker = output.NextUploadIdMarker
	}

	h.End()

	if hasListError != nil {
		printError(hasListError)
		return assist.UncompeletedError
	}

	totalFoldersNumber := len(totalFolders)
	totalUploadsNumber := len(totalUploads)
	if c.short {
		if totalFoldersNumber > 0 {
			printf("Folder list:")
			for _, prefix := range totalFolders {
				printf("obs://%s/%s", bucket, prefix)
			}
			printf("")
		}
		if totalUploadsNumber > 0 {
			printf("Upload list:")
			for _, val := range totalUploads {
				printf("obs://%s/%s %s", bucket, val.Key, val.UploadId)
			}
			printf("")
		}
	} else {
		if totalFoldersNumber > 0 {
			printf("Folder list:")
			for _, prefix := range totalFolders {
				printf("obs://%s/%s", bucket, prefix)
			}
			printf("")
		}
		if totalUploadsNumber > 0 {
			printf("Upload list:")
			printf("%-50s%-30s%-20s%-20s", "Key", "Initiated", "StorageClass", "UploadId")
			for _, val := range totalUploads {
				objectKey := "obs://" + bucket + "/" + val.Key
				if len(objectKey) >= 50 || assist.HasUnicode(objectKey) {
					printf("%s", objectKey)
					printf("%-50s%-30s%-20s%-20s", "", val.Initiated.Format(ISO8601_DATE_FORMAT),
						transStorageClassType(val.StorageClass), val.UploadId)
					printf("")
				} else {
					printf("%-50s%-30s%-20s%-20s", objectKey, val.Initiated.Format(ISO8601_DATE_FORMAT),
						transStorageClassType(val.StorageClass), val.UploadId)
					printf("")
				}
			}
		}
	}

	if nextKeyMarker != "" || nextUploadIdMarker != "" {
		printf("Next keyMarker is: %s", nextKeyMarker)
		printf("Next uploadIdMarker is: %s", nextUploadIdMarker)
	}

	printf("Folder number is: %d", totalFoldersNumber)
	printf("Upload number is: %d", totalUploadsNumber)
	return nil
}

func initLs() command {

	c := &lsCommand{}
	c.key = "ls"
	c.usage = "[cloud_url] [options...]"
	c.description = "list buckets or objects/multipart uploads in a bucket"

	c.define = func() {
		c.flagSet.BoolVar(&c.short, "s", false, "")
		c.flagSet.BoolVar(&c.storageClass, "sc", false, "")
		c.flagSet.BoolVar(&c.dir, "d", false, "")
		c.flagSet.BoolVar(&c.multipart, "m", false, "")
		c.flagSet.BoolVar(&c.all, "a", false, "")
		c.flagSet.BoolVar(&c.version, "v", false, "")
		c.flagSet.StringVar(&c.marker, "marker", "", "")
		c.flagSet.StringVar(&c.versionIdMarker, "versionIdMarker", "", "")
		c.flagSet.StringVar(&c.uploadIdMarker, "uploadIdMarker", "", "")
		c.flagSet.Int64Var(&c.limit, "limit", 1000, "")
		c.flagSet.IntVar(&c.jobs, "j", 0, "")
	}

	c.emptyArgsAction = func() error {
		c.printStart()
		return c.listBuckets()
	}

	c.action = func() error {
		cloudUrl, err := c.prepareCloudUrl()
		if err == emptyArgsError {
			return c.emptyArgsAction()
		}

		if err != nil {
			printError(err)
			return assist.InvalidArgsError
		}

		bucket, prefix, err := c.splitCloudUrl(cloudUrl)
		if err != nil {
			printError(err)
			return assist.InvalidArgsError
		}

		c.printStart()

		if c.all {
			var ret error
			if c.version {
				ret = c.listVersions(bucket, prefix)
			} else {
				ret = c.listObjects(bucket, prefix)
			}
			if ret != nil {
				return ret
			}
			printf("")
			return c.listMultipartUploads(bucket, prefix)
		}
		if c.multipart {
			return c.listMultipartUploads(bucket, prefix)
		}
		if c.version {
			return c.listVersions(bucket, prefix)
		}
		return c.listObjects(bucket, prefix)
	}

	c.help = func() {
		p := i18n.GetCurrentPrinter()
		p.Printf("Summary:")
		printf("%2s%s", "", p.Sprintf("list buckets or objects/multipart uploads in a bucket"))
		printf("")
		p.Printf("Syntax 1:")
		printf("%2s%s", "", "obsutil ls [-s] [-sc] [-j=1] [-limit=1] [-config=xxx]")
		printf("")
		p.Printf("Syntax 2:")
		printf("%2s%s", "", "obsutil ls obs://bucket[/prefix] [-s] [-d] [-v] [-marker=xxx] [-versionIdMarker=xxx] [-limit=1] [-config=xxx]")
		printf("")
		p.Printf("Syntax 3:")
		printf("%2s%s", "", "obsutil ls obs://bucket[/prefix] [-s] [-d] [-v] -m [-a] [-uploadIdMarker=xxx] [-marker=xxx] [-versionIdMarker=xxx] [-limit=1] [-config=xxx]")
		printf("")

		p.Printf("Options:")
		printf("%2s%s", "", "-s")
		printf("%4s%s", "", p.Sprintf("show results in brief mode"))
		printf("")
		printf("%2s%s", "", "-sc")
		printf("%4s%s", "", p.Sprintf("show storage class of each bucket"))
		printf("")
		printf("%2s%s", "", "-j=1")
		printf("%4s%s", "", p.Sprintf("the maximum number of concurrent jobs for querying the storage classes of buckets, the default value can be set in the config file"))
		printf("")
		printf("%2s%s", "", "-d")
		printf("%4s%s", "", p.Sprintf("list objects and sub-folders in the current folder"))
		printf("")
		printf("%2s%s", "", "-v")
		printf("%4s%s", "", p.Sprintf("list versions of objects in a bucket"))
		printf("")
		printf("%2s%s", "", "-m")
		printf("%4s%s", "", p.Sprintf("list multipart uploads"))
		printf("")
		printf("%2s%s", "", "-a")
		printf("%4s%s", "", p.Sprintf("list both objects and multipart uploads"))
		printf("")
		printf("%2s%s", "", "-marker=xxx")
		printf("%4s%s", "", p.Sprintf("the marker to list objects or multipart uploads"))
		printf("")
		printf("%2s%s", "", "-versionIdMarker=xxx")
		printf("%4s%s", "", p.Sprintf("the version ID marker to list versions of objects"))
		printf("")
		printf("%2s%s", "", "-uploadIdMarker=xxx")
		printf("%4s%s", "", p.Sprintf("the upload ID marker to list multipart uploads"))
		printf("")
		printf("%2s%s", "", "-limit=1000")
		printf("%4s%s", "", p.Sprintf("show results by limited number"))
		printf("")
		printf("%2s%s", "", "-config=xxx")
		printf("%4s%s", "", p.Sprintf("the path to the custom config file when running this command"))
		printf("")
	}
	return c
}
