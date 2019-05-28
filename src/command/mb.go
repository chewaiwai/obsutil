package command

import (
	"assist"
	"command/i18n"
	"fmt"
	"obs"
)

type mbCommand struct {
	cloudUrlCommand
	fs       bool
	acl      string
	sc       string
	location string
	az       string
}

func initMb() command {
	c := &mbCommand{}
	c.key = "mb"
	c.usage = "cloud_url [options...]"
	c.description = "create a bucket with the specified parameters"

	c.define = func() {
		c.flagSet.BoolVar(&c.fs, "fs", false, "")
		c.flagSet.StringVar(&c.acl, "acl", "", "")
		c.flagSet.StringVar(&c.sc, "sc", "", "")
		c.flagSet.StringVar(&c.az, "az", "", "")
		c.flagSet.StringVar(&c.location, "location", "", "")
	}

	c.additionalValidate = func(cloudUrl string) bool {
		return cloudUrlRegex.MatchString(cloudUrl)
	}

	c.action = func() error {
		cloudUrl, err := c.prepareCloudUrl()
		if err != nil {
			printError(err)
			return assist.InvalidArgsError
		}

		c.printStart()

		bucket := cloudUrl[6:]
		input := &obs.CreateBucketInput{}
		input.Bucket = bucket
		if c.location != "" {
			input.Location = c.location
		}

		if aclType, succeed := getAclType(c.acl); !succeed {
			return assist.InvalidArgsError
		} else {
			input.ACL = aclType
		}

		if storageClassType, succeed := getStorageClassType(c.sc); !succeed {
			return assist.InvalidArgsError
		} else {
			input.StorageClass = storageClassType
		}

		if availableZone, succeed := getAvailableZoneType(c.az); !succeed {
			return assist.InvalidArgsError
		} else {
			input.AvailableZone = availableZone
		}

		var output *obs.BaseModel
		if c.fs {
			newBucketInput := &obs.NewBucketInput{}
			newBucketInput.CreateBucketInput = *input
			output, err = obsClient.NewBucket(newBucketInput)
		} else {
			output, err = obsClient.CreateBucket(input)
		}
		if err == nil {
			printf("Create bucket [%s] successfully, request id [%s]", bucket, output.RequestId)
			doLog(LEVEL_INFO, "Create bucket [%s] successfully, request id [%s]", bucket, output.RequestId)
			printf("Notice: If the configured endpoint is a global domain name, " +
				"you may need to wait serveral minutes before performing uploading operations on the created bucket. " +
				"Therefore, configure the endpoint to a regional domain name if you want instant uploading operations on the bucket.")
			return nil
		}
		logError(err, LEVEL_INFO, fmt.Sprintf("Create bucket [%s] failed", bucket))
		return assist.ExecutingError
	}

	c.help = func() {
		p := i18n.GetCurrentPrinter()
		p.Printf("Summary:")
		printf("%2s%s", "", p.Sprintf("create a bucket with the specified parameters"))
		printf("")
		p.Printf("Syntax:")
		printf("%2s%s", "", "obsutil mb obs://bucket [-fs] [-az=xxx] [-acl=xxx] [-sc=xxx] [-location=xxx] [-config=xxx]")
		printf("")

		p.Printf("Options:")
		printf("%2s%s", "", "-fs")
		printf("%4s%s", "", p.Sprintf("create a bucket that supports POSIX"))
		printf("")
		printf("%2s%s", "", "-az=xxx")
		printf("%4s%s", "", p.Sprintf("the AZ of the bucket, possible values are [multi-az]"))
		printf("")
		printf("%2s%s", "", "-acl=xxx")
		printf("%4s%s", "", p.Sprintf("the ACL of the bucket, possible values are [private|public-read|public-read-write]"))
		printf("")
		printf("%2s%s", "", "-sc=xxx")
		printf("%4s%s", "", p.Sprintf("the default storage class of the bucket, possible values are [standard|warm|cold]"))
		printf("")
		printf("%2s%s", "", "-location=xxx")
		printf("%4s%s", "", p.Sprintf("the region where the bucket is located"))
		printf("")
		printf("%2s%s", "", "-config=xxx")
		printf("%4s%s", "", p.Sprintf("the path to the custom config file when running this command"))
		printf("")
	}

	return c
}
