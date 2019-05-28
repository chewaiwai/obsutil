package command

import (
	"assist"
	"command/i18n"
	"obs"
)

const obsUtilVersion = "5.1.4"

func initVersion() command {

	c := &defaultCommand{
		key:         "version",
		description: "show version",
		additional:  true,
	}

	c.action = func() error {
		args := c.flagSet.Args()
		if len(args) > 0 {
			c.showHelp()
			printf("Error: Invalid args: %v", args)
			return assist.InvalidArgsError
		}
		printf("obsutil version:%s, obssdk version:%s", obsUtilVersion, obs.OBS_SDK_VERSION)
		printf("operating system:%s, arch:%s", assist.GetOS(), assist.GetArch())
		return nil
	}

	c.help = func() {
		p := i18n.GetCurrentPrinter()
		p.Printf("Summary:")
		printf("%2s%s", "", p.Sprintf("show the version of this tool"))
		printf("")
		p.Printf("Syntax:")
		printf("%2s%s", "", "obsutil version")
		printf("")
	}

	return c
}
