package command

import (
	"assist"
	"command/i18n"
	"flag"
	"os"
	"os/signal"
	"progress"
	"regexp"
	"runtime/debug"
	"sort"
	"strings"
	"sync/atomic"
)

var commands = make(map[string]command)
var basic []command
var other []command
var basicCommandCnt int
var otherCommandCnt int
var terminalWidth int
var runningRound int = 0

type NilWriter struct {
}

func (NilWriter) Write(p []byte) (n int, err error) {
	if p != nil {
		return len(p), nil
	}
	return 0, nil
}

var nilWriter = &NilWriter{}

func initFlagSet() *flag.FlagSet {
	flagSet := flag.NewFlagSet("obsutil", flag.ContinueOnError)
	flagSet.SetOutput(nilWriter)
	return flagSet
}

func register(c command) {
	if c != nil {
		commands[c.getKey()] = c
		if c.getAdditional() {
			otherCommandCnt += 1
		} else {
			basicCommandCnt += 1
		}
	}
}

func initCommands() {
	commandFuncs := []func() command{
		initVersion,
		initHelp,
		initConfig,
		initLs,
		initMb,
		initRm,
		initStat,
		initCp,
		initSync,
		initRestore,
		initAbort,
		initClear,
		initChattri,
		initArchive,
		initMkdir,
		initSign,
		initMv,
	}

	for _, commandFunc := range commandFuncs {
		register(commandFunc())
	}

	basicKeys := make([]string, 0, basicCommandCnt)
	otherKeys := make([]string, 0, otherCommandCnt)

	for k, c := range commands {
		if c.getAdditional() {
			otherKeys = append(otherKeys, k)
		} else {
			basicKeys = append(basicKeys, k)
		}
	}

	sort.Strings(basicKeys)
	sort.Strings(otherKeys)

	basic = make([]command, 0, len(basicKeys))
	other = make([]command, 0, len(otherKeys))

	for _, k := range basicKeys {
		basic = append(basic, commands[k])
	}

	for _, k := range otherKeys {
		other = append(other, commands[k])
	}
}

func printCommands(commands []command, p *i18n.PrinterWrapper) {
	for _, c := range commands {
		if usages, ok := c.getUsage().([]string); ok && len(usages) > 0 {
			printf("%2s%-8s%-30s", "", c.getKey(), usages[0])
			for _, usage := range usages[1:] {
				printf("%10s%-30s", "", usage)
			}
		} else if usage, ok := c.getUsage().(string); ok {
			printf("%2s%-8s%-30s", "", c.getKey(), usage)
		} else {
			printf("%2s%-8s%-30s", "", c.getKey(), "")
		}
		printf("%-10s%-30s", "", c.getDescription(p))
		printf("")
	}
}

func usage() {
	p := i18n.GetCurrentPrinter()
	printf(p.Sprintf("Usage:") + " obsutil [command] [args...] [options...]")
	p.Printf("You can use \"obsutil help command\" to view the specific help of each command")

	printf("")
	p.Printf("Basic commands:")
	printCommands(basic, p)
	printf("")
	p.Printf("Other commands:")
	printCommands(other, p)
}

func runCommand(args []string, additionalAction func(c command)) error {
	cmd := args[0]
	if c, ok := commands[cmd]; ok {
		if additionalAction != nil {
			additionalAction(c)
		}
		return c.parse(args[1:])
	}
	printf("Error: No such command: \"%s\", please try \"help\" for more information!", cmd)
	return assist.InvalidArgsError
}

func logUserInput(inputs []string, c command) {
	userInput := strings.Join(inputs, " ")
	if _, ok := c.(*configCommand); !ok {
		doLog(LEVEL_INFO, "User input command \"%s\"", userInput)
		return
	}
	userInput += " "
	userInput = cleanUpAkRegex1.ReplaceAllString(userInput, "-i=xxx ")
	userInput = cleanUpAkRegex2.ReplaceAllString(userInput, "-i xxx ")
	userInput = cleanUpSkRegex1.ReplaceAllString(userInput, "-k=xxx ")
	userInput = cleanUpSkRegex2.ReplaceAllString(userInput, "-k xxx ")
	userInput = cleanUpTokenRegex1.ReplaceAllString(userInput, "-t=xxx ")
	userInput = cleanUpTokenRegex2.ReplaceAllString(userInput, "-t xxx ")
	doLog(LEVEL_INFO, "User input command \"%s\"", userInput)
}

func setCurrentLanguage() {
	i18n.SetCurrentLanguage(strings.ToLower(config["helpLanguage"]))
}

func Run() {
	if err := InitConfigFile(GetDefaultConfig(), true); err == nil {
		var exitErr error
		i18n.SetI18nStrings()
		setCurrentLanguage()

		var exitFlag int32 = 0

		//handle unexpected error
		defer func() {
			r := recover()
			if r != nil {
				printf("Unexpect error, please collect the logs and contact our support, %v", r)
				printf("%s", debug.Stack())
				doLog(LEVEL_ERROR, "%v", r)
				doLog(LEVEL_ERROR, "%s", debug.Stack())
			}

			if atomic.CompareAndSwapInt32(&exitFlag, 0, 1) {
				doClean()
			}
			assist.CheckErrorAndExit(exitErr)
		}()

		progress.InitCustomizeElements(config["colorfulProgress"] == "true")
		initCommands()

		if _terminalWidth, err := assist.GetTerminalWidth(); err == nil && _terminalWidth > 80 {
			terminalWidth = _terminalWidth
		} else {
			terminalWidth = 80
		}

		args := os.Args[1:]
		if len(args) <= 0 && !assist.IsWindows() {
			usage()
			exitErr = assist.InvalidArgsError
			return
		}

		if len(args) <= 0 {
			splitRegex := regexp.MustCompile("\\s+")
			ch := make(chan os.Signal, 1)
			signal.Notify(ch, os.Interrupt, os.Kill)
			var currentCommand command
			go func() {
				<-ch
				if currentCommand != nil {
					if _c, ok := currentCommand.(taskRecorder); ok && _c.getTaskId() != "" {
						printf("\nTask id is: %s", _c.getTaskId())
					}
				}

				if atomic.CompareAndSwapInt32(&exitFlag, 0, 1) {
					doClean()
				}
				assist.CheckErrorAndExit(assist.InterruptedError)
			}()

			additionalTips := func() {
				printf("Enter \"help\" or \"help command\" to show help docs")
			}

			callback := func(input string) {
				inputs := splitRegex.Split(input, -1)
				if len(inputs) == 1 && inputs[0] == "obsutil" {
					printf("Error: Invalid input, please try again")
					return
				}

				originInputs := inputs
				if inputs[0] == "obsutil" {
					inputs = inputs[1:]
				}

				progress.ResetContext()
				runningRound += 1

				runCommand(inputs, func(c command) {
					logUserInput(originInputs, c)
					currentCommand = c
				})
			}
			assist.EnterBashMode(additionalTips, callback)
			return
		}

		exitErr = runCommand(args, func(c command) {
			logUserInput(os.Args, c)

			ch := make(chan os.Signal, 1)
			signal.Notify(ch, os.Interrupt, os.Kill)
			go func() {
				<-ch
				if _c, ok := c.(taskRecorder); ok {
					_c.printTaskId()
				}

				if atomic.CompareAndSwapInt32(&exitFlag, 0, 1) {
					doClean()
				}

				assist.CheckErrorAndExit(assist.InterruptedError)
			}()
		})
	} else {
		printError(err)
		assist.CheckErrorAndExit(assist.InitializingError)
	}

}
