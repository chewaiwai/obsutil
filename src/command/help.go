package command

import (
	"assist"
	"command/i18n"
)

func initHelp() command {

	c := &defaultCommand{
		key:         "help",
		usage:       "[command]",
		description: "view command help information",
		additional:  true,
	}

	c.action = func() error {
		args := c.flagSet.Args()
		length := len(args)
		if length <= 0 {
			usage()
			return nil
		}
		if len(args) > 1 {
			c.showHelp()
			printf("Error: Invalid args, please refer to help doc")
			return assist.InvalidArgsError
		}
		if command, ok := commands[args[0]]; ok && command.getHelp() != nil {
			command.getHelp()()
			return nil
		}
		printf("Error: No such command: \"%s\", please try \"help\" for more information!", args[0])
		return assist.InvalidArgsError
	}

	c.help = func() {
		p := i18n.GetCurrentPrinter()
		p.Printf("Summary:")
		printf("%2s%s", "", p.Sprintf("view the commands supported by this tool or the help information of a specific command"))
		printf("")
		p.Printf("Syntax:")
		printf("%2s%s", "", "obsutil help [command]")
		printf("")
	}

	return c
}
