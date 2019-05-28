package assist

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
)

func EnterBashMode(additionalTips func(), callback func(value string)) {
	fmt.Println("Enter \"exit\" or \"quit\" to logout")
	if additionalTips != nil {
		additionalTips()
	}

	rd := bufio.NewReader(os.Stdin)
	for {
		fmt.Println("Input your command:")
		fmt.Printf("%s", "-->")
		input, err := ReadLine(rd)
		if err == nil {
			command := strings.TrimSpace(BytesToString(input))
			if command != "" {
				if strings.ToLower(command) == "exit" || strings.ToLower(command) == "quit" {
					return
				}
				callback(command)
				fmt.Println()
				continue
			}
		}
		if err == nil || err != io.EOF {
			fmt.Println("Error: Invalid input, please try again")
		}
	}
}
