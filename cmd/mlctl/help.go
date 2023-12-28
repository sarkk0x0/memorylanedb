package main

import (
	"fmt"
	"strings"
)

type HelpCommand struct {
}

func NewHelpCommand() *HelpCommand {
	hc := &HelpCommand{}
	return hc
}

func (hc *HelpCommand) Name() string {
	return "help"
}

func (hc *HelpCommand) Init(args []string) error {
	return nil
}

func (hc *HelpCommand) Run() error {
	usage := strings.TrimLeft(`
mlctl is a tool for inspecting memorylane databases.

Usage:

	mlctl command [arguments]

The commands are:

	put       	insert a key-value pair in the db
	get       	retrieve value of a key
	list     	list all keys in the db
	info       	print basic info
	help        print this screen
	stats       generate usage stats

Use "mlctl [command] -h" for more information about a command.
`, "\n")
	fmt.Println(usage)
	return nil
}
