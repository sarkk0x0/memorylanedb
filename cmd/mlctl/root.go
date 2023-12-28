package main

import (
	"errors"
)

var ErrInvalidArgs = errors.New("invalid arguments")

type SubCommand interface {
	Init([]string) error
	Run() error
	Name() string
}

func root(args []string) error {
	if len(args) < 1 {
		return errors.New("you must pass in a subcommand")
	}

	// all subcommands
	cmds := []SubCommand{
		NewGetCommand(),
		NewPutCommand(),
		NewListCommand(),
		NewHelpCommand(),
	}

	subcommand := args[0]

	for _, cmd := range cmds {
		if cmd.Name() == subcommand {
			err := cmd.Init(args[1:])
			if err != nil {
				return err
			}
			return cmd.Run()
		}
	}
	return errors.New("invalid subcommand")
}
