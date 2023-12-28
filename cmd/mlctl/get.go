package main

import (
	"errors"
	"flag"
	"fmt"

	mdb "github.com/sarkk0x0/memorylanedb"
)

type GetCommand struct {
	fs     *flag.FlagSet
	dbPath string
	key    string
}

func NewGetCommand() *GetCommand {
	gc := &GetCommand{
		fs: flag.NewFlagSet("get", flag.ContinueOnError),
	}
	gc.fs.StringVar(&gc.dbPath, "dbpath", defaultHomeDir, "path to the database directory")
	return gc
}

func (gc *GetCommand) Name() string {
	return gc.fs.Name()
}

func (gc *GetCommand) Init(args []string) error {
	if len(args) < 1 {
		return ErrInvalidArgs
	}
	key := args[0]
	gc.key = key
	return gc.fs.Parse(args)
}

func (gc *GetCommand) Run() error {
	db, err := mdb.NewDB(gc.dbPath, nil)
	if err != nil {
		return err
	}
	defer db.Close()
	value, getErr := db.Get(mdb.Key(gc.key))
	if errors.Is(getErr, mdb.ErrKeyNotFound) {
		fmt.Println("Not Found")
	} else {
		fmt.Printf("%s\n", value)
	}
	return nil
}
