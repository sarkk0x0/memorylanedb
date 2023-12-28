package main

import (
	"flag"
	"fmt"

	mdb "github.com/sarkk0x0/memorylanedb"
)

type ListCommand struct {
	fs     *flag.FlagSet
	dbPath string
}

func NewListCommand() *ListCommand {
	lc := &ListCommand{
		fs: flag.NewFlagSet("list", flag.ContinueOnError),
	}
	lc.fs.StringVar(&lc.dbPath, "dbpath", defaultHomeDir, "path to the database directory")
	return lc
}

func (lc *ListCommand) Name() string {
	return lc.fs.Name()
}

func (lc *ListCommand) Init(args []string) error {
	return lc.fs.Parse(args)
}

func (lc *ListCommand) Run() error {
	db, err := mdb.NewDB(lc.dbPath, nil)
	if err != nil {
		return err
	}
	defer db.Close()
	err = db.Fold(func(k mdb.Key) error {
		fmt.Printf("%s\n", string(k))
		return nil
	})
	return err
}
