package main

import (
	"flag"

	mdb "github.com/sarkk0x0/memorylanedb"
)

type PutCommand struct {
	fs     *flag.FlagSet
	dbPath string
	key    string
	value  []byte
}

func NewPutCommand() *PutCommand {
	pc := &PutCommand{
		fs: flag.NewFlagSet("put", flag.ContinueOnError),
	}
	pc.fs.StringVar(&pc.dbPath, "dbpath", defaultHomeDir, "path to the database directory")
	return pc
}

func (pc *PutCommand) Name() string {
	return pc.fs.Name()
}

func (pc *PutCommand) Init(args []string) error {
	if len(args) < 2 {
		return ErrInvalidArgs
	}
	key, val := args[0], args[1]
	pc.key = key
	pc.value = []byte(val)
	return pc.fs.Parse(args)
}

func (pc *PutCommand) Run() error {
	db, err := mdb.NewDB(pc.dbPath, nil)
	if err != nil {
		return err
	}
	defer db.Close()
	return db.Put(mdb.Key(pc.key), pc.value)
}
