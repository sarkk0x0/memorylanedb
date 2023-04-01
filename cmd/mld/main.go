package main

import (
	"fmt"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/sarkk0x0/memorylanedb"
)

func main() {
	defer func() {
		if recvr := recover(); recvr != nil {
			log.Error().Str("error", fmt.Sprintf("%v", recvr)).Msg("recovered from panic")
		}
	}()

	zerolog.SetGlobalLevel(zerolog.DebugLevel)

	path := "/Users/sarkk/bitcask/data"
	db, dbErr := memorylanedb.NewDB(path, memorylanedb.Option{})
	defer db.Close()
	if dbErr != nil {
		log.Error().Err(dbErr).Msg("error opening DB")
	}

}
