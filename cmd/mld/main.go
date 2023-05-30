package main

import (
	"fmt"
	"github.com/rs/zerolog/log"
	"github.com/sarkk0x0/memorylanedb"
	"github.com/sarkk0x0/memorylanedb/rpccommon"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Server struct {
	db          *memorylanedb.DB
	bindAddress string
}

func newServer(bindAddress, dbPath string) (*Server, error) {
	db, dbErr := memorylanedb.NewDB(dbPath, memorylanedb.Option{})
	if dbErr != nil {
		return nil, dbErr
	}
	return &Server{
		db,
		bindAddress,
	}, nil
}

func (s *Server) Put(args rpccommon.PutArgs, reply *rpccommon.PutReply) error {
	key := memorylanedb.Key(args.Key)
	err := s.db.Put(key, args.Value)
	if err != nil {
		log.Error().Err(err).Msg("error occurred")
		reply.Status = rpccommon.Failed
	} else {
		reply.Status = rpccommon.Ok
	}

	return nil
}

func (s *Server) Get(key []byte, reply *rpccommon.GetReply) error {
	value, err := s.db.Get(memorylanedb.Key(key))
	if err != nil {
		log.Error().Err(err).Msg("error occurred")
		reply.Status = rpccommon.Failed
	} else {
		reply.Status = rpccommon.Ok
		reply.Value = value
	}

	return nil
}

func main() {
	defer func() {
		if recvr := recover(); recvr != nil {
			log.Error().Str("error", fmt.Sprintf("%v", recvr)).Msg("recovered from panic")
		}
	}()
	dbPath := "data"
	bindAddress := "mldb.sock"

	s, err := newServer(bindAddress, dbPath)
	if err != nil {
		panic(err)
	}
	rpc.Register(s)
	rpc.HandleHTTP()
	os.Remove(bindAddress)
	l, e := net.Listen("unix", bindAddress)
	if e != nil {
		panic(e)
	}
	log.Info().Msgf("server started: %s", bindAddress)
	http.Serve(l, nil)

}
