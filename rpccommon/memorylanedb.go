package rpccommon

type Status int

const (
	Ok Status = iota
	Failed
)

type PutArgs struct {
	Key   []byte
	Value []byte
}

type PutReply struct {
	Status Status
}

type GetReply struct {
	Status Status
	Value  []byte
}
