package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
import "sync"

type Clerk struct {
	servers    []*labrpc.ClientEnd
	lastLeader int
	ID         int
	reqid      int
	mu         sync.Mutex
	// You will have to modify this struct.

}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.lastLeader = 0
	ck.reqid = 1
	ck.ID = int(nrand())
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	ck.mu.Lock()
	args := &GetArgs{
		key,
		ck.ID,
		ck.reqid,
	}
	ck.reqid++
	ck.mu.Unlock()
	DPrintf("Client Get operation on cmd ID %v\n", args.ReqId)
	for {
		reply := &GetReply{}
		if ok := ck.servers[ck.lastLeader].Call("RaftKV.Get", args, reply); ok {
			if !reply.WrongLeader {
				return reply.Value
			}
			break
		}
	}
	for {
		for i := 0; i < len(ck.servers); i++ {
			reply := &GetReply{}
			ok := ck.servers[i].Call("RaftKV.Get", args, reply)
			if ok && !reply.WrongLeader {
				ck.mu.Lock()
				ck.lastLeader = i
				ck.mu.Unlock()
				return reply.Value
			}
		}
	}

}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.mu.Lock()
	args := &PutAppendArgs{
		key,
		value,
		op,
		ck.ID,
		ck.reqid,
	}
	ck.reqid++
	ck.mu.Unlock()
	DPrintf("Client %v operation on cmd ID %v\n", args.Op, args.ReqId)
	for {
		reply := &PutAppendReply{}
		if ok := ck.servers[ck.lastLeader].Call("RaftKV.PutAppend", args, reply); ok {
			if !reply.WrongLeader {
				return
			}
			break
		}
	}

	for {
		for i := 0; i < len(ck.servers); i++ {
			reply := &PutAppendReply{}
			ok := ck.servers[i].Call("RaftKV.PutAppend", args, reply)
			if ok && !reply.WrongLeader {
				ck.mu.Lock()
				ck.lastLeader = i
				ck.mu.Unlock()
				return
			}
		}
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
