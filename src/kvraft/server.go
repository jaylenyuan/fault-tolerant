package raftkv

import (
	"bytes"
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Cmd   string
	Key   string
	Value string
	ID    int
	ReqId int
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	ack    map[int]int
	db     map[string]string
	logChs map[int]chan Op
}

func (kv *RaftKV) AppendEntryToLog(entry Op) bool {
	index, _, isLeader := kv.rf.Start(entry)
	if !isLeader {
		return false
	}

	kv.mu.Lock()
	ch, ok := kv.logChs[index]
	if !ok {
		ch = make(chan Op, 1)
		kv.logChs[index] = ch
	}

	kv.mu.Unlock()
	select {
	case op := <-ch:
		DPrintf("op == entry %v\n", op == entry)
		return op == entry
	case <-time.After(1000 * time.Millisecond):
		return false
	}
}
func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {

	entry := Op{
		"Get",
		args.Key,
		"",
		args.ID,
		args.ReqId,
	}

	if ok := kv.AppendEntryToLog(entry); !ok {
		reply.WrongLeader = true
	} else {
		reply.WrongLeader = false
		reply.Err = OK
		kv.mu.Lock()
		val, ok := kv.db[args.Key]
		if !ok {
			reply.Err = ErrNoKey
			reply.Value = ""
		} else {
			reply.Value = val
		}
		kv.ack[args.ID] = args.ReqId
		DPrintf("Server %v implemented Get, command ID %v, KEY %v VALUE %v\n", kv.me, args.ReqId, args.Key, reply.Value)
		kv.mu.Unlock()
	}

}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	entry := Op{
		args.Op,
		args.Key,
		args.Value,
		args.ID,
		args.ReqId,
	}
	if ok := kv.AppendEntryToLog(entry); !ok {
		reply.WrongLeader = true
	} else {
		reply.WrongLeader = false
		reply.Err = OK
		DPrintf("leader %v implement %v commandID %v, KEY %v, VALUE %v , Wrongleader %v\n", kv.me, args.Op, args.ReqId, args.Key, args.Value, reply.WrongLeader)
	}
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.ack = make(map[int]int)
	kv.db = make(map[string]string)
	kv.logChs = make(map[int]chan Op)
	checkLog := make(chan bool, 1)
	// You may need initialization code here.
	go func() {
		for {
			msg := <-kv.applyCh
			//DPrintf("Server %v gets apply messgae\n", kv.me)
			if !msg.UseSnapshot {
				cmd := msg.Command.(Op)
				kv.mu.Lock()
				if !kv.checkDup(cmd) {
					//	DPrintf("Server %v Not dup \n", kv.me)
					kv.Apply(cmd)
				}
				ch, ok := kv.logChs[msg.Index]
				if ok {
					// means it is leader
					select {
					case <-ch:
					default:
					}
					ch <- cmd
					DPrintf("Server %v send to RPC call\n", kv.me)
				}
				kv.mu.Unlock()
				if maxraftstate > 0 {
					if kv.checkFullyApplied() {
						checkLog <- true
					}
				}
			} else {
				DPrintf("########Sever %v to install snapshot\n", me)
				kv.processSnapShot(msg.Snapshot)
			}
		}
	}()
	if maxraftstate > 0 {
		go func() {
			var size int
			for {
				<-checkLog
				size = kv.stateSize()
				DPrintf("Server %v Size: %v, Max: %v\n", kv.me, size, maxraftstate)
				if size > maxraftstate {
					DPrintf("Server %v Should snapshot...\n", kv.me)
					kv.snapshot()
				}
				// size = kv.stateSize()
				// DPrintf("Size: %v, Max: %v\n", size, maxraftstate)
			}
		}()
	}

	return kv
}
func (kv *RaftKV) checkFullyApplied() bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.rf.CommitedEqual2Applied()
}
func (kv *RaftKV) processSnapShot(data []byte) {
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	kv.mu.Lock()
	d.Decode(&kv.db)
	kv.mu.Unlock()
}

func (kv *RaftKV) snapshot() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.rf.StartSnapshot(&kv.db)
}
func (kv *RaftKV) stateSize() int {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.rf.ReadStateSize()
}
func (kv *RaftKV) checkDup(args Op) bool {
	v, ok := kv.ack[args.ID]
	if ok {
		DPrintf("Server %v: reqId %v, appliedId %v\n", kv.me, args.ReqId, v)
		return args.ReqId <= v
	}
	return false
}
func (kv *RaftKV) Apply(args Op) {
	// DPrintf("Server %v Apply %v\n", kv.me, args.Cmd)
	switch args.Cmd {
	case "Put":
		kv.db[args.Key] = args.Value
	case "Append":
		kv.db[args.Key] += args.Value
	}
	kv.ack[args.ID] = args.ReqId
	//DPrintf("After %v: KEY %v VALUE %v, Final is %v\n", args.Cmd, args.Key, args.Value, kv.db[args.Key])
}
