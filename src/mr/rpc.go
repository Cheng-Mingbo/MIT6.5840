package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

type TaskReq struct {
	WorkerID int // Worker的ID
}

type Task struct {
	Type      int    // 系统的状态
	ID        int    // 任务ID
	ReduceNum int    // Reduce的数量
	FileName  string // 文件名
}

// FinishReq 任务完成通知
type FinishReq struct {
	Task     int // 任务ID
	Type     int // 任务类型
	WorkerId int // Worker的ID
}

type FinishRep struct {
	Success bool // 是否成功
}

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
