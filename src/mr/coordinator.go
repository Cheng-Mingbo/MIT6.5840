package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	status       int                 // 当前系统的状态
	mapChan      chan *Task          // map任务队列
	reduceChan   chan *Task          // reduce任务队列
	mapNum       int                 // map任务数量
	reduceNum    int                 // reduce任务数量
	mutex        sync.Mutex          // Worker获取任务时的锁
	taskMutex    sync.Mutex          // 修改mapDone和reduceDone时的锁
	mapDone      map[int]interface{} // map任务完成情况
	reduceDone   map[int]interface{} // reduce任务完成情况
	inChan       map[int]int         // 存放任务被分配给哪个Worker
	mapFinish    int                 // map任务完成数量
	reduceFinish int                 // reduce任务完成数量
}

const (
	MapStatus     int = iota // 处于map阶段
	ReduceStatus             // 处于reduce阶段
	Waiting                  // 等待任务
	MapWaiting               // 等待map任务
	ReduceWaiting            // 等待reduce任务
	Success                  // 任务完成
)

// MakeMapTask 产生map任务
func (c *Coordinator) MakeMapTask(files []string) {
	for i, file := range files {
		task := &Task{
			Type:      MapStatus,
			ID:        i,
			ReduceNum: c.reduceNum,
			FileName:  file,
		}
		c.inChan[i] = -1
		c.mapChan <- task
	}
}

// MakeReduceTask 产生reduce任务
func (c *Coordinator) MakeReduceTask() {
	for i := 0; i < c.reduceNum; i++ {
		task := &Task{
			Type:      ReduceStatus,
			ID:        i,
			ReduceNum: c.reduceNum,
			FileName:  "",
		}
		c.inChan[i] = -1
		c.reduceChan <- task
	}
}

// GetTask 获取任务
func (c *Coordinator) GetTask(args *TaskReq, reply *Task) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// 判断系统的状态
	switch c.status {
	case MapStatus:
		{
			// 判断是否还有map任务
			if len(c.mapChan) > 0 {
				*reply = *<-c.mapChan              // 从map任务队列中取出任务
				c.inChan[reply.ID] = args.WorkerID // 记录任务被分配给哪个Worker
				// 启动一个协程，用于判断任务是否超时
				go func(temp Task) {
					select {
					case <-time.After(time.Second * 10): // 任务超时时间为10s
						{
							c.taskMutex.Lock()
							if _, ok := c.mapDone[temp.ID]; !ok {
								c.mapChan <- &temp      // 将任务重新放入map任务队列
								c.status = MapStatus    // 系统状态改为map阶段
								c.inChan[reply.ID] = -1 // 任务被分配给哪个Worker改为-1
								log.Printf("map task %d timeout", temp.ID)
							}
							c.taskMutex.Unlock()
						}

					}
				}(*reply)
			} else {
				c.status = MapWaiting // 系统状态改为等待map任务完成
				reply.Type = Waiting
			}
		}
	case ReduceStatus:
		{
			// 判断是否还有reduce任务
			if len(c.reduceChan) > 0 {
				*reply = *<-c.reduceChan           // 从reduce任务队列中取出任务
				c.inChan[reply.ID] = args.WorkerID // 记录任务被分配给哪个Worker
				// 启动一个协程，用于判断任务是否超时
				go func(temp Task) {
					select {
					case <-time.After(time.Second * 10): // 任务超时时间为10s
						{
							c.taskMutex.Lock()
							if _, ok := c.reduceDone[temp.ID]; !ok {
								c.reduceChan <- &temp   // 将任务重新放入reduce任务队列
								c.status = ReduceStatus // 系统状态改为reduce阶段
								c.inChan[reply.ID] = -1 // 任务被分配给哪个Worker改为-1
								log.Printf("reduce task %d timeout", temp.ID)
							}
							c.taskMutex.Unlock()
						}
					}
				}(*reply)
			} else {
				c.status = ReduceWaiting // 系统状态改为等待reduce任务完成
				reply.Type = Waiting
			}
		}
	case MapWaiting:
		{
			if c.mapFinish == c.mapNum { // 判断map任务是否完成
				c.MakeReduceTask()      // 产生reduce任务
				c.status = ReduceStatus // 系统状态改为reduce阶段
				reply.Type = Waiting    // 返回等待任务
			} else {
				reply.Type = Waiting // 返回等待任务
			}
		}
	case ReduceWaiting:
		{
			if c.reduceFinish == c.reduceNum { // 判断reduce任务是否完成
				c.status = Success   // 系统状态改为任务完成
				reply.Type = Success // 返回任务完成
			} else {
				reply.Type = Waiting // 返回等待任务
			}
		}
	default:
		{
			reply.Type = Success // 返回任务完成
		}
	}
	return nil
}

// TaskDone 任务完成检查
func (c *Coordinator) TaskDone(args *FinishReq, reply *FinishRep) error {
	c.taskMutex.Lock()
	defer c.taskMutex.Unlock()

	if c.inChan[args.Task] != args.WorkerId { // 判断任务是否被分配给了该Worker
		return nil
	}
	if args.Type == MapStatus {
		c.mapDone[args.Task] = true // 记录map任务完成
		c.mapFinish++               // 完成的map任务数加1
		log.Printf("map task %d finish", args.Task)
	} else if args.Type == ReduceStatus {
		c.reduceDone[args.Task] = true // 记录reduce任务完成
		c.reduceFinish++               // 完成的reduce任务数加1
		log.Printf("reduce task %d finish", args.Task)
	}

	reply.Success = true // 返回任务完成
	return nil
}

// Your code here -- RPC handlers for the worker to call.

// Example an example RPC handler.
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// Done main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	if c.status == Success {
		ret = true
	}

	return ret
}

// MakeCoordinator create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		status:     MapStatus,
		mapNum:     len(files),
		reduceNum:  nReduce,
		mapChan:    make(chan *Task, len(files)),
		reduceChan: make(chan *Task, nReduce),
		mapDone:    make(map[int]interface{}),
		reduceDone: make(map[int]interface{}),
		inChan:     make(map[int]int),
	}

	// Your code here.
	c.MakeMapTask(files)
	c.server()
	return &c
}
