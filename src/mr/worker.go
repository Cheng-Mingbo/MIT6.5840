package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// ByKey for sorting by key.
type ByKey []KeyValue

// Len for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// KeyValue Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// CallGetTask is a wrapper for call() to get a task from coordinator.
func CallGetTask(workerID int) Task {
	args := TaskReq{
		WorkerID: workerID,
	}
	reply := Task{}

	ok := call("Coordinator.GetTask", &args, &reply)
	if ok == false {
		log.Fatal("GetTask RPC failed.")
	}
	return reply
}

// CallTaskDone is a wrapper for call() to tell coordinator that a task is done.
func CallTaskDone(req FinishReq) {
	reply := FinishRep{}
	ok := call("Coordinator.TaskDone", &req, &reply)
	if ok == false {
		log.Fatal("TaskDone RPC failed.")
	}
}

// mapSolve is a wrapper for mapf() to solve a map task.
func mapSolve(workerID int, task Task, mapf func(string, string) []KeyValue) {
	filename := task.FileName
	file, err := os.Open(filename) // 打开文件
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Fatalf("cannot close %v", filename)
		}
	}(file)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
		return
	}

	content, err := io.ReadAll(file) // 读取文件内容
	if err != nil {
		log.Fatalf("cannot read %v", filename)
		return
	}
	kvSlice := mapf(filename, string(content)) // 调用map函数
	// 生成中间文件
	reduceNum := task.ReduceNum
	HashKv := make([][]KeyValue, reduceNum)
	for _, kv := range kvSlice {
		HashKv[ihash(kv.Key)%reduceNum] = append(HashKv[ihash(kv.Key)%reduceNum], kv)
	}
	for i := 0; i < reduceNum; i++ {
		tempFile, err := os.CreateTemp("", "temp"+strconv.Itoa(rand.Int()))
		enc := json.NewEncoder(tempFile)
		if err != nil {
			log.Fatalf("cannot create temp file")
			return
		}
		// 保存中间文件
		for _, kv := range HashKv[i] {
			err = enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot write temp file")
				return
			}
		}

		oldName := tempFile.Name()
		err = os.Rename(oldName, "mr-"+strconv.Itoa(task.ID)+"-"+strconv.Itoa(i))
		if err != nil {
			log.Fatalf("cannot rename temp file")
			return
		}
		err = tempFile.Close()
		if err != nil {
			log.Fatalf("cannot close temp file")
			return
		}
	}
	req := FinishReq{
		Task:     task.ID,
		Type:     task.Type,
		WorkerId: workerID,
	}
	CallTaskDone(req)
}

// reduceSolve is a wrapper for reducef() to solve a reduce task.
func reduceSolve(workerID int, task Task, reducef func(string, []string) string) {
	// 读取中间文件
	id := strconv.Itoa(task.ID)
	files, err := os.ReadDir("./")
	if err != nil {
		log.Fatalf("cannot read dir")
		return
	}
	kv := make([]KeyValue, 0)
	mp := make(map[string][]string)

	for _, fileInfo := range files {
		if !strings.HasSuffix(fileInfo.Name(), id) { // 不是该reduce任务的中间文件
			continue
		}
		file, err := os.Open(fileInfo.Name())
		dec := json.NewDecoder(file)
		if err != nil {
			log.Fatalf("cannot open file")
			return
		}
		for {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				break
			}
			mp[kv.Key] = append(mp[kv.Key], kv.Value)
		}
	}
	for key, value := range mp {
		kv = append(kv, KeyValue{key, reducef(key, value)})
	}
	// 生成reduce文件
	newFile, err := os.CreateTemp("", "temp"+strconv.Itoa(rand.Int()))
	if err != nil {
		log.Fatalf("cannot create temp file")
		return
	}
	sort.Sort(ByKey(kv))
	for _, v := range kv {
		_, err := fmt.Fprintf(newFile, "%v %v\n", v.Key, v.Value)
		if err != nil {
			log.Fatalf("cannot write temp file")
			return
		}
	}
	oldName := newFile.Name()
	defer func(newFile *os.File) {
		err := newFile.Close()
		if err != nil {
			log.Fatalf("cannot close temp file")
			return
		}
	}(newFile)
	err = os.Rename(oldName, "mr-out-"+id)
	if err != nil {
		log.Fatalf("cannot rename temp file")
		return
	}
	req := FinishReq{
		Task:     task.ID,
		Type:     task.Type,
		WorkerId: workerID,
	}
	CallTaskDone(req)
}

// Worker
// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	rand.Seed(time.Now().UnixNano())
	workerID := rand.Int()
	fmt.Println("new worker: ", workerID)

	for true {
		task := CallGetTask(workerID)
		switch task.Type {
		case Success:
			return
		case Waiting:
			time.Sleep(time.Second * 2)

		case MapStatus:
			mapSolve(workerID, task, mapf)
		case ReduceStatus:
			reduceSolve(workerID, task, reducef)
		default:
			return
		}
		time.Sleep(time.Second * 5)
	}

}

// CallExample example function to show how to make an RPC call to the coordinator.
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer func(c *rpc.Client) {
		err := c.Close()
		if err != nil {
			log.Fatal("close error:", err)
		}
	}(c)

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
