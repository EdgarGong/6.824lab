package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//

type KeyValue struct {
	Key   string
	Value string
}

// ByKey for sorting by key.
type ByKey []KeyValue

// for sorting by key.

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		// declare an argument structure.
		args := AssignArgs{}
		// declare a reply structure.
		reply := AssignReply{}
		// send the RPC request, wait for the reply.
		// the "Coordinator.AssignTasks" tells the
		// receiving server that we'd like to call
		// the AssignTasks() method of struct Coordinator.
		ok := call("Coordinator.AssignTasks", &args, &reply)
		if !ok {
			fmt.Errorf("ErrRPCNetwork")
			//continue
			return
		}

		if reply.Done == MapMaybeDone || reply.Done == MaybeDone {
			time.Sleep(time.Second)
			continue
		}
		if reply.Done == Done {
			return
		}
		if reply.Task.TaskType == MapTask {
			DoMapTask(mapf, reply.Task)

		} else if reply.Task.TaskType == ReduceTask {
			// reduces can't start until the last map has finished.
			for {
				doneArgs := MapDoneArgs{}
				doneReply := MapDoneReply{}
				ok := call("Coordinator.MapDone", &doneArgs, &doneReply)
				if !ok {
					fmt.Errorf("ErrRPCNetwork")
					time.Sleep(time.Second)
					continue
				}
				if doneReply.Done {
					break
				}
				time.Sleep(time.Second)
			}

			DoReduceTask(reducef, reply.Task)
		}
		time.Sleep(time.Second)
	}

}

func DoMapTask(mapf func(string, string) []KeyValue, task Task) {
	filename := task.Filename
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	sort.Sort(ByKey(kva))
	fmt.Printf("Map id:%v\n", task.TaskId)

	// The map part of your worker can use the
	// ihash(key) function (in worker.go) to pick
	// the reduce task for a given key.
	hashedKV := make([][]KeyValue, task.NReduce)
	for _, kv := range kva {
		reduceTaskIdx := ihash(kv.Key) % task.NReduce
		hashedKV[reduceTaskIdx] = append(hashedKV[reduceTaskIdx], kv)
	}

	for i := 0; i < task.NReduce; i++ {
		oname := "mr-tmp-" + strconv.Itoa(task.TaskId+1) + "-" + strconv.Itoa(i+1)
		ofile, _ := os.Create(oname)
		enc := json.NewEncoder(ofile)
		for _, kv := range hashedKV[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf(err.Error())
				return
			}
		}
		ofile.Close()
	}

	//??? need for loop or not ?
	taskDoneArgs := DoneArgs{
		Task: task,
	}
	taskDoneReply := DoneReply{}
	ok := call("Coordinator.TaskDone", &taskDoneArgs, &taskDoneReply)
	if !ok {
		fmt.Errorf("ErrRPCNetwork")
		return
	}
	return
}

//return the files in the current dir that starts with
// "mr-tmp-" and ends up with the reduceNum
func selectReduceName(reduceNum int) []string {
	var s []string
	//the path of current dir
	path, _ := os.Getwd()
	//files are all files in the path
	files, _ := ioutil.ReadDir(path)
	for _, fi := range files {
		// 匹配对应的reduce文件
		// starts with "mr-tmp-" and ends up with "reduceNum"
		if strings.HasPrefix(fi.Name(), "mr-tmp-") && strings.HasSuffix(fi.Name(), strconv.Itoa(reduceNum)) {
			s = append(s, fi.Name())
		}
	}
	return s
}

func DoReduceTask(reducef func(string, []string) string, task Task) {
	files := selectReduceName(task.TaskId + 1)

	kva := []KeyValue{}

	for _, filename := range files {
		file, _ := os.Open(filename)

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}
	sort.Sort(ByKey(kva))
	oname := "mr-out-" + strconv.Itoa(task.TaskId+1)
	ofile, _ := os.Create(oname)

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
		i = j
	}
	ofile.Close()

	taskDoneArgs := DoneArgs{
		Task: task,
	}
	taskDoneReply := DoneReply{}
	ok := call("Coordinator.TaskDone", &taskDoneArgs, &taskDoneReply)
	if !ok {
		fmt.Errorf("ErrRPCNetwork")
		return
	}
}

// CallExample
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	//!!!
	// When passing a pointer to a reply struct to the RPC system, the object that *reply points to should be zero-allocated. The code for RPC calls should always look like
	//	reply := SomeType{}
	//	call(..., &reply)

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

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
