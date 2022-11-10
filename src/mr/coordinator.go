package mr

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

var mu sync.Mutex

type Coordinator struct {
	// Your definitions here.
	mapQueue             []Task            //the MapTasks waiting for being assigned
	mappingTasks         map[Task]bool     //the MapTasks under executing state
	mapTasksStartTime    map[int]time.Time //the start time of the executing map tasks
	reduceQueue          []int             //the id of ReduceTasks waiting for being assigned
	reducingTasks        map[int]bool      //the ReduceTasks under executing state
	reduceTasksStartTime map[int]time.Time //the start time of the executing reduce tasks
	nMap                 int               //the number of all MapTasks
	nReduce              int               //the number of all ReduceTasks
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) AssignTasks(args *AssignArgs, reply *AssignReply) error {
	mu.Lock()
	defer mu.Unlock()
	if len(c.mapQueue) == 0 && len(c.reduceQueue) == 0 &&
		len(c.mappingTasks) == 0 && len(c.reducingTasks) == 0 {
		//all the MapReduce tasks have already finished
		reply.Done = Done
		return nil
	}
	if len(c.mapQueue) != 0 {
		reply.Task = c.mapQueue[0]
		c.mapTasksStartTime[c.mapQueue[0].TaskId] = time.Now()
		c.mappingTasks[c.mapQueue[0]] = true
		c.mapQueue = c.mapQueue[1:]

	} else if len(c.mappingTasks) != 0 {
		reply.Done = MapMaybeDone
		return nil
	} else {
		//reduces can't start until the last map has finished.
		if len(c.reduceQueue) == 0 {
			//there aren't tasks in the reduceQueue.
			//But maybe there are tasks under executing state,and maybe
			//these tasks need to be executed again
			reply.Done = MaybeDone
			return nil
		}
		task := Task{
			TaskType: ReduceTask,
			TaskId:   c.reduceQueue[0],
		}
		reply.Task = task
		c.reduceTasksStartTime[c.reduceQueue[0]] = time.Now()
		c.reducingTasks[c.reduceQueue[0]] = true
		c.reduceQueue = c.reduceQueue[1:]
	}

	return nil
}

func (c *Coordinator) TaskDone(args *DoneArgs, reply *DoneReply) error {
	mu.Lock()
	defer mu.Unlock()
	if args.Task.TaskType == MapTask {
		if _, ok := c.mappingTasks[args.Task]; !ok {
			for _, task := range c.mapQueue {
				if task == args.Task {
					return nil
				}
			}
			log.Fatalf("there is no such map task")
			return errors.New("there is no such map task")
		} else {
			delete(c.mappingTasks, args.Task)
		}
	} else if args.Task.TaskType == ReduceTask {
		if _, ok := c.reducingTasks[args.Task.TaskId]; !ok {
			for _, taskId := range c.reduceQueue {
				if taskId == args.Task.TaskId {
					return nil
				}
			}
			log.Fatalf("there is no such reduce task")
			return errors.New("there is no such reduce task")
		} else {
			delete(c.reducingTasks, args.Task.TaskId)

		}
	}
	return nil
}

func (c *Coordinator) MapDone(args *MapDoneArgs, reply *MapDoneReply) error {
	mu.Lock()
	defer mu.Unlock()
	reply.Done = len(c.mapQueue) == 0 && len(c.mappingTasks) == 0
	return nil
}

// Example
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

// Done
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
// returns true when the MapReduce job is completely finished
func (c *Coordinator) Done() bool {
	//ret := false

	// Your code here.
	mu.Lock()
	defer mu.Unlock()
	return len(c.mapQueue) == 0 && len(c.reduceQueue) == 0 &&
		len(c.mappingTasks) == 0 && len(c.reducingTasks) == 0
}

func (c *Coordinator) CrashDetector() {
	for {
		time.Sleep(2 * time.Second)
		mu.Lock()

		if len(c.mapQueue) == 0 && len(c.reduceQueue) == 0 &&
			len(c.mappingTasks) == 0 && len(c.reducingTasks) == 0 {
			// all works done
			mu.Unlock()
			break
		}
		for k := range c.mappingTasks {
			if time.Since(c.mapTasksStartTime[k.TaskId]) > 9*time.Second {
				fmt.Println(k)
				c.mapQueue = append(c.mapQueue, k)
				delete(c.mappingTasks, k)
			}
		}
		for k := range c.reducingTasks {
			if time.Since(c.reduceTasksStartTime[k]) > 9*time.Second {
				fmt.Println(k)
				c.reduceQueue = append(c.reduceQueue, k)
				delete(c.reducingTasks, k)
			}
		}
		mu.Unlock()
	}
}

// MakeCoordinator
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Your code here.
	fmt.Printf("nReduce:%v\n", nReduce)
	fmt.Printf("len of files:%v\n", len(files))
	mapTask := []Task{}
	for i, file := range files {
		mapTask = append(mapTask, Task{
			TaskType: MapTask,
			TaskId:   i,
			NReduce:  nReduce,
			Filename: file,
		})
	}
	reduceQueue := []int{}
	for i := 0; i < nReduce; i++ {
		reduceQueue = append(reduceQueue, i)
	}
	c := Coordinator{
		mapQueue:             mapTask,
		mappingTasks:         make(map[Task]bool),
		reduceQueue:          reduceQueue,
		reducingTasks:        make(map[int]bool),
		nMap:                 len(files),
		nReduce:              nReduce,
		mapTasksStartTime:    make(map[int]time.Time),
		reduceTasksStartTime: make(map[int]time.Time),
	}

	c.server()
	go c.CrashDetector()
	return &c
}
