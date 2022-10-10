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

type WorkingEntry struct {
	work_id int
	t       time.Time
}

type Coordinator struct {
	// Your definitions here.
	// use index as map task id
	// read only
	split      []string
	num_reduce int
	// var below need lock to protect
	// used for assign work
	waiting []int
	working map[int]time.Time
	// used for receive result
	map_res     [][]string
	map_receive int
	red_receive int
	map_work    bool
	mu          sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) AssignWork(args *TaskRequestArgs, reply *TaskRequestReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// check for expire task
	for k, v := range c.working {
		curr := time.Now()
		if curr.Sub(v) >= 10000*time.Millisecond {
			delete(c.working, k)
			c.waiting = append(c.waiting, k)
			//fmt.Printf("adding work%v\n", k)
		}
	}
	last_idx := len(c.waiting) - 1
	if last_idx == -1 {
		// no work to give, just ask worker to wait
		reply.Wait = true
		return nil
	}
	// add the work to working list
	reply.TaskId = c.waiting[last_idx]
	c.waiting = c.waiting[:last_idx]
	c.working[reply.TaskId] = time.Now()
	if c.map_work {
		//reply.TaskFileName = c.split[reply.TaskId]
		reply.TaskFileName = make([]string, 1)
		reply.TaskFileName[0] = c.split[reply.TaskId]
		reply.PartitionNum = c.num_reduce
		reply.MapTask = true
		reply.Wait = false

		//fmt.Printf("assign map work%v to pid: %v, filename: %v\n", reply.TaskId, args.WorkerId, reply.TaskFileName)
	} else {
		for _, f_arr := range c.map_res {
			////fmt.Printf("send reduce task%v to worker, filename: %v\n", reply.TaskId, f_arr[reply.TaskId])
			reply.TaskFileName = append(reply.TaskFileName, f_arr[reply.TaskId])
		}

		reply.MapTask = false
		reply.Wait = false

		//fmt.Printf("assign reduce work to pid: %v\n", args.WorkerId)
	}
	return nil
}

func (c *Coordinator) ReceiveMapRes(args *TaskResult, reply *TaskEnd) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.map_work {
		// in map phase and already receive a map task result
		if args.MapTask == false {
			log.Fatalf("receive a reduce task in map phase\n")
		}
		if c.map_res[args.TaskId] != nil {
			return nil
		}
		////fmt.Printf("receive map%v success\n", args.TaskId)
		delete(c.working, args.TaskId)
		// remove in working, even if exceed time
		//for _, v := range args.Res {
		//////fmt.Printf("target file: %v\n", v)
		//}
		c.map_receive += 1
		c.map_res[args.TaskId] = args.Res
		reply.Success = true
		if c.map_receive == len(c.map_res) {
			c.map_work = false
			// adding reduce work to queue
			for i := 0; i < c.num_reduce; i += 1 {
				c.waiting = append(c.waiting, i)
			}
		}
	} else {
		if args.MapTask {
			// receive map task result in reduce phase
			return nil
		}
		// even if many reduce tasks to same task id, is safe
		reply.Success = true
		delete(c.working, args.TaskId)
	}
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return !c.map_work && len(c.waiting) == 0 && len(c.working) == 0
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	pa_num := len(files)
	c := Coordinator{split: files, num_reduce: nReduce, waiting: make([]int, pa_num), working: make(map[int]time.Time), map_res: make([][]string, pa_num), map_receive: 0, red_receive: 0, map_work: true}
	// init waiting
	for i := 0; i < len(files); i += 1 {
		c.waiting[i] = i
	}

	c.server()
	return &c
}
