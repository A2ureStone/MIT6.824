package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for AskTask(mapf, reducef) {
		time.Sleep(10 * time.Millisecond)
	}
}

// example function to show how to make an RPC call to the coordinator.
//
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
		//fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		//fmt.Printf("call failed!\n")
	}
}

func AskTask(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) bool {

	// declare an argument structure.
	args := TaskRequestArgs{os.Getpid()}
	//fmt.Printf("Worker: %v is asking task\n", args.WorkerId)

	// declare a reply structure.
	reply := TaskRequestReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.AssignWork", &args, &reply)
	if ok {
		// reply.Y should be 100.
		if reply.Wait {
			//fmt.Printf("worker pid:%v receive wait-signal from master\n", os.Getpid())
			return true
		}
	} else {
		//fmt.Printf("rpc call failed!\n")
		return false
	}

	dir, err := os.Getwd()
	if err != nil {
		log.Fatalf("cannot get work directory\n")
	}

	if reply.MapTask {
		//fmt.Printf("Receive Map task, filename: %v\n", reply.TaskFileName[0])
		file, err := os.Open(reply.TaskFileName[0])
		if err != nil {
			log.Fatalf("cannot open %v", reply.TaskFileName)
		}
		intermediate := []KeyValue{}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", reply.TaskFileName)
		}
		err = file.Close()
		if err != nil {
			log.Fatalf("fail to close the given map file\n")
		}
		// is a map task, deliver to map function
		kva := mapf(reply.TaskFileName[0], string(content))
		intermediate = append(intermediate, kva...)

		// hash kv to partition
		partition := make(map[int][]KeyValue)
		for _, kv := range kva {
			pa_num := ihash(kv.Key) % reply.PartitionNum
			partition[pa_num] = append(partition[pa_num], kv)
		}
		// create tmp file to store map result
		tmp_file_array := make([]string, reply.PartitionNum)
		for i := 0; i < reply.PartitionNum; i += 1 {
			f, err := ioutil.TempFile(dir, "MapTask"+strconv.Itoa(reply.TaskId))
			if err != nil {
				log.Fatalf("create tempfile failed\n")
			}
			defer func(f *os.File) {
				err := f.Close()
				if err != nil {
					log.Fatalf("fail to close tmp file\n")
				}
			}(f)
			tmp_file_array[i] = f.Name()
			enc := json.NewEncoder(f)
			for _, kv := range partition[i] {
				err := enc.Encode(&kv)
				if err != nil {
					log.Fatalf("json write error\n")
				}
			}
		}
		// atomic change the filename
		m_res := TaskResult{reply.TaskId, true, make([]string, reply.PartitionNum)}
		for pa, f := range tmp_file_array {
			str := dir + "/" + "mr-" + strconv.Itoa(reply.TaskId) + "-" + strconv.Itoa(pa)
			err = os.Rename(f, str)
			if err != nil {
				log.Fatalf("rename error\n")
			}
			m_res.Res[pa] = str
		}
		m_end := TaskEnd{}
		ok = call("Coordinator.ReceiveRes", &m_res, &m_end)
		if !ok {
			return false
		}
	} else {
		intermediate := []KeyValue{}
		for i := 0; i < len(reply.TaskFileName); i += 1 {
			f, err := os.Open(reply.TaskFileName[i])
			if err != nil {
				log.Fatalf("cannot open %v\n", reply.TaskFileName)
			}
			defer func(f *os.File) {
				err := f.Close()
				if err != nil {
					log.Fatalf("cannot close %v\n", reply.TaskFileName)
				}
			}(f)
			dec := json.NewDecoder(f)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					// eof
					break
				}
				intermediate = append(intermediate, kv)
			}
		}
		sort.Sort(ByKey(intermediate))
		oname := "reduce-" + strconv.Itoa(reply.TaskId) + "-*"
		ofile, err := ioutil.TempFile(dir, oname)
		if err != nil {
			log.Fatalf("cannot create tmp file %v\n", oname)
		}

		//
		// call Reduce on each distinct key in intermediate[],
		// and print the result to mr-out-0.
		//
		i := 0
		for i < len(intermediate) {
			j := i + 1
			for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, intermediate[k].Value)
			}
			output := reducef(intermediate[i].Key, values)

			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

			i = j
		}
		if err = ofile.Close(); err != nil {
			log.Fatalf("cannot close tmp file %v\n", oname)
		}
		oname = "/mr-out-" + strconv.Itoa(reply.TaskId)
		if err = os.Rename(ofile.Name(), dir+oname); err != nil {
			log.Fatalf("rename error\n")
		}
		//fmt.Printf("worker pid:%v finish reduce task%v\n", os.Getpid(), reply.TaskId)

		r_res := TaskResult{TaskId: reply.TaskId, MapTask: false}
		r_end := TaskEnd{}
		ok = call("Coordinator.ReceiveRes", &r_res, &r_end)
		if !ok {
			return false
		}
	}
	return true
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		// process may end here
		//log.Printf("dial http-fail\n")
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
