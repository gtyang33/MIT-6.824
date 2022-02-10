package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		reply := CallGetTask()
		if reply.IsComplete || reply.Typ ==Done{
			return
		}
		done := make(chan struct{})
		go func() {
			tick := time.NewTicker(reply.TimeOut)
			select {
			case <-done:
				CallReportTask(completed, reply.Id, reply.Typ)
			case <-tick.C:
				CallReportTask(timeout, reply.Id, reply.Typ)
			}
		}()

		if reply.Typ == MapTask{
			fileName := reply.FileName[0]
			file, err := os.Open(fileName)
			if err != nil {
				log.Fatalf("cannot open %v", fileName)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", fileName)
			}
			file.Close()
			kva := mapf(fileName, string(content))
			// todo: avoid file leak
			files := make([]*os.File, 0, reply.NReduce)
			for i := 0; i < reply.NReduce; i++ {
				f, err := os.Create(fmt.Sprintf("reduce-%d-%d.tmp", reply.Id, i))
				if err != nil {
					panic("can not open file")
				}
				files = append(files, f)
			}
			for _, kv := range kva {
				i := ihash(kv.Key) % reply.NReduce
				enc := json.NewEncoder(files[i])
				enc.Encode(kv)
			}
			select {
			case done <- struct{}{}:
				for i := range files {
					os.Rename(fmt.Sprintf("reduce-%d-%d.tmp", reply.Id, i), fmt.Sprintf("reduce-%d-%d", reply.Id, i))
				}
			default:
				for i := range files {
					os.Remove(fmt.Sprintf("reduce-%d-%d.tmp", reply.Id, i))
				}
			}
			for _, f := range files {
				f.Close()
			}
		} else{
			res := make(map[string][]string)
			for _, fileName := range reply.FileName {
				file, err := os.Open(fileName)
				if err != nil{
					log.Fatalf("read %s err", fileName)
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					res[kv.Key] = append(res[kv.Key], kv.Value)
				}

			}
			file, err := os.Create(fmt.Sprintf("mr-out-%d.tmp", reply.NReduce))
			if err != nil{
				log.Fatalf("cannot open file")
			}
			for k, v := range res {
				fmt.Fprintf(file, "%v %v\n", k, reducef(k, v))
			}
			select {
			case done <- struct{}{}:
				os.Rename(fmt.Sprintf("mr-out-%d.tmp", reply.NReduce), fmt.Sprintf("mr-out-%d", reply.NReduce))
			default:
				os.Remove(fmt.Sprintf("mr-out-%d.tmp", reply.NReduce))
			}
			file.Close()
		}

	}
}



// CallGetTask CallGetTask: gte tasks from coordinator.
func CallGetTask() GetTaskReply{
	args := GetTaskArgs{}
	reply := GetTaskReply{}

	reply.IsComplete = !call("Coordinator.GetTask", &args, &reply)
	return reply
}

func CallReportTask(status Status, id int, typ TaskType){
	args := ReportArgs{status, id, typ}
	reply := ReportReply{}

	call("Coordinator.ReportTask", &args, &reply)
}
//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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
