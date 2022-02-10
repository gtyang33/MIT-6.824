package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type reduceTask struct {
	files     []string
	reduceNum int
}

const maxWaitTime = 10 * time.Second

type Coordinator struct {
	// Your definitions here.
	sync.Mutex
	id                 int
	nReduce            int
	activeMapTasks     []string
	pendingMapTasks    map[int]string
	activeReduceTasks  map[int]*reduceTask
	pendingReduceTasks map[int]*reduceTask
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.Lock()
	defer c.Unlock()
	c.id++
	reply.Id = c.id
	for {
		reply.TimeOut = maxWaitTime
		if len(c.activeMapTasks) > 0 {
			file := c.activeMapTasks[0]
			c.activeMapTasks = c.activeMapTasks[1:]
			reply.FileName = []string{file}
			reply.NReduce = c.nReduce
			reply.Typ = MapTask
			c.pendingMapTasks[reply.Id] = file
			go func(id int) {
				time.Sleep(maxWaitTime)
				c.Lock()
				defer c.Unlock()
				if file, ok := c.pendingMapTasks[id]; ok {
					c.activeMapTasks = append(c.activeMapTasks, file)
					delete(c.pendingMapTasks, id)
				}
			}(reply.Id)
			return nil
		} else if len(c.pendingMapTasks) > 0 {
			c.Unlock()
			time.Sleep(time.Second)
			c.Lock()
		} else if len(c.activeReduceTasks) > 0 {
			for num, task := range c.activeReduceTasks {
				reply.NReduce = num
				reply.Typ = ReduceTask
				reply.FileName = task.files
				delete(c.activeReduceTasks, num)
				c.pendingReduceTasks[reply.Id] = task
				go func(id int) {
					time.Sleep(maxWaitTime)
					c.Lock()
					defer c.Unlock()
					if task, ok := c.pendingReduceTasks[id]; ok {
						c.activeReduceTasks[task.reduceNum] = task
						delete(c.pendingReduceTasks, id)
					}
				}(reply.Id)
				return nil
			}
		} else if len(c.pendingReduceTasks) > 0 {
			c.Unlock()
			time.Sleep(time.Second)
			c.Lock()
		} else {
			reply.Typ = Done
			return nil
		}
	}
}

func (c *Coordinator) ReportTask(args *ReportArgs, reply *ReportReply) error {
	id := args.Id
	c.Lock()
	defer c.Unlock()
	if args.Typ == MapTask {
		if args.Status == timeout {
			if file, ok := c.pendingMapTasks[id]; ok {
				c.activeMapTasks = append(c.activeMapTasks, file)
				delete(c.pendingMapTasks, id)
			}
		} else if args.Status == completed {
			for num, task := range c.activeReduceTasks {
				fileName := fmt.Sprintf("reduce-%d-%d", args.Id, num)
				task.files = append(task.files, fileName)
			}
			delete(c.pendingMapTasks, id)
		}
	} else {
		if args.Status == timeout {
			if task, ok := c.pendingReduceTasks[id]; ok {
				c.activeReduceTasks[task.reduceNum] = task
				delete(c.pendingReduceTasks, id)
			}
		} else if args.Status == completed {
			if task, ok := c.pendingReduceTasks[id]; ok {
				for _, file := range task.files {
					os.Remove(file)
				}
			}
			delete(c.pendingReduceTasks, id)
		}
	}
	return nil

}

//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	//ret := false

	// Your code here.

	c.Lock()
	defer c.Unlock()
	return len(c.activeMapTasks) == 0 && len(c.pendingMapTasks) == 0 && len(c.activeReduceTasks) == 0 && len(c.pendingReduceTasks) == 0
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.activeMapTasks = files
	c.pendingMapTasks = make(map[int]string)
	c.activeReduceTasks = make(map[int]*reduceTask)
	for i := 0; i < nReduce; i++ {
		c.activeReduceTasks[i] = &reduceTask{
			reduceNum: i,
		}
	}
	c.pendingReduceTasks = make(map[int]*reduceTask)
	c.nReduce = nReduce
	// Your code here.

	c.server()
	return &c
}
