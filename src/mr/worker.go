package mr

import (
  "fmt"
  "log"
  "net/rpc"
  "hash/fnv"
  "os"
  "io/ioutil"
  "encoding/json"
  "time"
  "sort"
)

//
// Map functions return a slice of KeyValue.
//
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

	// uncomment to send the Example RPC to the master.
	// CallExample()
  args := AssignTaskArgs{WorkerID:os.Getpid()}
  reply := AssignTaskReply{TaskType:NoTask}
  for CheckForTask(&args, &reply) {
    if verbose {
      fmt.Printf("Reply: %v\n", reply)
    }
    switch task := reply.TaskType; task {
      case MapTask:
        if verbose {
          fmt.Printf("Received %s\n", task)
        }
        intermediate := RunMapTask(mapf, &reply)
        if verbose {
          fmt.Println(intermediate)
        }
        ok := MarkCompletion(reply.TaskNo, task, intermediate)
        if !ok {
          return
        }
      case ReduceTask:
        if verbose {
          fmt.Printf("Received %s\n", task)
        }
        oFile := RunReduceTask(reducef, &reply)
        if verbose {
          fmt.Println("produced output: ", oFile)
        }
        ok := MarkCompletion(reply.TaskNo, task, []string{oFile})
        if !ok {
          return
        }
      case NoTask:
        if verbose {
          fmt.Println("No task for now, sleep")
        }
        time.Sleep(NoTaskTimeout)
      default:
        fmt.Println("Unrecogenized TaskType: ", task)
        return
    }
    args = AssignTaskArgs{WorkerID:os.Getpid()}
    reply = AssignTaskReply{TaskType:NoTask}
  }
}

func CheckForTask(args *AssignTaskArgs, reply *AssignTaskReply) bool {
  if verbose {
    fmt.Printf("%d calls AssignTask\n", args.WorkerID)
  }
  return call("Master.AssignTask", args, reply) 
}

func MarkCompletion(taskNo int, taskType string, files []string) bool {
  args := TaskCompletionArgs{os.Getpid(), taskNo, taskType, files}
  reply := TaskCompletionReply{}
  if verbose {
    fmt.Printf("%d marks %s #%d completed\n",
      args.WorkerID, taskType, taskNo)
  }
  return call("Master.TaskCompletion", &args, &reply)
}

// returns names of output files
func RunMapTask(mapf func(string, string) []KeyValue, 
  reply *AssignTaskReply) []string {
  // mapping output files to intermediate results
  m := make(map[string][]KeyValue)
  files, nReduce, mapTaskNo := reply.Files, reply.NOut, reply.TaskNo
  for _, filename := range files {
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
    for _, kv := range kva {
      reduceTaskNo := ihash(kv.Key) % nReduce
      fname := fmt.Sprintf("out-%d-%d", mapTaskNo, reduceTaskNo)
      m[fname] = append(m[fname], kv)
    }
  }

  // encode with JSON
  for fname, kva := range m {
    ofile, err := os.Create(fname)
    if err != nil {
      log.Fatalf("cannot create %v", fname)
    }
    enc := json.NewEncoder(ofile)
    for _, kv := range kva {
      err := enc.Encode(&kv)
      if err != nil {
        log.Fatalf("cannot encode %+v", kv)
      }
    }
  }

  // output file names 
  outfiles := make([]string, len(m))
  i := 0
  for k := range m {
    outfiles[i] = k
    i ++
  }
  return outfiles
}

func RunReduceTask(reducef func(string, []string) string,
  reply *AssignTaskReply) string {
  fnames, reduceTaskNo := reply.Files, reply.TaskNo
  // decode JSON 
  kva := []KeyValue{}
  for _, fname := range fnames {
    // realTaskNo, _ := ExtractLastNumber(fname)
    // if realTaskNo != reduceTaskNo {
    //   fmt.Printf("Inconsistent task no: should be %v, received %v\n", 
    //     realTaskNo, reduceTaskNo)
    //   reduceTaskNo = realTaskNo
    //   reply.TaskNo = realTaskNo
    // }
    file, err := os.Open(fname)
    if err != nil {
      log.Fatalf("cannot open %v", fname)
    }
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
  // create tmp output file
  // wd, err := os.Getwd()
  // if err != nil {
  //   log.Fatalf("cannot get wd: %v", err)
  // }
  tmpfile, err := os.CreateTemp(".", "reduce.*.tmp")
  if err != nil {
    log.Fatalf("cannot create %v", tmpfile)
  }
  defer os.Remove(tmpfile.Name())
  // call Reduce on distinct key in kva[]
  for i := 0; i < len(kva); {
    j := i + 1
    k := kva[i].Key
    for j < len(kva) && kva[j].Key == k {
      j ++
    }
    values := []string{}
    for k := i; k < j; k ++ {
      values = append(values, kva[k].Value)
    }
    output := reducef(k, values)
    fmt.Fprintf(tmpfile, "%v %v\n", k, output)
    i = j
  }

  fname := fmt.Sprintf("mr-out-%d", reduceTaskNo)
  tmpfile.Close()
  os.Rename(tmpfile.Name(), fname)
  return fname
}

//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
