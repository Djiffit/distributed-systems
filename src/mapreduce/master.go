package mapreduce

import "container/list"
import (
	"fmt"
	"time"
	"sync"
)

type WorkerInfo struct {
	address string
	// You can add definitions here.
	currentJob  DoJobReply
	Working     bool
	jobStarted  time.Time
	assignedJob DoJobArgs
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply;
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}

	}
	return l
}

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here
	mutex := &sync.Mutex{}
	go func() {
		for {
			x := <-mr.registerChannel
			mutex.Lock()
			info := WorkerInfo{currentJob: DoJobReply{true}, address: x, Working: true}
			mr.Workers[x] = &info
			mutex.Unlock()
		}
	}()
	jobList := make([]DoJobArgs, 0)
	for {
		mutex.Lock()
		for _, v := range mr.Workers {
			if v.currentJob.OK {
				v.currentJob.OK = false
				if len(jobList) > 0 {
					oldJob := jobList[0]
					jobList = jobList[1:]
					v.jobStarted = time.Now()
					v.assignedJob = oldJob
					call(v.address, "Worker.DoJob", &v.assignedJob, &v.currentJob)
				} else {
					if mr.currentMap < mr.nMap {
						v.jobStarted = time.Now()
						v.assignedJob = DoJobArgs{File: mr.file, JobNumber: mr.currentMap, Operation: Map, NumOtherPhase: 50}
						call(v.address, "Worker.DoJob", &v.assignedJob, &v.currentJob)

						mr.currentMap++
					} else if mr.currentReduce < mr.nReduce && len(jobList) == 0 {
						v.assignedJob = DoJobArgs{File: mr.file, JobNumber: mr.currentReduce, Operation: Reduce, NumOtherPhase: 100}
						v.jobStarted = time.Now()
						call(v.address, "Worker.DoJob", &v.assignedJob, &v.currentJob)

						mr.currentReduce++
					}
				}
			} else {
				if v.Working && v.jobStarted.Sub(time.Now()).Seconds() > -1 {
					jobList = append(jobList, v.assignedJob)
					v.Working = false
				}
			}
			if mr.currentReduce == mr.nReduce && mr.currentMap == mr.nMap && len(jobList) == 0 {
				time.Sleep(3 * time.Second)
				return mr.KillWorkers()
			}
		}
		mutex.Unlock()
	}
}
