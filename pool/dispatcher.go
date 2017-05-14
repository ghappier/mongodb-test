package pool

import (
	"github.com/ghappier/mongodb-test/storage"
	"github.com/golang/glog"
)

type Dispatcher struct {
	jobQueue chan Job
	// A pool of workers channels that are registered with the dispatcher
	workerPool    chan chan Job
	maxWorkers    int
	storageHolder *storage.StorageHolder
}

func NewDispatcher(queue chan Job, maxWorkers int, storageHolder *storage.StorageHolder) *Dispatcher {
	pool := make(chan chan Job, maxWorkers)
	return &Dispatcher{
		jobQueue:      queue,
		workerPool:    pool,
		maxWorkers:    maxWorkers,
		storageHolder: storageHolder,
	}
}

func (d *Dispatcher) Run() {
	// starting n number of workers
	for i := 0; i < d.maxWorkers; i++ {
		worker := NewWorker(d.workerPool, d.storageHolder)
		worker.Start()
	}
	glog.Infof("start %d worker complete\n", d.maxWorkers)
	go d.dispatch()
}

func (d *Dispatcher) dispatch() {
	for {
		select {
		case job := <-d.jobQueue:
			/*
				// a job request has been received
				go func(job Job) {
					// try to obtain a worker job channel that is available.
					// this will block until a worker is idle
					jobChannel := <-d.workerPool

					// dispatch the job to the worker job channel
					jobChannel <- job
				}(job)
			*/
			jobChannel := <-d.workerPool
			// dispatch the job to the worker job channel
			jobChannel <- job
		}
	}
}
