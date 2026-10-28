package coordinator

import (
	"errors"
	"fmt"
	"io"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/maxcillius/MapReducer/mr"
)

const (
	workerTimeout    = 10 * time.Second
	monitorInterval  = 2 * time.Second
	maxTaskAttempts  = 3
	noTaskAssigned   = -1
)

type Coordinator struct {
	mu               sync.Mutex
	maps             []mr.TaskRecord
	reduces          []mr.TaskRecord
	mapQueue         []int
	reduceQueue      []int
	outputs          [][]string
	workers          map[int]*mr.WorkerInfo
	nReduce          int
	generation       int
	mapsRemaining    int
	reducesRemaining int
	workDir          string
	inputTempDir     string
	outputPrefix     string
	checkpointPath   string
	checkpointLock   *os.File
	finished         bool
	failure          string
	done             chan struct{}
	finishOnce       sync.Once
	monitorOnce      sync.Once
	monitorStop      chan struct{}
	monitorDone      chan struct{}
}

func New(inputs []string, nReduce int, outputPrefix string) (*Coordinator, error) {
	return newWithWorkRoot(inputs, nReduce, outputPrefix, filepath.Dir(outputPrefix))
}

// NewWithWorkRoot stores job intermediates below workRoot, which must be
// mounted at the same path on the coordinator and every worker.
func NewWithWorkRoot(inputs []string, nReduce int, outputPrefix, workRoot string) (*Coordinator, error) {
	return newWithWorkRoot(inputs, nReduce, outputPrefix, workRoot)
}

// NewPersistent creates a job and writes an initial checkpoint before workers
// can receive tasks.
func NewPersistent(inputs []string, nReduce int, outputPrefix, workRoot, checkpointPath, inputTempDir string) (*Coordinator, error) {
	if checkpointPath == "" {
		return nil, errors.New("checkpoint path is required")
	}
	absOutput, err := filepath.Abs(outputPrefix)
	if err != nil {
		return nil, fmt.Errorf("resolve output prefix: %w", err)
	}
	checkpointPath, err = filepath.Abs(checkpointPath)
	if err != nil {
		return nil, fmt.Errorf("resolve checkpoint path: %w", err)
	}
	lock, err := acquireCheckpointLock(checkpointPath)
	if err != nil {
		return nil, err
	}
	keepLock := false
	defer func() {
		if !keepLock {
			releaseCheckpointLock(lock)
		}
	}()
	if inputTempDir != "" {
		inputTempDir, err = filepath.Abs(inputTempDir)
		if err != nil {
			return nil, fmt.Errorf("resolve input split directory: %w", err)
		}
	}
	c, err := newWithWorkRoot(inputs, nReduce, absOutput, workRoot)
	if err != nil {
		return nil, err
	}
	c.checkpointPath = checkpointPath
	c.checkpointLock = lock
	c.inputTempDir = inputTempDir
	if err := c.persistLocked(); err != nil {
		_ = os.RemoveAll(c.workDir)
		return nil, fmt.Errorf("write initial coordinator checkpoint: %w", err)
	}
	keepLock = true
	return c, nil
}

func newWithWorkRoot(inputs []string, nReduce int, outputPrefix, workRoot string) (*Coordinator, error) {
	if len(inputs) == 0 {
		return nil, errors.New("at least one input file is required")
	}
	if nReduce < 1 {
		return nil, errors.New("reduce count must be at least 1")
	}
	if outputPrefix == "" {
		return nil, errors.New("output path is required")
	}
	if workRoot == "" {
		return nil, errors.New("work root is required")
	}
	workRoot, err := filepath.Abs(workRoot)
	if err != nil {
		return nil, fmt.Errorf("resolve work root: %w", err)
	}
	if err := os.MkdirAll(workRoot, 0755); err != nil {
		return nil, fmt.Errorf("create work root: %w", err)
	}
	workDir, err := os.MkdirTemp(workRoot, ".mapreduce-tmp-*")
	if err != nil {
		return nil, fmt.Errorf("create temporary directory: %w", err)
	}
	if err := os.Chmod(workDir, 02775); err != nil {
		_ = os.RemoveAll(workDir)
		return nil, fmt.Errorf("set shared job directory permissions: %w", err)
	}
	c := &Coordinator{
		nReduce:          nReduce,
		generation:       1,
		mapsRemaining: len(inputs),
		reducesRemaining: nReduce,
		workDir:          workDir,
		outputPrefix:     outputPrefix,
		outputs:          make([][]string, len(inputs)),
		workers:          make(map[int]*mr.WorkerInfo),
		monitorStop:      make(chan struct{}),
		monitorDone:      make(chan struct{}),
		done:             make(chan struct{}),
	}
	for i, input := range inputs {
		c.maps = append(c.maps, mr.TaskRecord{Task: mr.Task{
			ID: i, Kind: mr.MapTask, Input: input, MapID: i, ReduceCount: nReduce,
		}})
		c.mapQueue = append(c.mapQueue, i)
		c.outputs[i] = make([]string, nReduce)
	}
	for rid := 0; rid < nReduce; rid++ {
		inputsForReducer := make([]string, len(inputs))
		id := len(inputs) + rid
		c.reduces = append(c.reduces, mr.TaskRecord{Task: mr.Task{
			ID: id, Kind: mr.ReduceTask, ReduceID: rid, ReduceCount: nReduce, Inputs: inputsForReducer,
		}})
		c.reduceQueue = append(c.reduceQueue, rid)
	}
	return c, nil
}

// Listen starts the RPC server and the background worker monitor.
func (c *Coordinator) Listen(address string) (string, func(), error) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		c.releaseCheckpointLock()
		return "", nil, err
	}
	server := rpc.NewServer()
	if err := server.RegisterName("Coordinator", c); err != nil {
		_ = listener.Close()
		c.releaseCheckpointLock()
		return "", nil, err
	}
	c.monitorOnce.Do(func() { go c.monitorWorkers() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go server.ServeConn(conn)
		}
	}()
	var closeOnce sync.Once
	closeServer := func() {
		closeOnce.Do(func() {
			_ = listener.Close()
			close(c.monitorStop)
			<-c.monitorDone
			c.releaseCheckpointLock()
		})
	}
	return listener.Addr().String(), closeServer, nil
}

func (c *Coordinator) RegisterWorker(args *mr.RegisterArgs, reply *mr.RegisterReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.recoverExpiredLocked(time.Now())
	if c.finished || c.failure != "" {
		reply.Done = true
		return nil
	}
	if current, ok := c.workers[args.WorkerID]; ok && current.State == mr.WorkerBusy {
		return fmt.Errorf("worker %d already owns task %d", args.WorkerID, current.TaskID)
	}
	c.workers[args.WorkerID] = &mr.WorkerInfo{
		ID: args.WorkerID, State: mr.WorkerIdle, LastSeen: time.Now(), TaskID: noTaskAssigned,
	}
	reply.Accepted = true
	reply.WorkDir = c.WorkDir()
	return nil
}

// Register is the short RPC name used by workers during startup.
func (c *Coordinator) Register(args *mr.RegisterArgs, reply *mr.RegisterReply) error {
	return c.RegisterWorker(args, reply)
}

func (c *Coordinator) RequestTask(args *mr.RequestArgs, reply *mr.RequestReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.recoverExpiredLocked(time.Now())
	worker, ok := c.workers[args.WorkerID]
	if !ok {
		return fmt.Errorf("worker %d is not registered or is no longer active", args.WorkerID)
	}
	if c.finished || c.failure != "" {
		worker.State = mr.WorkerExited
		worker.TaskID = noTaskAssigned
		reply.Task.Kind = mr.ExitTask
		return nil
	}
	if worker.State == mr.WorkerDead || worker.State == mr.WorkerExited {
		return fmt.Errorf("worker %d is not registered or is no longer active", args.WorkerID)
	}
	if worker.State == mr.WorkerBusy {
		return fmt.Errorf("worker %d requested a task while task %d is still assigned", worker.ID, worker.TaskID)
	}
	worker.LastSeen = time.Now()
	if task := c.assignTaskLocked(&c.maps, &c.mapQueue, worker); task != nil {
		if c.failure != "" {
			reply.Task.Kind = mr.ExitTask
			return nil
		}
		reply.Task = cloneTask(*task)
		return nil
	}
	if c.mapsRemaining > 0 {
		worker.State = mr.WorkerIdle
		worker.TaskID = noTaskAssigned
		reply.Task.Kind = mr.WaitTask
		return nil
	}
	if task := c.assignTaskLocked(&c.reduces, &c.reduceQueue, worker); task != nil {
		if c.failure != "" {
			reply.Task.Kind = mr.ExitTask
			return nil
		}
		for mapID := range c.maps {
			c.reduces[task.ReduceID].Task.Inputs[mapID] = c.outputs[mapID][task.ReduceID]
		}
		reply.Task = cloneTask(*task)
		return nil
	}
	worker.State = mr.WorkerIdle
	worker.TaskID = noTaskAssigned
	if c.reducesRemaining == 0 {
		c.finishLocked()
		worker.State = mr.WorkerExited
		reply.Task.Kind = mr.ExitTask
	} else {
		reply.Task.Kind = mr.WaitTask
	}
	return nil
}

// GetTask is the worker-facing RPC name used by the workflow protocol.
func (c *Coordinator) GetTask(args *mr.RequestArgs, reply *mr.RequestReply) error {
	return c.RequestTask(args, reply)
}

func (c *Coordinator) CompleteTask(args *mr.CompleteArgs, reply *mr.CompleteReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.finished || c.failure != "" {
		return nil
	}
	r := c.record(args.TaskID)
	if r == nil || r.Status != mr.Running || r.WorkerID != args.WorkerID || r.Task.Attempt != args.Attempt || r.Task.Generation != args.Generation {
		return nil
	}
	worker := c.workers[args.WorkerID]
	if args.Err != "" {
		c.setWorkerIdleLocked(worker)
		c.failAttemptLocked(r, args.Err, c.queueForTask(r))
		return nil
	}
	wantOutputs := 1
	if r.Task.Kind == mr.MapTask {
		wantOutputs = c.nReduce
	}
	if len(args.Outputs) != wantOutputs || !c.validOutputs(args.Outputs) {
		c.setWorkerIdleLocked(worker)
		c.failAttemptLocked(r, "worker returned missing or invalid output files", c.queueForTask(r))
		return nil
	}
	if r.Task.Kind == mr.ReduceTask {
		dst := fmt.Sprintf("%s-r-%05d", c.outputPrefix, r.Task.ReduceID)
		if err := publishOutput(args.Outputs[0], dst); err != nil {
			c.setWorkerIdleLocked(worker)
			c.failAttemptLocked(r, fmt.Sprintf("publish reduce output: %v", err), c.queueForTask(r))
			return nil
		}
	}
	if r.Task.Kind == mr.MapTask {
		c.outputs[r.Task.MapID] = append([]string(nil), args.Outputs...)
		c.mapsRemaining--
	} else {
		c.reducesRemaining--
	}
	r.Outputs = append([]string(nil), args.Outputs...)
	r.Status = mr.Done
	c.setWorkerIdleLocked(worker)
	if c.mapsRemaining == 0 && c.reducesRemaining == 0 {
		c.finishLocked()
	}
	c.checkpointLocked()
	if c.failure != "" {
		return nil
	}
	reply.Accepted = true
	return nil
}

func publishOutput(source, destination string) error {
	temp, err := os.CreateTemp(filepath.Dir(destination), ".mr-output-*")
	if err != nil {
		return err
	}
	tempPath := temp.Name()
	defer os.Remove(tempPath)

	src, err := os.Open(source)
	if err != nil {
		_ = temp.Close()
		return err
	}
	if _, err := io.Copy(temp, src); err != nil {
		_ = src.Close()
		_ = temp.Close()
		return err
	}
	if err := src.Close(); err != nil {
		_ = temp.Close()
		return err
	}
	if err := temp.Sync(); err != nil {
		_ = temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	return os.Rename(tempPath, destination)
}

// ReportTaskDone reports a completed or failed worker attempt.
func (c *Coordinator) ReportTaskDone(args *mr.CompleteArgs, reply *mr.CompleteReply) error {
	return c.CompleteTask(args, reply)
}

// Heartbeat renews the registered worker's liveness, whether it is idle or busy.
func (c *Coordinator) Heartbeat(args *mr.HeartbeatArgs, reply *mr.HeartbeatReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	worker, ok := c.workers[args.WorkerID]
	if !ok || worker.State == mr.WorkerDead || worker.State == mr.WorkerExited {
		return nil
	}
	worker.LastSeen = time.Now()
	reply.Accepted = true
	return nil
}

func (c *Coordinator) assignTaskLocked(tasks *[]mr.TaskRecord, queue *[]int, worker *mr.WorkerInfo) *mr.Task {
	for len(*queue) > 0 {
		index := (*queue)[0]
		*queue = (*queue)[1:]
		record := &(*tasks)[index]
		if record.Status != mr.Idle {
			continue
		}
		record.Status = mr.Running
		record.WorkerID = worker.ID
		record.Task.Attempt++
		record.Task.Generation = c.generation
		worker.State = mr.WorkerBusy
		worker.TaskID = record.Task.ID
		c.checkpointLocked()
		return &record.Task
	}
	return nil
}

func (c *Coordinator) monitorWorkers() {
	defer close(c.monitorDone)
	ticker := time.NewTicker(monitorInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.mu.Lock()
			c.recoverExpiredLocked(time.Now())
			c.failIfNoWorkersLocked()
			c.mu.Unlock()
		case <-c.monitorStop:
			return
		}
	}
}

func (c *Coordinator) recoverExpiredLocked(now time.Time) {
	for _, worker := range c.workers {
		if worker.State == mr.WorkerDead || worker.State == mr.WorkerExited || now.Sub(worker.LastSeen) <= workerTimeout {
			continue
		}
		if worker.State == mr.WorkerBusy {
			if task := c.record(worker.TaskID); task != nil && task.Status == mr.Running && task.WorkerID == worker.ID {
				queue := &c.mapQueue
				if task.Task.Kind == mr.ReduceTask {
					queue = &c.reduceQueue
				}
				c.failAttemptLocked(task, "worker heartbeat timed out", queue)
			}
		}
		worker.State = mr.WorkerDead
		worker.TaskID = noTaskAssigned
	}
}

func (c *Coordinator) failIfNoWorkersLocked() {
	if c.finished || c.failure != "" || len(c.workers) == 0 || (c.mapsRemaining == 0 && c.reducesRemaining == 0) {
		return
	}
	for _, worker := range c.workers {
		if worker.State == mr.WorkerIdle || worker.State == mr.WorkerBusy {
			return
		}
	}
	c.failure = "no live workers remain"
	c.finishLocked()
	c.checkpointLocked()
}

func (c *Coordinator) finishLocked() {
	c.finished = true
	c.finishOnce.Do(func() { close(c.done) })
}

func (c *Coordinator) failAttemptLocked(record *mr.TaskRecord, reason string, queue *[]int) {
	if record.Task.Attempt >= maxTaskAttempts {
		c.failure = fmt.Sprintf("task %d failed after %d attempts: %s", record.Task.ID, record.Task.Attempt, reason)
		c.finishLocked()
		c.checkpointLocked()
		return
	}
	record.Status = mr.Idle
	index := record.Task.MapID
	if record.Task.Kind == mr.ReduceTask {
		index = record.Task.ReduceID
	}
	*queue = append(*queue, index)
	c.checkpointLocked()
}

func (c *Coordinator) setWorkerIdleLocked(worker *mr.WorkerInfo) {
	if worker == nil || worker.State == mr.WorkerDead || worker.State == mr.WorkerExited {
		return
	}
	worker.State = mr.WorkerIdle
	worker.TaskID = noTaskAssigned
	worker.LastSeen = time.Now()
}

func (c *Coordinator) queueForTask(record *mr.TaskRecord) *[]int {
	if record.Task.Kind == mr.ReduceTask {
		return &c.reduceQueue
	}
	return &c.mapQueue
}

func cloneTask(task mr.Task) mr.Task {
	task.Inputs = append([]string(nil), task.Inputs...)
	return task
}

func (c *Coordinator) validOutputs(paths []string) bool {
	root, err := filepath.Abs(c.workDir)
	if err != nil {
		return false
	}
	seen := make(map[string]struct{}, len(paths))
	for _, path := range paths {
		full, err := filepath.Abs(path)
		if err != nil {
			return false
		}
		rel, err := filepath.Rel(root, full)
		if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
			return false
		}
		if _, exists := seen[full]; exists {
			return false
		}
		seen[full] = struct{}{}
		info, err := os.Stat(full)
		if err != nil || !info.Mode().IsRegular() {
			return false
		}
	}
	return true
}

func (c *Coordinator) record(id int) *mr.TaskRecord {
	if id >= 0 && id < len(c.maps) {
		return &c.maps[id]
	}
	reduceID := id - len(c.maps)
	if reduceID >= 0 && reduceID < len(c.reduces) {
		return &c.reduces[reduceID]
	}
	return nil
}

// WorkDir returns the directory used for this job's temporary intermediate files.
func (c *Coordinator) WorkDir() string { return filepath.Clean(c.workDir) }

// InputTempDir returns the directory containing coordinator-created map splits.
func (c *Coordinator) InputTempDir() string { return c.inputTempDir }

// Err reports a terminal task or output publication failure, if one occurred.
func (c *Coordinator) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failure == "" {
		return nil
	}
	return errors.New(c.failure)
}

// Done is closed when the job completes or enters a terminal failure state.
func (c *Coordinator) Done() <-chan struct{} { return c.done }

// WorkersStopped marks an unfinished job failed when its local worker set has exited.
func (c *Coordinator) WorkersStopped() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.finished {
		c.failure = "all workers exited before job completion"
		c.finishLocked()
		c.checkpointLocked()
	}
}
