package coordinator

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/maxcillius/MapReducer/mr"
	"github.com/maxcillius/MapReducer/worker"
)

func TestCoordinatorWorkersCompleteWordCount(t *testing.T) {
	root := t.TempDir()
	inputs := []string{
		writeInput(t, root, "part-0.txt", "alpha beta alpha"),
		writeInput(t, root, "part-1.txt", "beta gamma"),
		writeInput(t, root, "part-2.txt", "gamma alpha"),
	}
	output := filepath.Join(root, "counts")
	c, err := New(inputs, 3, output)
	if err != nil { t.Fatal(err) }
	defer os.RemoveAll(c.WorkDir())
	address, closeServer, err := c.Listen("127.0.0.1:0")
	if err != nil { t.Fatal(err) }
	defer closeServer()

	const workerCount = 5
	var wg sync.WaitGroup
	workerErrors := make(chan error, workerCount)
	for id := 0; id < workerCount; id++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			workerErrors <- worker.Run(address, id, filepath.Join(root, fmt.Sprintf("worker-local-%d", id)))
		}(id)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("workers did not finish the job before the timeout")
	}
	close(workerErrors)
	for err := range workerErrors { if err != nil { t.Fatalf("worker failed: %v", err) } }
	if err := c.Err(); err != nil { t.Fatal(err) }

	got := make(map[string]int)
	for rid := 0; rid < 3; rid++ {
		path := fmt.Sprintf("%s-r-%05d", output, rid)
		data, err := os.ReadFile(path)
		if err != nil { t.Fatalf("read reducer output: %v", err) }
		for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
			if line == "" { continue }
			var word string
			var count int
			if _, err := fmt.Sscanf(line, "%s\t%d", &word, &count); err != nil { t.Fatalf("parse reducer output %q: %v", line, err) }
			got[word] += count
		}
	}
	want := map[string]int{"alpha": 3, "beta": 2, "gamma": 2}
	if len(got) != len(want) { t.Fatalf("got counts %v, want %v", got, want) }
	for word, count := range want { if got[word] != count { t.Errorf("count for %q = %d, want %d", word, got[word], count) } }
}

func TestReduceTasksExistAtStartupAndWaitForMaps(t *testing.T) {
	root := t.TempDir()
	c, err := New([]string{"input"}, 2, filepath.Join(root, "out"))
	if err != nil { t.Fatal(err) }
	defer os.RemoveAll(c.WorkDir())
	if len(c.reduces) != 2 { t.Fatalf("created %d reduce tasks, want 2", len(c.reduces)) }
	registerTestWorker(t, c, 1)
	registerTestWorker(t, c, 2)
	var mapReply mr.RequestReply
	if err := c.RequestTask(&mr.RequestArgs{WorkerID: 1}, &mapReply); err != nil { t.Fatal(err) }
	var waitingReply mr.RequestReply
	if err := c.RequestTask(&mr.RequestArgs{WorkerID: 2}, &waitingReply); err != nil { t.Fatal(err) }
	if waitingReply.Task.Kind != mr.WaitTask { t.Fatalf("task kind = %q while map is running, want wait", waitingReply.Task.Kind) }

	outputs := make([]string, 2)
	for rid := range outputs {
		outputs[rid] = filepath.Join(c.WorkDir(), fmt.Sprintf("map-output-%d.json", rid))
		if err := os.WriteFile(outputs[rid], []byte(`{"word":1}`), 0600); err != nil { t.Fatal(err) }
	}
	var completeReply mr.CompleteReply
	completeArgs := &mr.CompleteArgs{WorkerID: 1, TaskID: mapReply.Task.ID, Attempt: mapReply.Task.Attempt, Generation: mapReply.Task.Generation, Outputs: outputs}
	if err := c.CompleteTask(completeArgs, &completeReply); err != nil { t.Fatal(err) }
	if !completeReply.Accepted { t.Fatal("coordinator rejected completed map task") }
	var reduceReply mr.RequestReply
	if err := c.RequestTask(&mr.RequestArgs{WorkerID: 2}, &reduceReply); err != nil { t.Fatal(err) }
	if reduceReply.Task.Kind != mr.ReduceTask { t.Fatalf("task kind after maps finish = %q, want reduce", reduceReply.Task.Kind) }
}

func TestNewWithWorkRootUsesSharedDirectory(t *testing.T) {
	workRoot := t.TempDir()
	outputRoot := t.TempDir()
	c, err := NewWithWorkRoot([]string{"input"}, 1, filepath.Join(outputRoot, "out"), workRoot)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(c.WorkDir())

	relative, err := filepath.Rel(workRoot, c.WorkDir())
	if err != nil {
		t.Fatal(err)
	}
	if relative == "." || strings.HasPrefix(relative, "..") {
		t.Fatalf("job directory %q is not under shared work root %q", c.WorkDir(), workRoot)
	}
}

func TestResumeRequeuesRunningTasksAndKeepsCompletedMaps(t *testing.T) {
	root := t.TempDir()
	workRoot := filepath.Join(root, "shared")
	checkpoint := filepath.Join(workRoot, "job.json")
	c, err := NewPersistent(
		[]string{filepath.Join(root, "input.txt")},
		1,
		filepath.Join(root, "output", "counts"),
		workRoot,
		checkpoint,
		filepath.Join(root, "splits"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(c.WorkDir())

	registerTestWorker(t, c, 1)
	var firstReply mr.RequestReply
	if err := c.RequestTask(&mr.RequestArgs{WorkerID: 1}, &firstReply); err != nil {
		t.Fatal(err)
	}
	if firstReply.Task.Kind != mr.MapTask || firstReply.Task.Attempt != 1 {
		t.Fatalf("first task = %+v, want first map attempt", firstReply.Task)
	}

	c.releaseCheckpointLock() // Simulate the coordinator process exiting.
	recovered, err := Resume(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	registerTestWorker(t, recovered, 2)
	var retryReply mr.RequestReply
	if err := recovered.RequestTask(&mr.RequestArgs{WorkerID: 2}, &retryReply); err != nil {
		t.Fatal(err)
	}
	if retryReply.Task.Kind != mr.MapTask || retryReply.Task.Attempt != firstReply.Task.Attempt || retryReply.Task.Generation <= firstReply.Task.Generation {
		t.Fatalf("recovered task = %+v, want same attempt in a newer generation", retryReply.Task)
	}
	var staleReply mr.CompleteReply
	staleArgs := &mr.CompleteArgs{
		WorkerID:   1,
		TaskID:     firstReply.Task.ID,
		Attempt:    firstReply.Task.Attempt,
		Generation: firstReply.Task.Generation,
	}
	if err := recovered.CompleteTask(staleArgs, &staleReply); err != nil {
		t.Fatal(err)
	}
	if staleReply.Accepted {
		t.Fatal("coordinator accepted completion from the previous generation")
	}

	mapOutput := filepath.Join(recovered.WorkDir(), "map-output.json")
	if err := os.WriteFile(mapOutput, []byte(`{"alpha":1}`), 0600); err != nil {
		t.Fatal(err)
	}
	var completeReply mr.CompleteReply
	completeArgs := &mr.CompleteArgs{
		WorkerID: 2,
		TaskID:   retryReply.Task.ID,
		Attempt:  retryReply.Task.Attempt,
		Generation: retryReply.Task.Generation,
		Outputs:  []string{mapOutput},
	}
	if err := recovered.CompleteTask(completeArgs, &completeReply); err != nil {
		t.Fatal(err)
	}
	if !completeReply.Accepted {
		t.Fatal("coordinator rejected recovered map completion")
	}

	recovered.releaseCheckpointLock() // Simulate another coordinator restart.
	finishedMapState, err := Resume(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	registerTestWorker(t, finishedMapState, 3)
	var reduceReply mr.RequestReply
	if err := finishedMapState.RequestTask(&mr.RequestArgs{WorkerID: 3}, &reduceReply); err != nil {
		t.Fatal(err)
	}
	if reduceReply.Task.Kind != mr.ReduceTask || len(reduceReply.Task.Inputs) != 1 || reduceReply.Task.Inputs[0] != mapOutput {
		t.Fatalf("task after recovery = %+v, want reducer with preserved map output", reduceReply.Task)
	}
}

func TestCheckpointLockRejectsSecondCoordinator(t *testing.T) {
	root := t.TempDir()
	checkpoint := filepath.Join(root, "job.json")
	c, err := NewPersistent([]string{"input"}, 1, filepath.Join(root, "out"), root, checkpoint, "")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(c.WorkDir())
	defer c.releaseCheckpointLock()

	if _, err := Resume(checkpoint); err == nil {
		t.Fatal("second coordinator acquired an active checkpoint")
	}
}

func TestPublishOutputWritesFinalFile(t *testing.T) {
	sourceRoot := t.TempDir()
	destinationRoot := t.TempDir()
	source := writeInput(t, sourceRoot, "reduce.tmp", "alpha\t2\n")
	destination := filepath.Join(destinationRoot, "counts-r-00000")

	if err := publishOutput(source, destination); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(destination)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "alpha\t2\n" {
		t.Fatalf("published output = %q, want %q", data, "alpha\t2\n")
	}
}

func TestConcurrentWorkersReceiveDistinctTasks(t *testing.T) {
	root := t.TempDir()
	inputs := make([]string, 32)
	for i := range inputs { inputs[i] = filepath.Join(root, fmt.Sprintf("input-%d", i)) }
	c, err := New(inputs, 1, filepath.Join(root, "out"))
	if err != nil { t.Fatal(err) }
	defer os.RemoveAll(c.WorkDir())

	const workers = 32
	ids := make(chan int, workers)
	var wg sync.WaitGroup
	for id := 0; id < workers; id++ {
		registerTestWorker(t, c, id)
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			var reply mr.RequestReply
			if err := c.RequestTask(&mr.RequestArgs{WorkerID: id}, &reply); err != nil { t.Errorf("request task: %v", err); return }
			ids <- reply.Task.ID
		}(id)
	}
	wg.Wait()
	close(ids)
	seen := make(map[int]bool)
	for id := range ids {
		if seen[id] { t.Errorf("task %d was assigned more than once", id) }
		seen[id] = true
	}
	if len(seen) != workers { t.Fatalf("assigned %d distinct tasks, want %d", len(seen), workers) }
}

func TestConcurrentHeartbeatsAreSafe(t *testing.T) {
	c, err := New([]string{"input"}, 1, filepath.Join(t.TempDir(), "out"))
	if err != nil { t.Fatal(err) }
	defer os.RemoveAll(c.WorkDir())
	registerTestWorker(t, c, 1)
	var taskReply mr.RequestReply
	if err := c.RequestTask(&mr.RequestArgs{WorkerID: 1}, &taskReply); err != nil { t.Fatal(err) }

	const heartbeatCount = 64
	var wg sync.WaitGroup
	for i := 0; i < heartbeatCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var reply mr.HeartbeatReply
			args := &mr.HeartbeatArgs{WorkerID: 1}
			if err := c.Heartbeat(args, &reply); err != nil { t.Errorf("heartbeat: %v", err); return }
			if !reply.Accepted { t.Error("valid concurrent heartbeat was rejected") }
		}()
	}
	wg.Wait()
}

func TestBackgroundMonitorRequeuesSilentWorker(t *testing.T) {
	c, err := New([]string{"input"}, 1, filepath.Join(t.TempDir(), "out"))
	if err != nil { t.Fatal(err) }
	defer os.RemoveAll(c.WorkDir())
	registerTestWorker(t, c, 1)
	registerTestWorker(t, c, 2)
	var taskReply mr.RequestReply
	if err := c.RequestTask(&mr.RequestArgs{WorkerID: 1}, &taskReply); err != nil { t.Fatal(err) }
	c.mu.Lock()
	c.workers[1].LastSeen = time.Now().Add(-workerTimeout - time.Second)
	c.mu.Unlock()
	go c.monitorWorkers()
	defer func() { close(c.monitorStop); <-c.monitorDone }()

	deadline := time.After(4 * time.Second)
	for {
		c.mu.Lock()
		requeued := c.workers[1].State == mr.WorkerDead && c.maps[0].Status == mr.Idle && len(c.mapQueue) == 1
		c.mu.Unlock()
		if requeued { return }
		select {
		case <-deadline: t.Fatal("background monitor did not requeue the silent worker's task")
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func TestExpiredTaskRetriesThenFails(t *testing.T) {
	c, err := New([]string{"input"}, 1, filepath.Join(t.TempDir(), "out"))
	if err != nil { t.Fatal(err) }
	defer os.RemoveAll(c.WorkDir())

	for attempt := 1; attempt <= maxTaskAttempts; attempt++ {
		registerTestWorker(t, c, attempt)
		var reply mr.RequestReply
		if err := c.RequestTask(&mr.RequestArgs{WorkerID: attempt}, &reply); err != nil { t.Fatal(err) }
		if reply.Task.Attempt != attempt { t.Fatalf("attempt = %d, want %d", reply.Task.Attempt, attempt) }
		c.mu.Lock()
		c.workers[attempt].LastSeen = time.Now().Add(-workerTimeout - time.Second)
		c.mu.Unlock()
	}
	c.mu.Lock()
	c.recoverExpiredLocked(time.Now())
	c.mu.Unlock()
	if err := c.Err(); err == nil || !strings.Contains(err.Error(), "worker heartbeat timed out") {
		t.Fatalf("expected heartbeat timeout failure, got %v", err)
	}
}

func TestStaleAttemptCompletionIsIgnored(t *testing.T) {
	c, err := New([]string{"input"}, 1, filepath.Join(t.TempDir(), "out"))
	if err != nil { t.Fatal(err) }
	defer os.RemoveAll(c.WorkDir())
	registerTestWorker(t, c, 1)
	var first mr.RequestReply
	if err := c.RequestTask(&mr.RequestArgs{WorkerID: 1}, &first); err != nil { t.Fatal(err) }
	c.mu.Lock()
	c.workers[1].LastSeen = time.Now().Add(-workerTimeout - time.Second)
	c.mu.Unlock()
	registerTestWorker(t, c, 2)
	var retry mr.RequestReply
	if err := c.RequestTask(&mr.RequestArgs{WorkerID: 2}, &retry); err != nil { t.Fatal(err) }
	if retry.Task.Attempt != first.Task.Attempt+1 { t.Fatalf("retry attempt = %d, want %d", retry.Task.Attempt, first.Task.Attempt+1) }
	output := filepath.Join(c.WorkDir(), "late-map-output.json")
	if err := os.WriteFile(output, []byte(`{"word":1}`), 0600); err != nil { t.Fatal(err) }
	var reply mr.CompleteReply
	args := &mr.CompleteArgs{WorkerID: 1, TaskID: first.Task.ID, Attempt: first.Task.Attempt, Generation: first.Task.Generation, Outputs: []string{output}}
	if err := c.CompleteTask(args, &reply); err != nil { t.Fatal(err) }
	if reply.Accepted { t.Fatal("stale completion was accepted") }
	if c.maps[0].Status != mr.Running || c.maps[0].WorkerID != 2 { t.Fatal("stale completion changed the current task assignment") }
}

func TestHeartbeatRenewsRegisteredWorker(t *testing.T) {
	c, err := New([]string{"input"}, 1, filepath.Join(t.TempDir(), "out"))
	if err != nil { t.Fatal(err) }
	defer os.RemoveAll(c.WorkDir())
	registerTestWorker(t, c, 7)
	var taskReply mr.RequestReply
	if err := c.RequestTask(&mr.RequestArgs{WorkerID: 7}, &taskReply); err != nil { t.Fatal(err) }
	started := time.Now().Add(-workerTimeout / 2)
	c.mu.Lock()
	c.workers[7].LastSeen = started
	c.mu.Unlock()
	var heartbeatReply mr.HeartbeatReply
	args := &mr.HeartbeatArgs{WorkerID: 7}
	if err := c.Heartbeat(args, &heartbeatReply); err != nil { t.Fatal(err) }
	if !heartbeatReply.Accepted { t.Fatal("current task heartbeat was rejected") }
	if time.Since(c.workers[7].LastSeen) > time.Second { t.Fatal("heartbeat did not renew the lease") }

	var staleReply mr.HeartbeatReply
	args.WorkerID = 99
	if err := c.Heartbeat(args, &staleReply); err != nil { t.Fatal(err) }
	if staleReply.Accepted { t.Fatal("unregistered worker heartbeat was accepted") }
}

func TestCompletionRejectsOutputOutsideJobDirectory(t *testing.T) {
	root := t.TempDir()
	c, err := New([]string{"input"}, 1, filepath.Join(root, "out"))
	if err != nil { t.Fatal(err) }
	defer os.RemoveAll(c.WorkDir())
	registerTestWorker(t, c, 1)
	var taskReply mr.RequestReply
	if err := c.RequestTask(&mr.RequestArgs{WorkerID: 1}, &taskReply); err != nil { t.Fatal(err) }
	outside := writeInput(t, root, "outside.json", `{"word":1}`)
	var completeReply mr.CompleteReply
	args := &mr.CompleteArgs{WorkerID: 1, TaskID: taskReply.Task.ID, Attempt: taskReply.Task.Attempt, Generation: taskReply.Task.Generation, Outputs: []string{outside}}
	if err := c.CompleteTask(args, &completeReply); err != nil { t.Fatal(err) }
	if completeReply.Accepted { t.Fatal("coordinator accepted output outside its temporary directory") }
	if c.maps[0].Status != mr.Idle { t.Fatalf("task state = %v, want idle for retry", c.maps[0].Status) }
}

func writeInput(t *testing.T, dir, name, contents string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(contents), 0600); err != nil { t.Fatal(err) }
	return path
}

func registerTestWorker(t *testing.T, c *Coordinator, id int) {
	t.Helper()
	var reply mr.RegisterReply
	if err := c.RegisterWorker(&mr.RegisterArgs{WorkerID: id}, &reply); err != nil { t.Fatal(err) }
	if !reply.Accepted { t.Fatalf("worker %d registration was rejected", id) }
}
