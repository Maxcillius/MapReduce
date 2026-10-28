package worker

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/maxcillius/MapReducer/mr"
)

const rpcCallTimeout = 5 * time.Second

func Run(address string, workerID int, workDir string) error {
	conn, err := net.DialTimeout("tcp", address, rpcCallTimeout)
	if err != nil {
		return err
	}
	client := rpc.NewClient(conn)
	defer client.Close()

	var registerReply mr.RegisterReply
	registerArgs := &mr.RegisterArgs{WorkerID: workerID}
	if err := call(client, "Coordinator.Register", registerArgs, &registerReply); err != nil {
		return err
	}
	if registerReply.Done {
		return nil
	}
	if !registerReply.Accepted {
		return fmt.Errorf("coordinator rejected worker %d registration", workerID)
	}
	if registerReply.WorkDir != "" {
		workDir = registerReply.WorkDir
	}

	stopHeartbeat, heartbeatErrors := startHeartbeat(client, workerID)
	defer stopHeartbeat()
	for {
		select {
		case err := <-heartbeatErrors:
			return fmt.Errorf("worker heartbeat: %w", err)
		default:
		}

		var response mr.RequestReply
		requestArgs := &mr.RequestArgs{WorkerID: workerID}
		if err := call(client, "Coordinator.GetTask", requestArgs, &response); err != nil {
			return err
		}

		task := response.Task
		switch task.Kind {
		case mr.ExitTask:
			return nil
		case mr.WaitTask:
			time.Sleep(100 * time.Millisecond)
		case mr.MapTask, mr.ReduceTask:
			outputs, taskErr := runTask(task, workDir)
			select {
			case err := <-heartbeatErrors:
				return fmt.Errorf("worker heartbeat: %w", err)
			default:
			}

			args := mr.CompleteArgs{
				WorkerID: workerID,
				TaskID:   task.ID,
				Attempt:  task.Attempt,
				Generation: task.Generation,
				Outputs:  outputs,
			}
			if taskErr != nil {
				args.Err = taskErr.Error()
			}
			var reply mr.CompleteReply
			if err := call(client, "Coordinator.ReportTaskDone", &args, &reply); err != nil {
				return err
			}
		default:
			return fmt.Errorf("coordinator returned unknown task type %q", task.Kind)
		}
	}
}

func startHeartbeat(client *rpc.Client, workerID int) (func(), <-chan error) {
	stop := make(chan struct{})
	done := make(chan struct{})
	heartbeatErrors := make(chan error, 1)

	go func() {
		defer close(done)
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				var reply mr.HeartbeatReply
				args := &mr.HeartbeatArgs{WorkerID: workerID}
				if err := call(client, "Coordinator.Heartbeat", args, &reply); err != nil {
					heartbeatErrors <- err
					return
				}
				if !reply.Accepted {
					heartbeatErrors <- errors.New("coordinator rejected heartbeat")
					return
				}
			}
		}
	}()

	return func() {
		close(stop)
		<-done
	}, heartbeatErrors
}

func call(client *rpc.Client, method string, args, reply any) error {
	result := make(chan error, 1)
	go func() {
		result <- client.Call(method, args, reply)
	}()

	timer := time.NewTimer(rpcCallTimeout)
	defer timer.Stop()
	select {
	case err := <-result:
		return err
	case <-timer.C:
		_ = client.Close()
		return fmt.Errorf("RPC %s timed out after %s", method, rpcCallTimeout)
	}
}

func runTask(task mr.Task, workDir string) ([]string, error) {
	if task.Kind == mr.MapTask {
		return runMap(task, workDir)
	}
	return runReduce(task, workDir)
}

func runMap(task mr.Task, workDir string) ([]string, error) {
	file, err := os.Open(task.Input)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanWords)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	partitions := make([]map[string]int, task.ReduceCount)
	for i := range partitions {
		partitions[i] = make(map[string]int)
	}
	for scanner.Scan() {
		word := scanner.Text()
		reduceID := partition(word, task.ReduceCount)
		partitions[reduceID][word]++
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	paths := make([]string, task.ReduceCount)
	for reduceID, counts := range partitions {
		path := filepath.Join(workDir, fmt.Sprintf(
			"map-%05d-attempt-%03d-generation-%03d-r-%05d.json",
			task.MapID,
			task.Attempt,
			task.Generation,
			reduceID,
		))
		if err := writeJSONAtomic(path, counts); err != nil {
			return nil, err
		}
		paths[reduceID] = path
	}
	return paths, nil
}

func runReduce(task mr.Task, workDir string) ([]string, error) {
	counts := make(map[string]int)
	for _, path := range task.Inputs {
		file, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		var partitionCounts map[string]int
		err = json.NewDecoder(file).Decode(&partitionCounts)
		closeErr := file.Close()
		if err != nil {
			return nil, err
		}
		if closeErr != nil {
			return nil, closeErr
		}
		for word, count := range partitionCounts {
			counts[word] += count
		}
	}

	keys := make([]string, 0, len(counts))
	for word := range counts {
		keys = append(keys, word)
	}
	sort.Strings(keys)

	tempPath := filepath.Join(workDir, fmt.Sprintf(
		"reduce-%05d-attempt-%03d-generation-%03d.tmp",
		task.ReduceID,
		task.Attempt,
		task.Generation,
	))
	file, err := os.Create(tempPath)
	if err != nil {
		return nil, err
	}
	writer := bufio.NewWriter(file)
	for _, word := range keys {
		if _, err := fmt.Fprintf(writer, "%s\t%d\n", word, counts[word]); err != nil {
			_ = file.Close()
			return nil, err
		}
	}
	if err := writer.Flush(); err != nil {
		_ = file.Close()
		return nil, err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return nil, err
	}
	if err := file.Close(); err != nil {
		return nil, err
	}
	return []string{tempPath}, nil
}

func writeJSONAtomic(path string, value any) error {
	tempPath := path + ".tmp"
	file, err := os.Create(tempPath)
	if err != nil {
		return err
	}
	if err := json.NewEncoder(file).Encode(value); err != nil {
		_ = file.Close()
		_ = os.Remove(tempPath)
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		_ = os.Remove(tempPath)
		return err
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(tempPath)
		return err
	}
	if err := os.Rename(tempPath, path); err != nil {
		_ = os.Remove(tempPath)
		return err
	}
	return nil
}

func partition(key string, count int) int {
	hash := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		hash ^= uint32(key[i])
		hash *= 16777619
	}
	return int(hash % uint32(count))
}
