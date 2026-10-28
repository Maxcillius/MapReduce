package coordinator

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"github.com/maxcillius/MapReducer/mr"
)

const checkpointVersion = 1

type checkpointState struct {
	Version           int
	Maps              []mr.TaskRecord
	Reduces           []mr.TaskRecord
	Outputs           [][]string
	NReduce           int
	Generation        int
	MapsRemaining     int
	ReducesRemaining  int
	WorkDir           string
	InputTempDir      string
	OutputPrefix      string
	Finished          bool
	Failure           string
}

// Resume loads a coordinator checkpoint. Worker registrations are ephemeral;
// unfinished tasks are made available to newly registered workers.
func Resume(checkpointPath string) (*Coordinator, error) {
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

	data, err := os.ReadFile(checkpointPath)
	if err != nil {
		return nil, fmt.Errorf("read coordinator checkpoint: %w", err)
	}
	var state checkpointState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("decode coordinator checkpoint: %w", err)
	}
	if state.Version != checkpointVersion {
		return nil, fmt.Errorf("unsupported coordinator checkpoint version %d", state.Version)
	}
	if state.NReduce < 1 || len(state.Maps) == 0 || len(state.Reduces) != state.NReduce || state.WorkDir == "" || state.OutputPrefix == "" {
		return nil, errors.New("coordinator checkpoint is incomplete")
	}
	if !state.Finished {
		if info, err := os.Stat(state.WorkDir); err != nil || !info.IsDir() {
			return nil, fmt.Errorf("checkpoint work directory %q is unavailable", state.WorkDir)
		}
	}
	if len(state.Outputs) != len(state.Maps) {
		return nil, errors.New("checkpoint has an invalid map output table")
	}
	if err := os.MkdirAll(filepath.Dir(state.OutputPrefix), 0755); err != nil {
		return nil, fmt.Errorf("create output directory while resuming: %w", err)
	}

	c := &Coordinator{
		maps:             state.Maps,
		reduces:          state.Reduces,
		outputs:          state.Outputs,
		nReduce:          state.NReduce,
		generation:       state.Generation + 1,
		mapsRemaining:    state.MapsRemaining,
		reducesRemaining: state.ReducesRemaining,
		workDir:          state.WorkDir,
		inputTempDir:     state.InputTempDir,
		outputPrefix:     state.OutputPrefix,
		checkpointPath:   checkpointPath,
		checkpointLock:   lock,
		workers:          make(map[int]*mr.WorkerInfo),
		done:             make(chan struct{}),
		monitorStop:      make(chan struct{}),
		monitorDone:      make(chan struct{}),
		finished:         state.Finished,
		failure:          state.Failure,
	}
	for i := range c.maps {
		if c.maps[i].Status != mr.Done {
			if c.maps[i].Status == mr.Running && c.maps[i].Task.Attempt > 0 {
				c.maps[i].Task.Attempt--
			}
			c.maps[i].Status = mr.Idle
			c.mapQueue = append(c.mapQueue, i)
		}
	}
	for i := range c.reduces {
		if c.reduces[i].Status != mr.Done {
			if c.reduces[i].Status == mr.Running && c.reduces[i].Task.Attempt > 0 {
				c.reduces[i].Task.Attempt--
			}
			c.reduces[i].Status = mr.Idle
			c.reduceQueue = append(c.reduceQueue, i)
		}
	}
	if c.finished || c.failure != "" {
		c.finished = true
		close(c.done)
	}

	// Rewrite the checkpoint immediately with all formerly running tasks idle.
	if err := c.persistLocked(); err != nil {
		return nil, fmt.Errorf("update checkpoint during recovery: %w", err)
	}
	keepLock = true
	return c, nil
}

func acquireCheckpointLock(checkpointPath string) (*os.File, error) {
	if err := os.MkdirAll(filepath.Dir(checkpointPath), 0755); err != nil {
		return nil, fmt.Errorf("create checkpoint directory: %w", err)
	}
	lock, err := os.OpenFile(checkpointPath+".lock", os.O_CREATE|os.O_RDWR, 0660)
	if err != nil {
		return nil, fmt.Errorf("open coordinator lock: %w", err)
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = lock.Close()
		return nil, fmt.Errorf("another coordinator holds checkpoint lock %q", checkpointPath)
	}
	return lock, nil
}

func releaseCheckpointLock(lock *os.File) {
	if lock == nil {
		return
	}
	_ = syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	_ = lock.Close()
}

func (c *Coordinator) releaseCheckpointLock() {
	if c.checkpointLock == nil {
		return
	}
	releaseCheckpointLock(c.checkpointLock)
	c.checkpointLock = nil
}

func (c *Coordinator) persistLocked() error {
	if c.checkpointPath == "" {
		return nil
	}
	state := checkpointState{
		Version:         checkpointVersion,
		Generation:      c.generation,
		Maps:             c.maps,
		Reduces:          c.reduces,
		Outputs:          c.outputs,
		NReduce:          c.nReduce,
		MapsRemaining:    c.mapsRemaining,
		ReducesRemaining: c.reducesRemaining,
		WorkDir:          c.workDir,
		InputTempDir:     c.inputTempDir,
		OutputPrefix:     c.outputPrefix,
		Finished:         c.finished,
		Failure:          c.failure,
	}

	if err := os.MkdirAll(filepath.Dir(c.checkpointPath), 0755); err != nil {
		return err
	}
	temp, err := os.CreateTemp(filepath.Dir(c.checkpointPath), ".mr-checkpoint-*")
	if err != nil {
		return err
	}
	tempPath := temp.Name()
	defer os.Remove(tempPath)
	if err := temp.Chmod(0660); err != nil {
		_ = temp.Close()
		return err
	}

	if err := json.NewEncoder(temp).Encode(&state); err != nil {
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
	if err := os.Rename(tempPath, c.checkpointPath); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(c.checkpointPath))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func (c *Coordinator) checkpointLocked() {
	if err := c.persistLocked(); err != nil {
		c.failure = fmt.Sprintf("persist coordinator checkpoint: %v", err)
		c.finishLocked()
	}
}
