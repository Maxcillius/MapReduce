// Package mr contains the wire types shared by the coordinator and workers.
package mr

import "time"

type TaskKind string

const (
	MapTask    TaskKind = "map"
	ReduceTask TaskKind = "reduce"
	WaitTask   TaskKind = "wait"
	ExitTask   TaskKind = "exit"
)

type WorkerState string

const (
	WorkerIdle   WorkerState = "idle"
	WorkerBusy   WorkerState = "busy"
	WorkerDead   WorkerState = "dead"
	WorkerExited WorkerState = "exited"
)

type Task struct {
	ID          int
	Kind        TaskKind
	Input       string
	MapID       int
	ReduceID    int
	ReduceCount int
	Inputs      []string
	Attempt     int
	Generation  int
}

type RequestArgs struct{ WorkerID int }
type RequestReply struct{ Task Task }

type CompleteArgs struct {
	WorkerID int
	TaskID   int
	Attempt  int
	Generation int
	Outputs  []string
	Err      string
}
type CompleteReply struct{ Accepted bool }

type RegisterArgs struct{ WorkerID int }
type RegisterReply struct {
	Accepted bool
	Done     bool
	WorkDir  string
}

type HeartbeatArgs struct{ WorkerID int }
type HeartbeatReply struct{ Accepted bool }

type WorkerInfo struct {
	ID       int
	State    WorkerState
	LastSeen time.Time
	TaskID   int
}

type TaskStatus int

const (
	Idle TaskStatus = iota
	Running
	Done
)

type TaskRecord struct {
	Task     Task
	Status   TaskStatus
	WorkerID int
	Outputs  []string
}
