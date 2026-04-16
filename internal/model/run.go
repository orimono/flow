package model

import "time"

type RunStatus string

const (
	RunStatusPending  RunStatus = "pending"
	RunStatusRunning  RunStatus = "running"
	RunStatusDone     RunStatus = "done"
	RunStatusFailed   RunStatus = "failed"
)

type StepRun struct {
	StepID    string         `json:"step_id"`
	Status    RunStatus      `json:"status"`
	Output    map[string]any `json:"output,omitempty"` // port name -> value
	Error     string         `json:"error,omitempty"`
	StartedAt *time.Time     `json:"started_at,omitempty"`
	EndedAt   *time.Time     `json:"ended_at,omitempty"`
}

type FlowRun struct {
	ID        string              `json:"id"`
	FlowID    string              `json:"flow_id"`
	FlowName  string              `json:"flow_name"`
	Status    RunStatus           `json:"status"`
	StepRuns  map[string]*StepRun `json:"step_runs"` // step_id -> StepRun
	CreatedAt time.Time           `json:"created_at"`
	UpdatedAt time.Time           `json:"updated_at"`
}
