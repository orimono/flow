package model

import "time"

// ── Port types ───────────────────────────────────────────────────────────────

type PortBase string

const (
	PortBaseString  PortBase = "string"
	PortBaseNumber  PortBase = "number"
	PortBaseBoolean PortBase = "boolean"
	PortBaseBytes   PortBase = "bytes"
	PortBaseStream  PortBase = "stream"
	PortBaseJSON    PortBase = "json"
	PortBaseAny     PortBase = "any"
)

type PortType struct {
	Base     PortBase       `json:"base"`
	Semantic string         `json:"semantic,omitempty"` // e.g. "node-id", "exit-code", "file-path"
	Schema   map[string]any `json:"schema,omitempty"`   // JSON Schema for base=json
}

type PortDirection string

const (
	PortInput  PortDirection = "input"
	PortOutput PortDirection = "output"
)

type Port struct {
	Name      string        `json:"name"`
	Direction PortDirection `json:"direction"`
	Type      PortType      `json:"type"`
	Required  bool          `json:"required"`
}

// PortRef identifies a specific port on a step.
type PortRef struct {
	StepID string `json:"step_id"`
	Port   string `json:"port"`
}

// ── Step ─────────────────────────────────────────────────────────────────────

type StepKind string

const (
	StepKindExecutor  StepKind = "executor"  // run a capability on a node
	StepKindCondition StepKind = "condition" // branch on a value
	StepKindFanOut    StepKind = "fan_out"   // scatter over a list
	StepKindBarrier   StepKind = "barrier"   // wait for all fan_out branches
)

// StepTarget describes which node(s) a step runs on.
// Exactly one of NodeID, NodeIDs, or FromPort should be set.
type StepTarget struct {
	NodeID   string   `json:"node_id,omitempty"`
	NodeIDs  []string `json:"node_ids,omitempty"`
	FromPort *PortRef `json:"from_port,omitempty"` // resolved at runtime
}

type Step struct {
	ID         string         `json:"id"`
	Kind       StepKind       `json:"kind"`
	Name       string         `json:"name"`
	Capability string         `json:"capability,omitempty"` // executor kind
	Config     map[string]any `json:"config,omitempty"`
	Target     StepTarget     `json:"target,omitempty"`
	Ports      []Port         `json:"ports,omitempty"`
}

// ── Edge ─────────────────────────────────────────────────────────────────────

type Edge struct {
	From      PortRef `json:"from"`
	To        PortRef `json:"to"`
	Transform string  `json:"transform,omitempty"` // JSONPath / expr applied to output value
}

// ── Trigger ──────────────────────────────────────────────────────────────────

type TriggerKind string

const (
	TriggerManual TriggerKind = "manual"
	TriggerCron   TriggerKind = "cron"
	TriggerEvent  TriggerKind = "event" // fires on a NATS subject
)

type Trigger struct {
	Kind    TriggerKind `json:"kind"`
	Cron    string      `json:"cron,omitempty"`
	Subject string      `json:"subject,omitempty"` // NATS subject for event triggers
}

// ── Flow definition ──────────────────────────────────────────────────────────

type Flow struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Steps     []Step    `json:"steps"`
	Edges     []Edge    `json:"edges"`
	Trigger   *Trigger  `json:"trigger,omitempty"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}
