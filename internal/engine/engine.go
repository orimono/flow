package engine

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/orimono/flow/internal/model"
	"github.com/orimono/flow/internal/store"
)

// Engine executes FlowRuns step by step, coordinating with loom via NATS.
type Engine struct {
	nc           *nats.Conn
	flowStore    *store.FlowStore
	runStore     *store.FlowRunStore
	loomTimeout  time.Duration
}

func New(nc *nats.Conn, flowStore *store.FlowStore, runStore *store.FlowRunStore, loomTimeout time.Duration) *Engine {
	return &Engine{
		nc:          nc,
		flowStore:   flowStore,
		runStore:    runStore,
		loomTimeout: loomTimeout,
	}
}

// StartRun creates a FlowRun record and launches execution in a goroutine.
func (e *Engine) StartRun(ctx context.Context, flow *model.Flow) (*model.FlowRun, error) {
	run, err := e.runStore.Create(ctx, flow.ID, flow.Name)
	if err != nil {
		return nil, err
	}
	go e.execute(context.Background(), flow, run)
	return run, nil
}

// execute drives the DAG: resolves ready steps, runs them, propagates outputs.
func (e *Engine) execute(ctx context.Context, flow *model.Flow, run *model.FlowRun) {
	slog.Info("flow run started", "run_id", run.ID, "flow", flow.Name)
	e.runStore.UpdateStatus(ctx, run.ID, model.RunStatusRunning)

	// Build an adjacency map: step_id -> edges whose From.StepID matches.
	outEdges := make(map[string][]model.Edge)
	for _, edge := range flow.Edges {
		outEdges[edge.From.StepID] = append(outEdges[edge.From.StepID], edge)
	}

	// Track resolved port values across steps: step_id.port -> value.
	resolved := make(map[string]any)

	// Simple topological walk: keep executing steps whose inputs are all satisfied.
	// TODO: fan_out / barrier need a parallel dispatch layer on top of this.
	remaining := make(map[string]model.Step, len(flow.Steps))
	for _, s := range flow.Steps {
		remaining[s.ID] = s
	}

	for len(remaining) > 0 {
		progress := false
		for id, step := range remaining {
			if !e.inputsReady(step, flow.Edges, resolved) {
				continue
			}
			delete(remaining, id)
			progress = true

			inputs := e.collectInputs(step, flow.Edges, resolved)
			output, err := e.runStep(ctx, run.ID, step, inputs)

			now := time.Now()
			sr := &model.StepRun{StepID: step.ID, StartedAt: &now}
			if err != nil {
				sr.Status = model.RunStatusFailed
				sr.Error = err.Error()
				e.runStore.UpdateStepRun(ctx, run.ID, sr)
				e.runStore.UpdateStatus(ctx, run.ID, model.RunStatusFailed)
				slog.Error("step failed", "run_id", run.ID, "step", step.ID, "err", err)
				return
			}
			ended := time.Now()
			sr.Status = model.RunStatusDone
			sr.Output = output
			sr.EndedAt = &ended
			e.runStore.UpdateStepRun(ctx, run.ID, sr)

			// Propagate output values into resolved map.
			for k, v := range output {
				resolved[step.ID+"."+k] = v
			}
		}

		if !progress {
			slog.Error("flow run stalled: cycle or unsatisfiable inputs", "run_id", run.ID)
			e.runStore.UpdateStatus(ctx, run.ID, model.RunStatusFailed)
			return
		}
	}

	e.runStore.UpdateStatus(ctx, run.ID, model.RunStatusDone)
	slog.Info("flow run done", "run_id", run.ID)
	e.nc.Publish("orimono.flow.run.done", mustMarshal(map[string]string{"run_id": run.ID, "flow_id": run.FlowID}))
}

// inputsReady returns true when all required input ports of step have a value in resolved.
func (e *Engine) inputsReady(step model.Step, edges []model.Edge, resolved map[string]any) bool {
	for _, port := range step.Ports {
		if port.Direction != model.PortInput || !port.Required {
			continue
		}
		// Find an edge that connects something to this input port.
		connected := false
		for _, edge := range edges {
			if edge.To.StepID == step.ID && edge.To.Port == port.Name {
				if _, ok := resolved[edge.From.StepID+"."+edge.From.Port]; ok {
					connected = true
				}
				break
			}
		}
		// A required input with no incoming edge is a flow authoring error,
		// but we treat it as "ready" (will use zero value / config default).
		if !connected {
			continue
		}
		if !connected {
			return false
		}
	}
	return true
}

// collectInputs gathers resolved values for all incoming edges of a step.
func (e *Engine) collectInputs(step model.Step, edges []model.Edge, resolved map[string]any) map[string]any {
	inputs := make(map[string]any)
	for _, edge := range edges {
		if edge.To.StepID != step.ID {
			continue
		}
		val, ok := resolved[edge.From.StepID+"."+edge.From.Port]
		if !ok {
			continue
		}
		// TODO: apply edge.Transform (JSONPath / expr) to val before binding.
		inputs[edge.To.Port] = val
	}
	return inputs
}

// runStep dispatches a single step to the appropriate handler.
func (e *Engine) runStep(ctx context.Context, runID string, step model.Step, inputs map[string]any) (map[string]any, error) {
	switch step.Kind {
	case model.StepKindExecutor:
		return e.runExecutorStep(ctx, step, inputs)
	case model.StepKindCondition:
		return e.runConditionStep(step, inputs)
	case model.StepKindFanOut:
		// TODO: fan_out spawns parallel sub-runs and waits on a barrier.
		return map[string]any{}, nil
	case model.StepKindBarrier:
		// TODO: barrier waits for all fan_out branches to complete.
		return map[string]any{}, nil
	default:
		return map[string]any{}, nil
	}
}

// runExecutorStep asks loom to run an executor on the target node via NATS.
func (e *Engine) runExecutorStep(ctx context.Context, step model.Step, inputs map[string]any) (map[string]any, error) {
	payload := map[string]any{
		"node_id":  step.Target.NodeID,
		"executor": step.Config,
		"inputs":   inputs,
	}
	msg, err := e.nc.Request("orimono.loom.executor.register", mustMarshal(payload), e.loomTimeout)
	if err != nil {
		return nil, err
	}
	var result map[string]any
	json.Unmarshal(msg.Data, &result)
	return result, nil
}

// runConditionStep evaluates a simple condition and emits a branch port.
func (e *Engine) runConditionStep(step model.Step, inputs map[string]any) (map[string]any, error) {
	// TODO: evaluate step.Config["expression"] against inputs.
	// For now emit both ports as false so downstream graph can handle it.
	return map[string]any{"true": false, "false": true}, nil
}

func mustMarshal(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}
