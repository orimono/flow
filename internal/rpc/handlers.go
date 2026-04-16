package rpc

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/nats-io/nats.go"
	"github.com/orimono/flow/internal/engine"
	"github.com/orimono/flow/internal/model"
	"github.com/orimono/flow/internal/store"
)

type Deps struct {
	FlowStore *store.FlowStore
	RunStore  *store.FlowRunStore
	Engine    *engine.Engine
}

func Register(nc *nats.Conn, deps Deps) {
	nc.Subscribe("orimono.flow.create", func(msg *nats.Msg) {
		var req struct {
			Name    string        `json:"name"`
			Steps   []model.Step  `json:"steps"`
			Edges   []model.Edge  `json:"edges"`
			Trigger *model.Trigger `json:"trigger,omitempty"`
		}
		if err := json.Unmarshal(msg.Data, &req); err != nil || req.Name == "" {
			msg.Respond(errResp("invalid request"))
			return
		}
		flow, err := deps.FlowStore.Create(context.Background(), req.Name, req.Steps, req.Edges, req.Trigger)
		if err != nil {
			msg.Respond(errResp(err.Error()))
			return
		}
		msg.Respond(mustMarshal(flow))
	})

	nc.Subscribe("orimono.flow.list", func(msg *nats.Msg) {
		flows, err := deps.FlowStore.List(context.Background())
		if err != nil {
			msg.Respond(errResp("query failed"))
			return
		}
		if flows == nil {
			flows = []*model.Flow{}
		}
		msg.Respond(mustMarshal(flows))
	})

	nc.Subscribe("orimono.flow.get", func(msg *nats.Msg) {
		var req struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(msg.Data, &req); err != nil || req.ID == "" {
			msg.Respond(errResp("id required"))
			return
		}
		flow, err := deps.FlowStore.Get(context.Background(), req.ID)
		if err != nil {
			msg.Respond(errResp("flow not found"))
			return
		}
		msg.Respond(mustMarshal(flow))
	})

	nc.Subscribe("orimono.flow.update", func(msg *nats.Msg) {
		var req struct {
			ID      string         `json:"id"`
			Name    string         `json:"name"`
			Steps   []model.Step   `json:"steps"`
			Edges   []model.Edge   `json:"edges"`
			Trigger *model.Trigger `json:"trigger,omitempty"`
		}
		if err := json.Unmarshal(msg.Data, &req); err != nil || req.ID == "" {
			msg.Respond(errResp("invalid request"))
			return
		}
		flow, err := deps.FlowStore.Update(context.Background(), req.ID, req.Name, req.Steps, req.Edges, req.Trigger)
		if err != nil {
			msg.Respond(errResp(err.Error()))
			return
		}
		msg.Respond(mustMarshal(flow))
	})

	nc.Subscribe("orimono.flow.delete", func(msg *nats.Msg) {
		var req struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(msg.Data, &req); err != nil || req.ID == "" {
			msg.Respond(errResp("id required"))
			return
		}
		if err := deps.FlowStore.Delete(context.Background(), req.ID); err != nil {
			msg.Respond(errResp(err.Error()))
			return
		}
		msg.Respond([]byte(`{"ok":true}`))
	})

	// ── Run management ───────────────────────────────────────────────────────

	nc.Subscribe("orimono.flow.run", func(msg *nats.Msg) {
		var req struct {
			FlowID string `json:"flow_id"`
		}
		if err := json.Unmarshal(msg.Data, &req); err != nil || req.FlowID == "" {
			msg.Respond(errResp("flow_id required"))
			return
		}
		flow, err := deps.FlowStore.Get(context.Background(), req.FlowID)
		if err != nil {
			msg.Respond(errResp("flow not found"))
			return
		}
		run, err := deps.Engine.StartRun(context.Background(), flow)
		if err != nil {
			msg.Respond(errResp(err.Error()))
			return
		}
		msg.Respond(mustMarshal(map[string]string{"run_id": run.ID}))
	})

	nc.Subscribe("orimono.flow.run.get", func(msg *nats.Msg) {
		var req struct {
			RunID string `json:"run_id"`
		}
		if err := json.Unmarshal(msg.Data, &req); err != nil || req.RunID == "" {
			msg.Respond(errResp("run_id required"))
			return
		}
		run, err := deps.RunStore.Get(context.Background(), req.RunID)
		if err != nil {
			msg.Respond(errResp("run not found"))
			return
		}
		msg.Respond(mustMarshal(run))
	})

	nc.Subscribe("orimono.flow.run.list", func(msg *nats.Msg) {
		var req struct {
			FlowID string `json:"flow_id"`
		}
		if err := json.Unmarshal(msg.Data, &req); err != nil || req.FlowID == "" {
			msg.Respond(errResp("flow_id required"))
			return
		}
		runs, err := deps.RunStore.ListByFlow(context.Background(), req.FlowID)
		if err != nil {
			msg.Respond(errResp("query failed"))
			return
		}
		if runs == nil {
			runs = []*model.FlowRun{}
		}
		msg.Respond(mustMarshal(runs))
	})

	slog.Info("flow rpc handlers registered")
}

func errResp(msg string) []byte {
	b, _ := json.Marshal(map[string]string{"error": msg})
	return b
}

func mustMarshal(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}
