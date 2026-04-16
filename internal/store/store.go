package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/orimono/flow/internal/model"
)

// ── Flow store ───────────────────────────────────────────────────────────────

type FlowStore struct {
	pool *pgxpool.Pool
}

func NewFlowStore(pool *pgxpool.Pool) *FlowStore {
	return &FlowStore{pool: pool}
}

func (s *FlowStore) Create(ctx context.Context, name string, steps []model.Step, edges []model.Edge, trigger *model.Trigger) (*model.Flow, error) {
	def, err := marshalDef(steps, edges, trigger)
	if err != nil {
		return nil, err
	}
	row := s.pool.QueryRow(ctx,
		`INSERT INTO flows (name, definition) VALUES ($1, $2)
		 RETURNING id, name, definition, created_at, updated_at`,
		name, def,
	)
	return scanFlow(row)
}

func (s *FlowStore) List(ctx context.Context) ([]*model.Flow, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, name, definition, created_at, updated_at FROM flows ORDER BY created_at DESC`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var flows []*model.Flow
	for rows.Next() {
		f, err := scanFlow(rows)
		if err != nil {
			return nil, err
		}
		flows = append(flows, f)
	}
	return flows, rows.Err()
}

func (s *FlowStore) Get(ctx context.Context, id string) (*model.Flow, error) {
	row := s.pool.QueryRow(ctx,
		`SELECT id, name, definition, created_at, updated_at FROM flows WHERE id = $1`,
		id,
	)
	return scanFlow(row)
}

func (s *FlowStore) Update(ctx context.Context, id, name string, steps []model.Step, edges []model.Edge, trigger *model.Trigger) (*model.Flow, error) {
	def, err := marshalDef(steps, edges, trigger)
	if err != nil {
		return nil, err
	}
	row := s.pool.QueryRow(ctx,
		`UPDATE flows SET name = $2, definition = $3, updated_at = NOW()
		 WHERE id = $1
		 RETURNING id, name, definition, created_at, updated_at`,
		id, name, def,
	)
	return scanFlow(row)
}

func (s *FlowStore) Delete(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM flows WHERE id = $1`, id)
	return err
}

// ── FlowRun store ────────────────────────────────────────────────────────────

type FlowRunStore struct {
	pool *pgxpool.Pool
}

func NewFlowRunStore(pool *pgxpool.Pool) *FlowRunStore {
	return &FlowRunStore{pool: pool}
}

func (s *FlowRunStore) Create(ctx context.Context, flowID, flowName string) (*model.FlowRun, error) {
	stepRuns, _ := json.Marshal(map[string]*model.StepRun{})
	row := s.pool.QueryRow(ctx,
		`INSERT INTO flow_runs (flow_id, flow_name, status, step_runs)
		 VALUES ($1, $2, 'pending', $3)
		 RETURNING id, flow_id, flow_name, status, step_runs, created_at, updated_at`,
		flowID, flowName, stepRuns,
	)
	return scanRun(row)
}

func (s *FlowRunStore) Get(ctx context.Context, id string) (*model.FlowRun, error) {
	row := s.pool.QueryRow(ctx,
		`SELECT id, flow_id, flow_name, status, step_runs, created_at, updated_at
		 FROM flow_runs WHERE id = $1`,
		id,
	)
	return scanRun(row)
}

func (s *FlowRunStore) ListByFlow(ctx context.Context, flowID string) ([]*model.FlowRun, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, flow_id, flow_name, status, step_runs, created_at, updated_at
		 FROM flow_runs WHERE flow_id = $1 ORDER BY created_at DESC`,
		flowID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var runs []*model.FlowRun
	for rows.Next() {
		r, err := scanRun(rows)
		if err != nil {
			return nil, err
		}
		runs = append(runs, r)
	}
	return runs, rows.Err()
}

func (s *FlowRunStore) UpdateStatus(ctx context.Context, id string, status model.RunStatus) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE flow_runs SET status = $2, updated_at = NOW() WHERE id = $1`,
		id, string(status),
	)
	return err
}

func (s *FlowRunStore) UpdateStepRun(ctx context.Context, runID string, step *model.StepRun) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE flow_runs
		 SET step_runs = jsonb_set(step_runs, $2, $3), updated_at = NOW()
		 WHERE id = $1`,
		runID,
		fmt.Sprintf("{%s}", step.StepID),
		mustMarshal(step),
	)
	return err
}

// ── Helpers ──────────────────────────────────────────────────────────────────

type scanner interface {
	Scan(dest ...any) error
}

type flowDef struct {
	Steps   []model.Step   `json:"steps"`
	Edges   []model.Edge   `json:"edges"`
	Trigger *model.Trigger `json:"trigger,omitempty"`
}

func marshalDef(steps []model.Step, edges []model.Edge, trigger *model.Trigger) ([]byte, error) {
	return json.Marshal(flowDef{Steps: steps, Edges: edges, Trigger: trigger})
}

func scanFlow(row scanner) (*model.Flow, error) {
	var f model.Flow
	var defRaw []byte
	var updatedAt *time.Time
	if err := row.Scan(&f.ID, &f.Name, &defRaw, &f.CreatedAt, &updatedAt); err != nil {
		return nil, err
	}
	if updatedAt != nil {
		f.UpdatedAt = *updatedAt
	}
	var def flowDef
	if err := json.Unmarshal(defRaw, &def); err != nil {
		return nil, err
	}
	f.Steps = def.Steps
	f.Edges = def.Edges
	f.Trigger = def.Trigger
	return &f, nil
}

func scanRun(row scanner) (*model.FlowRun, error) {
	var r model.FlowRun
	var stepRunsRaw []byte
	var updatedAt *time.Time
	if err := row.Scan(&r.ID, &r.FlowID, &r.FlowName, &r.Status, &stepRunsRaw, &r.CreatedAt, &updatedAt); err != nil {
		return nil, err
	}
	if updatedAt != nil {
		r.UpdatedAt = *updatedAt
	}
	if err := json.Unmarshal(stepRunsRaw, &r.StepRuns); err != nil {
		return nil, err
	}
	return &r, nil
}

func mustMarshal(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}
