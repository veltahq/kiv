package engine

import (
	"errors"
	"fmt"
	"sort"
	"time"
)

var (
	ErrTableNotFound     = errors.New("table not found in database")
	ErrIDNotFound        = errors.New("ID not found in table")
	ErrIDExists          = errors.New("ID already exists in table")
	ErrTableExists       = errors.New("table already exists in database")
	ErrInvalidQuery      = errors.New("invalid query")
	ErrTransactionFailed = errors.New("transaction failed")
)

func (db *NewDatabase) ExecuteQuery(query Query) (QueryResult, error) {
	plan, err := db.createExecutionPlan(query)

	if err != nil {
		return QueryResult{}, err
	}

	result, err := db.executeplan(plan)

	if err != nil {
		return QueryResult{}, err
	}

	return result, nil
}

func (db *NewDatabase) createExecutionPlan(query Query) (ExecutionPlan, error) {
	plan := ExecutionPlan{}

	scanOp := Operation{
		Type:  Scan,
		Table: query.From,
	}
	plan.Operations = append(plan.Operations, scanOp)

	if query.Where != "" {
		filterOp := Operation{
			Type:   Filter,
			Filter: query.Where,
			Parent: &plan.Operations[len(plan.Operations)-1],
		}
		plan.Operations = append(plan.Operations, filterOp)
	}

	projectOp := Operation{
		Type:    Project,
		Columns: query.Select,
		Parent:  &plan.Operations[len(plan.Operations)-1],
	}
	plan.Operations = append(plan.Operations, projectOp)

	if query.OrderBy != "" {
		sortOp := Operation{
			Type:   Sort,
			Order:  query.OrderBy,
			Parent: &plan.Operations[len(plan.Operations)-1],
		}
		plan.Operations = append(plan.Operations, sortOp)
	}

	if query.Limit > 0 {
		limitOp := Operation{
			Type:   LimitOp,
			Limit:  query.Limit,
			Parent: &plan.Operations[len(plan.Operations)-1],
		}
		plan.Operations = append(plan.Operations, limitOp)
	}

	return plan, nil
}

func (db *NewDatabase) executeplan(plan ExecutionPlan) (QueryResult, error) {
	var result QueryResult
	var rows []Row

	db.mu.RLock()
	defer db.mu.RUnlock()

	table, ok := db.Tables[plan.Operations[0].Table]
	if !ok {
		return result, fmt.Errorf("%w: %s", ErrTableNotFound, plan.Operations[0].Table)
	}

	rows = table.Rows

	for _, op := range plan.Operations {
		switch op.Type {
		case Filter:
			rows = filterRows(rows, op.Filter)
		case Project:
			result.Columns = op.Columns
			rows = projectRows(rows, op.Columns)
		case Sort:
			sortRows(rows, op.Order)
		case LimitOp:
			if len(rows) > op.Limit {
				rows = rows[:op.Limit]
			}
		}
	}

	result.Rows = rows
	return result, nil
}

func filterRows(rows []Row, filter string) []Row {
	var filtered []Row

	for _, row := range rows {
		if evaluateFilter(row, filter) {
			filtered = append(filtered, row)
		}
	}

	return filtered
}

func evaluateFilter(row Row, filter string) bool {
	return true
}

func projectRows(rows []Row, columns []string) []Row {
	var projected []Row
	for _, row := range rows {
		newRow := Row{Columns: make(map[string]interface{})}
		for _, col := range columns {
			if val, ok := row.Columns[col]; ok {
				newRow.Columns[col] = val
			}
		}
		projected = append(projected, newRow)
	}
	return projected
}

func sortRows(rows []Row, _ string) {
	sort.Slice(rows, func(i, j int) bool {
		return true
	})
}

func (db *NewDatabase) BeginTransaction() (*Transaction, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	transaction := &Transaction{
		ID:        generateTransactionID(),
		Status:    Pending,
		StartedAt: time.Now(),
	}

	return transaction, nil
}

func (db *NewDatabase) CommitTransaction(transaction *Transaction) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if transaction.Status != Pending {
		return ErrTransactionFailed
	}

	transaction.Status = Committed
	return nil
}

func (db *NewDatabase) RollbackTransaction(transaction *Transaction) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if transaction.Status != Pending {
		return ErrTransactionFailed
	}

	transaction.Status = RolledBack
	return nil
}

func generateTransactionID() int {
	return time.Now().Nanosecond()
}

func (db *NewDatabase) InsertRow(tableName, id string, data map[string]interface{}) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	table, ok := db.Tables[tableName]

	if !ok {
		return fmt.Errorf("%w: %s", ErrTableNotFound, tableName)
	}

	if rowKeyExists(table.Rows, id) {
		return fmt.Errorf("%w: %s in table %s", ErrIDExists, id, tableName)
	}

	newRow := Row{
		Columns: make(map[string]interface{}),
	}
	newRow.Columns["id"] = id

	for key, value := range data {
		newRow.Columns[key] = value
	}

	table.Rows = append(table.Rows, newRow)
	db.Tables[tableName] = table

	return nil
}

func (db *NewDatabase) UpdateRow(tableName, id string, newData map[string]interface{}) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	table, ok := db.Tables[tableName]

	if !ok {
		return fmt.Errorf("%w: %s", ErrTableNotFound, tableName)
	}

	for i, row := range table.Rows {
		if val, ok := row.Columns["id"].(string); ok && val == id {
			for key, value := range newData {
				table.Rows[i].Columns[key] = value
			}
			db.Tables[tableName] = table
			return nil
		}
	}

	return fmt.Errorf("%w: %s in table %s", ErrIDNotFound, id, tableName)
}

func (db *NewDatabase) DeleteRow(tableName, id string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	table, ok := db.Tables[tableName]

	if !ok {
		return fmt.Errorf("%w: %s", ErrTableNotFound, tableName)
	}

	for i, row := range table.Rows {
		if val, ok := row.Columns["id"].(string); ok && val == id {
			table.Rows = append(table.Rows[:i], table.Rows[i+1:]...)
			db.Tables[tableName] = table
			return nil
		}
	}

	return fmt.Errorf("%w: %s in table %s", ErrIDNotFound, id, tableName)
}

func (db *NewDatabase) GetRowByID(tableName, id string) (Row, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	table, ok := db.Tables[tableName]

	if !ok {
		return Row{}, fmt.Errorf("%w: %s", ErrTableNotFound, tableName)
	}

	for _, row := range table.Rows {
		if val, ok := row.Columns["id"].(string); ok && val == id {
			return row, nil
		}
	}

	return Row{}, fmt.Errorf("%w: %s in table %s", ErrIDNotFound, id, tableName)
}

func (db *NewDatabase) GetAllRows(tableName string) ([]Row, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	table, ok := db.Tables[tableName]

	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrTableNotFound, tableName)
	}

	return table.Rows, nil
}

func (db *NewDatabase) CountRows(tableName string) (int, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	table, ok := db.Tables[tableName]

	if !ok {
		return 0, fmt.Errorf("%w: %s", ErrTableNotFound, tableName)
	}

	return len(table.Rows), nil
}

func (db *NewDatabase) CreateTable(tableName string, columns []Column, indexes []Index) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if _, exists := db.Tables[tableName]; exists {
		return fmt.Errorf("%w: %s", ErrTableExists, tableName)
	}

	db.Tables[tableName] = Table{
		Name:    tableName,
		Columns: columns,
		Indexes: indexes,
		Rows:    []Row{},
	}

	return nil
}

func (db *NewDatabase) DropTable(tableName string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if _, ok := db.Tables[tableName]; !ok {
		return fmt.Errorf("%w: %s", ErrTableNotFound, tableName)
	}

	delete(db.Tables, tableName)
	return nil
}

func rowKeyExists(rows []Row, id string) bool {
	for _, row := range rows {
		if val, ok := row.Columns["id"].(string); ok && val == id {
			return true
		}
	}
	return false
}
