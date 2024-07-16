package engine

import (
	"errors"
	"fmt"
)

var (
	ErrTableNotFound = errors.New("table not found in database")
	ErrIDNotFound    = errors.New("ID not found in table")
	ErrIDExists      = errors.New("ID already exists in table")
	ErrTableExists   = errors.New("table already exists in database")
)

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
