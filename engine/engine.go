package engine

import (
	"fmt"
)

func (db *NewDatabase) InsertRow(tableName string, id string, data interface{}) error {
	table, ok := db.Tables[tableName]

	if !ok {
		return fmt.Errorf("table '%s' does not exist in database", tableName)
	}

	if RowKeyExists(table.Rows, id) {
		return fmt.Errorf("id '%s' already exists in table '%s'", id, tableName)
	}

	newRow := Row{
		Columns: make(map[string]interface{}),
	}
	newRow.Columns["id"] = id
	newRow.Columns["data"] = data

	table.Rows = append(table.Rows, newRow)

	db.Tables[tableName] = table

	return nil
}

func (db *NewDatabase) UpdateRow(tableName string, id string, newData map[string]interface{}) error {
	table, ok := db.Tables[tableName]

	if !ok {
		return fmt.Errorf("table '%s' does not exist in database", tableName)
	}

	index := -1

	for i, row := range table.Rows {
		if val, ok := row.Columns["id"].(string); ok && val == id {
			index = i
			break
		}
	}

	if index == -1 {
		return fmt.Errorf("id '%s' not found in table '%s'", id, tableName)
	}

	for key, value := range newData {
		table.Rows[index].Columns[key] = value
	}

	db.Tables[tableName] = table

	return nil
}

func (db *NewDatabase) DeleteRow(tableName string, id string) error {
	table, ok := db.Tables[tableName]

	if !ok {
		return fmt.Errorf("table '%s' does not exist in database", tableName)
	}

	index := -1

	for i, row := range table.Rows {
		if val, ok := row.Columns["id"].(string); ok && val == id {
			index = i
			break
		}
	}

	if index == -1 {
		return fmt.Errorf("id '%s' not found in table '%s'", id, tableName)
	}

	table.Rows = append(table.Rows[:index], table.Rows[index+1:]...)

	db.Tables[tableName] = table

	return nil
}

func (db *NewDatabase) GetRowByID(tableName string, id string) (Row, error) {
	table, ok := db.Tables[tableName]

	if !ok {
		return Row{}, fmt.Errorf("table '%s' does not exist in database", tableName)
	}

	for _, row := range table.Rows {
		if val, ok := row.Columns["id"].(string); ok && val == id {
			return row, nil
		}
	}

	return Row{}, fmt.Errorf("id '%s' not found in table '%s'", id, tableName)
}

func (db *NewDatabase) GetAllRows(tableName string) ([]Row, error) {
	table, ok := db.Tables[tableName]

	if !ok {
		return nil, fmt.Errorf("table '%s' does not exist in database", tableName)
	}

	return table.Rows, nil
}

func (db *NewDatabase) CountRows(tableName string) (int, error) {
	table, ok := db.Tables[tableName]

	if !ok {
		return 0, fmt.Errorf("table '%s' does not exist in database", tableName)
	}

	return len(table.Rows), nil
}

func (db *NewDatabase) CreateTable(tableName string, columns []Column, indexes []Index) error {
	_, exists := db.Tables[tableName]

	if exists {
		return fmt.Errorf("table '%s' already exists in database", tableName)
	}

	newTable := Table{
		Name:    tableName,
		Columns: columns,
		Indexes: indexes,
		Rows:    []Row{},
	}

	db.Tables[tableName] = newTable

	return nil
}

func (db *NewDatabase) DropTable(tableName string) error {
	_, ok := db.Tables[tableName]

	if !ok {
		return fmt.Errorf("table '%s' does not exist in database", tableName)
	}

	delete(db.Tables, tableName)

	return nil
}

func RowKeyExists(rows []Row, id string) bool {
	for _, row := range rows {
		if val, ok := row.Columns["id"].(string); ok && val == id {
			return true
		}
	}
	return false
}
