package engine

import (
	"time"
)

type NewDatabase struct {
	Name   string
	Tables map[string]Table
}

type Table struct {
	Name    string
	Columns []Column
	Indexes []Index
	Rows    []Row
}

type IndexEntry struct {
	Key interface{}
	Row Row
}

type Column struct {
	Name     string
	DataType DataType
	Nullable bool
}

type Index struct {
	Name    string
	Columns []string
}

type DataType int

const (
	Int DataType = iota
	Float
	String
	DateTime
	Bool
)

type Row struct {
	Columns map[string]interface{}
}

type Query struct {
	Select  []string
	From    string
	Where   string
	OrderBy string
	Limit   int
}

type ExecutionPlan struct {
	Operations []Operation
}

type Operation struct {
	Type     OperationType
	Table    string
	Columns  []string
	Filter   string
	Order    string
	Limit    int
	Parent   *Operation
	Children []*Operation
	Result   chan Row
}

type OperationType int

const (
	Scan OperationType = iota
	Filter
	Project
	Sort
	LimitOp
)

type Transaction struct {
	ID        int
	Status    TransactionStatus
	StartedAt time.Time
}

type TransactionStatus int

const (
	Pending TransactionStatus = iota
	Committed
	RolledBack
)

type QueryResult struct {
	Columns []string
	Rows    []Row
}

type QueryError struct {
	Message string
}
