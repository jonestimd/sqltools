package sqldump

import (
	"fmt"
	"os"

	"vitess.io/vitess/go/vt/sqlparser"
)

type Table struct {
	create    *sqlparser.CreateTable
	inserts   []*sqlparser.Insert
	pkColumns []int
}

func NewTable(create *sqlparser.CreateTable) *Table {
	pkColumns := make([]int, 0)
	for i, index := range create.TableSpec.Indexes {
		if index.Info.Primary {
			pkColumns = append(pkColumns, i)
		}
	}
	if len(pkColumns) == 0 {
		fmt.Fprintf(os.Stderr, "no primary key for %s\n", create.Table.Name)
	}
	return &Table{create: create, pkColumns: pkColumns}
}

func (table *Table) GetName() string {
	return table.create.Table.Name.String()
}

func (table *Table) getInsertValues(index int) (sqlparser.Values, int) {
	if block, ok := table.inserts[index].Rows.(sqlparser.Values); ok {
		return block, len(block)
	} else {
		panic(fmt.Errorf("unexpected row type %T", table.inserts[index].Rows))
	}
}

func emptyRowIterator() (sqlparser.ValTuple, bool) {
	return nil, false
}

func (table *Table) RowIterator() func() (sqlparser.ValTuple, bool) {
	blocks := len(table.inserts)
	if blocks == 0 {
		return emptyRowIterator
	}
	bi := 0
	ri := 0
	block, rows := table.getInsertValues(bi)
	return func() (sqlparser.ValTuple, bool) {
		row := block[ri]
		ri++
		if ri == rows {
			ri = 0
			bi++
			if bi < blocks {
				block, rows = table.getInsertValues(bi)
			} else {
				block = nil
			}
		}
		return row, ri < rows && bi < blocks
	}
}

func (table *Table) getRowPK(row sqlparser.ValTuple) sqlparser.ValTuple {
	if len(table.pkColumns) == 0 {
		return row
	}
	pk := make(sqlparser.ValTuple, 0)
	for _, i := range table.pkColumns {
		pk = append(pk, row[i])
	}
	return pk
}

func (table *Table) Compare(oldTable *Table) bool {
	name := table.GetName()
	if name != oldTable.GetName() {
		panic(fmt.Errorf("different table names %s vs %s", name, oldTable.GetName()))
	}
	if len(table.create.TableSpec.Columns) != len(oldTable.create.TableSpec.Columns) {
		fmt.Fprintf(os.Stderr, "different column count for %s\n", name)
		return false
	}
	for c, col := range table.create.TableSpec.Columns {
		col2 := oldTable.create.TableSpec.Columns[c]
		if !col.Name.Equal(col2.Name) {
			fmt.Fprintf(os.Stderr, "different columns for %s\n", name)
			return false
		}
		if col.Type.SQLType() != col2.Type.SQLType() {
			fmt.Fprintf(os.Stderr, "different type for %s.%s\n", name, col.Name.String())
			return false
		}
	}
	nextCurrent := table.RowIterator()
	nextOld := oldTable.RowIterator()
	same := true
	currentRow, okCurrent := nextCurrent()
	oldRow, okOld := nextOld()
	for ; okCurrent && okOld; oldRow, okOld = nextOld() {
		if sqlparser.EqualsValTuple(currentRow, oldRow) {
			currentRow, okCurrent = nextCurrent()
		} else if sqlparser.EqualsValTuple(table.getRowPK(currentRow), oldTable.getRowPK(oldRow)) {
			id := sqlparser.String(oldTable.getRowPK(oldRow))
			fmt.Fprintf(os.Stdout, "row %s updated from %s\n", id, name)
			same = false
			currentRow, okCurrent = nextCurrent()
		} else {
			id := sqlparser.String(oldTable.getRowPK(oldRow))
			fmt.Fprintf(os.Stdout, "row %s deleted from %s\n", id, name)
			same = false
		}
	}
	for ; okOld; oldRow, okOld = nextOld() {
		id := sqlparser.String(oldTable.getRowPK(oldRow))
		fmt.Fprintf(os.Stdout, "row %s deleted from %s\n", id, name)
		same = false
	}
	for ; okCurrent; currentRow, okCurrent = nextCurrent() {
		id := sqlparser.String(table.getRowPK(currentRow))
		fmt.Fprintf(os.Stdout, "row %s inserted into %s\n", id, name)
		same = false
	}
	return same
}
