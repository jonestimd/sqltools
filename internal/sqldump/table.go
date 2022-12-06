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
	for _, index := range create.TableSpec.Indexes {
		if index.Info.Primary {
			for _, indexColumn := range index.Columns {
				name := indexColumn.Column
				for ci, tableColumn := range create.TableSpec.Columns {
					if sqlparser.EqualsIdentifierCI(name, tableColumn.Name) {
						pkColumns = append(pkColumns, ci)
					}
				}
			}
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

func (table *Table) Compare(oldTable *Table) *TableChanges {
	name := table.GetName()
	if name != oldTable.GetName() {
		panic(fmt.Errorf("different table names %s vs %s", name, oldTable.GetName()))
	}
	if len(table.create.TableSpec.Columns) != len(oldTable.create.TableSpec.Columns) {
		panic(fmt.Errorf("different column count for %s\n", name))
	}
	for c, col := range table.create.TableSpec.Columns {
		col2 := oldTable.create.TableSpec.Columns[c]
		if !col.Name.Equal(col2.Name) {
			panic(fmt.Errorf("different columns for %s\n", name))
		}
		if col.Type.SQLType() != col2.Type.SQLType() {
			panic(fmt.Errorf("different type for %s.%s\n", name, col.Name.String()))
		}
	}
	nextCurrent := table.RowIterator()
	nextOld := oldTable.RowIterator()
	changes := &TableChanges{table: table}
	currentRow, okCurrent := nextCurrent()
	oldRow, okOld := nextOld()
	for ; okCurrent && okOld; oldRow, okOld = nextOld() {
		if sqlparser.EqualsValTuple(currentRow, oldRow) {
			currentRow, okCurrent = nextCurrent()
		} else if sqlparser.EqualsValTuple(table.getRowPK(currentRow), oldTable.getRowPK(oldRow)) {
			changes.updates = append(changes.updates, &TableUpdate{oldRow, currentRow})
			currentRow, okCurrent = nextCurrent()
		} else {
			changes.deletes = append(changes.deletes, oldRow)
		}
	}
	for ; okOld; oldRow, okOld = nextOld() {
		changes.deletes = append(changes.deletes, oldRow)
	}
	for ; okCurrent; currentRow, okCurrent = nextCurrent() {
		changes.inserts = append(changes.inserts, currentRow)
	}
	return changes
}
