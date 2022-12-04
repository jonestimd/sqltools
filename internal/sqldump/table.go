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

func (table *Table) GetInserts() []*sqlparser.Insert {
	return table.inserts
}

func (table *Table) GetRows() sqlparser.Values {
	rows := make(sqlparser.Values, 0)
	for _, insert := range table.inserts {
		if v, ok := insert.Rows.(sqlparser.Values); ok {
			rows = append(rows, v...)
		} else {
			panic(fmt.Errorf("unexpected row type %T", insert.Rows))
		}
	}
	return rows
}

func (table *Table) GetRowCount() int {
	var count int = 0
	for _, insert := range table.inserts {
		if v, ok := insert.Rows.(sqlparser.Values); ok {
			count += len(v)
		} else {
			panic(fmt.Errorf("unexpected row type %T", insert.Rows))
		}
	}
	return count
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
	rows := table.GetRows()
	oldRows := oldTable.GetRows()
	count := len(rows)
	oldCount := len(oldRows)
	r := 0
	var oldR int
	same := true
	for oldR = 0; r < count && oldR < oldCount; oldR++ {
		if sqlparser.EqualsValTuple(rows[r], oldRows[oldR]) {
			r++
		} else if sqlparser.EqualsValTuple(table.getRowPK(rows[r]), oldTable.getRowPK(oldRows[oldR])) {
			id := sqlparser.String(oldTable.getRowPK(oldRows[oldR]))
			fmt.Fprintf(os.Stdout, "row %s updated from %s\n", id, name)
			same = false
			r++
		} else {
			id := sqlparser.String(oldTable.getRowPK(oldRows[oldR]))
			fmt.Fprintf(os.Stdout, "row %s deleted from %s\n", id, name)
			same = false
		}
	}
	for ; oldR < oldCount; oldR++ {
		id := sqlparser.String(oldTable.getRowPK(oldRows[oldR]))
		fmt.Fprintf(os.Stdout, "row %s deleted from %s\n", id, name)
		same = false
	}
	for ; r < count; r++ {
		id := sqlparser.String(table.getRowPK(rows[r]))
		fmt.Fprintf(os.Stdout, "row %s inserted into %s\n", id, name)
		same = false
	}
	return same
}
