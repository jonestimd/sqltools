package sqldump

import (
	"fmt"
	"os"
	"strings"

	"vitess.io/vitess/go/vt/sqlparser"
)

type TableChanges struct {
	new     bool
	table   *Table
	inserts sqlparser.Values
	updates []*TableUpdate
	deletes sqlparser.Values
}

type TableUpdate struct {
	oldValues sqlparser.ValTuple
	newValues sqlparser.ValTuple
}

func (tc *TableChanges) IsEmpty() bool {
	return !tc.new && len(tc.inserts) == 0 && len(tc.updates) == 0 && len(tc.deletes) == 0
}

func (tc *TableChanges) Write(file *os.File) {
	tableName := sqlparser.CanonicalString(tc.table.create.Table.Name)
	if len(tc.inserts) > 0 {
		fmt.Fprintf(file, "INSERT INTO %s VALUES\n", tableName)
		rows := make([]string, 0)
		for _, row := range tc.inserts {
			rows = append(rows, sqlparser.String(row))
		}
		fmt.Fprintf(file, "%s;\n", strings.Join(rows, ",\n"))
	}
	for _, change := range tc.updates {
		separator := "SET"
		fmt.Fprintf(file, "UPDATE %s ", tableName)
		for ci, column := range tc.table.create.TableSpec.Columns {
			if !sqlparser.EqualsExpr(change.oldValues[ci], change.newValues[ci]) {
				fmt.Fprintf(file, "%s %s = %s", separator, sqlparser.CanonicalString(column.Name), sqlparser.String(change.newValues[ci]))
				separator = ","
			}
		}
		separator = "\nWHERE"
		for _, ci := range tc.table.pkColumns {
			fmt.Fprintf(file, "%s %s = %s", separator,
				sqlparser.CanonicalString(tc.table.create.TableSpec.Columns[ci].Name), sqlparser.String(change.newValues[ci]))
			separator = "\n  AND"
		}
		fmt.Fprint(file, ";\n")
	}
	if len(tc.deletes) > 0 {
		pkColumns := make([]string, 0)
		for _, ci := range tc.table.pkColumns {
			pkColumns = append(pkColumns, sqlparser.CanonicalString(tc.table.create.TableSpec.Columns[ci]))
		}
		idTuples := make([]string, 0)
		for _, delete := range tc.deletes {
			idTuples = append(idTuples, sqlparser.String(delete))
		}
		fmt.Fprintf(file, "DELETE FROM %s\nWHERE (%s) in (%s);\n", tableName, strings.Join(pkColumns, ", "), strings.Join(idTuples, ", "))
	}
}
