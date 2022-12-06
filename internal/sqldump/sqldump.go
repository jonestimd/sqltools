package sqldump

import (
	"compress/bzip2"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strings"

	"vitess.io/vitess/go/vt/sqlparser"
)

type SqlDump []*Table

func checkErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func openFile(filename string) io.Reader {
	raw, err := os.Open(filename)
	checkErr(err)
	if strings.HasSuffix(filename, ".bz2") {
		return bzip2.NewReader(raw)
	}
	return raw
}

func NewSqlDump(filename string) SqlDump {
	buf, err := ioutil.ReadAll(openFile(filename))
	checkErr(err)
	lines, err := sqlparser.SplitStatementToPieces(string(buf))
	checkErr(err)
	dump := make(SqlDump, 0)
	for _, line := range lines {
		stmt, err := sqlparser.Parse(line)
		checkErr(err)
		switch s := stmt.(type) {
		case *sqlparser.CreateTable:
			dump = append(dump, NewTable(s))
		case *sqlparser.Insert:
			dump.addInsert(s)
		}
	}
	return dump
}

func (dump SqlDump) addInsert(insert *sqlparser.Insert) {
	name := insert.Table.Name.String()
	for _, table := range dump {
		if table.create.Table.Name.String() == name {
			table.inserts = append(table.inserts, insert)
			return
		}
	}
	panic(fmt.Errorf("no create table for %s", name))
}

func (dump SqlDump) GetTable(name string) *Table {
	for _, table := range dump {
		if table.GetName() == name {
			return table
		}
	}
	return nil
}

func (dump SqlDump) Compare(oldDump SqlDump) []*TableChanges {
	changes := make([]*TableChanges, 0)
	for _, table := range dump {
		oldTable := oldDump.GetTable(table.GetName())
		if oldTable == nil {
			fmt.Fprintf(os.Stderr, "table added %s", table.GetName())
			changes = append(changes, &TableChanges{new: true, table: table})
		} else {
			change := table.Compare(oldTable)
			if !change.IsEmpty() {
				changes = append(changes, change)
			}
		}
	}
	return changes
}
