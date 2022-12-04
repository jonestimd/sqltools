package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path"
	"sort"

	"github.com/jonestimd/sqltools/internal/sqldump"
)

func main() {
	sortFiles := flag.Bool("s", false, "sort file names and process in reverse order")
	flag.Parse()
	log.SetOutput(os.Stderr)
	if len(flag.Args()) < 2 {
		fmt.Fprintf(os.Stderr, "need at least 2 input files\n")
		flag.Usage()
		os.Exit(1)
	}
	if *sortFiles {
		sort.Sort(sort.Reverse(sort.StringSlice(flag.Args())))
	}
	currentName := flag.Arg(0)
	current := sqldump.NewSqlDump(currentName)
	for _, filename := range flag.Args()[1:] {
		fmt.Fprintf(os.Stderr, "%s:\n", currentName)
		previous := sqldump.NewSqlDump(filename)
		if current.Compare(previous) {
			fmt.Fprintf(os.Stderr, "no change for %s and %s\n", path.Base(currentName), path.Base(filename))
		}
		currentName = filename
		current = previous
	}
}
