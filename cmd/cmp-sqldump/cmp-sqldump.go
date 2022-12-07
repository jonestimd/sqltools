package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path"
	"regexp"
	"sort"

	"github.com/jonestimd/sqltools/internal/sqldump"
)

var filenameRegex = regexp.MustCompile("(.bz2)?$")

func main() {
	flagSet := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	flagSet.Usage = func() {
		fmt.Fprintf(flagSet.Output(), "Usage: %s [options] file1 file2 ...\nOptions:\n", os.Args[0])
		flagSet.PrintDefaults()
	}
	sortFiles := flagSet.Bool("sort", false, "sort file names and process in reverse order")
	noSave := flagSet.Bool("no-save", false, "write diff(s) to stdout")
	outputSuffix := flagSet.String("o", ".diff", "output file suffix")
	flagSet.Parse(os.Args[1:])
	log.SetOutput(os.Stderr)
	if len(flagSet.Args()) < 2 {
		fmt.Fprintf(os.Stderr, "need at least 2 input files\n")
		flagSet.Usage()
		os.Exit(1)
	}
	if *sortFiles {
		sort.Sort(sort.Reverse(sort.StringSlice(flagSet.Args())))
	}
	currentName := flagSet.Arg(0)
	current := sqldump.NewSqlDump(currentName)
	for _, oldName := range flagSet.Args()[1:] {
		previous := sqldump.NewSqlDump(oldName)
		tableChanges := current.Compare(previous)
		if len(tableChanges) == 0 {
			fmt.Fprintf(os.Stderr, "no change for %s and %s\n", path.Base(currentName), path.Base(oldName))
		} else {
			fmt.Fprintf(os.Stderr, "changes for %s and %s\n", path.Base(currentName), path.Base(oldName))
			var err error
			outfile := os.Stdout
			if !*noSave {
				outfileName := filenameRegex.ReplaceAllString(currentName, *outputSuffix)
				if outfile, err = os.OpenFile(outfileName, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0666); err != nil {
					panic(fmt.Errorf("error creating file %s - %v", outfileName, err))
				}
			}
			for _, tc := range tableChanges {
				tc.Write(outfile)
			}
			if !*noSave {
				outfile.Close()
			}
		}
		currentName = oldName
		current = previous
	}
}
