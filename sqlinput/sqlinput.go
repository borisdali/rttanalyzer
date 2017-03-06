/*
Copyright 2016 Google Inc. All Rights Reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package sqlinput either parses an AWR report or reads a library cache
// in the attempt to auto generate an rtta.sqlinput file.
//
// This is not a recommended way to setup rtta.sqlinput, however it's a way
// to get started with RTTA if the business transactions that user
// cares about most are not particularly obvious.
package sqlinput

import (
	"fmt"
	"io/ioutil"
	"math"
	"path/filepath"
	"strconv"
	"strings"
	"unicode"

	"github.com/borisdali/rttanalyzer/rttanalyzer"
)

const (
	sqlOrderedByELA   = "SQL ordered by Elapsed Time"
	cntrlL            = ""
	fieldsInAWRReport = 7
	topSQLStmts       = 8
	bumpUpPercent     = 0.2
)

const boilerPlate = `#
# Real Time Trace Analyzer (RTTAnalyzer)
# rtta.sqlinput: one of the two input files (the other is rtta.conf) for the RTTA utility:
#
# Formating rules:
# - One line per business transaction that users care about most
# - Each non-commented line must consist of at least 3 comma-delimited fields:
#      "Business Tx Name", "Monitoring/Alerting Threshold", list (but at least one of) SQL IDs.
#
# Each field may be optionally enclosed in double quotes.  Blank lines are ignored.
#

`

var Debug bool

// ExpensiveSQL is a structure to hold SQL statements from AWR report.
type ExpensiveSQL struct {
	sqlid            string
	elaPerExec       float64
	elaPerExecString string
	ela              string
	execs            string
	percentTotal     string
	percentCPU       string
	percentIO        string
}

// PersistSQLInput saves the SQL statements mined from an AWR report into rtta.sqlinput file.
func PersistSQLInput(expSQL []ExpensiveSQL) error {
	fileName := filepath.Join(rttanalyzer.Dir(), "rtta.sqlinput.fromAWR")

	var expSQLAll string
	expSQLAllComment := "# Top AWR statements from SQL ordered by Elapsed Time section:\n"
	traceStmt := "# Trace enable helper:\n#   alter system set events 'sql_trace [sql:"

	// A single set trace event command doesn't appear to allow more than 8 statements (ORA-49100, ORA-49165)
	if Debug { fmt.Printf("dbg> expSQL before chop: %v", expSQL)}
	if len(expSQL) > topSQLStmts {
		expSQL = expSQL[:topSQLStmts]
	}
	if Debug { fmt.Printf("dbg> expSQL after chop: %v", expSQL)}
	for i, v := range expSQL {
		// Rounding rules for the threshold:
		//   If the current "elapsed time per execution" is less than 1, set it to 1 [second].
		//   If it's greater than 1 [second], add bumpUpPercent (20% by default) and round up.
		threshold := 1.0
		if v.elaPerExec > 1 {
			threshold = math.Floor(v.elaPerExec + v.elaPerExec*bumpUpPercent + .5)
		}
		expSQLAll += fmt.Sprintf("ExpensiveSQL#%d, %.0f, %s\n", i+1, threshold, v.sqlid)
		expSQLAllComment += "#   ela=" + v.ela + ", execs=" + v.execs + ", elaPerExec=" +
			v.elaPerExecString + ", % Total=" + v.percentTotal + ", % CPU=" + v.percentCPU + ",% I/O=" +
			v.percentIO + ", SQLid=" + v.sqlid + "\n"
		traceStmt += v.sqlid + "|"
	}
	traceStmt = traceStmt[:len(traceStmt)-1]
	traceStmt += "]';\n#\n"
	out := []byte(boilerPlate + expSQLAll + "\n" + expSQLAllComment + "\n" + traceStmt)
	ioutil.WriteFile(fileName, out, 0644)
	return nil
}

func parseAWRLine(rec string, fieldsNumber int) (*ExpensiveSQL, error) {
	words := strings.FieldsFunc(rec, func(r rune) bool {
		return unicode.IsSpace(r)
	})
	if len(words) != fieldsNumber {
		return nil, fmt.Errorf("parseAWRLine: Skip. Expected number of words in a valid SQL id line is %d. Got %d instead. words=%v\n", fieldsInAWRReport, len(words), words)
	}
	if Debug { fmt.Printf("dbg> words=%q, words[2]=%v, words[6]=%v\n", words, words[2], words[6])}
	elaPerExec, err := strconv.ParseFloat(words[2], 64)
	if err != nil {
		return nil, fmt.Errorf("parseAWRLine: Skip. elaPerExec doesn't appear to be a number: words[2]=%v, err=%v\n", words[2], err)
	}

	return &ExpensiveSQL{
		sqlid:            strings.TrimSpace(words[6]),
		elaPerExec:       elaPerExec,
		elaPerExecString: words[2],
		ela:              words[0],
		execs:            words[1],
		percentTotal:     words[3],
		percentCPU:       words[4],
		percentIO:        words[5],
	}, nil
}

// ParseAWR parses an AWR report and auto generates rtta.sqlinput file.
func ParseAWR(dbName string, fileName string) ([]ExpensiveSQL, error) {
	f, err := rttanalyzer.OpenTraceFile(fileName, nil)
	if err != nil {
		return nil, err
	}

	var expSQL []ExpensiveSQL
	analyzeLineByLine := false

	for {
		strs, err := f.ReadRecords()
		if err != nil {
			return nil, err
		}
		if len(strs) == 0 {
			break
		}
		for _, v := range strs {
			// Skip the content until get to the "SQL ordered by Elapsed Time" section.
			// Then flip the analyzeLineByLine bit to true to trigger line-by-line inspection.
			if strings.HasPrefix(v, sqlOrderedByELA) {
				analyzeLineByLine = true
				break
			}
			// If "SQL ordered by Elapsed Time" section spans multiple pages
			// of the report, limit the analysis only to the first one.

			if !analyzeLineByLine {
				continue
			}
			if strings.HasPrefix(v, cntrlL) {
				analyzeLineByLine = false
				break
			}
			// Iterate line-by-line until we get to the next Cntrl-L section.
			if Debug { fmt.Printf("dbg> %v", v)}
			if s, err := parseAWRLine(v, fieldsInAWRReport); err != nil {
				if Debug { fmt.Print(err)}
			} else {
				if Debug { fmt.Printf("dbg> s=%v", *s)}
				expSQL = append(expSQL, *s)
			}
		}
	}
	if Debug { fmt.Printf("dbg> ExpensiveSQL=%v\n", expSQL)}
	return expSQL, nil
}
