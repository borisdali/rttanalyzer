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

// Package sink satisfies the Dumper interface receiving a SQL file to read
// from a Watchdog. Dumper is later passed by a Watchdog to a Miner.
package sink

import (
	"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"log"
	"golang.org/x/net/context"
	"github.com/borisdali/rttanalyzer/cursor"
	rttpubsub "github.com/borisdali/rttanalyzer/pubsub"
	bqgen "google.golang.org/api/bigquery/v2"
	"cloud.google.com/go/pubsub"
	"github.com/borisdali/rttanalyzer/rttanalyzer"
)

const (
	traceRecordTypeInvalid         = iota // Not a valid trace line
	traceRecordTypeParsingInCursor        // Initial cursor parsing (that leads to "openning" a new cursor and adding it to the map)
	traceRecordTypeParseExecFetch         // Actual PARSE, EXEC or FETCH cursor execution stages
)

var Debug bool
var errExistingCursor = fmt.Errorf("existing-cursor")

// CursorTrackerProtected is a syncronization mechanism to access the CursorTracker map.
type CursorTrackerProtected struct {
	sync.RWMutex
	Cursors map[int64]*cursor.Cursor
}

// Get performs a protected read of the underlying map.  It is safe to use from multiple
// goroutines simultaneously.
func (c *CursorTrackerProtected) get(key int64) *cursor.Cursor {
	c.RLock()
	defer c.RUnlock()
	return c.Cursors[key]
}

// hasValue performs a protected map lookup.  It is safe to use from multiple goroutines
// simultaneously.
func (c *CursorTrackerProtected) hasValue(key int64) bool {
	c.RLock()
	_, ok := c.Cursors[key]
	c.RUnlock()
	return ok
}

// set performs a protected write.  It is safe to use from multiple goroutines simultaneously.
func (c *CursorTrackerProtected) set(key int64, value *cursor.Cursor) {
	c.Lock()
	c.Cursors[key] = value
	c.Unlock()
}

// delete performs a protected write, and is safe to use from multiple goroutines.
func (c *CursorTrackerProtected) delete(key int64) {
	c.Lock()
	delete(c.Cursors, key)
	c.Unlock()
}

// compareAndSet automically checks if a cursor is new or an existing one.
// It a cursor is new, it then proceeds with "openning it" and "setting" it in the Cursor map.
func (c *CursorTrackerProtected) compareAndSet(key int64, open func() (*cursor.Cursor, error)) (*cursor.Cursor, error) {
	c.Lock()
	defer c.Unlock()
	if _, ok := c.Cursors[key]; ok {
		if Debug { fmt.Printf("[%v] dbg> compareAndSet: existing cursor: ok=%v\n", time.Now().Format("2006-01-02 15:04:05"), ok) }
		return nil, errExistingCursor
	}
	cur, err := open()
	if err != nil {
		return nil, fmt.Errorf("compareAndSet: error from calling open: %v", err)
	}

	// c.set(key, cur): this would cause double locking due to lock/unlock in the set method.
	c.Cursors[key] = cur
	if Debug { fmt.Printf("[%v] dbg> c.Cursors[key]: key=%v, value=%v\n", time.Now().Format("2006-01-02 15:04:05"), key, c.Cursors[key]) }
	return cur, nil
}

// Generic hosts common members of all structs and is meant to reduce code duplication.
type Generic struct {
	DBName        string
	FileSQL       string
	Client	      *pubsub.Client
	MonitoredSQLs []MonitoredSQL
	CursorTracker *CursorTrackerProtected
}

// Varz provides specific implemenation of the Dumper interface for Varz.
type Varz struct {
	Generic
	Dir           string
	FilePrefix    string
	FileExtension string
}

// Dump method specific to Varz target.
func (v *Varz) Dump(ctx context.Context, client *pubsub.Client, service *bqgen.Service, traceRec string) error {
	// TODO(bdali): This method may perform a *lot* of IO and so it may need to be refactored.
	// Spin up another goroutine that only dumps the varz line once every 30 seconds?
	pr, err := parseRecord(traceRec, v.MonitoredSQLs, v.CursorTracker)
	if err != nil {
		return err
	}
	if !pr.isViolation {
		return nil
	}
	// TODO(bdali): need to check/replace special characters with perhaps underscores.
	fileName := filepath.Join(v.Dir, v.FilePrefix+"."+v.DBName+"."+normalizeName(pr.businessTxName)+v.FileExtension)
	varzMessage := fmt.Sprintf("rttanalyzer{id=%s,businesstxname=%q,runtimethreshold=%.1f,sqlid=%s} map:stats lastela:%.3f worstela:%.3f violations:%d\n",
		v.DBName, pr.businessTxName, pr.threshold, pr.sqlID, pr.lastELA, pr.worstELA, pr.numViolations)
	out := []byte(varzMessage)
	if Debug { fmt.Printf("[%v] dbg> varz=%v\n", time.Now().Format("2006-01-02 15:04:05"), varzMessage)}
	ioutil.WriteFile(fileName, out, 0644)
	return nil
}

// mustLoadSQL loads SQL statements to watch for.
func mustLoadSQL(f string) ([]MonitoredSQL, error) {
	sql, err := loadSQL(f)
	if err != nil {
		return nil, err
	}
	//log.V(1).Infof("BusTx / SQL statements of interest: %v", sql)
	return sql, nil
}

// LoadSQL uploads user-provided mapping of business transactions to SQL statements.
func (v *Varz) LoadSQL() error {
	sql, err := mustLoadSQL(v.FileSQL)
	if err != nil {
		return err
	}
	if Debug { fmt.Printf("[%v] dbg> varz.LoadSQL: BusTx / SQL statements of interest: %v\n", time.Now().Format("2006-01-02 15:04:05"), sql)}
	v.MonitoredSQLs = sql
	return nil
}

// PubSub provides specific implementation of the Dumper interface for Cloud Pub/Sub.
// Not implemented yet..
type PubSub struct {
	Generic
	// PubSub specific attributes go here..
}

// Dump method specific to PubSub target.
func (ps *PubSub) Dump(ctx context.Context, client *pubsub.Client, service *bqgen.Service, traceRec string) error {
	pr, err := parseRecord(traceRec, ps.MonitoredSQLs, ps.CursorTracker)
	if err != nil {
		return err
	}
	if !pr.isViolation {
		return nil
	}

	psMessage := &rttpubsub.PayloadSummary{
		DB:             ps.DBName,
		IsViolation:    true,
		BusinessTxName: pr.businessTxName,
		Threshold:      pr.threshold,
		SQLID:          pr.sqlID,
		WorstELA:       pr.worstELA,
		LastELA:        pr.lastELA,
		NumViolations:  pr.numViolations,
		EnqueueTime:    time.Now(),
	}
	if err := rttpubsub.Enqueue(ctx, client, psMessage); err != nil {
		return fmt.Errorf("sink.Dump for PubSub: error in calling rttpubsub.Enqueue: %v", err)
	}
	return nil
}

// LoadSQL uploads user-provided mapping of business transactions to SQL statements.
func (ps *PubSub) LoadSQL() error {
	sql, err := mustLoadSQL(ps.FileSQL)
	if err != nil {
		return err
	}
	if Debug { fmt.Printf("[%v] dbg> pubsub.LoadSQL: BusTx / SQL statements of interest: %v\n", time.Now().Format("2006-01-02 15:04:05"), sql)}
	ps.MonitoredSQLs = sql
	return nil
}

// Streamz provides specific implementation of the Dumper interface for Monarch's StreamZ.
// Not implemented yet..
type Streamz struct {
	Generic
	// Streamz specific attributes go here..
}

// Dump method specific to Streamz target.
func (s *Streamz) Dump(ctx context.Context, client *pubsub.Client, service *bqgen.Service, traceRec string) error {
	pr, err := parseRecord(traceRec, s.MonitoredSQLs, s.CursorTracker)
	if err != nil {
		return err
	}
	if !pr.isViolation {
		return nil
	}

	// TODO(bdali): Not finished..
	return nil
}

// LoadSQL uploads user-provided mapping of business transactions to SQL statements.
func (s *Streamz) LoadSQL() error {
	sql, err := mustLoadSQL(s.FileSQL)
	if err != nil {
		return err
	}
	if Debug { fmt.Printf("[%v] dbg> streamz.LoadSQL: BusTx / SQL statements of interest: %v\n", time.Now().Format("2006-01-02 15:04:05"), sql)}
	s.MonitoredSQLs = sql
	return nil
}

// MonitoredSQL lists SQL statements that belong to each Business Tx of interest.
type MonitoredSQL struct {
	BusinessTxName string
	ELAThreshold   int64
	SQLID          []string
	LastELA        float64
	WorstELA       float64
	NumViolations  int64
}

// loadSQL lets RTTanalyzer know what SQL statements to look for by loading
// SQL statements from a user input file.
func loadSQL(filename string) ([]MonitoredSQL, error) {
	var s []MonitoredSQL
	// TODO(bdali): add a check if sqlInput is a fully qualified path as opposed to relative
	// and in this case do not join with Dir:
	fh, err := os.Open(filepath.Join(rttanalyzer.Dir(), filename))
	// fh, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	r := csv.NewReader(fh)
	r.FieldsPerRecord = -1
	r.TrimLeadingSpace = true
	r.Comment = '#'
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		elaThres, err := strconv.Atoi(record[1])
		if err != nil {
			return nil, err
		}
		s = append(s, MonitoredSQL{
			BusinessTxName: record[0],
			ELAThreshold:   int64(elaThres),
			SQLID:          record[2:],
		})
	}
	return s, nil
}

// parsedSummary consolidates trace file record data.
type parsedSummary struct {
	isViolation    bool
	businessTxName string
	threshold      float64
	sqlID          string
	worstELA       float64
	lastELA        float64
	numViolations  int64
}

// openCursor parses a trace record, gets other attributes and "opens" a cursor
// by instantiating a new cursor variable that is used to create a new tracker map entry.
func openCursor(rec string, getCursorID int64, getSQLID, businessTxName string, elaThreshold int64) (*cursor.Cursor, error) {
	// To open a cursor we are to get/parse the other cursor attributes.
	otherAttr, err := parseOtherAttr(rec)
	if err != nil {
		return nil, err
	}
	cur := cursor.NewCursor(getCursorID, getSQLID, businessTxName, elaThreshold,
		otherAttr.hashValue, otherAttr.length, otherAttr.depth, otherAttr.uID, otherAttr.lID, otherAttr.oct)
	return cur, nil
}

// traceRecordType determines whether or not a record is a valid (relevant) trace file record.
func traceRecordType(rec string) int {
	if strings.HasPrefix(rec, "PARSING IN CURSOR") && strings.Contains(rec, "sqlid") {
		return traceRecordTypeParsingInCursor
	}
	if strings.HasPrefix(rec, "PARSE #") || strings.HasPrefix(rec, "EXEC #") || strings.HasPrefix(rec, "FETCH #") {
		return traceRecordTypeParseExecFetch
	}
	// There are likely be more cases in the future when we introduce detailed logging.
	return traceRecordTypeInvalid
}

// parsingInCursor deals with "PARSING IN CURSOR" trace records, identifying if a SQL is the one that belongs
// to a business tx of interest and if so, opening/recording a cursor in a map.
// This function returns a newCursor (bool) to signal whether or not a new cursor has been opened.
// If an existing cursor has been used instead, it also returns a cursor ID (getCursorID int64), a SQL
// that this cursor was opened for (getSQLID string), a Business Tx in question that this SQL works for
// (businessTxName string) and the monitoring threshold on the SQL elapsed time (elaThreshold int64).
func parsingInCursor(rec string, wantSQL []MonitoredSQL, curTracker *CursorTrackerProtected) (newCursor bool, getCursorID int64, getSQLID, businessTxName string, elaThreshold int64, err error) {
	// Quick minimal parse just to get the SQL ID and Cursor# (the rest may not be needed for majority of trace records)
	// if the SQL ID is not the one of interest.
	getSQLID, getCursorString, err := parseSQLID(rec)
	if err != nil {
		return false, 0, "", "", 0, err
	}
	getCursorInt, err := strconv.Atoi(getCursorString)
	if err != nil {
		return false, 0, "", "", 0, fmt.Errorf("parsingInCursor: error in CursorID string->int conversion [SQLID=%s]: %v", getSQLID, err)
	}
	getCursorID = int64(getCursorInt)
	if Debug { fmt.Printf("[%v] dbg> getSQLID=%v, getCursorID=%d\n", time.Now().Format("2006-01-02 15:04:05"), getSQLID, getCursorID)}

	// Is the parsed cursor for our SQL ID of interest?
	var isInterestingSQL bool
	isInterestingSQL, businessTxName, elaThreshold = interestingSQL(getSQLID, wantSQL)
	if !isInterestingSQL {
		if Debug { fmt.Printf("[%v] dbg> parsingInCursor: a valid trace record containing PARSING IN CURSOR keywords, but not the SQL ID of interest(getSQLID=%v, wantSQL=%v): %v. Skipping..\n", time.Now().Format("2006-01-02 15:04:05"), getSQLID, wantSQL, strings.Replace(rec, "\n", "", 1))}
		return true, -1, "", "", -1, nil
	}

	// So we are parsing a cursor for a SQL of interest. Is the cursor already "Open"?
	// If not-> open a cursor. If yes-> check if the cursor is open for our SQL.
	fmt.Printf("[%v] info> interesting SQL found: %s (BusinessTxName=%s, ELA Threshold=%v)\n", time.Now().Format("2006-01-02 15:04:05"), getSQLID, businessTxName, elaThreshold)

	// Replace "if !curTracker.hasValue(getCursorID) {" test with the one below with stronger atomicity guarantees:
	_, err = curTracker.compareAndSet(getCursorID, func() (*cursor.Cursor, error) {
		return openCursor(rec, getCursorID, getSQLID, businessTxName, elaThreshold)
	})

	if err == errExistingCursor {
		fmt.Printf("[%v] info> parsingInCursor: existingCursor\n", time.Now().Format("2006-01-02 15:04:05"))
		return false, getCursorID, getSQLID, businessTxName, elaThreshold, nil
	}
	if err != nil {
		fmt.Printf("[%v] error> parsingInCursor: unexpected error in a new cursor.\n", time.Now().Format("2006-01-02 15:04:05"))
		return true, -1, "", "", -1, err
	}
	fmt.Printf("[%v] info> parsingInCursor: New cursor# %v. Open for SQLID=%v, BusinessTxName=%s: %s\n", time.Now().Format("2006-01-02 15:04:05"), getCursorID, getSQLID, businessTxName, rec)
	return true, -1, "", "", -1, nil
}

// parseRecord dissects the trace record, extract a cursor# and SQL ID.
// First off check whether a record is a valid trace record (starts with PARSING|PARSE|EXEC|FETCH).
// Next check whether a cursor is parsed for one of the SQL IDs of interest.
// Next check if that cursor# is already "Open" (i.e. known to us). If not, opne that cursor.
// Next we are ready to receive PARSE|EXEC|FETCH trace records for the previously opened cursor.
// Get the run time of each execution phase and compare against business tx. thresholds.
// Record a violation if that threshold is crossed.
func parseRecord(rec string, wantSQL []MonitoredSQL, curTracker *CursorTrackerProtected) (*parsedSummary, error) {
	recValidClassifier := traceRecordType(rec)
	if Debug { fmt.Printf("[%v] dbg> parseRecord: traceRecordType=%d\n", time.Now().Format("2006-01-02 15:04:05"), recValidClassifier)}
	switch recValidClassifier {
	case traceRecordTypeInvalid:
		if Debug { fmt.Printf("[%v] dbg> parseRecord: not a valid trace record of interest: %v\n", time.Now().Format("2006-01-02 15:04:05"), rec)}
	case traceRecordTypeParsingInCursor:
		newCursor, getCursorID, getSQLID, businessTxName, elaThreshold, err := parsingInCursor(rec, wantSQL, curTracker)
		if err != nil {
			return nil, fmt.Errorf("parseRecord: error from calling parsingInCursor: %v", err)
		}

		if !newCursor {
			if Debug { fmt.Printf("[%v] dbg> curTracker.cursors[getCursorID]=%v\n", time.Now().Format("2006-01-02 15:04:05"), curTracker.get(getCursorID)) }
			openSQLID := curTracker.get(getCursorID).SQLID
			if getSQLID == openSQLID {
				if Debug { fmt.Printf("[%v] dbg> parseRecord: cursor# %v is already open for our SQLID=%v, BusTxName=%v. Skipping.. rec=%s\n", time.Now().Format("2006-01-02 15:04:05"), getCursorID, getSQLID, curTracker.get(getCursorID).BusinessTxName, rec)}
				// TODO(bdali): enhance to count the number of parses.
				return &parsedSummary{}, nil
			}

			// Close the old cursor (for a different SQL ID) and open a new one (for the right SQL ID).
			// TODO(bdali): this may race; protect it similar to compareAndSet.
			curTracker.delete(getCursorID)
			cur, err := openCursor(rec, getCursorID, getSQLID, businessTxName, elaThreshold)
			if err != nil {
				return nil, err
			}
			curTracker.set(getCursorID, cur)
			if Debug { fmt.Printf("[%v] dbg> parseRecord: cursor# %v is already open, but for a different SQL. Close and Reopen for SQLID=%v, BusTxName=%s (rec=%s)\n", time.Now().Format("2006-01-02 15:04:05"), getCursorID, getSQLID, businessTxName, rec)}
		}

	case traceRecordTypeParseExecFetch:
		if Debug{ fmt.Printf("[%v] dbg> parse|exec|fetch record: %s\n", time.Now().Format("2006-01-02 15:04:05"), rec)}
		isKnown, cursorID, cursorType, cpu, ela, err := parseExec(rec, curTracker)
		if err != nil {
			return nil, err
		}
		if !isKnown {
			if Debug { fmt.Printf("[%v] dbg> parseRecord: a valid PARSE|EXEC|FETCH record, but for unknown cursor. Skipping (rec=%v)\n", time.Now().Format("2006-01-02 15:04:05"), rec) }
			return &parsedSummary{}, nil
		}

		curTemp := curTracker.get(cursorID)

		threshold := float64(curTemp.ELAThreshold)
		elaF := float64(ela) / 1000
		cpuF := float64(cpu) / 1000
		if elaF < threshold {
			fmt.Printf("[%v] info> %s [SQL_ID=%s] ran for %.3f [ms] (cpu=%.3f [ms]) during %s phase (threshold of %.3f [ms])\n", time.Now().Format("2006-01-02 15:04:05"), curTemp.BusinessTxName, curTemp.SQLID, elaF, cpuF, cursorType, threshold)
			return &parsedSummary{}, nil
		}

		// TODO(bdali): Printing is not logging. Need to look into a proper logging solution in the future:
		fmt.Printf("[%v] warning> %s [SQL_ID=%s] ran for %.3f [ms] (cpu=%.3f [ms]) during %s phase (threshold of %.3f [ms]): \n", time.Now().Format("2006-01-02 15:04:05"), curTemp.BusinessTxName, curTemp.SQLID, elaF, cpuF, cursorType, threshold)

		worstELA, lastELA, numViolations, err := setViolations(wantSQL, curTemp.BusinessTxName, threshold, curTemp.SQLID, elaF)

		if err != nil {
			fmt.Printf("[%v] error> could not set the violations: %v\n", time.Now().Format("2006-01-02 15:04:05"), err)
			return nil, err
		}
		fmt.Printf("[%v] info> lastela:%.3f worstela:%.3f violations:%d\n", time.Now().Format("2006-01-02 15:04:05"), lastELA, worstELA, numViolations)
		if Debug { fmt.Printf("[%v] dbg> parseRecord: wantSQL=%v\n", time.Now().Format("2006-01-02 15:04:05"), wantSQL)}

		return &parsedSummary{
			isViolation:    true,
			businessTxName: curTemp.BusinessTxName,
			threshold:      threshold,
			sqlID:          curTemp.SQLID,
			worstELA:       worstELA,
			lastELA:        lastELA,
			numViolations:  numViolations,
		}, nil

	default:
		return nil, fmt.Errorf("unknown trace record type: %v", recValidClassifier)
	}
	return &parsedSummary{}, nil
}

// parseSQLID parses a PARSE IN CURSOR record to extract a SQL ID and Cursor#.
func parseSQLID(rec string) (string, string, error) {
	words := strings.Fields(rec)
	if len(words) <= 12 {
		return "", "", fmt.Errorf("parseSQLID: expected number of words is 12. Got %d instead (words=%v)", len(words), words)
	}
	return strings.Replace(words[12][7:], "'", "", 2), words[3][1:], nil
}

type parsedOtherAttr struct {
	hashValue string
	length    int
	depth     int
	uID       int
	lID       int
	oct       int
}

// parseOtherAttr parses a PARSE IN CURSOR record to extract Other cursor attributes.
func parseOtherAttr(rec string) (*parsedOtherAttr, error) {
	words := strings.Fields(rec)
	if Debug { fmt.Printf("[%v] dbg> parseOtherAttr: words=%v\n", time.Now().Format("2006-01-02 15:04:05"), words)}
	if len(words) <= 10 {
		return nil, fmt.Errorf("parseOtherAttr: expected number of words is 10. Got %d instead (words=%v)", len(words), words)
	}
	if Debug { fmt.Printf("[%v] dbg> parseOtherAttr: len=%v, dep=%v, uid=%v, oct=%v, lid=%v, hashValue=%v\n", time.Now().Format("2006-01-02 15:04:05"), words[4][4:], words[5][4:], words[6][4:], words[7][4:], words[8][4:], words[10][3:])}
	// is there a way to create an array and a loop here to avoid repetions?
	length, err := strconv.Atoi(words[4][4:])
	if err != nil {
		return nil, fmt.Errorf("parseOtherAttr: can't parse length: %v", err)
	}

	depth, err := strconv.Atoi(words[5][4:])
	if err != nil {
		return nil, fmt.Errorf("parseOtherAttr: can't parse depth: %v", err)
	}

	uID, err := strconv.Atoi(words[6][4:])
	if err != nil {
		return nil, fmt.Errorf("parseOtherAttr: can't parse uID: %v", err)
	}

	oct, err := strconv.Atoi(words[7][4:])
	if err != nil {
		return nil, fmt.Errorf("parseOtherAttr: can't parse oct: %v", err)
	}

	lID, err := strconv.Atoi(words[8][4:])
	if err != nil {
		return nil, fmt.Errorf("parseOtherAttr: can't parse lID: %v", err)
	}
	return &parsedOtherAttr{
		hashValue: words[10][3:],
		length:    length,
		depth:     depth,
		uID:       uID,
		lID:       lID,
		oct:       oct,
	}, nil
}

// Loop over the SQL to monitor to see if the SQL Id mined is the one we are interested in.
func interestingSQL(getSQLID string, wantSQL []MonitoredSQL) (bool, string, int64) {
	for _, sw := range wantSQL {
		if Debug { fmt.Printf("[%v] dbg> BusinessTxName=%s, ELA Threshold=%v, SQLs=%v\n", time.Now().Format("2006-01-02 15:04:05"), sw.BusinessTxName, sw.ELAThreshold, sw.SQLID)}
		for _, wantSQLID := range sw.SQLID {
			// log.V(2).Infof("  SQL ID=%v", wantSQLID)
			if getSQLID == wantSQLID {
				return true, sw.BusinessTxName, sw.ELAThreshold
			}
		}
	}
	return false, "", -1
}

// setViolations sets the number of violations of the user set threshold for the SQL elapsed time,
// returning the worst recorded elapsed time for a SQL statement in question, last elapsed time
// and the total number of times the threshold has been crossed.
func setViolations(wantSQL []MonitoredSQL, busTxName string, threshold float64, sqlID string, elaF float64) (float64, float64, int64, error) {
	for i, sw := range wantSQL {
		// log.V(2).Infof("setViolations: sw.BusinessTxName=%s, wantSQL[i]=%s", sw.BusinessTxName, wantSQL[i].BusinessTxName)
		if sw.BusinessTxName == busTxName {
			wantSQL[i].LastELA = elaF
			if sw.WorstELA < elaF {
				wantSQL[i].WorstELA = elaF
			}
			wantSQL[i].NumViolations++
			return wantSQL[i].WorstELA, wantSQL[i].LastELA, wantSQL[i].NumViolations, nil
		}
	}
	if Debug { fmt.Printf("[%v] dbg> setViolations: wantSQL=%v", time.Now().Format("2006-01-02 15:04:05"), wantSQL)}
	return 0, 0, 0, fmt.Errorf("setViolations: unexpected error, could not find last ELA a BusTx [%s] and SQL [%s]", busTxName, sqlID)
}

// parseExec deals with parsing PARSE|EXEC|FETCH records returning a boolean
// flag of whether or not a cursor has already been parsed for this record,
// and also a cursor#, cursor type, CPU time and Elapsed time.
func parseExec(rec string, curTracker *CursorTrackerProtected) (bool, int64, string, int64, int64, error) {
	words := strings.FieldsFunc(rec, func(r rune) bool {
		switch r {
		case '#', ':', ',', '=', ' ':
			return true
		}
		return false
	})
	if len(words) <= 5 {
		return false, 0, "", 0, 0, fmt.Errorf("parseExec: expected number of words is least 5. Got %d instead: rec=%q, words=%v", len(words), rec, words)
	}
	if Debug { log.Printf("[%v] dbg> words=%q, cursor#=%v, c=%v, e=%v\n", time.Now().Format("2006-01-02 15:04:05"), words, words[1], words[3], words[5])}
	cursorType := words[0]
	cursorString := words[1]
	cursorInt, err := strconv.Atoi(cursorString)
	if err != nil {
		return false, 0, "", 0, 0, fmt.Errorf("parseExec: cursor# doesn't appear to be a number: cursor#=%v, err=%v", cursorString, err)
	}

	// isCursorOpen := cur.IsCursorOpen(int64(cursorInt))
	if !curTracker.hasValue(int64(cursorInt)) {
		return false, 0, "", 0, 0, nil
	}

	cString := words[3]
	cInt, err := strconv.Atoi(cString)
	if err != nil {
		log.Fatal(err)
		return false, 0, "", 0, 0, fmt.Errorf("parseExec: strconv.Atoi(eString), cannot get CPU: %v", err)
	}
	eString := words[5]
	eInt, err := strconv.Atoi(eString)
	if err != nil {
		log.Fatal(err)
		return false, 0, "", 0, 0, fmt.Errorf("parseExec: strconv.Atoi(eString), cannot get ELA: %v", err)
	}
	return true, int64(cursorInt), cursorType, int64(cInt), int64(eInt), nil
}

// normalizeName normalizes a business tx name by converting it to lower case and replacing spaces and # with underscrores.
func normalizeName(name string) string {
	return strings.ToLower(strings.Replace(strings.Replace(name, " ", "_", -1), "#", "_", -1))
}
