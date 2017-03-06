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

// Package rttanalyzer provides basic primitives for analyzing
// Oracle SQL/10046 trace files in real time.
package rttanalyzer

import (
	"bufio"
	"encoding/json"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"fmt"
	"time"
)

// RosterFile is the default file to load the rttanalyzer roster from.
var (
	RttaHome string
	RosterFile = filepath.Join(Dir(), "rtta.roster")
	recordsCount = 1
	Debug bool
)

// Dir returns the directory where rttanalyzer configuration data is stored.
func Dir() string {
	//return "/opt/dbe/bin"
	return RttaHome
}

// Roster is a mapping of trace file names to traces.
type Roster struct {
	sync.RWMutex
	R map[string]jsonTraceFile
}

// LoadRoster loads the roster from disk.
// If a Roster object doesn't exist, this function creates one.
func LoadRoster(fileName string) (*Roster, error) {
	out, err := ioutil.ReadFile(fileName)
	if os.IsNotExist(err) {
		r := &Roster{R: make(map[string]jsonTraceFile)}
		if Debug { fmt.Printf("[%v] dbg> os.IsNotExist(err)=%v, created a new roster, err=%v\n", time.Now().Format("2006-01-02 15:04:05"), os.IsNotExist(err), err)}
		return r, nil
	}
	if err != nil {
		if Debug { fmt.Printf("[%v] dbg> ioutil.ReadFile: err=%v\n", time.Now().Format("2006-01-02 15:04:05"), err)}
		return &Roster{}, err
	}
	var r Roster
	if err := json.Unmarshal(out, &r); err != nil {
		if Debug { fmt.Printf("[%v] dbg> unmarshal: err=%v, out=%v\n", time.Now().Format("2006-01-02 15:04:05"), err, out)}
		return &Roster{}, err
	}
	return &r, nil
}

// TraceFile opens the trace and if it's a known trace (the one in the Roster), it advances to the last offset.
func (r *Roster) TraceFile(fileName string) (*TraceFile, error) {
	if Debug { fmt.Printf("[%v] dbg> roster.TraceFile: r[fileName]=%v\n", time.Now().Format("2006-01-02 15:04:05"), r.R[fileName])}
	rf, ok := r.R[fileName]
	if Debug { fmt.Printf("[%v] dbg> roster.TraceFile: known file? ok=%v [rf=%v]\n", time.Now().Format("2006-01-02 15:04:05"), ok, rf)}
	if !ok {
		return OpenTraceFile(fileName, r)
	}
	fh, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	tf := &TraceFile{
		Name:          rf.Name,
		DirectoryName: rf.DirectoryName,
		version:       rf.Version,
		offset:        rf.Offset,
		fileHandle:    fh,
		roster:        r,
	}
	if Debug { fmt.Printf("[%v] dbg> roster.TraceFile: New trace file, tf = %v\n", time.Now().Format("2006-01-02 15:04:05"), tf)}
	return tf, nil
}

// Save saves the roster to disk.  This creates the directory by default,
// since for packaging reasons it's impractical to always ensure it's there.
func (r *Roster) Save(fileName string, tf TraceFile) error {
	r.Lock()
	defer r.Unlock()
	traceKey := filepath.Join(tf.DirectoryName, tf.Name)
	r.R[traceKey] = jsonTraceFile{Name: tf.Name, DirectoryName: tf.DirectoryName, Version: tf.version, Offset: tf.offset}
	out, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		if Debug { fmt.Printf("[%v] dbg> marshal: fileName=%q, tf=%v, err=%v, out=%v\n", time.Now().Format("2006-01-02 15:04:05"), fileName, tf, err, out)}
		return err
	}
	if err := os.MkdirAll(filepath.Dir(fileName), 0755); err != nil {
		return err
	}
	if Debug { fmt.Printf("[%v] dbg> Saved trace %q of version %d with the offset of %d\n", time.Now().Format("2006-01-02 15:04:05"), r.R[traceKey].Name, r.R[traceKey].Version, r.R[traceKey].Offset)}
	return ioutil.WriteFile(fileName, out, 0644)
}

// TraceFile corresponds to each monitored trace file.
type TraceFile struct {
	Name          string
	DirectoryName string   // File system path to the trace file
	version       int      // File may get overwritten by a different process
	offset        int64    // Last read position (lseek) in a file
	fileHandle    *os.File // File Handle to avoid reopening the file
	roster        *Roster  // Pointer to the Roster
}

// jsonTraceFile is used for persisting TraceFile data.
type jsonTraceFile struct {
	Name          string
	DirectoryName string
	Version       int
	Offset        int64
}

// OpenTraceFile gets a file handle to a trace file.
func OpenTraceFile(fileName string, r *Roster) (*TraceFile, error) {
	fh, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	if Debug { fmt.Printf("[%] dbg> OpenTraceFile: New traceFile = %q [fileHandle=%v], roster=%v\n", time.Now().Format("2006-01-02 15:04:05"), fileName, fh, r)}
	return &TraceFile{
		Name:          filepath.Base(fileName),
		DirectoryName: filepath.Dir(fileName),
		version:       1,
		offset:        0,
		fileHandle:    fh,
		roster:        r,
	}, nil
}

// UpdateRoster persists a trace file offset to a roster.
func (tf *TraceFile) UpdateRoster() error {
	return tf.roster.Save(RosterFile, *tf)
}

// ReadRecords reads up to <records> of data from a trace file from the <starting> position.
// Either find a SQL_ID you need, hit EOF or reach the <b> bytes limit
func (tf *TraceFile) ReadRecords() ([]string, error) {
	if err := tf.seek(); err != nil {
		return nil, err
	}

	scanner := bufio.NewScanner(tf.fileHandle)
	//TODO(bdali): there's a potential issue here if the trace file has a half-completed file.
	// If this comes up, consider writting a custom bufio.ScanFunc.

	var records []string
	var n int
	for scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return nil, err
		}
		if n < recordsCount {
			n++
			tf.offset += int64(len(scanner.Bytes()) + 1)
			str := scanner.Text()
			if Debug { fmt.Printf("[%v] dbg> Scanned %d bytes, tf.offset=%d: %v\n", time.Now().Format("2006-01-02 15:04:05"), len(scanner.Bytes()), tf.offset, str)}
			records = append(records, str+"\n")
		} else {
			return records, nil
		}
	}
	return records, nil
}

// ReadRecords2 reads trace file records assuming they end with a LF.
// Instead of relying on bufio.NewReader().NewScanner(), this function reads bufio.NewReader.ReadString(
// It also returns a string and not []string and a lastPositionRead / offset
func (tf *TraceFile) readRecords2(recordsCount int, offset int64) (string, int, int64, error) {
	_, err := tf.fileHandle.Seek(offset, io.SeekStart)
	if err != nil {
		return "", 0, 0, err
	}

	reader := bufio.NewReader(tf.fileHandle)

	var records string
	var n, rnum int
	for rnum = 0; rnum < recordsCount; rnum++ {
		n++
		rec, err := reader.ReadString('\n')
		if err != nil {
			// TODO(bdali): This is the right way, but need to fix err handling:
			//return records, rnum, tf.offset, err
			return records, rnum, tf.offset, nil
		}
		tf.offset += int64(len(rec)) // +1)
		records += rec
		//records += "\n"
		//n = rnum
	}
	return records, n, tf.offset, nil
}

func (tf *TraceFile) seek() error {
	ret, err := tf.fileHandle.Seek(tf.offset, io.SeekStart)
	if err != nil {
		return err
	}
	tf.offset = ret
	if Debug { fmt.Printf("[%v] dbg> seek: set trace %v to offset %d\n", time.Now().Format("2006-01-02 15:04:05"), tf.Name, tf.offset)}
	return nil
}

// Close simply closes a file handle.
func (tf *TraceFile) Close() error {
	// TODO(bdali): add further cleanups here
	return tf.fileHandle.Close()
}
