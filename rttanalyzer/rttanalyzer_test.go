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

// Rttanalyzer_test loads the Roster and runs unit tests on ReadRecords() function
package rttanalyzer

import (
	"io/ioutil"
	"os"
	"testing"
)

func TestReadRecords(t *testing.T) {
	var testCases = []struct {
		records          int
		positionStart    int64
		wantPositionLast int
		wantStr          string
	}{
		{records: 1,
			positionStart:    0,
			wantPositionLast: 11,
			wantStr:          "First line\n"},
		{records: 1,
			positionStart:    11,
			wantPositionLast: 12,
			wantStr:          "Second line\n"},
		{records: 1,
			positionStart:    23,
			wantPositionLast: 11,
			wantStr:          "Third line\n"},
		{records: 1,
			positionStart:    46,
			wantPositionLast: 12,
			wantStr:          "Fourth line\n"},
		{records: 1,
			positionStart:    71,
			wantPositionLast: 11,
			wantStr:          "Fifth line\n"},
	}

	file, err := ioutil.TempFile("", "TestReadRecords")
	if err != nil {
		t.Fatalf("ioutil.TempFile() failed: couldn't open tmp file: %v", err)
	}
	defer file.Close()
	defer os.Remove(file.Name())
	if _, err := file.Write([]byte(sampleDataReadRecords)); err != nil {
		t.Fatalf("file.Write() failed: couldn't write to tmp file: %v", err)
	}

        r, err := LoadRoster(RosterFile)
        if err != nil {
                t.Fatalf("failed to load Roster %q", RosterFile)
        }

	fileName := file.Name()
	f, err := OpenTraceFile(fileName, r)
	if err != nil {
		t.Fatalf("OpenTraceFile(%q) failed: couldn't open tmp file: %v", fileName, err)
	}
	defer f.Close()

	for _, ent := range testCases {
		gotStr, err := f.ReadRecords()
		gotRecords := len(gotStr)

		var str string
		for _, v := range gotStr {
			str += v
		}
		if err != nil {
			t.Errorf("f.ReadRecords(%d, %d) = %v", ent.records, ent.positionStart, err)
			continue
		}
		if gotRecords != ent.records {
			t.Errorf("f.ReadRecords(%d, %d) = got %d records, want %d records", ent.records, ent.positionStart, gotRecords, ent.records)
		}
		if str != ent.wantStr {
			t.Errorf("f.ReadRecords(%d, %d) = got string %q, want string %q", ent.records, ent.positionStart, str, ent.wantStr)
		}
		if len(str) != ent.wantPositionLast {
			t.Errorf("f.ReadRecords(%d, %d) = got positionLast %d, want positionLast %d", ent.records, ent.positionStart, len(str), ent.wantPositionLast)
		}
	}
}

const sampleDataReadRecords = `First line
Second line
Third line
Fourth line
Fifth line

Seventh line
Eighth line
`
