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

//Sink_test runs unit tests on the LoadSQL function.
package sink

import (
	"io/ioutil"
	"os"
	"reflect"
	"testing"
	"log"

	"github.com/kylelemons/godebug/pretty"
)

func auxLoadSQL() *os.File {
	const sampleDataSQL = `"EBS/Month End Job", 1, "123", "456"
EBS/Post GL, 10, qweabcl, 456, abc123defg
EBS/Sample Long Running Job, 15, 458, 56aa, abcdefg, 123456789
`
	fh, err := ioutil.TempFile("", "TestLoadSQL")
	if err != nil {
		log.Fatalf("ioutil.TempFile() failed: couldn't open tmp file: %v", err)
	}

	if _, err := fh.WriteString(sampleDataSQL); err != nil {
		log.Fatalf("file.Write() failed: couldn't write to tmp file: %v", err)
	}
	return fh
}

func TestLoadSQL(t *testing.T) {
	fhSQL := auxLoadSQL()

	defer func() {
		fhSQL.Close()
		os.Remove(fhSQL.Name())
	}()

	sql, err := loadSQL(fhSQL.Name())
	if err != nil {
		t.Fatalf("loadSQL() failed: %v", err)
	}

	var sqlWant = []MonitoredSQL{
		MonitoredSQL{
			BusinessTxName: "EBS/Month End Job",
			ELAThreshold:   1,
			SQLID:          []string{"123", "456"},
		},
		MonitoredSQL{
			BusinessTxName: "EBS/Post GL",
			ELAThreshold:   10,
			SQLID:          []string{"qweabcl", "456", "abc123defg"},
		},
		MonitoredSQL{
			BusinessTxName: "EBS/Sample Long Running Job",
			ELAThreshold:   15,
			SQLID:          []string{"458", "56aa", "abcdefg", "123456789"},
		},
	}
	if !reflect.DeepEqual(sql, sqlWant) {
		t.Errorf("loadSQL(): got %#v, want %#v", sql, sqlWant)
		t.Errorf("loadSQL(): -> diff -got +want\n%s", pretty.Compare(sql, sqlWant))

	}
}
