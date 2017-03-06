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

package main

import (
	"io/ioutil"
	"os"
	"reflect"
	"testing"

	"github.com/kylelemons/godebug/pretty"
)

func TestLoadConfig(t *testing.T) {
	const sampleConfig = `# A comment line to verify that it indeed gets ignored
dbname = BOOdatabase
dirname = /some/Fancy/Directory/Name
sqlinput=sqlinput.txt 
outputtype=pubsub
# outputtype=varz
appcredentials = "/work/rttanalyzer/Very long JSON file name.json"
projectname = MyProjectName
# Intentionally Wrong String
`

	wanted := &config{
		dbName:      "BOOdatabase",
		dirName:     "/some/Fancy/Directory/Name",
		sqlInput:    "/work/rttanalyzer/sqlinput.txt",
		outputType:  "pubsub",
		appCred:     "/work/rttanalyzer/Very long JSON file name.json",
		projectName: "MyProjectName",
	}

	fh, err := ioutil.TempFile("", "configFileCopy")
	if err != nil {
		t.Fatalf("cannot open a temp file to copy the original config file to: %v", err)
	}
	defer func() {
		fh.Close()
		os.Remove(fh.Name())
	}()

	if _, err := fh.WriteString(sampleConfig); err != nil {
		t.Fatal(err)
	}
	if err := fh.Sync(); err != nil {
		t.Fatalf("fh.Sync() failed: %v", err)
	}

	config, err := loadConfig(fh.Name())

	if err != nil {
		t.Fatalf("error loading %q config file: %v. Aborting.", err, fh.Name())
	}

	if !reflect.DeepEqual(config, wanted) {
		t.Errorf("loadSQL(): -> diff -got +want\n%s", pretty.Compare(config, wanted))
	}
}
