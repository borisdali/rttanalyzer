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

package sqlinput

import (
	"io/ioutil"
	"os"
	"reflect"
	"testing"

	"github.com/kylelemons/godebug/pretty"
)

func TestParseAWR(t *testing.T) {
	const sampleAWR = `WORKLOAD REPOSITORY report for

DB Name         DB Id    Instance     Inst Num Startup Time    Release     RAC
------------ ----------- ------------ -------- --------------- ----------- ---
...
Load Profile                    Per Second   Per Transaction  Per Exec  Per Call
~~~~~~~~~~~~~~~            ---------------   --------------- --------- ---------
             DB Time(s):               2.5               0.0      0.00      0.00
              DB CPU(s):               1.3               0.0      0.00      0.00
      Redo size (bytes):         409,740.9           3,052.4
...
direct path write temp                2,638  3.2       1     .1 User I/O

Wait Classes by Total Wait Time
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
                                                        Avg             Avg
                                        Total Wait     Wait   % DB   Active
Wait Class                  Waits       Time (sec)     (ms)   time Sessions
---------------- ---------------- ---------------- -------- ------ --------
DB CPU                                       1,187            53.3      1.3
...
                          ------------------------------------------------------

SQL ordered by Elapsed Time  DB/Inst: TestDbName/TestDbName  Snaps: 252424-252425
-> Resources reported for PL/SQL code includes the resources used by all SQL
   statements called by the code.
-> % Total DB Time is the Elapsed Time of the SQL statement divided
   into the Total Database Time multiplied by 100
-> %Total - Elapsed Time  as a percentage of Total DB time
-> %CPU   - CPU Time      as a percentage of Elapsed Time
-> %IO    - User I/O Time as a percentage of Elapsed Time
-> Captured SQL account for   18.9% of Total DB Time (s):           2,227
-> Captured PL/SQL account for    0.5% of Total DB Time (s):           2,227

        Elapsed                  Elapsed Time
        Time (s)    Executions  per Exec (s)  %Total   %CPU    %IO    SQL Id
---------------- -------------- ------------- ------ ------ ------ -------------
            70.5         13,464          0.01    3.2  119.1     .0 5vx5qmyh3hj7v
Module: <moduleName#1>
SELECT something something FROM someTable#1

            37.8          2,151          0.02    1.7    5.8   98.1 2x71fvt35jqht
Module: <moduleName#2>
SELECT something something FROM someTable#2

            31.7             55          0.58    1.4   99.9     .0 3na5pctfjsqff
Module: <moduleName#3>
SELECT something something FROM someTable#3
...

SQL ordered by CPU Time      DB/Inst: TestDbName/TestDbName Snaps: 252424-252425
-> Resources reported for PL/SQL code includes the resources used by all SQL
   statements called by the code.
-> %Total - CPU Time      as a percentage of Total DB CPU
-> %CPU   - CPU Time      as a percentage of Elapsed Time
-> %IO    - User I/O Time as a percentage of Elapsed Time
-> Captured SQL account for   27.4% of Total CPU Time (s):           1,187
-> Captured PL/SQL account for    1.0% of Total CPU Time (s):           1,187

    CPU                   CPU per           Elapsed
  Time (s)  Executions    Exec (s) %Total   Time (s)   %CPU    %IO    SQL Id
---------- ------------ ---------- ------ ---------- ------ ------ -------------
Module: <moduleName#1>
SELECT something something FROM someTable#1
...
`

	wantedSQL := []ExpensiveSQL{
		{
			sqlid:            "5vx5qmyh3hj7v",
			elaPerExec:       0.01,
			elaPerExecString: "0.01",
			ela:              "70.5",
			execs:            "13,464",
			percentTotal:     "3.2",
			percentCPU:       "119.1",
			percentIO:        ".0",
		},
		{
			sqlid:            "2x71fvt35jqht",
			elaPerExec:       0.02,
			elaPerExecString: "0.02",
			ela:              "37.8",
			execs:            "2,151",
			percentTotal:     "1.7",
			percentCPU:       "5.8",
			percentIO:        "98.1",
		},
		{
			sqlid:            "3na5pctfjsqff",
			elaPerExec:       0.58,
			elaPerExecString: "0.58",
			ela:              "31.7",
			execs:            "55",
			percentTotal:     "1.4",
			percentCPU:       "99.9",
			percentIO:        ".0",
		},
	}

	fh, err := ioutil.TempFile("", "tempAWRFile")
	if err != nil {
		t.Fatalf("cannot open a temp file to store AWR mock report: %v", err)
	}
	defer func() {
		fh.Close()
		os.Remove(fh.Name())
	}()

	if _, err := fh.WriteString(sampleAWR); err != nil {
		t.Fatal(err)
	}
	if err := fh.Sync(); err != nil {
		t.Fatalf("fh.Sync() failed: %v", err)
	}

	expSQL, err := ParseAWR("TestDbName", fh.Name())
	if err != nil {
		t.Fatalf("error parsing %q mock AWR file: %v. Aborting.", err, fh.Name())
	}

	if !reflect.DeepEqual(expSQL, wantedSQL) {
		t.Errorf("loadSQL(): -> diff -got +want\n%s", pretty.Compare(expSQL, wantedSQL))
	}
}
