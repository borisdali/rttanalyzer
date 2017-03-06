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

// Package watchdog checks for changes in a specific directory.
// If a change happens to be to a trace file a Miner is called to analyze it.
//
// See https://godoc.org/gopkg.in/fsnotify.v1#example-NewWatcher
package watchdog

import (
	"fmt"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"
	"log"

	"github.com/borisdali/rttanalyzer/cursor"
	"github.com/borisdali/rttanalyzer/miner"
	"github.com/borisdali/rttanalyzer/rttanalyzer"
	"github.com/borisdali/rttanalyzer/sink"
	"github.com/howeyc/fsnotify"

	"golang.org/x/net/context"
	bqgen "google.golang.org/api/bigquery/v2"
	"cloud.google.com/go/pubsub"
)

const varzDir = "/opt/mg-agent-xp/data.d"

var Debug bool

// output returns an instantiated object of the output media: a Varz, Streamz or Pub/Sub.
// outputType can be one of varz, pubsub (with streamz not implemented yet).
func output(dbName string, outputType string, sqlFile string, client *pubsub.Client) (miner.Dumper, error) {
	cur := &sink.CursorTrackerProtected{Cursors: make(map[int64]*cursor.Cursor)}

	switch outputType {
	case "varz":
                fmt.Printf("[%v] info> the output media requested for RTTAnalyzer is an ASCII file (referred to as VarZ).\n", time.Now().Format("2006-01-02 15:04:05"))
		vzDump := &sink.Varz{
			Generic: sink.Generic{
				DBName:        dbName,
				FileSQL:       sqlFile,
				Client:        client,
				CursorTracker: cur,
			},
			Dir:           varzDir,
			FilePrefix:    "rttanalyzer",
			FileExtension: ".varz",
		}
		if err := vzDump.LoadSQL(); err != nil {
			log.Fatalf("varz LoadSQL: error reading SQL statements input file: %v. Aborting.", err)
			return nil, fmt.Errorf("varz LoadSQL: error reading SQL statements input file: %v. Aborting", err)
		}
		return vzDump, nil
	case "pubsub":
                fmt.Printf("[%v] info> the output media requested for RTTAnalyzer is Pub/Sub.\n", time.Now().Format("2006-01-02 15:04:05"))
		psDump := &sink.PubSub{
			Generic: sink.Generic{
				DBName:        dbName,
				FileSQL:       sqlFile,
				Client:        client,
				CursorTracker: cur,
			},
		}
		if err := psDump.LoadSQL(); err != nil {
			log.Fatalf("pubsub LoadSQL: error reading SQL statements input file: %v. Aborting.", err)
			return nil, fmt.Errorf("pubsub LoadSQL: error reading SQL statements input file: %v. Aborting", err)
		}
		return psDump, nil
	}
	errStr := fmt.Sprintf("outputtype can be one of varz, pubsub (with streamz not implemented yet). Got %v instead.", outputType)
	return nil, fmt.Errorf("output error: %s", errStr)
}

// stat is a syncronization mechanism to access the traces map.
type stat struct {
	sync.RWMutex
	traces map[string]chan struct{}
}

// addOrGetTrace reports false if the channel already exists (and thus the miner is already running),
// and true if the channel was just created and the miner needs to be called.
func (s *stat) addOrGetTrace(key string) (bool, chan struct{}) {
	s.RLock()
	i, ok := s.traces[key]
	if Debug { fmt.Printf("[%v] dbg> already in traces map? %v. if so, what is the value? %v\n", time.Now().Format("2006-01-02 15:04:05"), ok, i)}
	s.RUnlock()
	if ok {
		return false, i
	}
	s.Lock()
	defer s.Unlock()

	// Second check (with the write lock now) if the trace is known to us.
	i, ok = s.traces[key]
	if Debug { fmt.Printf("[%v] dbg> already in traces map (second check)? %v. if so, what is the value? %v\n", time.Now().Format("2006-01-02 15:04:05"), ok, i)}
	if ok {
		return false, i
	}
	s.traces[key] = make(chan struct{})
	return true, s.traces[key]
}

func (s *stat) deleteTrace(key string) {
	s.Lock()
	defer s.Unlock()
	delete(s.traces, key)
}

func checkFile(ctx context.Context, client *pubsub.Client, service *bqgen.Service, fileName string, mode string, s *stat, dumper miner.Dumper, dbName string, r *rttanalyzer.Roster) {
	// Skip any files that don't have a trc extension:
	if filepath.Ext(fileName) != ".trc" {
		if Debug { fmt.Printf("[%v] dbg> file %s has no .trc file extension, so not a trace file -> skipping..\n", time.Now().Format("2006-01-02 15:04:05"), fileName)}
		return
	}
	if !strings.HasPrefix(path.Base(fileName), dbName+"_ora_") {
		if Debug { fmt.Printf("[%v] dbg> file %s has no %s_ora_ file prefix, so not a trace file -> skipping..\n", time.Now().Format("2006-01-02 15:04:05"), path.Base(fileName), dbName)}
		return
	}
	launchMiner, ch := s.addOrGetTrace(fileName)
	if Debug { fmt.Printf("[%v] dbg> a call to s.addOrGetTrace(fileName) returned launchMiner=%v, ch=%v\n", time.Now().Format("2006-01-02 15:04:05"), launchMiner, ch)}
	if !launchMiner {
		if Debug { fmt.Printf("[%v] dbg> file %s already has a Miner working on it -> skipping..\n", time.Now().Format("2006-01-02 15:04:05"), fileName)}
		// TODO(bdali): there's a potential race condition here that may need to be addressed.
		ch <- struct{}{} // wake up the Miner
		return
	}

	f, err := r.TraceFile(fileName)
	if err != nil {
		log.Fatal(err)
	}

	// Miner starts in the background, letting the watchdog continue
	go func() {
		if err := miner.Mine(ctx, client, service, ch, dumper, f); err != nil {
			// On a hiccup just remove the trace from a map let watchdog pick it up on the next pass.
			fmt.Printf("[%v] a hiccup in the Miner: traceFile=%q, error=%v\n", time.Now().Format("2006-01-02 15:04:05"), fileName, err)
			s.deleteTrace(fileName)
		}
	}()
	if Debug { fmt.Printf("[%v] dbg> active traces/miners:active channels=%v (ch=%v)\n", time.Now().Format("2006-01-02 15:04:05"), s.traces, ch)}
}

// Run calls output to initialize a dumper and sets up a watcher on a directory of choice.
func Run(ctx context.Context, client *pubsub.Client, service *bqgen.Service, dbName string, dirName string, sqlInput string, mode string, outputType string, projectName string) error {

	if Debug {
		sink.Debug = Debug
		miner.Debug = Debug
	}

	dumper, err := output(dbName, outputType, sqlInput, client)
	if err != nil {
		return fmt.Errorf("watchdog: output error: %v", err)
	}

	// Keep trace of known/already opened trace files:
	t := &stat{traces: make(map[string]chan struct{})}

	// Catch SIGTERM, close open traces/miners/channels and signal to return back to RTTA.
	c := make(chan os.Signal, 1)
	cntrlc := make(chan bool, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		fmt.Printf("\n[%v] Cntrl-C pressed. Starting the cleanup..", time.Now().Format("2006-01-02 15:04:05"))
		for _, ch := range t.traces {
			if Debug { fmt.Printf("[%v] dbg> closing channel %v\n", time.Now().Format("2006-01-02 15:04:05"), ch)}
			close(ch)
		}
		cntrlc <- true
	}()

	r, err := rttanalyzer.LoadRoster(rttanalyzer.RosterFile)
	if err != nil {
		fmt.Printf("[%v] rttanalyzer.LoadRoster crashed with err=%v. Terminating..\n", time.Now().Format("2006-01-02 15:04:05"), err)
		os.Exit(1)
	}
	if Debug { fmt.Printf("[%v] dbg> rttanalyzer.LoadRoster = %v\n", time.Now().Format("2006-01-02 15:04:05"), r)}

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("watchdog: fsnotify.NewWatcher error: %v", err)
	}

	// err = watcher.Watch(dirName)
	err = watcher.Watch(dirName)
	if err != nil {
		return fmt.Errorf("watchdog: watcher.Watch error: %v", err)
	}

	for {
		select {
		case <-cntrlc:
			fmt.Printf("[%v] Cntrl-C is pressed and so returning from the Watchdog back to RTTA.", time.Now().Format("2006-01-02 15:04:05"))
			return nil
		case event := <-watcher.Event:
			if Debug { fmt.Printf("[%v] dbg> event:%v\n", time.Now().Format("2006-01-02 15:04:05"), event)}
			switch {
			case mode == "write" && (event.IsModify() || event.IsCreate()):
				checkFile(ctx, client, service, event.Name, mode, t, dumper, dbName, r)
			case mode == "create" && event.IsCreate():
				checkFile(ctx, client, service, event.Name, mode, t, dumper, dbName, r)
			case mode != "write" && mode != "create":
				return fmt.Errorf("--mode flag is not set to write or create: %v", mode)
			}
		case err := <-watcher.Error:
			fmt.Printf("[%v] event error:%v\n", time.Now().Format("2006-01-02 15:04:05"), err)
		}
	}
}
