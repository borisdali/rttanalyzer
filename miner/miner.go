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

// Package miner gets the work from the Watchdog daemon. One Miner for every trace file.
package miner

import (
	"os"
	"fmt"
	"time"

	"golang.org/x/net/context"
	"github.com/borisdali/rttanalyzer/rttanalyzer"
	bqgen "google.golang.org/api/bigquery/v2"
	"cloud.google.com/go/pubsub"
)

var Debug bool

// Dumper is used to output the results.
type Dumper interface {
	// Dump receives a record mined from a trace file and uses it for further processing.
	Dump(context.Context, *pubsub.Client, *bqgen.Service, string) error
}

// Mine opens a requested trace file and starts reading/analyzing it.
// Values should be sent to the channel when the underlying file is written to.
// The miner exits when the channel is closed.
func Mine(ctx context.Context, client *pubsub.Client, service *bqgen.Service, notify <-chan struct{}, dumper Dumper, tf *rttanalyzer.TraceFile) error {
	if Debug { fmt.Printf("[%v] dbg> Miner started with pid %d for trace %v\n", time.Now().Format("2006-01-02 15:04:05"), os.Getpid(), tf.Name)}
	if Debug { fmt.Printf("[%v] dbg> dumper=%v\n", time.Now().Format("2006-01-02 15:04:05"), dumper)}

	var reloads int

	for {
		strs, err := tf.ReadRecords()
		if err != nil {
			return err
		}
		recordsRead := len(strs)

		for _, v := range strs {
			if Debug { fmt.Printf("[%v] dbg> (fileName=%v, recordsRead=%d, len=%d) %v\n", time.Now().Format("2006-01-02 15:04:05"), tf.Name, recordsRead, len(v), v)}
			if err := dumper.Dump(ctx, client, service, v); err != nil {
				return err
			}
		}

		if recordsRead == 0 {
			tf.UpdateRoster()

			if Debug { fmt.Printf("[%v] dbg> blocking on channel %v\n", time.Now().Format("2006-01-02 15:04:05"), notify)}
			_, ok := <-notify
			if !ok {
				if Debug { fmt.Printf("[%v] dbg> can't unblock the notify channel", time.Now().Format("2006-01-02 15:04:05"))}
				return nil
			}
			if Debug { fmt.Printf("[%v] dbg> unblocking on channel %v\n", time.Now().Format("2006-01-02 15:04:05"), notify)}
		}
		reloads++
		if Debug { fmt.Printf("[%v] dbg> reloaded %d times\n", time.Now().Format("2006-01-02 15:04:05"), reloads)}
	}
}
