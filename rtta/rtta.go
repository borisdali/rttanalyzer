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

// RTTA = Real Time Trace Analyzer.
// This is a wrapper that reads the config file and calls either a watchdog or a dequeue.

package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"flag"
	"log"
	"time"

	rttpubsub "github.com/borisdali/rttanalyzer/pubsub"
	"github.com/borisdali/rttanalyzer/rttanalyzer"
	"github.com/borisdali/rttanalyzer/sqlinput"
	"github.com/borisdali/rttanalyzer/watchdog"

	"golang.org/x/net/context"
	daemon "github.com/borisdali/rttanalyzer/service"
	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/pubsub"
	bqgen "google.golang.org/api/bigquery/v2"
	oauth2google "golang.org/x/oauth2/google"
)

const usage = `
	Real Time Trace Analyzer (RTTAnalyzer)

Tool for monitoring and alerting SLO violations of business transactions of interest.
See github.com/borisdali/rttanalyzer for details.

Available options:
  - debug: As the name implies, adding debugging clutter.
  - service: Run the trace file tracker as a service/daemon.
  - dequeue: (For running in PubSub mode on GCP) dequeue events, persist in BQ.
  - setup: Presently supports only one option: -awr, as in:
 	rtta -setup -awr <AWR report file path name>

`

var help = flag.Bool("help", false, "Basic usage banner")
var debug = flag.Bool("debug", false, "Add some debugging clutter")
var dequeue = flag.Bool("dequeue", false, "Activates dequeue mode in favor of the default mode of a watchdog.")
var setup = flag.Bool("setup", false, "Activates setup mode to generate rtta.sqlinput automagically.")
var awrFile = flag.String("awr", "", "In the -setup mode, -awr flag is mandatory and it points to the AWR input file.")
var serviceAction = flag.String("service", "", "Service action: run, start, stop, install, remove.")

var serviceG *bqgen.Service
var projectNameG string
var configG *config

type config struct {
	dbName      string
	dirName     string
	mode        string
	sqlInput    string
	outputType  string
	appCred     string
	projectName string
}

// loadConfig reads, parses and loads the input parameters.
func loadConfig(fileName string) (*config, error) {
	fh, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	defer fh.Close()

	r := csv.NewReader(fh)
	//r.FieldsPerRecord = -1
	r.TrimLeadingSpace = true
	r.Comma = '='
	r.Comment = '#'

	var dbName, dirName, mode, sqlInput, outputType, appCred, projectName string
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if *debug { fmt.Printf("[%v] dbg> record[0]=%s, record[1]=%s\n", time.Now().Format("2006-01-02 15:04:05"), strings.TrimSpace(record[0]), strings.TrimSpace(record[1])) }

		switch strings.TrimSpace(record[0]) {
		case "dbname":
			dbName = strings.TrimSpace(record[1])
		case "dirname":
			dirName = strings.TrimSpace(record[1])
		case "mode":
			mode = strings.TrimSpace(record[1])
		case "sqlinput":
			sqlInput = strings.TrimSpace(record[1])
		case "outputtype":
			outputType = strings.TrimSpace(record[1])
		case "appcredentials":
			appCred = strings.TrimSpace(record[1])
		case "projectname":
			projectName = strings.TrimSpace(record[1])
		default:
			return nil, fmt.Errorf("unknown config parameter: %v", strings.TrimSpace(record[0]))
		}
	}
	return &config{
		dbName:      dbName,
		dirName:     dirName,
		mode:        mode,
		sqlInput:    sqlInput,
		outputType:  outputType,
		appCred:     appCred,
		projectName: projectName,
	}, nil
}

func getService(ctx context.Context) (*bqgen.Service, error) {
	httpClient, err := oauth2google.DefaultClient(ctx, bigquery.Scope)
	if err != nil {
		return nil, fmt.Errorf("getService: oauth2google.DefaultClient error: %v", err)
	}

	service, err := bqgen.New(httpClient)
	if err != nil {
		return nil, fmt.Errorf("getService: bigquery.New(httpClient) error: %v", err)
	}
	return service, nil
}

func dequeueWrap(ctx context.Context, client *pubsub.Client) {
	if err := rttpubsub.Dequeue(ctx, client, serviceG, projectNameG); err != nil {
		fmt.Printf("a call to rttpubsub.Dequeue fails. Aborting. err: %v\n", err)
		os.Exit(1)
	}
}

func watchdogWrap(ctx context.Context, client *pubsub.Client) {
	if err := watchdog.Run(ctx, client, serviceG, configG.dbName, configG.dirName, configG.sqlInput, configG.mode, configG.outputType, configG.projectName); err != nil {
		fmt.Printf("a call to watchdog.Run fails. Is DB trace directory set correctly (path, permissions)? Aborting. err: %v\n", err)
		os.Exit(1)
	}
}

func main() {
	flag.Parse()
	if *help {
		fmt.Println(usage)
		os.Exit(0)
	}
	fmt.Println("Real Time Trace Analyzer (RTTAnalyzer): github.com/borisdali/rttanalyzer")
	if *debug {
		watchdog.Debug = *debug
		rttanalyzer.Debug = *debug
		rttpubsub.Debug = *debug
		sqlinput.Debug = *debug
		fmt.Printf("[%v] dbg> os.Args = %#v\n", time.Now().Format("2006-01-02 15:04:05"), os.Args)
	}

	pwd, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	rttanalyzer.RttaHome = pwd

	configFileName := filepath.Join(rttanalyzer.Dir(), "rtta.conf")
	config, err := loadConfig(configFileName)
	if err != nil {
		fmt.Printf("error loading %q config file: %v. Aborting.\n", err, configFileName)
		os.Exit(1)
	}

	if config.dbName == "" {
		fmt.Printf("dbname parameter is not provided in %q config file. Aborting.\n", configFileName)
		os.Exit(1)
	}
	if config.dirName == "" {
		fmt.Printf("dirname parameter is not provided in %q config file. Aborting.\n", configFileName)
		os.Exit(1)
	}
	if config.sqlInput == "" {
		fmt.Printf("sqlinput parameter is not provided in %q config file. Aborting.\n", configFileName)
		os.Exit(1)
	}
	if *dequeue && config.outputType != "pubsub" {
		fmt.Printf("a dequeue mode is requested on the command line, but outputtype is not set to pubsub (outputtype is set to %s) in the %q config file. Aborting.\n", config.outputType, configFileName)
		os.Exit(1)
	}

	ctx := context.Background()

	var service *bqgen.Service
	projectName := config.projectName
	if config.outputType == "pubsub" {
		if config.appCred == "" {
			fmt.Println("a Pub/Sub mode is requested (via outputtype config parameter), yet appcredential mandatory parameter is not set. Aborting.")
			os.Exit(1)
		}
		if err := os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", config.appCred); err != nil {
			fmt.Printf("Cannot set GOOGLE_APPLICATION_CREDENTIALS env variable: %v. Aborting.\n", err)
			os.Exit(1)
		}
		jsonFile := config.appCred
		if *debug { fmt.Printf("[%v] dbg> GOOGLE_APPLICATION_CREDENTIALS=%s\n", time.Now().Format("2006-01-02 15:04:05"), jsonFile) }

		if projectName == "" {
			fmt.Println("a Pub/Sub mode is requested (via outputtype config parameter), yet projectname mandatory parameter is not set. Aborting.")
			os.Exit(1)
		}
		if err := os.Setenv("GOOGLE_PROJECT_NAME", projectName); err != nil {
			fmt.Printf("Cannot set GOOGLE_PROJECT_NAME env variable: %v. Aborting.\n", err)
			os.Exit(1)
		}

		if *debug { fmt.Printf("[%v] dbg> GOOGLE_PROJECT_NAME=%s\n", time.Now().Format("2006-01-02 15:04:05"), projectName) }

		var err error
		service, err = getService(ctx)
		if err != nil {
			fmt.Printf("Call to getService failed. Aborting. err: %v", err)
			os.Exit(1)
		}
	}

	client, err := pubsub.NewClient(ctx, projectName)
	if err != nil {
		log.Fatalf("Creating bigquery client: %v", err)
	}

	if config.mode == "" {
		config.mode = "write"
	}

	serviceG = service
	projectNameG = projectName
	configG = config

	if *dequeue {
		if *serviceAction == "" {
			if *debug { fmt.Printf("[%v] dbg> Running Dequeue in a non-service mode.\n", time.Now().Format("2006-01-02 15:04:05")) }
			dequeueWrap(ctx, client)
		}
		if *debug { fmt.Printf("[%v] dbg> Running Dequeue in a service mode.\n", time.Now().Format("2006-01-02 15:04:05")) }
		daemon.Create(ctx, "rttaDequeue", "RTTAnalyzer Dequeue Service", client, dequeueWrap, *serviceAction)

	} else if *setup && *serviceAction == "" {
		if *debug { fmt.Printf("[%v] dbg> Running Setup in a non-service mode.\n", time.Now().Format("2006-01-02 15:04:05")) }
		if *awrFile == "" {
			fmt.Println("A path to an AWR file is not provided (-awr flag missing). AWR file is mandatory in the -setup mode. Aborting.")
			os.Exit(1)
		}
		expSQL, err := sqlinput.ParseAWR(configG.dbName, *awrFile)
		if err != nil {
			fmt.Printf("a call to sqlinput.ParseAWR fails. Aborting. err: %v\n", err)
			os.Exit(1)
		}
		if err := sqlinput.PersistSQLInput(expSQL); err != nil {
			fmt.Printf("a call to sqlinput.PersistSQLInput fails. Aborting. err: %v\n", err)
			os.Exit(1)
		} else {
			fmt.Println("\nRTTAnalyzer rtta.sqlinput.fromAWR input file has been generated.\nPlease review and if it looks acceptable, use it as a real SQL input file, i.e.\n\t$ mv rtta.sqlinput.fromAWR rtta.sqlinput\n")
		}

	} else {
		if *serviceAction == "" {
			if *debug { fmt.Printf("[%v] dbg> Running Watchdog in a non-service mode.\n", time.Now().Format("2006-01-02 15:04:05")) }
			watchdogWrap(ctx, client)
		}
		if *debug { fmt.Printf("[%v] dbg> Running Watchdog in a service mode.\n", time.Now().Format("2006-01-02 15:04:05")) }
		daemon.Create(ctx, "rtta", "RTTAnalyzer Service", client, watchdogWrap, *serviceAction)
	}
}
