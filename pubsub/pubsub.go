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

// Package pubsub provides RTTAnalyzer with the ability to pass the messages through
// Pub Sub to further persist in Biq Query database.
package pubsub

import (
	"encoding/json"
	"fmt"
	"time"

	"golang.org/x/net/context"
	"cloud.google.com/go/pubsub"
	bqgen "google.golang.org/api/bigquery/v2"
	"google.golang.org/api/iterator"
)

const (
	topicName   = "rttanalyzertopic"
	subName     = "rttanalyzersub"
	datasetName = "rttanalyzer"
	tableName   = "traces"
	msgRead     = 1	// 10
)

var Debug bool

// PayloadSummary represents a Pub Sub message containing a summary of threshold violations.
type PayloadSummary struct {
	DB             string
	IsViolation    bool
	BusinessTxName string
	Threshold      float64
	SQLID          string
	WorstELA       float64
	LastELA        float64
	NumViolations  int64
	EnqueueTime    time.Time
}

// Enqueue sends a starter message, checking the existence of a topic and a subscription
// If topic/subscription don't exist, Enqueue() creates them.
func Enqueue(ctx context.Context, client *pubsub.Client, msg *PayloadSummary) error {
	// Create a topic and subscription if don't exist.
	topic := client.Topic(topicName)
	if ok, err := topic.Exists(ctx); !ok || err != nil {
		if topic, err = client.CreateTopic(ctx, topicName); err != nil {
			fmt.Errorf("[%v] Enqueue: can't create a topic: %v", time.Now().Format("2006-01-02 15:04:05"), err)
		}
	}

	b, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	msgIDs, err := topic.Publish(ctx, &pubsub.Message{
		Data: b,
	})
	if err != nil {
		return err
	}
	fmt.Printf("[%v] info> Published a message: id=%s (on topic=%v)\n", time.Now().Format("2006-01-02 15:04:05"), msgIDs[0], topic)
	return nil
}

// Dequeue receives messages from a topic it is subscribed to in an infinite loop.
func Dequeue(ctx context.Context, client *pubsub.Client, service *bqgen.Service, projectName string) error {
	count := 1
	topic := client.Topic(topicName)

	// Topic and Subscription should already exist at this point
	for {
		sub := client.Subscription(subName)
		if ok, err := sub.Exists(ctx); !ok || err != nil {
			if sub, err = client.CreateSubscription(ctx, subName, topic, 0, nil); err != nil {
				fmt.Errorf("[%v] Enqueue: can't create a sub: %v\n", time.Now().Format("2006-01-02 15:04:05"), err)
			} else {
				if Debug { fmt.Printf("[%v] dbg> Subscripiton %q created.\n", time.Now().Format("2006-01-02 15:04:05"), subName)}
			}
		} else {
			if Debug { fmt.Printf("[%v] dbg> Subscripiton %q already exists.\n", time.Now().Format("2006-01-02 15:04:05"), subName)}
		}

		// Pull up to 10 messages (maybe fewer) from the subscription.
		// Blocks for an indeterminate amount of time.
		msgs, err := sub.Pull(ctx)
		if err != nil {
			return fmt.Errorf("pubsub.Dequeue: could not pull a message: %v\n", err)
		}
		defer msgs.Stop()

		if Debug { fmt.Printf("[%v] dbg> Listening on topic: %v; count=%d; msgs: %v\n", time.Now().Format("2006-01-02 15:04:05"), topic, count, msgs)}

		// Consume msgRead messages
		for i:=0; i < msgRead; i++ {
			msg, err := msgs.Next()
			// if Debug { fmt.Printf("[%v] dbg> msg.id=%v, msg=%v\n", time.Now().Format("2006-01-02 15:04:05"), msg.ID, msg)}
			if err == iterator.Done {
				if Debug { fmt.Printf("[%v] dbg> msg=%v, break-ing out (i.e. err=iterator.Done)\n", time.Now().Format("2006-01-02 15:04:05"), msg)}
				break
			}
			if err != nil {
				return fmt.Errorf("pubsub.Dequeue: could not iterate over consumed messages: %v\n", err)
			}
			//payload := string(msg.Data)
			var payload PayloadSummary
			if err := json.Unmarshal(msg.Data, &payload); err != nil {
				fmt.Errorf("[%v] error> could not decode message data. err=%v, msg=%v\n", time.Now().Format("2006-01-02 15:04:05"), err, msg)
				go msg.Done(true)
				//go pubsub.Ack(ctx, subName, msg.AckID)
				continue
			}

			fmt.Printf("[%v] count=%d: Processing message id=%s from topic=%v: DB=%s, payload=%v, IsViolation=%v, BusinessTxName=%s, Threshold=%f, SQLID=%s, WorstELA=%f, LastELA=%f, NumViolations=%d\n", time.Now().Format("2006-01-02 15:04:05"), count, msg.ID, topic, payload.DB, payload, payload.IsViolation, payload.BusinessTxName, payload.Threshold, payload.SQLID, payload.WorstELA, payload.LastELA, payload.NumViolations)

			if err := insertBQ(service, projectName, payload); err != nil {
				fmt.Errorf("[%v] BQInsert error inserting a message [msgID=%s]: DB=%s, payload=%v, IsViolation=%v, BusinessTxName=%s, LastELA=%f, err=%v\n",
					time.Now().Format("2006-01-02 15:04:05"), msg.ID, payload.DB, payload, payload.IsViolation, payload.BusinessTxName, payload.LastELA, err)
			} else {
				if Debug { fmt.Printf("[%v] dbg> insertBQ didn't error out, continuing..\n", time.Now().Format("2006-01-02 15:04:05"))}
			}
			msg.Done(true)
			count++
		}
	}
}

// insertBQ receives PubSub payload and persists it in BQ table.
func insertBQ(service *bqgen.Service, projectName string, payload PayloadSummary) error {
	tableDataService := bqgen.NewTabledataService(service)
	if Debug { fmt.Printf("[%v] dbg> insertBQ: tableDataService=%v\n", time.Now().Format("2006-01-02 15:04:05"), tableDataService)}

	request := new(bqgen.TableDataInsertAllRequest)

	// TODO(bdali): need to figure out auto-increment values and timestamp in BQ.
	jsonRow := map[string]bqgen.JsonValue{
		"id":             bqgen.JsonValue(1),
		"database":       bqgen.JsonValue(payload.DB),
		"businesstxname": bqgen.JsonValue(payload.BusinessTxName),
		"threshold":      bqgen.JsonValue(payload.Threshold),
		"sqlid":          bqgen.JsonValue(payload.SQLID),
		"lastela":        bqgen.JsonValue(payload.LastELA),
		"worstela":       bqgen.JsonValue(payload.WorstELA),
		"violations":     bqgen.JsonValue(payload.NumViolations),
		"enqueued_at":    bqgen.JsonValue(payload.EnqueueTime),
		"dequeued_at":    bqgen.JsonValue(time.Now()),
	}

	request.Rows = append(request.Rows, &bqgen.TableDataInsertAllRequestRows{Json: jsonRow})

	resp, err := tableDataService.InsertAll(projectName, datasetName, tableName, request).Do()
	if err != nil {
		return fmt.Errorf("[%v] insertBQ: tableDataService.InsertAll error: %v\n", time.Now().Format("2006-01-02 15:04:05"), err)
	}
	if Debug { fmt.Printf("[%v] dbg> insertBQ resp=%v\n", time.Now().Format("2006-01-02 15:04:05"), resp)}
	return nil
}
