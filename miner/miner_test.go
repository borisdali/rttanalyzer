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

package miner

import (
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"testing"
	"time"

	"golang.org/x/net/context"
	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/pubsub"
	"github.com/borisdali/rttanalyzer/rttanalyzer"
	"github.com/kylelemons/godebug/pretty"
	bqgen "google.golang.org/api/bigquery/v2"
        oauth2google "golang.org/x/oauth2/google"
)

const projectName = "REPLACE-ME"

func getService(ctx context.Context) (*bqgen.Service, error) {
	if err := os.Setenv("GOOGLE_PROJECT_NAME", projectName); err != nil {
		return nil, fmt.Errorf("getService: error setting up env variables: %v", err)
	}

	const jsonFileContent = `{
  "type": "service_account",
  "private_key_id": "a6a4a9c243770c385af263bbc7870f98bddd4569",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCw3LOBf1jV0RWe\neQ0niLuJZK2ZBfO/jslyyest6pI9Nhoz1QolhYxAUhQ6kHGUlCmKcYvPS1R+mhYy\n39Cok82w1eu6URRfu+3DgpHMSxb5YdlDAq4xlILy+TCDdQSYQ/mNF2uAjmT418kS\n0YTWcwY1QtMuKMNGnOIDsf7RJzY1fNoh4Zmh1ElhbniGZYPkVro3T+6qZ2zsDvfN\nw6Kef+u6HuAKMNx20Fpv0olu8Nc1o+bze0AfcZEXeexYNBXB0EXR7ObhvdYceQRX\nT0F2F16M7GbtN6TU8yA+8adV6V8G98qulvmv2XpU3T8cxeJw6Jol0/gOWXZtlJgr\necIlIP4hAgMBAAECggEAepK6pYkInEcn4IisoTWWI4Zu+Zqyb5QZ8UAXid8EMwAH\n8Nw5CAAiT4GTG3N4BC0bDSPcbXIbwRIxSFjtV6f36hKYKKyAnki0Y8deHGP+LB+w\nX0zARQRULOiewryazpxtm2cbziDhwcRrSmuC0M0vBEKn8Vm23H1l9oGpxHKUk0Qn\no5ndzeSBncB53KzuRyvqDVDz987sYimmI4Q0ArtMhMzSi85DdjWLJac6X85RfDYX\nqx3v5SKhiakpcuBC89jl8FjGZbxu3IDx66AJHlVFnIwcViJmNKTYOYNG6NX4CIa4\nvYSmrGnGKgWgAZsxxfpDCb2yguMsGoqTJKfjDKJAcQKBgQDsd9Ln3/qac4oVkxOE\nylUTCV1WTSs8HPGtkJBUrDX4WHc9c7DOTRcopazjlgyq3PsGKcwHajfeXkNSTSOB\n9WtRldq8hgPwiPcbkLLCTbPkWPNZTktYDbv70UlClK/3lRn9AFMtcLFGND9JhWIy\nBBrFM9kYsvuQE0dnkuerB2HvIwKBgQC/eH6lRMHEBM8J2HnRLTPaqmcC0WqcKbB2\ngtoHRgC6dFZRiNsYwawFPrbttDqgZPjBlBuxFOSHtKqfvzaZxbMEjOFFqzoVxb02\nPNl6I6+LgebU7f73InX+RMlI1+CMfrOdRmQ9PKbqUNeeTQpBkGFLN4zUCTPtLJwn\n66HlZrez6wKBgFJoI6jJBnWC5FFGcxvowyMiNVPZCsMlNxgMdC/938UPV2akBa6v\ncO2qZwjdBscYwaZRNJg07QligkWROlmU5HSHK7ZdYcwWfz8s+w75s0JVuWCbB2jF\nSIimU8iPNo+qd2cTEOmaBz13AcMmZ6UUhxvISNTxsvdvsIeoy3Fv3jJPAoGAbmEz\neWky3yL6jC18xIbvjs6PhgNeF1q9PQTgVjMX9mVc4bIoxJPZ7EgkQtKUvaSX91i/\nwg1OVaFkf6VG/80GD8h7kaNNYAHXu23G3e+2opEnlncBdk0qgoP7GgBBlhzwxOkw\nnlDZ1Nj4BG1bTULDpA4JJK+NfNwrxaPEeDAIFTMCgYEA1B3PIUem6v+OiQQQm5C4\nZihcKDaFVCJ2kn1imEwMz+eMm0b4z/VcQE78AWwGryHtQ/3nGuBrpdVpHGC1xXff\n7WCSb5VlYDSAJERrfSLfLQ40ccuMnABFbCeWdcPZzyBuV4J4psQoFoMlNXR4lO/P\n7Lr2UdSbQJb3F7LYDtplDQE=\n-----END PRIVATE KEY-----\n",
  "client_email": "projectName-service-account@projectName.iam.gserviceaccount.com",
  "client_id": "108549917536921964152",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://accounts.google.com/o/oauth2/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/projectName-service-account%40projectName.iam.gserviceaccount.com"
}
`

	fh2, err := ioutil.TempFile("", "jsonFile")
	if err != nil {
		return nil, fmt.Errorf("ioutil.TempFile() failed: couldn't open a tmp jsonFile: %v", err)
	}
	if _, err := fh2.WriteString(jsonFileContent); err != nil {
		return nil, fmt.Errorf("file.Write() 1 failed: couldn't write to tmp jsonFile: %v", err)
	}
	if err := fh2.Sync(); err != nil {
		return nil, fmt.Errorf("fh2.Sync() failed: %v", err)
	}

	if err := os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", fh2.Name()); err != nil {
		return nil, fmt.Errorf("getService: error setting up env variables: %v", err)
	}

	httpClient, err := oauth2google.DefaultClient(ctx, bigquery.Scope)
	if err != nil {
		return nil, fmt.Errorf("getService: oauth2google.DefaultClient error: %v", err)
	}

	service, err := bqgen.New(httpClient)
	if err != nil {
		return nil, fmt.Errorf("getService: bigquery.New(httpClient) error: %v", err)
	}
	fh2.Close()
	os.Remove(fh2.Name())
	return service, err
}

func TestMine(t *testing.T) {
	const sampleDataStartMining1 = `line#1
line#2
very long line#3
line#4
`
	const sampleDataStartMining2 = `line#5
line#6
another very long line#7
line#8
`

	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, projectName)
	if err != nil {
		t.Fatalf("Creating bigquery client: %v", err)
	}

	service, err := getService(ctx)
	if err != nil {
		t.Fatalf("getService error: %v", err)
	}

	notify := make(chan struct{})

	fh, err := ioutil.TempFile("", "TestStartMining")
	if err != nil {
		t.Fatalf("ioutil.TempFile() failed: couldn't open tmp file: %v", err)
	}

	defer func() {
		fh.Close()
		os.Remove(fh.Name())
	}()

	initializerString := "test"

	if _, err := fh.WriteString(sampleDataStartMining1); err != nil {
		t.Fatalf("file.Write() 1 failed: couldn't write to tmp file: %v", err)
	}
	if err := fh.Sync(); err != nil {
		t.Fatalf("fh.Sync() failed: %v", err)
	}

	r, err := rttanalyzer.LoadRoster(rttanalyzer.RosterFile)
	if err != nil {
                t.Fatalf("rttanalyzer.LoadRoster crashed with err=%v. Terminating..\n", err)
	}

	f, err := r.TraceFile(fh.Name())
	if err != nil {
		t.Fatal(err)
	}

	td := &testDumper{str: initializerString}
	var closed bool
	go func() {
		if err = Mine(ctx, client, service, notify, td, f); err != nil {
		// if err = Mine(ctx, service, fh.Name(), notify, td); err != nil {
			t.Fatalf("Mine: %v", err)
		}
		closed = true
	}()
	time.Sleep(time.Second)

	wanted := initializerString + sampleDataStartMining1
	if !reflect.DeepEqual(td.str, wanted) {
		t.Errorf("loadSQL(): -> diff -got +want\n%s", pretty.Compare(td.str, wanted))

	}

	// Write some more, send another notify to the channel, check that the Dumper sees new strings too.
	if _, err := fh.WriteString(sampleDataStartMining2); err != nil {
		t.Fatalf("file.Write() 2 failed: couldn't write to tmp file: %v", err)
	}

	if err := fh.Sync(); err != nil {
		t.Fatalf("fh.Sync() failed: %v", err)
	}

	notify <- struct{}{}
	time.Sleep(time.Second)

	wanted = initializerString + sampleDataStartMining1 + sampleDataStartMining2
	if !reflect.DeepEqual(td.str, wanted) {
		t.Errorf("loadSQL(): -> diff -got +want\n%s", pretty.Compare(td.str, wanted))

	}

	close(notify)
	time.Sleep(time.Second)
	if !closed {
		t.Error("miner didn't exit after closing notify channel")
	}
}

type testDumper struct {
	str string
}

func (d *testDumper) Dump(ctx context.Context, client *pubsub.Client, service *bqgen.Service, s string) error {
	d.str += s
	return nil
}
