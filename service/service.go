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

// Package Service is a thin wrapper around github.com/kardianos/service

package service

import (
	"fmt"
	"time"
	"log"

	"cloud.google.com/go/pubsub"
	"golang.org/x/net/context"
	"github.com/kardianos/service"
)

var logger service.Logger

const defaultTimeout = 15 * time.Second

type program struct {
	run     func(context.Context, *pubsub.Client)
	ctx     context.Context
	client  *pubsub.Client
	cancel  context.CancelFunc
	done    chan struct{}
	timeout time.Duration
}

func (p *program) Start(s service.Service) error {
	go func() {
		p.run(p.ctx, p.client)
		close(p.done)
	}()
	return nil
}

func (p *program) Stop(s service.Service) error {
	p.cancel()
	select {
	case <-p.done:
		return nil
	case <-time.After(p.timeout):
		return fmt.Errorf("failed to shutdown within timeout %s", p.timeout)
	}
}


// Create is a simple helper to create a new service.
func Create(ctx context.Context, srvName, srvNameDisplay string, client *pubsub.Client, run func(context.Context, *pubsub.Client), action string) {
	svcConfig := &service.Config{
		Name:        srvName,
		DisplayName: srvNameDisplay,
		Description: "",
	}

	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})

	prg := &program{
		run:     run,
		ctx:     ctx,
		client:  client,
		cancel:  cancel,
		done:    done,
		timeout: defaultTimeout,
	}
	svc, err := service.New(prg, svcConfig)
	if err != nil {
		log.Fatal(err)
	}
	logger, err = svc.Logger(nil)
	if err != nil {
		log.Fatal(err)
	}

	switch action {
	case "run":
		if err = svc.Run(); err != nil {
			logger.Error(err)
		}
	case "install":
		if err := svc.Install(); err != nil {
			logger.Errorf("failed to install service %s: %s", srvName, err)
		}
	case "remove":
		if err := svc.Uninstall(); err != nil {
			logger.Errorf("failed to remove service %s: %s", srvName, err)
		}
	case "start":
		if err := svc.Start(); err != nil {
			logger.Errorf("failed to start service %s: %s", srvName, err)
		}
	case "stop":
		if err := svc.Stop(); err != nil {
			logger.Errorf("failed to stop service %s: %s", srvName, err)
		}
	default:
		fmt.Printf("%q is not a valid argument.\n", action)
	}

}

