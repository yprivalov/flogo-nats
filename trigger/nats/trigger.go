package nats

import (
	"github.com/TIBCOSoftware/flogo-lib/core/action"
	"github.com/TIBCOSoftware/flogo-lib/core/trigger"
	"github.com/nats-io/go-nats"
	"fmt"
	"context"
	"encoding/json"
)

// NatsTriggerFactory My Trigger factory
type NatsTriggerFactory struct{
	metadata *trigger.Metadata
}

//NewFactory create a new Trigger factory
func NewFactory(md *trigger.Metadata) trigger.Factory {
	return &NatsTriggerFactory{metadata:md}
}

//New Creates a new trigger instance for a given id
func (t *NatsTriggerFactory) New(config *trigger.Config) trigger.Trigger {
	return &NatsTrigger{metadata: t.metadata, config:config}
}

// NatsTrigger is a stub for your Trigger implementation
type NatsTrigger struct {
	metadata *trigger.Metadata
	runner   action.Runner
	config   *trigger.Config
	handlers []*trigger.Handler
}

// Initialize implements trigger.Initializable.Initialize
func (t *NatsTrigger) Initialize(ctx trigger.InitContext) error {
	t.handlers = ctx.GetHandlers()
	return nil
}

// Init implements trigger.Trigger.Init
func (t *NatsTrigger) Init(runner action.Runner) {
	t.runner = runner
}

// Metadata implements trigger.Trigger.Metadata
func (t *NatsTrigger) Metadata() *trigger.Metadata {
	return t.metadata
}

// Start implements trigger.Trigger.Start
func (t *NatsTrigger) Start() error {
	nc, _ := nats.Connect(nats.DefaultURL)

	for _, handler := range t.handlers {
		//subject := handler.GetStringSetting("subject")

		nc.Subscribe("*", func(m *nats.Msg) {
		//nc.Subscribe(subject, func(m *nats.Msg) {
			fmt.Printf("Received a message: %s\n", string(m.Data))
			var f map[string]interface{}
			json.Unmarshal(m.Data, &f)
			handler.Handle(context.Background(), f)
		})
	}
	// start the trigger
	return nil
}

// Stop implements trigger.Trigger.Start
func (t *NatsTrigger) Stop() error {
	// stop the trigger
	return nil
}
