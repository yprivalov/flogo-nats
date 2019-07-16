package nats

import (
	"context"
	"encoding/json"

	"github.com/TIBCOSoftware/flogo-lib/core/action"
	"github.com/TIBCOSoftware/flogo-lib/core/trigger"
	"github.com/TIBCOSoftware/flogo-lib/logger"
	nats "github.com/nats-io/go-nats"
)

var log = logger.GetLogger("nats-trigger")

// NatsTriggerFactory My Trigger factory
type NatsTriggerFactory struct {
	metadata *trigger.Metadata
}

//NewFactory create a new Trigger factory
func NewFactory(md *trigger.Metadata) trigger.Factory {
	return &NatsTriggerFactory{metadata: md}
}

//New Creates a new trigger instance for a given id
func (t *NatsTriggerFactory) New(config *trigger.Config) trigger.Trigger {
	return &NatsTrigger{metadata: t.metadata, config: config}
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

const jsonObjectMarker = byte('{')
const jsonArrayMarker = byte('[')
const jsonStringMarker = byte('"')

const IS_UNKNOWN = 0
const IS_OBJECT = 1
const IS_ARRAY = 2
const IS_STRING = 3

// Start implements trigger.Trigger.Start
func (t *NatsTrigger) Start() error {
	nc, _ := nats.Connect("nats://10.103.1.115:4222")

	for _, handler := range t.handlers {
		//subject := handler.GetStringSetting("subject")

		nc.Subscribe(">", func(m *nats.Msg) {

			jsonType := getJsonType(m.Data)
			switch jsonType {
			case IS_OBJECT:
				var content map[string]interface{}
				json.Unmarshal(m.Data, &content)
				log.Debugf("IS OBJECT %v", content)
				handler.Handle(context.Background(), map[string]interface{}{
					"subject": m.Subject,
					"content": content,
				})
				break
			case IS_ARRAY:
				var content map[string]([]interface{})
				json.Unmarshal(m.Data, &content)
				log.Debugf("IS ARRAY %v", content)
				handler.Handle(context.Background(), map[string]interface{}{
					"subject": m.Subject,
					"content": content,
				})
				break
			case IS_STRING:
				var content string
				json.Unmarshal(m.Data, &content)
				handler.Handle(context.Background(), map[string]interface{}{
					"subject": m.Subject,
					"content": content,
				})
				break
			case IS_UNKNOWN:
				log.Errorf("Could not extract type from message %v", m.Data)
			}
		})
	}
	// start the trigger
	return nil
}

func getJsonType(data []byte) int {
	for _, element := range data {
		switch element {
		case jsonObjectMarker:
			return IS_OBJECT
		case jsonArrayMarker:
			return IS_ARRAY
		case jsonStringMarker:
			return IS_STRING
		}
	}
	return IS_UNKNOWN
}

// Stop implements trigger.Trigger.Start
func (t *NatsTrigger) Stop() error {
	// stop the trigger
	return nil
}
