package main

import (
	"fmt"
	"sync"

	nats "github.com/nats-io/go-nats"
)

func main() {
	nc, err := nats.Connect("nats://10.103.1.115:4222")
	if err != nil {
		fmt.Println(err)
	}
	nc.Subscribe(">", func(m *nats.Msg) {
		fmt.Printf("Received a message: %s on %s\n", len(m.Data), m.Subject)
	})
	var wg sync.WaitGroup
	wg.Add(1)

	nc.Publish("foo.bar.baz", []byte("Hello World"))

	wg.Wait()
}
