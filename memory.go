// Copyright (C) 2018 The Nori Authors info@nori.io
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public
// License as published by the Free Software Foundation; either
// version 3 of the License, or (at your option) any later version.
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
// Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, see <http://www.gnu.org/licenses/>.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"sync"

	cfg "github.com/nori-io/nori-common/config"
	"github.com/nori-io/nori-common/interfaces"
	"github.com/nori-io/nori-common/meta"
	noriPlugin "github.com/nori-io/nori-common/plugin"
)

type plugin struct {
	instance interfaces.PubSub
}

type instance struct {
	sid         uint
	mutex       *sync.Mutex
	exchanges   map[string]chan []byte
	subscribers map[string]map[uint]chan []byte
	stop        chan uint
	pub         interfaces.Publisher
	sub         interfaces.Subscriber
}

type publisher struct {
	marshaller interfaces.Marshaller
	ch         chan<- []byte
	err        chan error
}

type subscriber struct {
	id           uint
	unMarshaller interfaces.Unmarshaller
	stop         chan<- uint
	errors       chan error
	subscribed   <-chan []byte
}

type message struct {
	unmarshaller interfaces.Unmarshaller
	body         []byte
}

var (
	Plugin plugin
)

var defaultMarshal = json.Marshal

var defaultUnmarshal = json.Unmarshal

const defaultExchange = "default"

func (p *plugin) Init(_ context.Context, configManager cfg.Manager) error {
	configManager.Register(p.Meta())
	return nil
}

func (p *plugin) Instance() interface{} {
	return p.instance
}

func (p plugin) Meta() meta.Meta {
	return &meta.Data{

		ID: meta.ID{
			ID:      "nori/pubsub/memory",
			Version: "1.0.0",
		},
		Author: meta.Author{
			Name: "Nori",
			URI:  "https://nori.io",
		},
		Core: meta.Core{
			VersionConstraint: ">=1.0.0, <2.0.0",
		},
		Dependencies: []meta.Dependency{},
		Description: meta.Description{
			Name: "Nori: InMemory Message Queue",
		},
		Interface: meta.PubSub,
		License: meta.License{
			Title: "",
			Type:  "GPLv3",
			URI:   "https://www.gnu.org/licenses/"},
		Tags: []string{"queue", "mq", "pubsub", "memory"},
	}

}

func (p *plugin) Start(ctx context.Context, registry noriPlugin.Registry) error {
	if p.instance == nil {
		instance := &instance{
			mutex:       new(sync.Mutex),
			exchanges:   make(map[string]chan []byte),
			subscribers: make(map[string]map[uint]chan []byte),
			stop:        make(chan uint),
		}

		go func(mutex *sync.Mutex, stop chan uint) {
			for {
				select {
				case stopId := <-stop:
					mutex.Lock()

					for name, exs := range instance.subscribers {
						for sid, ch := range exs {
							if sid == stopId {
								close(ch)
								delete(instance.subscribers[name], sid)
							}
						}
					}

					mutex.Unlock()
				}
			}
		}(instance.mutex, instance.stop)

		f := func(opt interface{}) {
			str := opt.(*string)
			*str = defaultExchange
		}

		pub, err := instance.NewPublisher(defaultMarshal, f)
		if err != nil {
			return nil
		}
		instance.pub = pub

		sub, err := instance.NewSubscriber(defaultUnmarshal, f)
		if err != nil {
			return nil
		}
		instance.sub = sub

		p.instance = instance
	}
	return nil
}

func (p *plugin) Stop(_ context.Context, _ noriPlugin.Registry) error {
	p.instance = nil
	return nil
}

func (i *instance) NewPublisher(m interfaces.Marshaller, opts ...func(interface{})) (interfaces.Publisher, error) {
	var exchange string
	for _, opt := range opts {
		opt(&exchange)
	}

	if len(exchange) == 0 {
		return nil, errors.New("Need setup the exchange name with opts")
	}

	ex, ok := i.exchanges[exchange]
	if !ok {
		ex = i.newExchange(exchange)
	}

	return &publisher{
		ch:         ex,
		err:        make(chan error, 100),
		marshaller: m,
	}, nil
}

func (i *instance) NewSubscriber(um interfaces.Unmarshaller, opts ...func(interface{})) (interfaces.Subscriber, error) {
	i.sid++
	i.mutex.Lock()
	defer i.mutex.Unlock()

	var exchange string
	for _, opt := range opts {
		opt(&exchange)
	}

	if len(exchange) == 0 {
		return nil, errors.New("Need setup the exchange name with opts")
	}

	_, ok := i.exchanges[exchange]
	if !ok {
		i.newExchange(exchange)
	}

	ch := make(chan []byte)
	if i.subscribers[exchange] == nil {
		i.subscribers[exchange] = make(map[uint]chan []byte)
	}
	i.subscribers[exchange][i.sid] = ch

	sub := newSubscriber(i.sid, ch, i.stop, um)

	return sub, nil
}

func (i instance) Publisher() interfaces.Publisher {
	return i.pub
}

func (i instance) Subscriber() interfaces.Subscriber {
	return i.sub
}

func (i *instance) newExchange(name string) chan []byte {
	if ex, ok := i.exchanges[name]; ok {
		return ex
	}
	ex := make(chan []byte)
	i.exchanges[name] = ex
	stop := make(chan []byte)

	go func(name string, exchange chan []byte, stop chan []byte, mutex *sync.Mutex) {
		for {
			select {
			case msg := <-exchange:
				mutex.Lock()
				lst := i.subscribers[name]
				for _, ch := range lst {
					ch <- msg
				}
				mutex.Unlock()
			}
		}
	}(name, ex, stop, i.mutex)

	return ex
}

func (p *publisher) Publish(ctx context.Context, key string, msg interface{}) error {
	if p.marshaller == nil {
		p.marshaller = defaultMarshal
	}
	body, err := p.marshaller(msg)
	if err != nil {
		return nil
	}
	p.ch <- body

	return nil
}

func (p *publisher) Errors() <-chan error {
	return p.err
}

func newSubscriber(id uint, subscribe <-chan []byte, stop chan<- uint, unmarshaller interfaces.Unmarshaller) interfaces.Subscriber {
	return &subscriber{
		id:           id,
		stop:         stop,
		errors:       make(chan error, 100),
		subscribed:   subscribe,
		unMarshaller: unmarshaller,
	}
}

func (s *subscriber) Start() <-chan interfaces.Message {
	output := make(chan interfaces.Message)
	go func(s *subscriber, output chan interfaces.Message) {
		defer func() {
			close(output)
		}()
		for {
			select {
			case msg := <-s.subscribed:
				output <- &message{
					unmarshaller: s.unMarshaller,
					body:         msg,
				}
			default:
				// void
			}
		}
	}(s, output)
	return output
}

func (s *subscriber) Errors() <-chan error {
	return s.errors
}

func (s *subscriber) Stop() error {
	s.stop <- s.id
	return nil
}

func (m *message) UnMarshal(msg interface{}) error {
	if m.unmarshaller == nil {
		return defaultUnmarshal(m.body, msg)
	}
	return m.unmarshaller(m.body, msg)
}

func (m *message) Done() error {
	return nil
}
