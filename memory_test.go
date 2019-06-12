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
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/nori-io/nori-interfaces/interfaces"
)

type testStruct struct {
	Data string
}

var testData = "test"

func TestPlugin(t *testing.T) {
	assert := assert.New(t)

	p := new(plugin)

	assert.NotNil(p.Meta())
	assert.NotEmpty(p.Meta().Id())

	err := p.Start(nil, nil)
	assert.Nil(err)

	pubsub, ok := p.Instance().(interfaces.PubSub)
	assert.True(ok)
	assert.NotNil(pubsub)

	sub := pubsub.Subscriber()
	assert.NotNil(sub)

	pub := pubsub.Publisher()
	assert.NotNil(pub)

	wg := new(sync.WaitGroup)
	wg.Add(1)
	var msg interfaces.Message
	go func() {
		defer wg.Done()
		msg = <-sub.Start()
		assert.NotNil(msg)
	}()

	data := &testStruct{testData}
	err = pub.Publish(nil, "", data)
	assert.Nil(err)

	wg.Wait()

	newData := new(testStruct)
	err = msg.UnMarshal(newData)
	assert.Nil(err)

	assert.Equal(newData.Data, data.Data)

	err = p.Stop(nil, nil)
	assert.Nil(err)
	assert.Nil(p.Instance())
}
