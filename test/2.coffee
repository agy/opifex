Opifex = require 'opifex'

Opifex('amqp://test:test@172.17.42.1:5672/wot', 'test01.source', 'test01.sink', () ->
	this.hello = (message...) ->
		console.log message
	this['*'] = (message...) ->
		if message[0] instanceof Buffer
			console.log message.toString()
		else
			console.log JSON.stringify message
)
