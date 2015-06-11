Opifex = require 'opifex'

module = () ->
	this.hello = (message...) ->
		console.log message
	this['*'] = (message...) ->
		if message[0] instanceof Buffer
			console.log message.toString()
		else
			console.log JSON.stringify message

amqp = process.env['AMQP'] || 'amqp://test:test@172.17.42.1:5672/wot'
source = process.env['SOURCE'] || 'test01.source'
sink = process.env['SINK'] || 'test01.sink'
args = []

Opifex.apply(Opifex,[ amqp, source, sink, module, args ])
