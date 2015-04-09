# Opifex.coffee
#
#	© 2013 Dave Goehrig <dave@dloh.org>
#
Amqp = require 'amqplib/event_api'

Opifex = (Url,Source,Sink,Module,Args...) ->

	connection = new Amqp(Url)

	connection.on 'error', (Message) ->
		console.log "[opifex] error #{Message }"
		process.exit 1
	
	connection.on 'closed', () ->
		console.log "[opifex] got connection close"
		process.exit 2
	
	self = (message, headers)  ->
		$ = arguments.callee
		try
			[ method, args... ] = JSON.parse message.data.toString()
		catch e
			console.log "not json #{message.data}"
			# attempt to pass the data to a wildcard handler so we can interpret
			$["*"].apply $, [ message.data ] if message.data?
			return
		console.log "got headers #{JSON.stringify headers}"
		# if there is no method at all, attempt the wildcard handler on the message data
		if not method and $["*"]
			# NB: we will have a json object or we would have caught above!!!
			$["*"].apply $, [ JSON.parse(message.data) ]
			return
		if not $[method] and $["*"]
			$["*"].apply $, [method].concat(args)
		else
			$[method]?.apply $, args

	connection.on 'connected', () ->
		input = connection.createChannel()

		input.on 'queue_declared', (m,queue) ->
			input.consume queue, { noAck: false }

		input.on 'subscribed', (m,queue) ->
			input.on 'message', (m) ->
				self(m.content,m.fields.headers)
				input.ack(m)

		input.declareQueue Source, {}

		
		output = connection.createChannel()

		[ exchange, key ] = Sink.split '/'
		self.send = (msg,headers) ->
			output.publish exchange, key, msg, headers || {}

		if typeof(Module) == 'function'
			Module.apply(self,Args)
		else
			(require "opifex.#{Module}").apply(self,Args)

	self

module.exports = Opifex

