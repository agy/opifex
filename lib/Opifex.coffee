# Opifex.coffee
#
#	© 2013 Dave Goehrig <dave@dloh.org>
#
Amqp = require('amqplib/event_api').AMQP

Opifex = (Url,Source,Sink,Module,Args...) ->

	# connect to RabbitMQ
	connection = new Amqp(Url)

	# bail on error
	connection.on 'error', (Message) ->
		console.log "[opifex] error #{Message }"
		process.exit 1
	
	# bail on loss of connectivity
	connection.on 'closed', () ->
		console.log "[opifex] got connection close"
		process.exit 2
	
	# this is our message handler, takes a Buffer, and an object
	self = (message, headers)  ->
		console.log("got",message,headers)
		$ = arguments.callee
		try
			# try to parse as JSON
			json = JSON.parse message.toString()
		catch e
			# ok it isn't a JSON, dispatch as binary
			try 	
				$["*"]?.apply $, [ message ] if message
			catch e
				console.log "[opifex] failed to dispatch as binary: #{e}"
			return

		try
			# check to see if it is an s-exp
			[ method, args... ] = json
		catch e
			# it isn't an s-exp so pass it to our wildcard handler
			try
				$["*"]?.apply $, [ json ]
			catch e
				console.log "[opifex] failed to dispatch as json: #{e}"
			return
	
		try
			# we're a JSON s-exp, dispatch method	
			$[method]?.apply $, args
		catch e
			console.log "[opifex] failed to dispatch as s-exp: #{e}"

	# Once we're connected we'll initialize the input and output channels
	input = {}
	output = {}
	connection.on 'connected', () ->
		console.log "connected"

		# the input channel will be used for all inbound messages
		input = connection.createChannel()
		console.log "created input"

		# input error
		input.on 'error', (e) ->
			console.log "[opifex] input error #{e}"

		# once the channel is opened, we declare the input queue
		input.on 'channel_opened', () ->
			console.log "input opened"
			input.declareQueue Source, {}

		# once we have the queue declared, we will start the subscription
		input.on 'queue_declared', (m,queue) ->
			console.log "input queue declared"
			input.declareExchange Source, 'topic', {}

		# exchange declared
		input.on 'exchange_declared', (m,exchange) ->
			console.log "input exchange declared #{exchange}"
			if exchange == Source
				input.bindQueue Source, Source, '#', {}		
		
		# once we're bound, setup consumption
		input.on 'queue_bound', (m, a) ->
			console.log "input queue bound #{a}"
			input.consume a[0], { noAck: false }

		# once we have our subscription, we'll setup the message handler
		input.on 'subscribed', (m,queue) ->
			console.log "input subscribed"
			input.on 'message', (m) ->
				self(m.content,m.fields.headers)
				input.ack(m)	 # NB: we ack after invoking our handler!
		
		# finally open the channel
		input.open()

		# the output channel will be used for all outbound messages
		output = connection.createChannel()
		console.log "output created"

		[ Exchange, Key ] = Sink.split '/'

		output.on 'error', (e) ->
			console.log "[opifex] output error #{e}"

		# by declaring our exchange we're assured that it will exist before we send
		output.on 'channel_opened', () ->
			console.log "output opened"
			output.declareExchange Exchange, 'topic', {}
		
		# Our opifex has a fixed route out.
		output.on 'exchange_declared', (m, exchange) ->
			console.log "output exchange declared #{exchange}"
			if exchange == Exchange
				output.declareQueue exchange, {}

		output.on 'queue_declared', (m,queue) ->
			console.log "output queue declared"
			output.bindQueue queue,Exchange,'#', {}

		output.on 'queue_bound', (m,a) ->
			console.log "output queue bound"
			# once our exchange is declared we can expose the send interface
			self.send = (msg) ->
				console.log "sending message #{Exchange} #{Key} #{msg}"
				output.publish Exchange, Key, new Buffer(msg), {}

		# until that happens just blackhole requests
		self.send = () ->
			console.log "[opifex] error send called before exchange initialized"

		# now we open our channel
		output.open()

		# Finally mix in the behaviors either by method or module
		if typeof(Module) == 'function'
			Module.apply(self,Args)
		else
			(require "opifex.#{Module}").apply(self,Args)

	self

module.exports = Opifex

