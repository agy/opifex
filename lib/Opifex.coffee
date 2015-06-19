# Opifex.coffee
#
#	© 2013 Dave Goehrig <dave@dloh.org>
#
Amqp = require('wot-amqplib/event_api').AMQP

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

		return if not message

		# Raw mode. Try to dispatch to '*' with no examination or manipulation of message.
		if process.env['FORCE_RAW_MESSAGES']
			if $["*"]?
				$["*"].apply $, [ message ]
			else
				console.log '[opifex] could not dispatch raw to "*"'

		else
			# Try to interpret message as JSON. If it isn't, try to dispatch to '*'.
			try
				json = JSON.parse message.toString()
			catch e
				if $["*"]?
					$["*"].apply $, [ message ]
				else
					console.log '[opifex] could not dispatch binary to "*"'
				return

			# If message is an array, try to interpret as s-exp.
			# In checking to see if the first element is a method, try to avoid accidental matches.
			# If no matching method is found, try to dispatch to '*'.
			if json instanceof Array
				if json.length > 0 and $.hasOwnProperty(json[0]) and $[json[0]] instanceof Function
					method = json.shift()
					$[method].apply $, json
				else if $["*"]?
					$["*"].apply $, json
				else
					console.log '[opifex] could not dispatch #{json}'

			# Not an array, so no method matching. Try to dispatch to '*'.
			else if $["*"]?
				$["*"].apply $, [ json ]
			else
				console.log '[opifex] could not dispatch #{json}'

	# Once we're connected we'll initialize the input and output channels
	input = {}
	output = {}
	connection.on 'connected', () ->
		console.log "connected"

		# the input channel will be used for all inbound messages
		input = connection.createChannel()
		console.log "created input"

		[ SourceExchange, SourceQueue, SourceKey ] = Source.split '/'
		SourceQueue ||= SourceExchange
		SourceKey ||= '#'

		# input error
		input.on 'error', (e) ->
			console.log "[opifex] input error #{e}"

		# once the channel is opened, we declare the input queue
		input.on 'channel_opened', () ->
			console.log "input opened"
			input.declareQueue SourceQueue, {}

		# once we have the queue declared, we will start the subscription
		input.on 'queue_declared', (m,queue) ->
			console.log "input queue declared"
			input.declareExchange SourceExchange, 'topic', {}

		# exchange declared
		input.on 'exchange_declared', (m,exchange) ->
			console.log "input exchange declared #{exchange}"
			if exchange == SourceExchange
				input.bindQueue SourceQueue, SourceExchange, SourceKey, {}
		
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

		[ SinkExchange, SinkKey ] = Sink.split '/'

		output.on 'error', (e) ->
			console.log "[opifex] output error #{e}"

		# by declaring our exchange we're assured that it will exist before we send
		output.on 'channel_opened', () ->
			console.log "output opened"
			output.declareExchange SinkExchange, 'topic', {}
		
		# Our opifex has a fixed route out.
		output.on 'exchange_declared', (m, exchange) ->
			console.log "output exchange declared #{exchange}"
			if exchange == SinkExchange
				output.declareQueue exchange, {}

		output.on 'queue_declared', (m,queue) ->
			console.log "output queue declared #{queue}"
			output.bindQueue queue,SinkExchange,'#', {}

		output.on 'queue_bound', (m,a) ->
			console.log "output queue bound"
			# once our exchange is declared we can expose the send interface
			self.send = (msg) ->
				console.log "sending message #{SinkExchange} #{SinkKey} #{msg}"
				output.publish SinkExchange, SinkKey, new Buffer(msg), {}

				# Finally mix in the behaviors either by method or module
				if typeof(Module) == 'function'
					Module.apply(self,Args)
				else
					(require "opifex.#{Module}").apply(self,Args)

		# until that happens just blackhole requests
		self.send = () ->
			console.log "[opifex] error send called before exchange initialized"

		# now we open our channel
		output.open()

	self

module.exports = Opifex

