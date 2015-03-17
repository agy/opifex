# Opifex.coffee
#
#	© 2013 Dave Goehrig <dave@dloh.org>
#
amqp = require 'wot-amqp'

Opifex = (Url,Module,Args...) ->
	[ proto, user, password, host, port, domain, exchange, key, queue, dest, path ] = unescape(Url).match(
		///([^:]+)://([^:]+):([^@]+)@([^:]+):(\d+)/([^\/]*)/([^\/]+)/([^\/]+)/([^\/]*)/*([^\/]*)/*([^\/]*)///
	)[1...]
	dest ||= exchange # for publish only
	path ||= key # for publish only, NB: you can not send to # or * routes
	self = (message, headers, info)  ->
		$ = arguments.callee
		try 
			[ method, args... ] = JSON.parse message.data.toString()
		catch e
			console.log "not json #{message.data}"
			# attempt to pass the data to a wildcard handler so we can interpret
			$["*"].apply $, [ message.data ] if message.data?
			return
		console.log "got headers #{JSON.stringify headers}"
		$.headers = headers
		$.key = info.routingKey
		$.exchange = info.exchange
		$.queue = info.queue
		$.dest = dest
		$.path = path
		# if there is no method at all, attempt the wildcard handler on the message data
		if not method and $["*"]
			# NB: we will have a json object or we would have caught above!!!
			$["*"].apply $, [ JSON.parse(message.data) ]
			return
		if not $[method] and $["*"]
			$["*"].apply $, [method].concat(args)
		else
			$[method]?.apply $, args

	if typeof(Module) == 'function'
		Module.apply(self,Args)
	else
		(require "opifex.#{Module}").apply(self,Args)
			
	self.exchanges = {}
	self.queues = {}
	self.connection = amqp.createConnection
		host: host,
		port: port*1,
		login: user,
		password: password,
		vhost: domain || '/',
		heartbeat: 10
	self.connection.on 'error', (Message) ->
		console.log "Connection error", Message
		process.exit 1
	self.connection.on 'close', () ->
		console.log "[OPIFEX] GOT CONNECTION CLOSE"
		process.exit 0
	self.connection.on 'end', () ->
		console.log "Connection closed"
		process.exit 0
	self.connection.on 'ready', () ->
		self.connection.exchange exchange, { durable: false, type: 'topic', autoDelete: true }, (Exchange) ->
			self.exchanges[exchange] = Exchange
			Exchange.on 'close', () ->
				console.log "[EXCHANGE] got exchange close for #{route}"
				process.exit 0
			Exchange.on 'error', (error) ->
				console.log "[EXCHANGE] got exchange error for #{route} with #{error}"
			self.connection.queue queue,{ arguments: { "x-message-ttl" : 60000 } }, (Queue) ->
				self.queues[queue] = Queue
				Queue.on 'close', () ->
					console.log "[QUEUE] got queue close for source #{queue}"
					process.exit 0
				Queue.on 'error', (error) ->
					console.log "[QUEUE] got queue error for source #{queue} with #{error}"
					process.exit 1
				Queue.bind exchange, key
				(Queue.subscribe self).addCallback (ok) ->
					self['init']?.apply(self,[])
		self.send = (msg,route,recipient) -># route & recipient are optional, default to destination exchange and key respectively
			route ?= dest
			recipient ?= path
			if !msg then return
			if self.exchanges[route]
				self.exchanges[route].publish(recipient,msg, { headers: self.headers || {} })
			else
				self.connection.exchange route, { durable: false, type: 'topic', autoDelete: true }, (Exchange) ->
					self.exchanges[route] = Exchange
					Exchange.on 'close', () ->
						console.log "[EXCHANGE] got exchange close for #{route}"
						self.exchanges[route] = null
					Exchange.on 'error', (error) ->
						console.log "[EXCHANGE] got exchange error for #{route} with #{error}"
					Exchange?.publish(recipient,msg,{ headers: self.headers || {} })
	self

module.exports = Opifex

