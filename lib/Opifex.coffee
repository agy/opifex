# Opifex.coffee
#
#	© 2013 Dave Goehrig <dave@dloh.org>
#
amqp = require 'wot-amqp'
stats = require 'wot-stats'

Opifex = (Url,Module,Args...) ->
	[ proto, user, password, host, port, domain, exchange, key, queue, dest, path ] = unescape(Url).match(
		///([^:]+)://([^:]+):([^@]+)@([^:]+):(\d+)/([^\/]*)/([^\/]+)/([^\/]+)/([^\/]*)/*([^\/]*)/*([^\/]*)///
	)[1...]
	dest ||= exchange # for publish only
	path ||= key # for publish only, NB: you can not send to # or * routes
	console.log [ proto, user, password, host, port, domain, exchange, key, queue, dest, path ]
	
	# main message handler
	self = (message, headers, info)  ->
		$ = arguments.callee
		
		# expose the configuration information to this instance
		$.account = domain
		$.headers = headers
		$.key = info.routingKey
		$.exchange = info.exchange
		$.queue = info.queue
		$.dest = dest
		$.path = path

		try # to parse as if it were a JSON message
			[ method, args... ] = JSON.parse message.data.toString()

			# If there's no method ie { "foo": "bar" }, attempt to pass it to the wildcard
			if not method 
				return $["*"]?.apply $, [ JSON.parse(message.data) ]
		
			# If the module doesn't process this type of method, try the wildcard handler
			if typeof($[method]) != 'function'
				return $["*"]?.apply $, [method].concat(args)
			
			# otherwise attempt to process it as a method invocation
			return $[method]?.apply $, args

		catch e # if not JSON attempt to pass the data to a wildcard handler as raw data
			try 
				return $["*"]?.apply $, [ message.data ] if message.data?
			catch ee
				console.log "[opifex] failed to process #{message.data}, #{ee}"

	# mixin stats
	stats.call(self)

	# If we get passed a function apply to mixin
	if typeof(Module) == 'function'
		Module.apply(self,Args)
	else # load the module of the associated name and mixin
		(require "opifex.#{Module}").apply(self,Args)
			
	# Once we're subscribed, we apply any init method defined in the mixin
	self.subscribed = () ->
		console.log "[opifex] subscription ready"
		self['init']?.apply(self,[])

	# AMQP connection handling
	self.connect = () ->
		console.log "[opifex] connecting to #{ Url }"
		self.connection = amqp.createConnection
			host: host,
			port: port*1,
			login: user,
			password: password,
			vhost: domain || '/'

		self.connection.on 'error', (Message) ->
			console.log "[opifex] connection error", Message
			process.exit 1
		
		self.connection.on 'close', () ->
			console.log "[opifex] connection closed"
			process.exit 0

		self.connection.on 'end', () ->
			console.log "[opifex] connection closed"
			process.exit 0

		self.connection.on 'ready', () ->
			# create exchange if necesssary
			console.log "[opifex] connection ready"
			self.connection?.exchange exchange, { durable: false, type: 'topic', autoDelete: true }, (Exchange) ->
				console.log "[opifex] exchange #{exchange} ready"
				self.exchange = Exchange
				# create queue if necessary
				self.connection?.queue queue,{ arguments: { "x-message-ttl" : 60000 } }, (Queue) ->
					console.log "[opifex] queue #{queue} ready"
					self.queue = Queue
					# bind the echange & key to the queue
					self.queue.bind exchange, key, () ->
						console.log "[opifex] #{exchange}/#{key} bound to #{queue}"
						# setup the subscription
						(self.queue.subscribe self).addCallback self.subscribed


	# sends a message to the given exchange and metadata
	# route & meta are optional, default to destination exchange and key respectively
	self.send = (msg,route,meta,headers) -> 
		route ?= dest
		meta ?= path
		if !msg then return
		if typeof msg != "string"
			msg = JSON.stringify msg
		# if we have the route already defined, publish to it
		if self[route]
			self.sent_messages "/#{domain}/#{route}/#{meta}", 1
			self[route].publish(meta, msg, { headers: self.headers || {} })
		else	# otherwise establish the route and try again
			self.connection?.exchange route, { durable: false, type: 'topic', autoDelete: true }, (Exchange) ->
				self[route] = Exchange
				self.send(msg,route,meta,headers)

	# attempt our initial connection	
	self.connect()
	self

module.exports = Opifex

