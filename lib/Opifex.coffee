# Opifex.coffee
#
#	© 2013 Dave Goehrig <dave@dloh.org>
#
amqp = require 'wot-amqp'
request = require 'request'

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
			return
		$.key = info.routingKey
		$.exchange = info.exchange
		$.queue = info.queue
		$.dest = dest
		$.path = path
		if not $[method] and $["*"]
			$["*"].apply $, [method].concat(args)
		else
			$[method]?.apply $, args

	if typeof(Module) == 'function'
		Module.apply(self,Args)
	else
		(require "opifex.#{Module}").apply(self,Args)
			
	self.exchanges = {}
	self.connection = amqp.createConnection
		host: host,
		port: port*1,
		login: user,
		password: password,
		vhost: domain || '/'
	self.connection.on 'error', (Message) ->
		console.log "Connection error", Message
	self.connection.on 'end', () ->
		console.log "Connection closed"
	self.connection.on 'ready', () ->
		self.connection.exchange exchange, { durable: false, type: 'topic', autoDelete: true }, (Exchange) ->
			self.exchange = Exchange
			# The management/monitoring exchange must remain auto-delete, but the federation from wot/monitoring becomes unbound when the exchange dies.
			# If this endpoint is connected to the management vhost we need to make sure that the federation gets re-bound as well.
			if domain == 'management' && exchange == 'monitoring'
				reqOpts =
					url: "http://#{user}:#{password}@#{host}:15672/api/bindings/#{domain}/e/#{exchange}/q/#{exchange}"
					method: 'POST'
					json: '{"routing_key":"#","arguments":[]}'
				request.get reqOpts, (error,response,body) ->
					if !error
						console.log "Failed to bind monitoring federation. Please check RabbitMQ management console."
					else
						console.log "Created monitoring federation."
		self.connection.queue queue,{ arguments: { "x-message-ttl" : 60000 } }, (Queue) ->
			self.queue = Queue
			self.queue.bind exchange, key
			self.queue.subscribe self
			self['init']?.apply(self,[])
	self.send = (msg,route,recipient) -># route & recipient are optional, default to destination exchange and key respectively
		route ?= dest
		recipient ?= path
		if typeof msg != "string"
			msg = JSON.stringify msg
		if self[route]
			self[route].publish(recipient,msg)
		else
			self.connection.exchange route, { durable: false, type: 'topic', autoDelete: true }, (Exchange) ->
				self[route] = Exchange
				Exchange?.publish(recipient,msg)
	self

module.exports = Opifex

