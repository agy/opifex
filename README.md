opifex
======

Opifex is a coffeescript module for doing useful work.

Usage
-----

First write a script like:

	Opifex = require 'opifex'
	Opifex('amqp://user:password@host:port/domain', 'source', 'destination', () ->
		@facit = (command) ->
			console.log "Got command #{command}"
	)


And then on the given host send to the appropriate vhost on the given exchange a message:

	[ "facit", "some command" ]

And it will log "Got command some command" to the console!
