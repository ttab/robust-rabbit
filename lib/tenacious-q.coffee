Q   = require 'q'
Ack = require './ack'
log = require 'bog'

module.exports = class TenaciousQ
    
    constructor: (@amqpc, @queue, retryDelay=60000, @maxRetries=3, @prefetchCount=1) ->
        qname = queue.name
        exname = "#{qname}-flow"
        @exchange = @amqpc.exchange exname, { autoDelete: true, confirm: true }
        @exchange.then => [
            @amqpc.queue "#{qname}-retries",
                durable: true
                autoDelete: false
                arguments:
                    'x-message-ttl': retryDelay
                    'x-dead-letter-exchange': '',
                    'x-dead-letter-routing-key': qname
            @amqpc.queue "#{qname}-failures",
                durable: true
                autoDelete: false
            ]
        .spread (retries, failures) ->
            retries.bind exname, 'retry'
            failures.bind exname, 'fail'
        .fail (err) ->
            log.error err
        .done()

    subscribe: (options, listener) =>
        if typeof options == 'function'
            listener = options
            options = {}
        options.ack = true
        options.prefetchCount = @prefetchCount
        @queue.subscribe options, (msg, headers, info, ack) =>
            listener msg, headers, info,
                new Ack(@exchange, msg, headers, info, ack, @maxRetries)
