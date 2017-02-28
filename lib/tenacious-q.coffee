Ack = require './ack'
log = require 'bog'

class TenaciousQ

    constructor: (@amqpc, @queue, options={}) ->
        @retryDelay = (options.retry?.delay or 10) * 1000
        @maxRetries = (options.retry?.max or 60) * 1000 / @retryDelay
        @prefetchCount = options.prefetchCount or 1

        @qname = @queue.name
        exname = "#{@qname}-flow"
        @exchange = @amqpc.exchange exname, { autoDelete: true, confirm: true }
        @exchange.then => Promise.all [
            @amqpc.queue "#{@qname}-retries",
                durable: true
                autoDelete: false
                arguments:
                    'x-message-ttl': @retryDelay
                    'x-dead-letter-exchange': '',
                    'x-dead-letter-routing-key': @qname
            @amqpc.queue "#{@qname}-failures",
                durable: true
                autoDelete: false
            ]
        .then ([retries, failures]) ->
            retries.bind exname, 'retry'
            failures.bind exname, 'fail'
        .catch (err) ->
            log.error err

    _listen: (listener, msg, headers, info, ack) ->
        ret = null
        Promise.resolve().then ->
            ret = listener msg, headers, info, ack
        .then ->
            ack.acknowledge() if ret?.then
        .catch (err) =>
            ack.retry()
            log.error "#{@qname} error", (if err.stack then err.stack else err)

    subscribe: (options, listener) =>
        if typeof options == 'function'
            listener = options
            options = {}
        options.ack = true
        options.prefetchCount = @prefetchCount
        @exchange.then (ex) =>
            @queue.subscribe options, (msg, headers, info, ack) =>
                @_listen listener, msg, headers, info, new Ack(ex, msg, headers, info, ack, @maxRetries)
        .catch (err) ->
            log.error err

module.exports = (amqpc, queue, options) ->
    q = new TenaciousQ(amqpc, queue, options)
    q.exchange.then -> q
