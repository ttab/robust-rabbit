Q   = require 'q'
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

    _mkopts: (headers, info, retryCount) ->
        opts = {}
        (opts[key] = val for key, val of info when key in ['contentType', 'contentEncoding'])
        opts.headers = headers || {}
        opts.headers.retryCount = retryCount
        opts

    _msgbody: (msg, contentType) ->
        if contentType == 'application/json'
            return msg
        else
            msg.data

    subscribe: (options, listener) =>
        if typeof options == 'function'
            listener = options
            options = {}
        options.ack = true
        options.prefetchCount = @prefetchCount
        @queue.subscribe options, (msg, headers, info, ack) =>
            listener msg, headers, info, {
                acknowledge: =>
                    ack.acknowledge()
                retry: (messageId='<unknown>') =>
                    rc = (headers.retryCount || 0) + 1
                    @exchange.then (ex) =>
                        if rc <= @maxRetries 
                            log.warn "retrying #{messageId}, retryCount=#{rc}"
                            ex.publish 'retry', @_msgbody(msg, info.contentType), @_mkopts(headers, info, rc), (err) ->
                                ack.acknowledge() if not err
                        else
                            log.warn "failing #{messageId}, too many retries (#{@maxRetries})"
                            ex.publish 'fail', @_msgbody(msg, info.contentType), @_mkopts(headers, info, rc), (err) ->
                                ack.acknowledge() if not err
                    .fail (err) ->
                        log.error err.stack
                    .done()
                fail: (messageId='<unknown>') =>
                    @exchange.then (ex) =>
                        log.warn "failing #{messageId}"
                        ex.publish 'fail', @_msgbody(msg, info.contentType), @_mkopts(headers, info, headers.retryCount || 0), (err) ->
                            ack.acknowledge() if not err
                    .fail (err) ->
                        console.log err
                        log.error err.stack
                    .done()
            }
