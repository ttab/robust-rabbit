log = require 'bog'

module.exports = class Ack

    constructor: (@exchange, @msg, @headers, @info, @ack, @maxRetries=3) ->

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

    acknowledge: =>
        @ack.acknowledge()
        
    retry: (messageId='<unknown>') =>
        rc = (@headers.retryCount || 0) + 1
        @exchange.then (ex) =>
            if rc <= @maxRetries
                log.warn "retrying #{messageId}, retryCount=#{rc}"
                ex.publish 'retry', @_msgbody(@msg, @info.contentType), @_mkopts(@headers, @info, rc)
                .then =>
                    @ack.acknowledge()
            else
                log.warn "failing #{messageId}, too many retries (#{@maxRetries})"
                ex.publish 'fail', @_msgbody(@msg, @info.contentType), @_mkopts(@headers, @info, rc)
                .then => @ack.acknowledge()
        .fail (err) ->
            log.error err.stack
        
    fail: (messageId='<unknown>') =>
        @exchange.then (ex) =>
            log.warn "failing #{messageId}"
            ex.publish 'fail', @_msgbody(@msg, @info.contentType), @_mkopts(@headers, @info, @headers.retryCount || 0)
            .then => @ack.acknowledge() 
        .fail (err) ->
            log.error err.stack
            
