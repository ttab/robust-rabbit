log = require 'bog'
Q   = require 'q'

module.exports = class Ack

    constructor: (@exchange, @msg, @headers, @info, @ack, @maxRetries=3) ->
        @resolved = false

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
        unless @resolved
            @ack.acknowledge()
            @resolved = true
        
    retry: (messageId='<unknown>') =>
        rc = (@headers.retryCount || 0) + 1
        Q().then =>
            unless @resolved
                if rc <= @maxRetries
                    log.warn "retrying #{messageId}, retryCount=#{rc}"
                    @exchange.publish 'retry', @_msgbody(@msg, @info.contentType), @_mkopts(@headers, @info, rc)
                    .then @acknowledge
                else
                    log.warn "failing #{messageId}, too many retries (#{@maxRetries})"
                    @exchange.publish 'fail', @_msgbody(@msg, @info.contentType), @_mkopts(@headers, @info, rc)
                    .then @acknowledge
        .fail (err) ->
            log.error err.stack
        
    fail: (messageId='<unknown>') =>
        Q().then =>
            unless @resolved
                log.warn "failing #{messageId}"
                @exchange.publish 'fail', @_msgbody(@msg, @info.contentType), @_mkopts(@headers, @info, @headers.retryCount || 0)
                .then @acknowledge 
        .fail (err) ->
            log.error err.stack
            
