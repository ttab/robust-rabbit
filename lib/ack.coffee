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

    retry: =>
        rc = (@headers.retryCount || 0) + 1
        Q().then =>
            unless @resolved
                if rc <= @maxRetries
                    @exchange.publish 'retry', @_msgbody(@msg, @info.contentType), @_mkopts(@headers, @info, rc)
                    .then @acknowledge
                    .then true
                else
                    @exchange.publish 'fail', @_msgbody(@msg, @info.contentType), @_mkopts(@headers, @info, rc)
                    .then @acknowledge
                    .then false
        .fail (err) ->
            log.error err.stack

    fail: =>
        Q().then =>
            unless @resolved
                @exchange.publish 'fail', @_msgbody(@msg, @info.contentType), @_mkopts(@headers, @info, @headers.retryCount || 0)
                .then @acknowledge
                .then false
        .fail (err) ->
            log.error err.stack
