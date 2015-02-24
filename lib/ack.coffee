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

    _unlessResolved: (fn) =>
        if @resolved
            Q()
        else
            @resolved = true
            Q.fcall(fn).fail (err) ->
                console.log err
                log.error err.stack

    acknowledge: =>
        @_unlessResolved =>
            @ack.acknowledge()

    retry: =>
        @_unlessResolved =>
            rc = (@headers.retryCount || 0) + 1
            if rc <= @maxRetries
                @exchange.publish 'retry', @_msgbody(@msg, @info.contentType), @_mkopts(@headers, @info, rc)
                .then => @ack.acknowledge()
                .then -> rc
            else
                @exchange.publish 'fail', @_msgbody(@msg, @info.contentType), @_mkopts(@headers, @info, rc)
                .then => @ack.acknowledge()
                .then -> 0

    fail: =>
        @_unlessResolved =>
            @exchange.publish 'fail', @_msgbody(@msg, @info.contentType), @_mkopts(@headers, @info, @headers.retryCount || 0)
            .then => @ack.acknowledge()
            .then -> 0
