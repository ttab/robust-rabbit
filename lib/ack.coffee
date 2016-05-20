log = require 'bog'
Q   = require 'q'

module.exports = class Ack

    constructor: (@exchange, @msg, @headers, @info, @ack, @maxRetries, @failureExpiration) ->
        @resolved = false

    _mkopts: (headers, info, retryCount, failure=false) ->
        opts = {}
        (opts[key] = val for key, val of info when key in ['contentType', 'contentEncoding'])
        opts.headers = headers || {}
        opts.headers.retryCount = retryCount
        opts.expiration = "#{@failureExpiration}" if failure
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
            Q.fcall(fn)
            .then (res) =>
                @ack.acknowledge()
                return res
            .fail (err) ->
                log.error err.stack

    acknowledge: => @_unlessResolved ->

    retry: =>
        @_unlessResolved =>
            rc = (@headers.retryCount || 0) + 1
            if rc <= @maxRetries
                @exchange.publish 'retry', @_msgbody(@msg, @info.contentType), @_mkopts(@headers, @info, rc)
                .then -> rc
            else
                @exchange.publish 'fail', @_msgbody(@msg, @info.contentType), @_mkopts(@headers, @info, rc, true)
                .then -> 0

    fail: =>
        @_unlessResolved =>
            @exchange.publish 'fail', @_msgbody(@msg, @info.contentType), @_mkopts(@headers, @info, @headers.retryCount || 0, true)
            .then -> 0
