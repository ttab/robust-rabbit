log = require 'bog'

module.exports = class Ack

    constructor: (@exchange, @msg, @headers, @info, @ack, @maxRetries=3) ->
        @resolved = false

    _mkopts: (headers, info, retryCount) ->
        opts = {}
        (opts[key] = val for key, val of info when key in ['contentType', 'contentEncoding', 'deliveryMode'])
        opts.headers = headers || {}
        opts.headers['tq-retry-count'] = retryCount
        opts

    _msgbody: (msg, contentType) ->
        if contentType == 'application/json'
            return msg
        else
            msg.data or msg

    _unlessResolved: (fn) =>
        if @resolved
            Promise.resolve()
        else
            @resolved = true
            Promise.resolve().then ->
                fn()
            .then (res) =>
                @ack.acknowledge()
                return res
            .catch (err) ->
                log.error err.stack

    acknowledge: => @_unlessResolved ->

    retry: =>
        @_unlessResolved =>
            rc = (@headers['tq-retry-count'] || 0) + 1
            if rc <= @maxRetries
                @exchange.publish 'retry', @_msgbody(@msg, @info.contentType), @_mkopts(@headers, @info, rc)
                .then -> rc
            else
                @exchange.publish 'fail', @_msgbody(@msg, @info.contentType), @_mkopts(@headers, @info, rc)
                .then -> 0

    fail: =>
        @_unlessResolved =>
            console.log @msg instanceof Buffer
            console.log typeof @msg
            console.log @info.contentType
            @exchange.publish 'fail', @_msgbody(@msg, @info.contentType), @_mkopts(@headers, @info, @headers['tq-retry-count'] || 0)
            .then -> 0
