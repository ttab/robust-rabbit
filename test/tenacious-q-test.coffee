Q          = require 'q'
TenaciousQ = require '../lib/tenacious-q'
Ack        = require '../lib/ack'

describe 'TenaciousQ', ->
    tq = queue = exchange = amqpc = undefined

    beforeEach ->
        exchange = publish: stub().returns Q()

        amqpc =
            queue: stub().returns Q { bind: -> }
            exchange: stub().returns Q exchange

        queue =
            name: 'test'
            subscribe: spy()
        TenaciousQ(amqpc, queue).then (_tq) -> tq = _tq

    describe '.constructor()', ->

        it 'should parse the options object', ->
            TenaciousQ(amqpc, queue, {
                    retry: { delay: 15, max: 60 },
                    failure: { expiration: 60 }
                    prefetchCount: 7 }).then (tq) ->
                tq.retryDelay.should.equal 15000
                tq.maxRetries.should.equal 4
                tq.failureExpiration.should.equal 60000
                tq.prefetchCount.should.equal 7

    describe '._listen()', ->
        listener = msg = headers = info = ack = undefined
        beforeEach ->
            listener = stub().returns Q()
            msg = {}
            headers = {}
            info = {}
            ack = { acknowledge: spy(), retry: spy(), fail: spy() }

        it 'should invoke the listener', ->
            tq._listen listener, msg, headers, info, ack
            .then ->
                listener.should.have.been.calledWith msg, headers, info, ack

        it 'should call ack.acknowledge() on success', ->
            tq._listen listener, msg, headers, info, ack
            .then ->
                ack.acknowledge.should.have.been.calledOnce
                ack.retry.should.not.have.been.called
                ack.fail.should.not.have.been.called

        it 'should call ack.retry() on failure', ->
            listener.throws new Error 'such fail!'
            tq._listen listener, msg, headers, info, ack
            .then ->
                ack.retry.should.have.been.calledOnce
                ack.acknowledge.should.not.have.been.called
                ack.fail.should.not.have.been.called

        describe 'when listener doesnt return a promise', ->

            it 'should damn well NOT ack.acknowledge', ->
                listener = -> undefined
                tq._listen listener, msg, headers, info, ack
                .then ->
                    ack.acknowledge.should.not.have.been.called
                    ack.retry.should.not.have.been.called
                    ack.fail.should.not.have.been.called

            it 'should however retry on fail', ->
                listener = -> throw 'Bad bad'
                tq._listen listener, msg, headers, info, ack
                .then ->
                    ack.retry.should.have.been.called #!
                    ack.acknowledge.should.not.have.been.calledOnce
                    ack.fail.should.not.have.been.called

    describe '.subscribe()', ->

        it 'should use the default exchange', ->
            amqpc.exchange.should.have.been.calledWith 'test-flow', { autoDelete: true, confirm: true }

        it 'should set up a retry queue', ->
            tq.exchange.then ->
                amqpc.queue.should.have.been.calledWith 'test-retries',
                    durable: true
                    autoDelete: false
                    arguments:
                        'x-message-ttl': 10000
                        'x-dead-letter-exchange': '',
                        'x-dead-letter-routing-key': 'test'

        it 'should set up a failures queue', ->
            tq.exchange.then ->
                amqpc.queue.should.have.been.calledWith 'test-failures',
                    durable: true
                    autoDelete: false

        it 'should use default options if none are given', ->
            tq.subscribe(->).then ->
                queue.subscribe.should.have.been.calledWith { ack: true, prefetchCount: 1}, match.func

        it 'should use specified options, but set ack and prefetchCount', ->
            tq.subscribe({ panda: 'cub' }, ->).then ->
                queue.subscribe.should.have.been.calledWith { panda: 'cub', ack: true, prefetchCount: 1}, match.func

        describe 'should call subscribe on the underlying queue', ->
            beforeEach ->
                queue.subscribe = spy()
                TenaciousQ(amqpc, queue).then (_tq) -> tq = _tq

            it 'with a callback that in turn will invoke the listener', (done) ->
                listener = ->
                    done()
                tq.subscribe(listener).then ->
                    queue.subscribe.getCall(0).args[1](undefined, undefined, undefined, { acknowledge: -> })

            it 'and when invoked, the listener should recieve an ack object', (done) ->
                listener = (msg, headers, info, ack)->
                    ack.exchange.should.equal exchange
                    ack.should.be.instanceOf Ack
                    ack.msg.should.equal 'msg'
                    ack.headers.should.equal 'headers'
                    ack.info.should.equal 'info'
                    ack.ack.should.have.property 'acknowledge'
                    done()
                tq.subscribe(listener).then ->
                    queue.subscribe.getCall(0).args[1]('msg', 'headers', 'info', { acknowledge: -> })
