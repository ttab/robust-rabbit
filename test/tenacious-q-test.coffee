Q          = require 'q'
TenaciousQ = require '../lib/tenacious-q'
Ack        = require '../lib/ack'

describe 'TenaciousQ', ->
    
    describe '.subscribe()', ->
        rm = queue = exchange = amqpc = undefined

        beforeEach ->
            exchange = publish: stub().returns Q()
            
            amqpc =
                queue: stub().returns Q { bind: -> }
                exchange: stub().returns Q exchange

            queue =
                name: 'test'
                subscribe: spy()
            rm = new TenaciousQ amqpc, queue

        it 'should use the default exchange', ->
            amqpc.exchange.should.have.been.calledWith 'test-flow', { autoDelete: true, confirm: true }

        it 'should set up a retry queue', ->
            rm.exchange.then ->
                amqpc.queue.should.have.been.calledWith 'test-retries',
                    durable: true
                    autoDelete: false
                    arguments:
                        'x-message-ttl': 60000
                        'x-dead-letter-exchange': '',
                        'x-dead-letter-routing-key': 'test'
    
        it 'should set up a failures queue', ->
            rm.exchange.then ->
                amqpc.queue.should.have.been.calledWith 'test-failures',
                    durable: true
                    autoDelete: false
            
        it 'should use default options if none are given', ->
            rm.subscribe(->).then ->
                queue.subscribe.should.have.been.calledWith { ack: true, prefetchCount: 1}, match.func
            
        it 'should use specified options, but set ack and prefixCount', ->
            rm.subscribe({ panda: 'cub' }, ->).then ->
                queue.subscribe.should.have.been.calledWith { panda: 'cub', ack: true, prefetchCount: 1}, match.func

        describe 'should call subscribe on the underlying queue', ->
            beforeEach ->
                queue.subscribe = spy()
                rm = new TenaciousQ amqpc, queue
                
            it 'with a callback that in turn will invoke the listener', (done) ->
                listener = ->
                    done()
                rm.subscribe(listener).then ->
                    queue.subscribe.getCall(0).args[1]()

            it 'and when invoked, the listener should recieve an ack object', (done) ->
                listener = (msg, headers, info, ack)->
                    ack.exchange.should.equal exchange
                    ack.should.be.instanceOf Ack
                    ack.msg.should.equal 'msg'
                    ack.headers.should.equal 'headers'
                    ack.info.should.equal 'info'
                    ack.ack.should.equal 'ack'
                    done()
                rm.subscribe(listener).then ->
                    queue.subscribe.getCall(0).args[1]('msg', 'headers', 'info', 'ack')

            
