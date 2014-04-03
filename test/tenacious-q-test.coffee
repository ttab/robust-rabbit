chai          = require 'chai'
expect        = chai.expect
chai.should()
chai.use(require 'chai-as-promised')
chai.use(require 'sinon-chai')
{ stub, spy, match } = require 'sinon'

Q = require 'q'
TenaciousQ = require '../lib/tenacious-q'

describe 'TenaciousQ', ->
        
    describe '._mkopts()', ->
        rm = new TenaciousQ {
            queue: -> Q {}
            exchange: -> Q {}
            }, { name: 'test' }

        it 'should copy relevant headers from deliveryInfo', ->
            opts = rm._mkopts {}, {
                contentType: 'text/panda',
                contentEncoding: 'us-ascii',
                myHeader: 'myValue' }
            opts.should.have.property 'contentType', 'text/panda'
            opts.should.have.property 'contentEncoding', 'us-ascii'
            opts.should.not.have.property 'myHeader'

        it 'should copy existing headers, and add a retryCount', ->
            opts = rm._mkopts { panda: 'cub' }, {}, 23
            opts.should.have.property 'headers'
            opts.headers.should.eql
                panda: 'cub'
                retryCount: 23

        it 'should set the retryCount even if there are no existing headers', ->
            opts = rm._mkopts undefined, {}, 23
            opts.should.have.property 'headers'
            opts.headers.should.eql
                retryCount: 23
    
    describe '.subscribe()', ->
        rm = undefined

        amqpc =
            queue: stub().returns Q { bind: -> }
            exchange: stub().returns Q {}

        queue =
            name: 'test'
            subscribe: spy()

        beforeEach ->
            rm = new TenaciousQ amqpc, queue

        it 'should use the default exchange', ->
            amqpc.exchange.should.have.been.calledWith 'test-flow', { autoDelete: true, confirm: true }

        it 'should set up a retry queue', ->
            amqpc.queue.should.have.been.calledWith 'test-retries',
                durable: true
                autoDelete: false
                arguments:
                    'x-message-ttl': 60000
                    'x-dead-letter-exchange': '',
                    'x-dead-letter-routing-key': 'test'
    
        it 'should set up a failures queue', ->
            amqpc.queue.should.have.been.calledWith 'test-failures',
                durable: true
                autoDelete: false
            
        it 'should use default options if none are given', ->
            rm.subscribe ->
            queue.subscribe.should.have.been.calledWith { ack: true, prefetchCount: 1}, match.func
            
        it 'should use specified options, but set ack and prefixCount', ->
            rm.subscribe { panda: 'cub' }, ->
            queue.subscribe.should.have.been.calledWith { panda: 'cub', ack: true, prefetchCount: 1}, match.func

        describe 'should call subscribe on the underlying queue', ->
            beforeEach ->
                queue.subscribe = spy()
                rm = new TenaciousQ amqpc, queue
                
            it 'with a callback that in turn will invoke the listener', (done) ->
                listener = ->
                    done()
                rm.subscribe listener
                queue.subscribe.getCall(0).args[1]()

            it 'and when invoked, the listener should recieve an ack functions', (done) ->
                listener = (msg, headers, info, ack)->
                    ack.acknowledge.should.be.a 'function'
                    ack.retry.should.be.a 'function'
                    done()
                rm.subscribe listener
                queue.subscribe.getCall(0).args[1]()
