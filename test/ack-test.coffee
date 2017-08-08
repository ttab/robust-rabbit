Ack = require '../lib/ack'

describe 'Ack', ->
    exchange = msg = info = headers = ack = _ack = undefined
    beforeEach ->
        exchange = { publish: stub().returns Promise.resolve() }
        msg = { name: 'panda' }
        headers = {}
        info = { contentType: 'application/json' }
        _ack = { acknowledge: spy() }
        ack = new Ack exchange, msg, headers, info, _ack

    describe '._mkopts()', ->

        it 'should copy relevant headers from deliveryInfo', ->
            opts = ack._mkopts {}, {
                deliveryMode    : 2
                contentType     : 'text/panda',
                contentEncoding : 'us-ascii',
                myHeader        : 'myValue' }
            opts.should.have.property 'deliveryMode', 2
            opts.should.have.property 'contentType', 'text/panda'
            opts.should.have.property 'contentEncoding', 'us-ascii'
            opts.should.not.have.property 'myHeader'

        it 'should copy existing headers, and add a retryCount', ->
            opts = ack._mkopts { panda: 'cub' }, {}, 23
            opts.should.have.property 'headers'
            opts.headers.should.eql
                panda: 'cub'
                'tq-retry-count': 23

        it 'should copy the original routingKey', ->
            opts = ack._mkopts {}, {
                routingKey: 'panda'
            }
            opts.should.have.property 'headers'
            opts.headers.should.have.property 'tq-routing-key', 'panda'

        it 'should set the retryCount and routingKey even if there are no existing headers', ->
            opts = ack._mkopts undefined, { routingKey: 'panda' }, 23
            opts.should.have.property 'headers'
            opts.headers.should.eql
                'tq-routing-key' : 'panda'
                'tq-retry-count' : 23

    describe '._msgbody()', ->

        it 'should return the msg if this it a plain javascript object', ->
            msg = { name: 'panda' }
            ack._msgbody(msg, 'application/json').should.eql msg

        it 'should return the data part if it exists and is a Buffer', ->
            body = new Buffer('panda')
            msg =
                data: body
                contentType: 'application/octet-stream'
            ack._msgbody(msg, 'application/octet-stream').should.eql body

    describe '.acknowledge()', ->

        it 'should call the underlying acknowledge() fn at most once', ->
            Promise.all [
                ack.acknowledge()
                ack.acknowledge()
            ]
            .then ->
                _ack.acknowledge.should.have.been.calledOnce

    describe '.retry()', ->

        it 'should queue the message for retry if allowed by retry count', ->
            headers['tq-retry-count'] = 1
            ack.retry().then (v) ->
                exchange.publish.should.have.been.calledWith 'retry', msg, match { contentType: 'application/json', headers: 'tq-retry-count': 2 }
                _ack.acknowledge.should.have.been.calledOnce
                v.should.eql 2

        it 'should queue the message as a failure if we have reached max number of retries', ->
            headers['tq-retry-count'] = 3
            ack.retry().then (v) ->
                exchange.publish.should.have.been.calledWith 'fail', msg, match { contentType: 'application/json', headers: 'tq-retry-count': 4 }
                _ack.acknowledge.should.have.been.calledOnce
                v.should.eql 0

        it 'should not retry if acknowledge() has already been called', ->
            ack.acknowledge().then ->
                ack.retry().then (v) ->
                    _ack.acknowledge.should.have.been.calledOnce
                    exchange.publish.should.not.have.been.called
                    expect(v).to.be.undefined

        it 'should not fail if acknowledge() has already been called', ->
            headers['tq-retry-count'] = 3
            ack.acknowledge().then ->
                ack.retry().then (v) ->
                    exchange.publish.should.not.have.been.called
                    expect(v).to.be.undefined

        it 'should not be possible to retry many times in a row', ->
            Promise.all [
                ack.retry()
                ack.retry()
                ack.retry()
                ack.retry()
                ack.retry()
            ]
            .then ->
                _ack.acknowledge.should.have.been.calledOnce
                exchange.publish.should.have.been.calledOnce

    describe '.fail()', ->

        it 'should queue the message as a failure', ->
            ack.fail().then (v) ->
                exchange.publish.should.have.been.calledWith 'fail', msg, match { contentType: 'application/json', headers: { 'tq-retry-count': 0 } }
                _ack.acknowledge.should.have.been.calledOnce
                v.should.eql 0

        it 'should do nothing if acknowledge() has already been called', ->
            ack.acknowledge().then ->
                ack.fail().then (v) ->
                    _ack.acknowledge.should.have.been.calledOnce
                    exchange.publish.should.not.have.been.called
                    expect(v).to.be.undefined

        it 'should not be possible to fail many times in a row', ->
            Promise.all [
                ack.fail()
                ack.fail()
                ack.fail()
                ack.fail()
                ack.fail()
            ]
            .then ->
                _ack.acknowledge.should.have.been.calledOnce
                exchange.publish.should.have.been.calledOnce
