Q = require 'q'
Ack = require '../lib/ack'

describe 'Ack', ->
    exchange = msg = info = headers = ack = _ack = undefined
    beforeEach ->
        exchange = { publish: stub().returns Q() }
        msg = { name: 'panda' }
        headers = {}
        info = { contentType: 'application/json' }
        _ack = { acknowledge: spy() }
        ack = new Ack exchange, msg, headers, info, _ack, 3, 1440

    describe '._mkopts()', ->

        it 'should copy relevant headers from deliveryInfo', ->
            opts = ack._mkopts {}, {
                contentType: 'text/panda',
                contentEncoding: 'us-ascii',
                myHeader: 'myValue' }
            opts.should.have.property 'contentType', 'text/panda'
            opts.should.have.property 'contentEncoding', 'us-ascii'
            opts.should.not.have.property 'myHeader'
            opts.should.not.have.property 'expiration'

        it 'should copy existing headers, and add a retryCount', ->
            opts = ack._mkopts { panda: 'cub' }, {}, 23
            opts.should.have.property 'headers'
            opts.headers.should.eql
                panda: 'cub'
                retryCount: 23

        it 'should set the retryCount even if there are no existing headers', ->
            opts = ack._mkopts undefined, {}, 23
            opts.should.have.property 'headers'
            opts.headers.should.eql
                retryCount: 23

        it 'sets expiration for failures', ->
            opts = ack._mkopts undefined, {}, 23, true
            opts.should.have.property 'expiration', '1440'

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
            Q.all [
                ack.acknowledge()
                ack.acknowledge()
            ]
            .then ->
                _ack.acknowledge.should.have.been.calledOnce

    describe '.retry()', ->

        it 'should queue the message for retry if allowed by retry count', ->
            headers.retryCount = 1
            ack.retry().then (v) ->
                exchange.publish.should.have.been.calledWith 'retry', msg, { contentType: 'application/json', headers: retryCount: 2 }
                _ack.acknowledge.should.have.been.calledOnce
                v.should.eql 2

        it 'should queue the message as a failure if we have reached max number of retries', ->
            spy ack, '_mkopts'
            headers.retryCount = 3
            ack.retry().then (v) ->
                exchange.publish.should.have.been.calledWith 'fail', msg, { contentType: 'application/json', expiration: "1440", headers: retryCount: 4 }
                _ack.acknowledge.should.have.been.calledOnce
                v.should.eql 0

        it 'should not retry if acknowledge() has already been called', ->
            ack.acknowledge().then ->
                ack.retry().then (v) ->
                    _ack.acknowledge.should.have.been.calledOnce
                    exchange.publish.should.not.have.been.called
                    expect(v).to.be.undefined

        it 'should not fail if acknowledge() has already been called', ->
            headers.retryCount = 3
            ack.acknowledge().then ->
                ack.retry().then (v) ->
                    exchange.publish.should.not.have.been.called
                    expect(v).to.be.undefined

        it 'should not be possible to retry many times in a row', ->
            Q.all [
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
            spy ack, '_mkopts'
            ack.fail().then (v) ->
                exchange.publish.should.have.been.calledWith 'fail', msg, { contentType: 'application/json', expiration: "1440", headers: { retryCount: 0 } }
                _ack.acknowledge.should.have.been.calledOnce
                v.should.eql 0

        it 'should do nothing if acknowledge() has already been called', ->
            ack.acknowledge().then ->
                ack.fail().then (v) ->
                    _ack.acknowledge.should.have.been.calledOnce
                    exchange.publish.should.not.have.been.called
                    expect(v).to.be.undefined

        it 'should not be possible to fail many times in a row', ->
            Q.all [
                ack.fail()
                ack.fail()
                ack.fail()
                ack.fail()
                ack.fail()
            ]
            .then ->
                _ack.acknowledge.should.have.been.calledOnce
                exchange.publish.should.have.been.calledOnce
