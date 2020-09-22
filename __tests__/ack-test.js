/*
 * decaffeinate suggestions:
 * DS102: Remove unnecessary code created because of implicit returns
 * Full docs: https://github.com/decaffeinate/decaffeinate/blob/master/docs/suggestions.md
 */
const { Ack } = require('../src/ack');

describe('Ack', function() {
    let _ack, ack, headers, info, msg;
    let exchange = (msg = (info = (headers = (ack = (_ack = undefined)))));

    beforeEach(function() {
        exchange = { publish: stub().returns(Promise.resolve()) };
        msg = { name: 'panda' };
        headers = {};
        info = { contentType: 'application/json' };
        _ack = { acknowledge: spy() };
        ack = new Ack(exchange, msg, headers, info, _ack);
    });

    describe('._mkopts()', function() {

        it('should copy relevant headers from deliveryInfo', function() {
            const opts = ack._mkopts({}, {
                deliveryMode    : 2,
                contentType     : 'text/panda',
                contentEncoding : 'us-ascii',
                myHeader        : 'myValue' });
            opts.should.have.property('deliveryMode', 2);
            opts.should.have.property('contentType', 'text/panda');
            opts.should.have.property('contentEncoding', 'us-ascii');
            opts.should.not.have.property('myHeader');
        });

        it('should copy existing headers, and add a retryCount', function() {
            const opts = ack._mkopts({ panda: 'cub' }, {}, 23);
            opts.should.have.property('headers');
            opts.headers.should.eql({
                panda: 'cub',
                'tq-retry-count': 23
            });
        });

        it('should copy the original routingKey', function() {
            const opts = ack._mkopts({}, {
                routingKey: 'panda'
            });
            opts.should.have.property('headers');
            opts.headers.should.have.property('tq-routing-key', 'panda');
        });

        it('should set the retryCount and routingKey even if there are no existing headers', function() {
            const opts = ack._mkopts(undefined, { routingKey: 'panda' }, 23);
            opts.should.have.property('headers');
            opts.headers.should.eql({
                'tq-routing-key' : 'panda',
                'tq-retry-count' : 23
            });
        });
    });

    describe('._msgbody()', function() {

        it('should return the msg if this it a plain javascript object', function() {
            msg = { name: 'panda' };
            ack._msgbody(msg, 'application/json').should.eql(msg);
        });

        it('should return the data part if it exists and is a Buffer', function() {
            const body = Buffer.from('panda');
            msg = {
                data: body,
                contentType: 'application/octet-stream'
            };
            expect(ack._msgbody(msg, 'application/octet-stream')).to.eql(body);
        });
    });

    describe('.acknowledge()', () => {
        it('should call the underlying acknowledge() fn at most once', () => Promise.all([
            ack.acknowledge(),
            ack.acknowledge()
        ]).then(() => _ack.acknowledge.should.have.been.calledOnce))
    });

    describe('.retry()', function() {

        it('should queue the message for retry if allowed by retry count', function() {
            headers['tq-retry-count'] = 1;
            return ack.retry().then(function(v) {
                exchange.publish.should.have.been.calledWith('retry', msg, match({ contentType: 'application/json', headers: {'tq-retry-count': 2} }));
                _ack.acknowledge.should.have.been.calledOnce;
                return v.should.eql(2);
            });
        });

        it('should queue the message as a failure if we have reached max number of retries', function() {
            headers['tq-retry-count'] = 3;
            return ack.retry().then(function(v) {
                exchange.publish.should.have.been.calledWith('fail', msg, match({ contentType: 'application/json', headers: {'tq-retry-count': 4} }));
                _ack.acknowledge.should.have.been.calledOnce;
                return v.should.eql(0);
            });
        });

        it('should not retry if acknowledge() has already been called', () => ack.acknowledge().then(() => ack.retry().then(function(v) {
            _ack.acknowledge.should.have.been.calledOnce;
            exchange.publish.should.not.have.been.called;
            return expect(v).to.be.undefined;
        })));

        it('should not fail if acknowledge() has already been called', function() {
            headers['tq-retry-count'] = 3;
            return ack.acknowledge().then(() => ack.retry().then(function(v) {
                exchange.publish.should.not.have.been.called;
                return expect(v).to.be.undefined;
            }));
        });

        it('should not be possible to retry many times in a row', () => Promise.all([
            ack.retry(),
            ack.retry(),
            ack.retry(),
            ack.retry(),
            ack.retry()
        ])
        .then(function() {
            _ack.acknowledge.should.have.been.calledOnce;
            return exchange.publish.should.have.been.calledOnce;
        }));
    });

    describe('.fail()', function() {

        it('should queue the message as a failure', () => ack.fail().then(function(v) {
            exchange.publish.should.have.been.calledWith('fail', msg, match({ contentType: 'application/json', headers: { 'tq-retry-count': 0 } }));
            _ack.acknowledge.should.have.been.calledOnce;
            return v.should.eql(0);
        }));

        it('should do nothing if acknowledge() has already been called', () => ack.acknowledge().then(() => ack.fail().then(function(v) {
            _ack.acknowledge.should.have.been.calledOnce;
            exchange.publish.should.not.have.been.called;
            return expect(v).to.be.undefined;
        })));

        it('should not be possible to fail many times in a row', () => Promise.all([
            ack.fail(),
            ack.fail(),
            ack.fail(),
            ack.fail(),
            ack.fail()
        ])
        .then(function() {
            _ack.acknowledge.should.have.been.calledOnce;
            return exchange.publish.should.have.been.calledOnce;
        }));
    });
});
