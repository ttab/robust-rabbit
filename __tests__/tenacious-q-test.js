/*
 * decaffeinate suggestions:
 * DS102: Remove unnecessary code created because of implicit returns
 * Full docs: https://github.com/decaffeinate/decaffeinate/blob/master/docs/suggestions.md
 */
const TenaciousQ = require('../lib/tenacious-q').default;
const Ack        = require('../lib/ack');

describe('TenaciousQ', function() {
    let amqpc, exchange, queue;
    let tq = (queue = (exchange = (amqpc = undefined)));

    beforeEach(function() {
        exchange = {publish: stub().returns(Promise.resolve())};

        amqpc = {
            queue: stub().returns(Promise.resolve({ bind() {} })),
            exchange: stub().returns(Promise.resolve(exchange))
        };

        queue = {
            name: 'test',
            subscribe: spy()
        };

        return TenaciousQ(amqpc, queue).then(_tq => tq = _tq);
    });

    describe('.constructor()', () => {
        it('should parse the options object', () =>
            TenaciousQ(amqpc, queue, { retry: { delay: 15, max: 60 }, prefetchCount: 7 }).then(function(tq) {
                tq.retryDelay.should.equal(15000);
                tq.maxRetries.should.equal(4);
                tq.prefetchCount.should.equal(7);
    }))});

    describe('._listen()', function() {
        let ack, headers, info, msg;
        let listener = (msg = (headers = (info = (ack = undefined))));
        beforeEach(function() {
            listener = stub().returns(Promise.resolve());
            msg = {};
            headers = {};
            info = {};
            return ack = { acknowledge: spy(), retry: spy(), fail: spy() };});

        it('should invoke the listener', () => tq._listen(listener, {my:'message'}, {my:'headers'}, {my:'info'}, ack)
        .then(() => listener.should.have.been.calledWith(
            {my:'message'},
            {my:'headers'},
            {my:'info'},
            ack)));

        it('should restore the original routing key if this is a retry', () => tq._listen(listener, {my:'message'}, {my:'headers', 'tq-routing-key': 'panda'}, {my:'info'}, ack)
        .then(() => listener.should.have.been.calledWith(
            {my:'message'},
            {my:'headers', 'tq-routing-key': 'panda'},
            {my:'info', routingKey: 'panda'},
            ack)));

        it('should call ack.acknowledge() on success', () => tq._listen(listener, msg, headers, info, ack)
        .then(function() {
            ack.acknowledge.should.have.been.calledOnce;
            ack.retry.should.not.have.been.called;
            return ack.fail.should.not.have.been.called;
        }));

        it('should call ack.retry() on failure', function() {
            listener.throws(new Error('such fail!'));
            return tq._listen(listener, msg, headers, info, ack)
            .then(function() {
                ack.retry.should.have.been.calledOnce;
                ack.acknowledge.should.not.have.been.called;
                return ack.fail.should.not.have.been.called;
            });
        });

        describe('when listener doesnt return a promise', function() {

            it('should damn well NOT ack.acknowledge', function() {
                listener = () => undefined;
                return tq._listen(listener, msg, headers, info, ack)
                .then(function() {
                    ack.acknowledge.should.not.have.been.called;
                    ack.retry.should.not.have.been.called;
                    return ack.fail.should.not.have.been.called;
                });
            });

            it('should however retry on fail', function() {
                listener = function() { throw new Error('Bad bad'); };
                return tq._listen(listener, msg, headers, info, ack)
                .then(function() {
                    ack.retry.should.have.been.called; //!
                    ack.acknowledge.should.not.have.been.calledOnce;
                    return ack.fail.should.not.have.been.called;
                });
            });
        });
    });

    describe('.subscribe()', function() {

        it('should use the default exchange', () => {
            amqpc.exchange.should.have.been.calledWith('test-flow', { autoDelete: true })
        });

        it('should set up a retry queue', () => tq.exchange.then(function() {
            return amqpc.queue.should.have.been.calledWith('test-retries', {
                durable: true,
                autoDelete: false,
                arguments: {
                    'x-message-ttl': 10000,
                    'x-dead-letter-exchange': '',
                    'x-dead-letter-routing-key': 'test'
                }
            }
            );
        }));

        it('should set up a failures queue', () => tq.exchange.then(() => amqpc.queue.should.have.been.calledWith('test-failures', {
            durable: true,
            autoDelete: false
        }
        )));

        it('should use default options if none are given', () => tq.subscribe(function() {}).then(() => queue.subscribe.should.have.been.calledWith({ ack: true, prefetchCount: 1}, match.func)));

        it('should use specified options, but set ack and prefetchCount', () => tq.subscribe({ panda: 'cub' }, function() {}).then(() => queue.subscribe.should.have.been.calledWith({ panda: 'cub', ack: true, prefetchCount: 1}, match.func)));

        describe('should call subscribe on the underlying queue', function() {
            beforeEach(function() {
                queue.subscribe = spy();
                return TenaciousQ(amqpc, queue).then(_tq => tq = _tq);
            });

            it('with a callback that in turn will invoke the listener', function(done) {
                const listener = () => done();
                return tq.subscribe(listener).then(() => queue.subscribe.getCall(0).args[1]({}, {}, {}, { acknowledge() {} }));
            });

            it('and when invoked, the listener should recieve an ack object', function(done) {
                const listener = function(msg, headers, info, ack) {
                    try {
                        ack.exchange.should.equal(exchange);
                        // ack.should.be.instanceOf(Ack);
                        ack.msg.should.equal('msg');
                        ack.headers.should.equal('headers');
                        ack.info.should.equal('info');
                        ack.ack.should.have.property('acknowledge');
                        done();
                    } catch (err) {
                        done(err)
                    }
                };
                tq.subscribe(listener).then(() =>
                    queue.subscribe.getCall(0).args[1]('msg', 'headers', 'info', { acknowledge() {} })
                );
            });
        });
    });
});
