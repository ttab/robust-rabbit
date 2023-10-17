import { TenaciousQ, TqCallback } from "../src/tenacious-q";
import { stub, spy, match, SinonStub } from "sinon";
import { expect } from "chai";
import { TqMessageHeaders } from "../src/ack";
import amqp from "amqp-as-promised";

describe("TenaciousQ", function() {
  let amqpc, exchange, queue;
  let tq: TenaciousQ<unknown>;

  beforeEach(function() {
    exchange = { publish: stub().returns(Promise.resolve()) };

    amqpc = {
      queue: stub().returns(Promise.resolve({ bind() { } })),
      exchange: stub().returns(Promise.resolve(exchange)),
    };

    queue = {
      name: "test",
      subscribe: spy(),
    };

    tq = new TenaciousQ(amqpc, queue);
  });

  describe(".constructor()", () => {
    it("should parse the options object", () => {
      tq = new TenaciousQ(amqpc, queue, {
        retry: { delay: 15, max: 60 },
        prefetchCount: 7,
      });
      expect(tq.retryDelay).to.eql(15000);
      expect(tq.maxRetries).to.eql(4);
      expect(tq.prefetchCount).to.eql(7);
    });
  });

  describe("._listen()", function() {
    let ack, headers, info, msg;
    let listener: TqCallback<unknown> & SinonStub;

    beforeEach(function() {
      listener = stub().returns(Promise.resolve());
      msg = {};
      headers = {};
      info = {};
      ack = {
        acknowledge: spy(),
        retry: spy(),
        fail: spy()
      };
    });

    it("should invoke the listener", () =>
      tq
        ._listen(
          listener,
          { my: "message" },
          { my: "headers" } as unknown as TqMessageHeaders,
          { my: "info" } as unknown as amqp.DeliveryInfo,
          ack
        )
        .then(() =>
          listener.should.have.been.calledWith(
            { my: "message" },
            { my: "headers" },
            { my: "info" },
            ack
          )
        ));

    it("should restore the original routing key if this is a retry", () =>
      tq
        ._listen(
          listener,
          { my: "message" },
          {
            my: "headers",
            "tq-routing-key": "panda",
          } as unknown as TqMessageHeaders,
          { my: "info" } as unknown as amqp.DeliveryInfo,
          ack
        )
        .then(() =>
          listener.should.have.been.calledWith(
            { my: "message" },
            { my: "headers", "tq-routing-key": "panda" },
            { my: "info", routingKey: "panda" },
            ack
          )
        ));

    it("should call ack.acknowledge() on success", () =>
      tq._listen(listener, msg, headers, info, ack).then(function() {
        ack.acknowledge.should.have.been.calledOnce;
        ack.retry.should.not.have.been.called;
        ack.fail.should.not.have.been.called;
      }));

    it("should call ack.retry() on failure", (done) => {
      listener.throws(new Error("such fail!"));
      tq.on('error', () => {
        try {
          ack.retry.should.have.been.calledOnce;
          ack.acknowledge.should.not.have.been.called;
          ack.fail.should.not.have.been.called;
          done()
        } catch (err) {
          done(err)
        }
      })
      tq._listen(listener, msg, headers, info, ack)
    });

    describe("when listener doesnt return a promise", () => {

      it("should damn well NOT ack.acknowledge", async () => {
        listener.returns(undefined);
        await tq._listen(listener, msg, headers, info, ack);
        ack.acknowledge.should.not.have.been.called;
        ack.retry.should.not.have.been.called;
        ack.fail.should.not.have.been.called;
      });

      it("should however retry on fail", async () => {
        listener.throws(new Error("Bad bad"));
        await tq._listen(listener, msg, headers, info, ack);
        ack.retry.should.have.been.called; //!
        ack.acknowledge.should.not.have.been.calledOnce;
        ack.fail.should.not.have.been.called;
      });
    });
  });

  describe(".subscribe()", () => {
    it("should use the default exchange", () => {
      amqpc.exchange.should.have.been.calledWith("test-flow", {
        autoDelete: true,
      });
    });

    it("should set up a retry queue", () => tq.exchange.then(function() {
      return amqpc.queue.should.have.been.calledWith("test-retries", {
        durable: true,
        autoDelete: false,
        arguments: {
          "x-message-ttl": 10000,
          "x-dead-letter-exchange": "",
          "x-dead-letter-routing-key": "test",
        },
      });
    }));

    it("should set up a failures queue", () => tq.exchange.then(() => amqpc.queue.should.have.been.calledWith("test-failures", {
      durable: true,
      autoDelete: false,
    })
    ));

    it("should use default options if none are given", () => tq
      .subscribe(function() { })
      .then(() => queue.subscribe.should.have.been.calledWith(
        { ack: true, prefetchCount: 1 },
        match.func
      )
      ));

    it("should use specified options, but set ack and prefetchCount", async () => {
      await tq.subscribe(({ panda: "cub" } as amqp.SubscribeOpts), function() { });
      queue.subscribe.should.have.been.calledWith(
        { panda: "cub", ack: true, prefetchCount: 1 },
        match.func
      );
    });

    describe("should call subscribe on the underlying queue", () => {
      beforeEach(() => {
        queue.subscribe = spy();
        tq = new TenaciousQ(amqpc, queue);
      });

      it("with a callback that in turn will invoke the listener", async (done) => {
        const listener = () => done();
        await tq.subscribe(listener);
        queue.subscribe.getCall(0).args[1]({}, {}, {}, { acknowledge() { } });
      });

      it("and when invoked, the listener should recieve an ack object", (done) => {
        const listener = (msg, headers, info, ack) => {
          try {
            ack.exchange.should.equal(exchange);
            // ack.should.be.instanceOf(Ack);
            ack.msg.should.equal("msg");
            ack.headers.should.equal("headers");
            ack.info.should.equal("info");
            ack.ack.should.have.property("acknowledge");
            done();
          } catch (err) {
            done(err);
          }
        };
        tq.subscribe(listener).then(() => queue.subscribe
          .getCall(0)
          .args[1]("msg", "headers", "info", { acknowledge() { } })
        );
      });
    });
  });
});
