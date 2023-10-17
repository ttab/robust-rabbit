import EventEmitter from "events";
import { stub, spy, match } from "sinon";
import TypedEventEmitter from "typed-emitter";
import { expect } from 'chai'

import { Ack } from "../src/ack";
import { TqEvents } from "../src/tenacious-q";

describe("Ack", function() {
  let _ack, ack, headers, info, msg, exchange
  let events: TypedEventEmitter<TqEvents<unknown>>;

  beforeEach(function() {
    exchange = { publish: stub().returns(Promise.resolve()) };
    msg = { name: "panda" };
    headers = {};
    info = { contentType: "application/json" };
    _ack = { acknowledge: spy() };
    events = new EventEmitter() as TypedEventEmitter<TqEvents<unknown>>;
    ack = new Ack(exchange, msg, headers, info, events, _ack);
  });

  describe("._mkopts()", () => {
    it("should copy relevant headers from deliveryInfo", () => {
      const opts = ack._mkopts(
        {},
        {
          deliveryMode: 2,
          contentType: "text/panda",
          contentEncoding: "us-ascii",
          myHeader: "myValue",
        }
      );
      opts.should.have.property("deliveryMode", 2);
      opts.should.have.property("contentType", "text/panda");
      opts.should.have.property("contentEncoding", "us-ascii");
      opts.should.not.have.property("myHeader");
    });

    it("should copy existing headers, and add a retryCount", () => {
      const opts = ack._mkopts({ panda: "cub" }, {}, 23);
      opts.should.have.property("headers");
      opts.headers.should.eql({
        panda: "cub",
        "tq-retry-count": 23,
      });
    });

    it("should copy the original routingKey", () => {
      const opts = ack._mkopts(
        {},
        {
          routingKey: "panda",
        }
      );
      opts.should.have.property("headers");
      opts.headers.should.have.property("tq-routing-key", "panda");
    });

    it("should set the retryCount and routingKey even if there are no existing headers", () => {
      const opts = ack._mkopts(undefined, { routingKey: "panda" }, 23);
      opts.should.have.property("headers");
      opts.headers.should.eql({
        "tq-routing-key": "panda",
        "tq-retry-count": 23,
      });
    });
  });

  describe("._msgbody()", () => {

    it("should return the msg if this it a plain javascript object", () => {
      msg = { name: "panda" };
      ack._msgbody(msg, "application/json").should.eql(msg);
    });

    it("should return the data part if it exists and is a Buffer", () => {
      const body = Buffer.from("panda");
      msg = {
        data: body,
        contentType: "application/octet-stream",
      };
      expect(ack._msgbody(msg, "application/octet-stream")).to.eql(body);
    });
  });

  describe(".acknowledge()", () => {

    it("should call the underlying acknowledge() fn at most once", async () => {
      await Promise.all([ack.acknowledge(), ack.acknowledge()]);
      _ack.acknowledge.should.have.been.calledOnce;
    });

    it('should emit a acknowledged event', (done) => {
      events.on('acknowledged', () => done())
      ack.acknowledge()
    })

  });

  describe(".retry()", () => {

    it("should queue the message for retry if allowed by retry count", async () => {
      headers["tq-retry-count"] = 1;
      const v = await ack.retry()
      exchange.publish.should.have.been.calledWith(
        "retry",
        msg,
        match({
          contentType: "application/json",
          headers: { "tq-retry-count": 2 },
        })
      );
      _ack.acknowledge.should.have.been.calledOnce;
      v.should.eql(2);
    });

    it("should queue the message as a failure if we have reached max number of retries", async () => {
      headers["tq-retry-count"] = 3;
      const v = await ack.retry()
      exchange.publish.should.have.been.calledWith(
        "fail",
        msg,
        match({
          contentType: "application/json",
          headers: { "tq-retry-count": 4 },
        })
      );
      _ack.acknowledge.should.have.been.calledOnce;
      v.should.eql(0);
    });

    it("should not retry if acknowledge() has already been called", async () => {
      await ack.acknowledge()
      const v = await ack.retry()
      _ack.acknowledge.should.have.been.calledOnce;
      exchange.publish.should.not.have.been.called;
      expect(v).to.be.undefined;
    });

    it("should not fail if acknowledge() has already been called", async () => {
      headers["tq-retry-count"] = 3;
      await ack.acknowledge()
      const v = await ack.retry()
      exchange.publish.should.not.have.been.called;
      return expect(v).to.be.undefined;
    });

    it("should not be possible to retry many times in a row", async () => {
      await Promise.all([
        ack.retry(),
        ack.retry(),
        ack.retry(),
        ack.retry(),
        ack.retry(),
      ]);
      _ack.acknowledge.should.have.been.calledOnce;
      exchange.publish.should.have.been.calledOnce;
    });

    it('should emit a retried event', (done) => {
      events.on('retried', () => done());
      ack.retry();
    });
  });

  describe(".fail()", () => {
    it("should queue the message as a failure", async () => {
      const v = await ack.fail()
      exchange.publish.should.have.been.calledWith(
        "fail",
        msg,
        match({
          contentType: "application/json",
          headers: { "tq-retry-count": 0 },
        })
      );
      _ack.acknowledge.should.have.been.calledOnce;
      v.should.eql(0);
    });

    it("should do nothing if acknowledge() has already been called", () => ack.acknowledge().then(() => ack.fail().then(function(v) {
      _ack.acknowledge.should.have.been.calledOnce;
      exchange.publish.should.not.have.been.called;
      expect(v).to.be.undefined;
    })
    ));

    it("should not be possible to fail many times in a row", async () => {
      await Promise.all([
        ack.fail(),
        ack.fail(),
        ack.fail(),
        ack.fail(),
        ack.fail(),
      ]);
      _ack.acknowledge.should.have.been.calledOnce;
      exchange.publish.should.have.been.calledOnce;
    });

    it('should emit a retried event', (done) => {
      events.on('retried', () => done());
      ack.retry();
    });
  });
});
