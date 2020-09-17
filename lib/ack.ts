/*
 * decaffeinate suggestions:
 * DS102: Remove unnecessary code created because of implicit returns
 * Full docs: https://github.com/decaffeinate/decaffeinate/blob/master/docs/suggestions.md
 */

import amqp from 'amqp-as-promised';
import log = require('bog');

export interface TqMessageHeaders extends amqp.MessageHeaders {
    'tq-retry-count': number
    'tq-routing-key': string
}

export class Ack<T> {
    exchange: amqp.Exchange
    msg: T
    headers: TqMessageHeaders
    info: amqp.DeliveryInfo
    ack: amqp.Ack
    maxRetries: number
    resolved: boolean

    constructor(exchange: amqp.Exchange, msg: T, headers: TqMessageHeaders, info: amqp.DeliveryInfo, ack: amqp.Ack, maxRetries=3) {
        this._unlessResolved = this._unlessResolved.bind(this);
        this.acknowledge = this.acknowledge.bind(this);
        this.retry = this.retry.bind(this);
        this.fail = this.fail.bind(this);
        this.exchange = exchange;
        this.msg = msg;
        this.headers = headers;
        this.info = info;
        this.ack = ack;
        this.maxRetries = maxRetries;
        this.resolved = false;
    }

    _mkopts(headers: TqMessageHeaders, info: amqp.DeliveryInfo, retryCount: number) {
        const opts: any = {};
        for (let key in info) { const val = info[key]; if (['contentType', 'contentEncoding', 'deliveryMode'].includes(key)) { opts[key] = val; } }
        opts.headers = headers || {};
        opts.headers['tq-retry-count'] = retryCount;
        if (info.routingKey) { opts.headers['tq-routing-key'] = info.routingKey; }
        return opts;
    }

    _msgbody(msg, contentType) {
        if (contentType === 'application/json') {
            return msg;
        } else {
            return msg.data || msg;
        }
    }

    _unlessResolved(fn) {
        if (this.resolved) {
            return Promise.resolve();
        } else {
            this.resolved = true;
            return Promise.resolve().then(() => fn()).then(res => {
                this.ack.acknowledge();
                return res;
            }).catch(err => log.error(err.stack));
        }
    }

    acknowledge(): Promise<void> { return this._unlessResolved(function() {}); }

    retry(): Promise<number> {
        return this._unlessResolved(() => {
            const rc = (this.headers['tq-retry-count'] || 0) + 1;
            if (rc <= this.maxRetries) {
                return this.exchange.publish('retry', this._msgbody(this.msg, this.info.contentType), this._mkopts(this.headers, this.info, rc))
                .then(() => rc);
            } else {
                return this.exchange.publish('fail', this._msgbody(this.msg, this.info.contentType), this._mkopts(this.headers, this.info, rc))
                .then(() => 0);
            }
        });
    }

    fail(): Promise<number> {
        return this._unlessResolved(() => {
            return this.exchange.publish('fail', this._msgbody(this.msg, this.info.contentType), this._mkopts(this.headers, this.info, this.headers['tq-retry-count'] || 0))
            .then(() => 0);
        });
    }
}
