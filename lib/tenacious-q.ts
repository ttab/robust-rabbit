/*
 * decaffeinate suggestions:
 * DS102: Remove unnecessary code created because of implicit returns
 * DS207: Consider shorter variations of null checks
 * Full docs: https://github.com/decaffeinate/decaffeinate/blob/master/docs/suggestions.md
 */

import amqp from 'amqp-as-promised';
import { Ack, TqMessageHeaders } from './ack';
import log = require('bog');

export interface TqOptions {
    retry?: {
        delay?: number
        max?: number
    },
    prefetchCount?: number
}

declare interface TqCallback<T> {
    (msg: T, header: amqp.MessageHeaders, info: amqp.DeliveryInfo, ack: Ack<T>): Promise<void>
}


class TenaciousQ<T> {
    amqpc: amqp.AmqpClient
    queue: amqp.Queue<T>
    qname: string
    exchange: Promise<amqp.Exchange>
    retryDelay: number
    maxRetries: number
    prefetchCount: number

    constructor(amqpc: amqp.AmqpClient, queue: amqp.Queue<T>, options: TqOptions = {}) {
        this.subscribe = this.subscribe.bind(this);
        this.amqpc = amqpc;
        this.queue = queue;
        this.retryDelay = ((options.retry != null ? options.retry.delay : undefined) || 10) * 1000;
        this.maxRetries = (((options.retry != null ? options.retry.max : undefined) || 60) * 1000) / this.retryDelay;
        this.prefetchCount = options.prefetchCount || 1;

        this.qname = this.queue['name'];
        const exname = `${this.qname}-flow`;
        this.exchange = this.amqpc.exchange(exname, { autoDelete: true });
        this.exchange.then(function() { return Promise.all([
            this.amqpc.queue(`${this.qname}-retries`, {
                durable: true,
                autoDelete: false,
                arguments: {
                    'x-message-ttl': this.retryDelay,
                    'x-dead-letter-exchange': '',
                    'x-dead-letter-routing-key': this.qname
                }
            }
                            ),
            this.amqpc.queue(`${this.qname}-failures`, {
                durable: true,
                autoDelete: false
            }
                            )
        ]); }.bind(this))
        // @ts-ignore
            .then(function([retries, failures]) {
                retries.bind(exname, 'retry');
                return failures.bind(exname, 'fail');}).catch(err => log.error(err));
    }

    _listen(listener, msg, headers, info, ack) {
        let ret = null;
        return Promise.resolve().then(function() {
            if (headers['tq-routing-key']) { info.routingKey = headers['tq-routing-key']; }
            return ret = listener(msg, headers, info, ack);
        }).then(function() {
                if (ret != null ? ret.then : undefined) { return ack.acknowledge(); }}).catch(err => {
                    ack.retry();
                    return log.error(`${this.qname} error`, (err.stack ? err.stack : err));
                });
    }

    subscribe(options: amqp.SubscribeOpts, listener: TqCallback<T>) {
        if (typeof options === 'function') {
            listener = options;
            options = {};
        }
        options.ack = true;
        options.prefetchCount = this.prefetchCount;
        return this.exchange.then(ex => {
            return this.queue.subscribe(options, (msg, headers, info, ack) => {
                return this._listen(listener, msg, headers, info, new Ack(ex, msg, headers as TqMessageHeaders, info, ack, this.maxRetries));
            });
        }).catch(err => log.error(err));
    }
}

export default function<T>(amqpc: amqp.AmqpClient, queue: amqp.Queue<T>, options: TqOptions) {
    const q = new TenaciousQ<T>(amqpc, queue, options);
    return q.exchange.then(() => q);
};
