import amqp from 'amqp-as-promised';
import { Ack, TqMessageHeaders } from './ack';
import { EventEmitter } from 'events'
import TypedEmitter from 'typed-emitter'

export { Ack } from './ack';

export type TqEvents<T> = {
    error: (error: Error, msg: T, headers: amqp.MessageHeaders, info: amqp.DeliveryInfo, ack: Ack<T>, qname: string) => void    
    acknowledged: (msg: T, headers: amqp.MessageHeaders, info: amqp.DeliveryInfo) => void
    retried: (msg: T, headers: amqp.MessageHeaders, info: amqp.DeliveryInfo) => void
    failed: (msg: T, headers: amqp.MessageHeaders, info: amqp.DeliveryInfo) => void
}

export interface TqOptions<T> {
    retry?: {
        delay?: number
        max?: number
    },
    prefetchCount?: number,
    errorHandler?: TqErrorHandler<T>
}

export interface TqCallback<T> {
    (msg: T, header: amqp.MessageHeaders, info: amqp.DeliveryInfo, ack: Ack<T>): Promise<void> | void
}

export type TqErrorHandler<T> = TqEvents<T>['error']

async function setup(amqpc: amqp.AmqpClient, qname: string, retryDelay: number) {
    const exname = `${qname}-flow`;
    let [retries, failures, exchange] = await Promise.all([
        amqpc.queue(`${qname}-retries`, {
            durable: true,
            autoDelete: false,
            arguments: {
                'x-message-ttl': retryDelay,
                'x-dead-letter-exchange': '',
                'x-dead-letter-routing-key': qname
            }
        }),
        amqpc.queue(`${qname}-failures`, {
            durable: true,
            autoDelete: false
        }),
        amqpc.exchange(exname, { autoDelete: true })
    ])
    await retries.bind(exname, 'retry');
    await failures.bind(exname, 'fail');
    return exchange
}

/**
 * An AMQP queue with automatic acknowledge/retry behaviour.
 */
export class TenaciousQ<T> extends (EventEmitter as { new<T>(): TypedEmitter<TqEvents<T>>})<T> {
    amqpc: amqp.AmqpClient
    queue: amqp.Queue<T>
    qname: string
    exchange: Promise<amqp.Exchange>
    retryDelay: number
    maxRetries: number
    prefetchCount: number

    constructor(amqpc: amqp.AmqpClient, queue: amqp.Queue<T>, options: TqOptions<T> = { }) {
        super()
        this.subscribe = this.subscribe.bind(this);
        this.amqpc = amqpc;
        this.queue = queue;
        this.retryDelay = ((options.retry != null ? options.retry.delay : undefined) || 10) * 1000;
        this.maxRetries = (((options.retry != null ? options.retry.max : undefined) || 60) * 1000) / this.retryDelay;
        this.prefetchCount = options.prefetchCount || 1;
        this.qname = this.queue['name'];
        this.exchange = setup(amqpc, this.qname, this.retryDelay)
        if (options.errorHandler) {
            this.on('error', options.errorHandler)
        } else {
            this.on('error', async (error, _msg, _headers, _info, ack) => {
                console.log(`${this.qname} error`, error)
                await ack.retry()
            })
        }
    }

    async _listen(listener: TqCallback<T>, msg: T, headers: TqMessageHeaders, info: amqp.DeliveryInfo, ack: Ack<T>) {
        try {
            if (headers['tq-routing-key']) {
                info.routingKey = headers['tq-routing-key'];
            }
            let ret = listener(msg, headers, info, ack);
            if (ret && ret['then']) {
                await ret
                await ack.acknowledge()
            }
        } catch (error) {
            this.emit('error', error, msg, headers, info, ack, this.qname)
        }
    }

    /**
     * Subscribe a callback function to the queue.
     *
     * @param listener - Callback function that gets called on each
     *                   message. In addition to the usual arguments
     *                   (`message`, `headers`, `deliveryInfo`), it
     *                   also recieves an `Ack` object that can be
     *                   used to `acknowledge`, `retry` or `fail` the message.
     *
     *                   If the listener returns a `Promise`, TenaciousQ
     *                   will automatically `acknowledge` or `retry`
     *                   the message depending on the outcome, if it
     *                   hasn't already been handled in the
     *                   application logic.
     *
     */
    async subscribe(listener: TqCallback<T>): Promise<void>;
    /**
     * Subscribe a callback function to the queue.
     *
     * @param options - AMQP queue subscription options.
     *
     * @param listener - Callback function that gets called on each
     *                   message. In addition to the usual arguments
     *                   (`message`, `headers`, `deliveryInfo`), it
     *                   also recieves an `Ack` object that can be
     *                   used to `acknowledge`, `retry` or `fail` the
     *                   message.
     *
     *                   If the listener returns a `Promise`, TenaciousQ
     *                   will automatically `acknowledge` or `retry`
     *                   the message depending on the outcome, if it
     *                   hasn't already been handled in the
     *                   application logic.
     *
     */
    async subscribe(options: amqp.SubscribeOpts, listener: TqCallback<T>): Promise<void>;
    async subscribe(options: amqp.SubscribeOpts | TqCallback<T>, listener?: TqCallback<T>): Promise<void> {
        if (typeof options === 'function') {
            listener = options;
            options = {};
        }
        options.ack = true;
        options.prefetchCount = this.prefetchCount;

        let ex = await this.exchange
        await this.queue.subscribe(options, (msg, headers, info, ack) => {
            this._listen(listener, msg, headers as TqMessageHeaders, info, 
                new Ack(ex, msg, headers as TqMessageHeaders, info, this, ack, this.maxRetries));
        })
    }

    /**
     * Unsubscribe the current callback function.
     */
    async unsubscribe(): Promise<void> {
        await this.queue.unsubscribe()
    }
}
