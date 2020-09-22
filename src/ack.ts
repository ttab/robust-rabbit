import amqp from 'amqp-as-promised';

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

    _msgbody(msg: any, contentType: string) {
        if (contentType === 'application/json') {
            return msg;
        } else {
            return msg.data || msg;
        }
    }

    async _unlessResolved(fn: () => Promise<number>): Promise<number> {
        if (this.resolved) return undefined
        this.resolved = true;
        let res = await fn()
        this.ack.acknowledge()
        return res
    }

    acknowledge() { return this._unlessResolved(async () => undefined); }

    retry() {
        return this._unlessResolved(async () => {
            const rc = (this.headers['tq-retry-count'] || 0) + 1;
            if (rc <= this.maxRetries) {
                await this.exchange.publish(
                    'retry',
                    this._msgbody(this.msg, this.info.contentType),
                    this._mkopts(this.headers, this.info, rc))
                return rc
            } else {
                await this.exchange.publish(
                    'fail',
                    this._msgbody(this.msg, this.info.contentType),
                    this._mkopts(this.headers, this.info, rc))
                return 0
            }
        });
    }

    async fail() {
        return this._unlessResolved(async () => {
            await this.exchange.publish(
                'fail',
                this._msgbody(this.msg, this.info.contentType),
                this._mkopts(this.headers, this.info, this.headers['tq-retry-count'] || 0))
            return 0
        });
    }
}
