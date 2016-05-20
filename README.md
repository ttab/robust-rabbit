TenaciousQ
==========

A simple mechanism for robust handling of messages on a AMQP
queue. Messages are not removed until ACKed, and if processing fails
the message is put away on a separate `retry queue` and will be
retried at a later point in time. Messages that fail repeatedly will
eventually end up in a `fail queue`.

## Usage

    TenaciousQ = require 'tenacious-q'

    amqpc = [ create an amqpc object ]
    queue = amqpc.queue 'myqueue', ...

    TenaciousQ(amqpc, queue, options).then (tq) ->
	    tq.subscribe (msg, headers, info) ->
		    ... do stuff

## Options

`TenaciousQ()` accepts the follow `options` parameter

  * `retry`
    + `delay` - the delay (in seconds, not milliseconds) to wait before
      retrying a failed message. The default is 10 seconds.
    + `max` - the maximum time to wait (again, seconds not
      milliseconds) before giving up on retrying a message. The
      default is 1 minutes.
  * `prefetchCount` - the number of messages to process in parallell. The
    default is 1.

## .subscribe([options, ] listener)

The subscribe function works pretty much like the normal
`queue.subscribe(listener)` function. For each received message, the
listener will be invoked thus:

    listener(msg, headers, info, ack)

### Acknowledning and retrying 

The `ack` object has three methods which can be called to acknowledge
or retry a message:

  * `ack.acknowledge()` - call this when you are done processing the
    message.
  * `ack.retry()` - if we've reached the maximum time limit, put the
    message on the `fail queue`, otherwise put it on the `retry queue`
    and attempt it again later.
  * `ack.fail()` - give up on the message and put it directly on the
    `fail queue`.

However, if the listener doesn't explicitly call one of these methods,
`TenaciousQ` will automatically call either `ack.retry()` (if the
listener threw an `Error` or returned a rejected promise) or
`ack.acknowledge()` (if there were no errors).








