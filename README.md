kinesis2sse
===========

The kinesis2sse package subscribes to a Kinesis Stream using
[vmware-go-kcl-v2][kcl], loads events into [memlog][memlog], and then exposes
them as [server-sent events (SSE)][sse] to clients.

Usage
-----

Assuming you've already set up a Kinesis Stream "test-server-events" in
us-east-2:

```sh
go build
./sse \
  --routes '[{"path":"/","stream":"test-server-events"}]' \
  --region us-east-2
```

```
$ curl 0.0.0.0:4444
:ok

data: {"hello":"world"}
```

If you want to resume streaming from a particular timestamp, you can pass this
using the `since` query parameter. This behavior is inspired by
[Wikimedia's EventStreams][wikimedia].

```sh
curl 0.0.0.0:4444?since=1970-01-01T00%3A00%3A00.000Z
```

Note that whether or not you actually receive historical records is completely
dependant on what we have in memory.

Background
----------

The general idea is that, if there are not "too many" records in Kinesis, we can
retain up to N events no older than M age in memory, and let clients use
Last-Event-ID header to control from where they resume consumption.

This is a simpler, cheaper system to operate than Kafka, which may still enable
similar use cases. For example,

- on-call engineers can use this to gain insight into what's currently
  happening in the cluster.
- customers can see realtime events for their account.

[kcl]: https://github.com/vmware/vmware-go-kcl-v2
[memlog]: https://github.com/embano1/memlog
[sse]: https://en.wikipedia.org/wiki/Server-sent_events
[wikimedia]: https://wikitech.wikimedia.org/wiki/Event_Platform/EventStreams
