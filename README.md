<img class="logo" alt="NCHAN" src="https://nchan.slact.net/github-logo.png" />

https://nchan.slact.net

Nchan is a scalable, flexible pub/sub server for the modern web, built as a module for the [Nginx](http://nginx.org) web server. It can be configured as a standalone server, or as a shim between your application and hundreds, thousands, or millions of live subscribers. It can buffer messages in memory, on-disk, or via [Redis](http://redis.io). All connections are handled asynchronously and distributed among any number of worker processes. It can also scale to many Nginx servers with [Redis](http://redis.io).

Messages are [published](#publisher-endpoints) to channels with HTTP `POST` requests or Websocket, and [subscribed](#subscriber-endpoint) also through [Websocket](#websocket), [long-polling](#long-polling), [EventSource](#eventsource) (SSE), old-fashioned [interval polling](#interval-polling), [and](#http-chunked-transfer) [more](#http-multipart-mixed).

In a web browser, you can use Websocket or EventSource directly, or the [NchanSubscriber.js](https://github.com/slact/nchan/blob/master/NchanSubscriber.js) wrapper library. It supports Long-Polling, EventSource, and resumable Websockets, and has a few other added convenience options.

## Features
 - RESTful, HTTP-native API.
 - Supports [Websocket](https://nchan.slact.net/#websocket), [EventSource (Server-Sent Events)](https://nchan.slact.net/#eventsource), [Long-Polling](https://nchan.slact.net/#long-polling) and other HTTP-based subscribers.
 - No-repeat, no-loss message delivery guarantees with per-channel configurable message buffers.
 - Subscribe to [hundreds of channels](#channel-multiplexing) over a single subscriber connection.
 - HTTP request [callbacks and hooks](https://nchan.slact.net/details#application-callbacks) for easy integration.
 - Introspection with [channel events](https://nchan.slact.net/details#channel-events) and [url for monitoring performance statistics](https://nchan.slact.net/details#nchan_stub_status).
 - Fast ephemeral local message storage and optional, slower, persistent storage with [Redis](https://nchan.slact.net/details#connecting-to-a-redis-server).
 - Horizontally scalable (using [Redis](https://nchan.slact.net/details#connecting-to-a-redis-server)).
 - Highly Available with no single point of failure (using [Redis Cluster](https://nchan.slact.net/details#redis-cluster)).
 
<!-- toc -->

## Status and History

The latest Nchan release is v1.0.8 (November 28, 2016) ([changelog](https://nchan.slact.net/changelog)).

The first iteration of Nchan was written in 2009-2010 as the [Nginx HTTP Push Module](https://pushmodule.slact.net), and was vastly refactored into its present state in 2014-2016. The present release is in the **testing** phase. The core features and old functionality are thoroughly tested and stable. Some of the new functionality, especially Redis Cluster may be a bit buggy.

#### Upgrade from Nginx HTTP Push Module

Although Nchan is backwards-compatible with all Push Module configuration directives, some of the more unusual and rarely used settings have been disabled and will be ignored (with a warning). See the [upgrade page](https://nchan.slact.net/upgrade) for a detailed list of changes and improvements, as well as a full list of incompatibilities.


## Does it scale?

<img class="benchmark_graph" alt="benchmarking internal subscriber response times" src="https://nchan.slact.net/img/benchmark_internal_total.png" />

Yes it does. Like Nginx, Nchan can easily handle as much traffic as you can throw at it. I've tried to benchmark it, but my benchmarking tools are much slower than Nchan. The data I've gathered is on how long Nchan itself takes to respond to every subscriber after publishing a message -- this excludes TCP handshake times and internal HTTP request parsing. Basically, it measures how Nchan scales assuming all other components are already tuned for scalability. The graphed data are averages of 5 runs with 50-byte messages.

With a well-tuned OS and network stack on commodity server hardware, expect to handle upwards of 300K concurrent subscribers per second at minimal CPU load. Nchan can also be scaled out to multiple Nginx instances using the [Redis storage engine](#nchan_use_redis), and that too can be scaled up beyond a single-point-of-failure by using [Redis Cluster](https://nchan.slact.net/details#using-redis).

Currently, Nchan's main bottleneck is not CPU load but memory bandwidth. This can be improved significantly in future versions with fewer allocations and better use of contiguous memory pools. Please consider supporting Nchan to speed up the work of memory cache optimization.

## Install

#### Download Packages
 - [Arch Linux](https://archlinux.org): [nginx-nchan](https://aur.archlinux.org/packages/nginx-nchan/) and [nginx-nchan-git](https://aur.archlinux.org/packages/nginx-nchan-git/) are available in the Arch User Repository.  
 - Mac OS X: a [homebrew](http://brew.sh) package is available. `brew tap homebrew/nginx; brew install nginx-full --with-nchan-module`
 - [Debian](https://www.debian.org/): A dynamic module build for is available in the Debian package repository: [libnginx-mod-nchan](https://packages.debian.org/sid/libnginx-mod-nchan).  
 Additionally, you can use the pre-built static module packages [nginx-common.deb](https://nchan.slact.net/download/nginx-common.deb) and [nginx-extras.deb](https://nchan.slact.net/download/nginx-extras.deb). Download both and install them with `dpkg -i`, followed by `sudo apt-get -f install`.
 - [Ubuntu](http://www.ubuntu.com/):  [nginx-common.ubuntu.deb](https://nchan.slact.net/download/nginx-common.ubuntu.deb) and [nginx-extras.ubuntu.deb](https://nchan.slact.net/download/nginx-extras.ubuntu.deb). Download both and install them with `dpkg -i`, followed by `sudo apt-get -f install`. Who knows when Ubuntu will add them to their repository?...
 - [Fedora](https://fedoraproject.org): Dynamic module builds for Nginx > 1.10.0 are available: [nginx-mod-nchan.x86_64.rpm](https://nchan.slact.net/download/nginx-mod-nchan.x86-64.rpm), [nginx-mod-nchan.src.rpm](https://nchan.slact.net/download/nginx-mod-nchan.src.rpm). 
 - A statically compiled binary and associated linux nginx installation files are also [available as a tarball](https://nchan.slact.net/download/nginx-nchan-latest.tar.gz).


#### Build From Source
Grab the latest copy of Nginx from [nginx.org](http://nginx.org). Grab the latest Nchan source from [github](https://github.com/slact/nchan/releases). Follow the instructions for [building Nginx](https://www.nginx.com/resources/wiki/start/topics/tutorials/install/#source-releases), except during the `configure` stage, add
```
./configure --add-module=path/to/nchan ...
```

If you're using Nginx  > 1.9.11, you can build Nchan as a [dynamic module](https://www.nginx.com/blog/dynamic-modules-nginx-1-9-11/) with `--add-dynamic-module=path/to/nchan`

Run `make`, `make install`, and enjoy. (Caution, contents may be hot.)

## Getting Started

Once you've built and installed Nchan, it's very easy to start using. Add two locations to your nginx config:

```nginx
#...
http {  
  server {
    #...
    
    location = /sub {
      nchan_subscriber;
      nchan_channel_id $arg_id;
    }
    
    location = /pub {
      nchan_publisher;
      nchan_channel_id $arg_id;
    }
  }
}
```

You can now publish messages to channels by `POST`ing data to `/sub?id=channel_id` , and subscribe by pointing Websocket, EventSource, or [NchanSubscriber.js](https://github.com/slact/nchan/blob/master/NchanSubscriber.js) to `sub/?id=channel_id`. It's that simple.

But Nchan is very flexible and highly configurable. So, of course, it can get a lot more complicated...

## Conceptual Overview

The basic unit of most pub/sub solutions is the messaging *channel*. Nchan is no different. Publishers send messages to channels with a certain *channel id*, and subscribers subscribed to those channels receive them. Some number of messages may be buffered for a time in a channel's message buffer before they are deleted. Pretty simple, right? 

Well... the trouble is that nginx configuration does not deal with channels, publishers, and subscribers. Rather, it has several sections for incoming requests to match against *server* and *location* sections. **Nchan configuration directives map servers and locations onto channel publishing and subscribing endpoints**:

```nginx
#very basic nchan config
worker_processes 5;

http {  
  server {
    listen       80;
    
    location = /sub {
      nchan_subscriber;
      nchan_channel_id foobar;
    }
    
    location = /pub {
      nchan_publisher;
      nchan_channel_id foobar;
    }
  }
}
```

The above maps requests to the URI `/sub` onto the channel `foobar`'s *subscriber endpoint* , and similarly `/pub` onto channel `foobar`'s *publisher endpoint*.


#### Publisher Endpoints

Publisher endpoints are Nginx config *locations* with the [*`nchan_publisher`*](#nchan_publisher) directive.

Messages can be published to a channel by sending HTTP **POST** requests with the message contents to the *publisher endpoint* locations. You can also publish messages through a **Websocket** connection to the same location.

<!-- tag:publisher -->

##### Publishing Messages

Requests and websocket messages are responded to with information about the channel at time of message publication. Here's an example from publishing with `curl`:

```console
>  curl --request POST --data "test message" http://127.0.0.1:80/pub

 queued messages: 5
 last requested: 18 sec. ago
 active subscribers: 0
 last message id: 1450755280:0
```

The response can be in plaintext (as above), JSON, or XML, based on the request's *`Accept`* header:

```console
> curl --request POST --data "test message" -H "Accept: text/json" http://127.0.0.2:80/pub

 {"messages": 6, "requested": 55, "subscribers": 0, "last_message_id": "1450755317:0" }
```

Websocket publishers also receive the same responses when publishing, with the encoding determined by the *`Accept`* header present during the handshake.

The response code for an HTTP request is *`202` Accepted* if no subscribers are present at time of publication, or *`201` Created* if at least 1 subscriber was present.

Metadata can be added to a message when using an HTTP POST request for publishing. A `Content-Type` header will be associated as the message's content type (and output to Long-Poll, Interval-Poll, and multipart/mixed subscribers). A `X-EventSource-Event` header can also be used to associate an EventSource `event:` line value with a message.

##### Other Publisher Endpoint Actions

**HTTP `GET`** requests return channel information without publishing a message. The response code is `200` if the channel exists, and `404` otherwise:  
```console
> curl --request POST --data "test message" http://127.0.0.2:80/pub
  ...

> curl -v --request GET -H "Accept: text/json" http://127.0.0.2:80/pub

 {"messages": 1, "requested": 7, "subscribers": 0, "last_message_id": "1450755421:0" }
```


**HTTP `DELETE`** requests delete a channel and end all subscriber connections. Like the `GET` requests, this returns a `200` status response with channel info if the channel existed, and a `404` otherwise.

For an in-depth explanation of how settings are applied to channels from publisher locations, see the [details page](https://nchan.slact.net/details#publisher-endpoint-configs).

#### Subscriber Endpoints

Subscriber endpoints are Nginx config *locations* with the [*`nchan_subscriber`*](#nchan_subscriber) directive.

Nchan supports several different kinds of subscribers for receiving messages: [*Websocket*](#websocket), [*EventSource*](#eventsource) (Server Sent Events),  [*Long-Poll*](#long-polling), [*Interval-Poll*](#interval-polling). [*HTTP chunked transfer*](#http-chunked-transfer), and [*HTTP multipart/mixed*](#http-multipart-mixed).

- ##### Long-Polling
  The tried-and-true server-push method supported by every browser out there.  
  Initiated by sending an HTTP `GET` request to a channel subscriber endpoint.  
  The long-polling subscriber walks through a channel's message queue via the built-in cache mechanism of HTTP clients, namely with the "`Last-Modified`" and "`Etag`" headers. Explicitly, to receive the next message for given a long-poll subscriber response, send a request with the "`If-Modified-Since`" header set to the previous response's "`Last-Modified`" header, and "`If-None-Match`" likewise set to the previous response's "`Etag`" header.  
  Sending a request without a "`If-Modified-Since`" or "`If-None-Match`" headers returns the oldest message in a channel's message queue, or waits until the next published message, depending on the value of the `nchan_subscriber_first_message` config directive.  
  A message's associated content type, if present, will be sent to this subscriber with the `Content-Type` header.
  <!-- tag:subscriber-longpoll -->
  
- ##### Interval-Polling
  Works just like long-polling, except if the requested message is not yet available, immediately responds with a `304 Not Modified`.
  There is no way to differentiate between long-poll and interval-poll subscriber requests, so long-polling must be disabled for a subscriber location if you wish to use interval-polling.

- ##### Websocket
  Bidirectional communication for web browsers. Part of the [HTML5 spec](http://www.w3.org/TR/2014/REC-html5-20141028/single-page.html). Nchan supports the latest protocol version 13 ([RFC 6455](https://tools.ietf.org/html/rfc6455)).  
  Initiated by sending a websocket handshake to the desired subscriber endpoint location.  
  If the websocket connection is closed by the server, the `close` frame will contain the HTTP response code and status line describing the reason for closing the connection. Server-initiated keep-alive pings can be configured with the [`nchan_websocket_ping_interval`](#nchan_websocket_ping_interval) config directive. Websocket extensions are not yet supported.  
  Messages published through a websocket connection can be forwarded to an upstream application with the [`nchan_publisher_upstream_request`](#nchan_publisher_upstream_request) config directive.   
  Websocket subscribers can use the custom `ws+meta.nchan` subprotocol to receive message metadata with messages, making websocket connections resumable. Messages received with this subprotocol are of the form
  <pre>
  id: message_id
  content-type: message_content_type
  \n
  message_data
  </pre>   
  The `content-type:` line may be omitted.
  <!-- tag:subscriber-websocket -->
  
- ##### EventSource
  Also known as [Server-Sent Events](https://en.wikipedia.org/wiki/Server-sent_events) or SSE, it predates Websockets in the [HTML5 spec](http://www.w3.org/TR/2014/REC-html5-20141028/single-page.html), and is a [very simple protocol](http://www.w3.org/TR/eventsource/#event-stream-interpretation).  
  Initiated by sending an HTTP `GET` request to a channel subscriber endpoint with the "`Accept: text/event-stream`" header.    
  Each message `data: ` segment will be prefaced by the message `id: `.  
  To resume a closed EventSource connection from the last-received message, one *should* start the connection with the "`Last-Event-ID`" header set to the last message's `id`.  
  Unfortunately, browsers [don't support setting](http://www.w3.org/TR/2011/WD-eventsource-20111020/#concept-event-stream-last-event-id) this header for an `EventSource` object, so by default the last message id is set either from the "`Last-Event-Id`" header or the `last_event_id` url query string argument.  
  This behavior can be configured via the [`nchan_subscriber_last_message_id`](#nchan_subscriber_last_message_id) config.  
  A message's `content-type` will not be received by an EventSource subscriber, as the protocol makes no provisions for this metadata.
  A message's associated `event` type, if present, will be sent to this subscriber with the `event:` line.  
  <!-- tag:subscriber-eventsource -->
  
- ##### HTTP [multipart/mixed](http://www.w3.org/Protocols/rfc1341/7_2_Multipart.html#z0)
  The `multipart/mixed` MIMEtype was conceived for emails, but hey, why not use it for HTTP? It's easy to parse and includes metadata with each message.  
  Initiated by including an `Accept: multipart/mixed` header.  
  The response headers and the unused "preamble" portion of the response body are sent right away, with the boundary string generated randomly for each subscriber.  Each subsequent message will be sent as one part of the multipart message, and will include the message time and tag (`Last-Modified` and `Etag`) as well as the optional `Content-Type` headers.  
  Each message is terminated with the next multipart message's boundary **without a trailing newline**. While this conforms to the multipart spec, it is unusual as multipart messages are defined as *starting*, rather than ending with a boundary.  
  A message's associated content type, if present, will be sent to this subscriber with the `Content-Type` header.
  <!-- tag:subscriber-multipart -->
  
- ##### HTTP Raw Stream
  A simple subscription method similar to the [streaming subscriber](https://github.com/wandenberg/nginx-push-stream-module/blob/master/docs/directives/subscribers.textile#push_stream_subscriber) of the [Nginx HTTP Push Stream Module](https://github.com/wandenberg/nginx-push-stream-module). Messages are appended to the response body, separated by a newline or configurable by `nchan_subscriber_http_raw_stream_separator`.
  <!-- tag:subscriber-rawstream -->

- ##### HTTP [Chunked Transfer](http://www.w3.org/Protocols/rfc2616/rfc2616-sec3.html#sec3.6.1)
  This subscription method uses the `chunked` `Transfer-Encoding` to receive messages.   
  Initiated by explicitly including `chunked` in the [`TE` header](http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.39):  
  `TE: chunked` (or `TE: chunked;q=??` where the qval > 0)  
  The response headers are sent right away, and each message will be sent as an individual chunk. Note that because a zero-length chunk terminates the transfer, **zero-length messages will not be sent** to the subscriber.  
  Unlike the other subscriber types, the `chunked` subscriber cannot be used with http/2 because it dissallows chunked encoding.
  <!-- tag:subscriber-chunked -->

<!-- tag:subscriber -->

#### PubSub Endpoint  

PubSub endpoints are Nginx config *locations* with the [*`nchan_pubsub`*](#nchan_pubsub) directive.

A combination of *publisher* and *subscriber* endpoints, this location treats all HTTP `GET`
requests as subscribers, and all HTTP `POST` as publishers. One simple use case is an echo server:

```nginx
  location = /pubsub {
    nchan_pubsub;
    nchan_channel_id foobar;
  }
```

A more applicable setup may set different publisher and subscriber channel ids:

```nginx
  location = /pubsub {
    nchan_pubsub;
    nchan_publisher_channel_id foo;
    nchan_subscriber_channel_id bar;
  }
```

Here, subscribers will listen for messages on channel `foo`, and publishers will publish messages to channel `bar`. This can be useful when setting up websocket proxying between web clients and your application.

<!-- tag:pubsub -->

### The Channel ID

So far the examples have used static channel ids, which is not very useful in practice. It can be set to any nginx *variable*, such as a querystring argument, a header value, or a part of the location url:

```nginx
  location = /sub_by_ip {
    #channel id is the subscriber's IP address
    nchan_subscriber;
    nchan_channel_id $remote_addr;
  }
  
  location /sub_by_querystring {
    #channel id is the query string parameter chanid
    # GET /sub/sub_by_querystring?foo=bar&chanid=baz will have the channel id set to 'baz'
    nchan_subscriber;
    nchan_channel_id $arg_chanid;
  }

  location ~ /sub/(\w+)$ {
    #channel id is the word after /sub/
    # GET /sub/foobar_baz will have the channel id set to 'foobar_baz'
    # I hope you know your regular expressions...
    nchan_subscriber;
    nchan_channel_id $1; #first capture of the location match
  }
```

<!-- tag:channel-id -->

#### Channel Multiplexing

With channel multiplexing, subscribers can subscribe to up to 255 channels per connection. Messages published to all the specified channels will be delivered in-order to the subscriber. There are two ways to enable multiplexing:

Up to 7 channel ids can be specified for the `nchan_channel_id` or `nchan_channel_subscriber_id` config directive:

```nginx
  location ~ /multisub/(\w+)/(\w+)$ {
    nchan_subscriber;
    nchan_channel_id "$1" "$2" "common_channel";
    #GET /multisub/foo/bar will be subscribed to:
    # channels 'foo', 'bar', and 'common_channel',
    #and will receive messages from all of the above.
  }
```

For more than 7 channels, `nchan_channel_id_split_delimiter` can be used to split the `nchan_channel_id` or `nchan_channel_subscriber_id` into up to 255 individual channel ids:

```nginx
  location ~ /multisub-split/(.*)$ {
    nchan_subscriber;
    nchan_channel_id "$1";
    nchan_channel_id_split_delimiter ",";
    #GET /multisub-split/foo,bar,baz,a will be subscribed to:
    # channels 'foo', 'bar', 'baz', and 'a'
    #and will receive messages from all of the above.
  }
```

Publishing to multiple channels with a single request is also possible, with similar configuration:

```nginx
  location ~ /multipub/(\w+)/(\w+)$ {
    nchan_publisher;
    nchan_channel_id "$1" "$2" "another_channel";
  }
```

`DELETE` requests to a multiplexed channel broadcast the deletion to each of the channels it multiplexes, deletes all their messages and kicks out all clients subscribed to any of the channel ids.

See the [details page](https://nchan.slact.net/details#securing-channels) for more information about using good IDs and keeping channels secure.

<!-- tag:channel-multiplexing -->

## Storage

Nchan can stores messages in memory, on disk, or via Redis. Memory storage is much faster, whereas Redis has additional overhead as is considerably slower for publishing messages, but offers near unlimited scalability for broadcast use cases with far more subscribers than publishers.

<!-- tag:storage -->

### Memory Storage

This storage method uses a segment of shared memory to store messages and channel data. Large messages as determined by Nginx's caching layer are stored on-disk. The size of the memory segment is configured with `nchan_max_reserved_memory`. Data stored here is not persistent, and is lost if Nginx is restarted or reloaded.

<!-- tag:memstore -->

### Redis

Nchan can also store messages and channels on a Redis server, or in a Redis cluster. To use a Redis server, set `nchan_use_redis on;` and set the server url with `nchan_redis_url`. These two settings are inheritable by nested locations, so it is enough to set them within an `http { }` block to enable Redis for all Nchan locations in that block. Different locations can also use different Redis servers.

To use a Redis Cluster, the Redis servers acting as cluster nodes need to be configured in an `upstream { }` block:

```nginx
  upstream redis_cluster {
    nchan_redis_server redis://127.0.0.1:7000;
    nchan_redis_server redis://127.0.0.1:7001;
    nchan_redis_server redis://127.0.0.1:7002;
  }
```

It is best to specify all master cluster nodes, but this is not required -- as long as Nchan can connect to at least 1 node, it will discover and connect to the whole cluster.

To use Redis Cluster in an Nchan location, use the `nchan_redis_pass` setting:

```nginx
  location ~ /pubsub/(\w+)$ {
    nchan_channel_id $1;
    nchan_pubsub;
    nchan_redis_pass redis_cluster;
  }

```

Note that `nchan_redis_pass` implies `nchan_use_redis on;`, and that this setting is *not* inherited by nested locations.

When connecting several Nchan servers to the same Redis server (or cluster), the servers **must have their times synced up**. Failure to do so may result in missing and duplicated messages.

See the [details page](https://nchan.slact.net/details#using-redis) for more information on using Redis.

<!-- tag:redis -->

## Variables

Nchan makes several variables usabled in the config file:
 
- `$nchan_channel_id`  
  The channel id extracted from a publisher or subscriber location request. For multiplexed locations, this is the first channel id in the list.

- `$nchan_channel_id1`, `$nchan_channel_id2`, `$nchan_channel_id3`, `$nchan_channel_id4`  
  As above, but for the nth channel id in multiplexed channels.

- `$nchan_subscriber_type`  
  For subscriber locations, this variable is set to the subscriber type (websocket, longpoll, etc.).

- `$nchan_publisher_type`  
  For subscriber locations, this variable is set to the subscriber type (http or websocket).
  
- `$nchan_prev_message_id`, `$nchan_message_id`
  The current and previous (if applicable) message id for publisher request or subscriber response.

- `$nchan_channel_event`
  For channel events, this is the event name. Useful when configuring `nchan_channel_event_string`.
  
Additionally, `nchan_stub_status` data is also exposed as variables. These are available only when `nchan_stub_status` is enabled on at least one location:

- `$nchan_stub_status_total_published_messages`  
- `$nchan_stub_status_stored_messages`  
- `$nchan_stub_status_shared_memory_used`  
- `$nchan_stub_status_channels`  
- `$nchan_stub_status_subscribers`  
- `$nchan_stub_status_redis_pending_commands`  
- `$nchan_stub_status_redis_connected_servers`  
- `$nchan_stub_status_total_ipc_alerts_received`  
- `$nchan_stub_status_ipc_queued_alerts`  
- `$nchan_stub_status_total_ipc_send_delay`  
- `$nchan_stub_status_total_ipc_receive_delay`  


## Configuration Directives

- **nchan_channel_id**  
  arguments: 1 - 7  
  default: `(none)`  
  context: server, location, if  
  > Channel id for a publisher or subscriber location. Can have up to 4 values to subscribe to up to 4 channels.    
  [more details](#the-channel-id)  

- **nchan_channel_id_split_delimiter**  
  arguments: 1  
  default: `(none)`  
  context: server, location, if  
  > Split the channel id into several ids for multiplexing using the delimiter string provided.    
  [more details](#channel-multiplexing)  

- **nchan_eventsource_event**  
  arguments: 1  
  default: `(none)`  
  context: server, location, if  
  > Set the EventSource `event:` line to this value. When used in a publisher location, overrides the published message's `X-EventSource-Event` header and associates the message with the given value. When used in a subscriber location, overrides all messages' associated `event:` string with the given value.    

- **nchan_longpoll_multipart_response** `[ off | on | raw ]`  
  arguments: 1  
  default: `off`  
  context: server, location, if  
  > when set to 'on', enable sending multiple messages in a single longpoll response, separated using the multipart/mixed content-type scheme. If there is only one available message in response to a long-poll request, it is sent unmodified. This is useful for high-latency long-polling connections as a way to minimize round-trips to the server. When set to 'raw', sends multiple messages using the http-raw-stream message separator.    

- **nchan_publisher** `[ http | websocket ]`  
  arguments: 0 - 2  
  default: `http websocket`  
  context: server, location, if  
  legacy name: push_publisher  
  > Defines a server or location as a publisher endpoint. Requests to a publisher location are treated as messages to be sent to subscribers. See the protocol documentation for a detailed description.    
  [more details](#publisher-endpoints)  

- **nchan_publisher_channel_id**  
  arguments: 1 - 7  
  default: `(none)`  
  context: server, location, if  
  > Channel id for publisher location.    

- **nchan_publisher_upstream_request** `<url>`  
  arguments: 1  
  context: server, location, if  
  > Send POST request to internal location (which may proxy to an upstream server) with published message in the request body. Useful for bridging websocket publishers with HTTP applications, or for transforming message via upstream application before publishing to a channel.    
  > The upstream response code determines how publishing will proceed. A `200 OK` will publish the message from the upstream response's body. A `304 Not Modified` will publish the message as it was received from the publisher. A `204 No Content` will result in the message not being published.    
  [more details](https://nchan.slact.net/details#message-publishing-callbacks)  

- **nchan_pubsub** `[ http | websocket | eventsource | longpoll | intervalpoll | chunked | multipart-mixed | http-raw-stream ]`  
  arguments: 0 - 6  
  default: `http websocket eventsource longpoll chunked multipart-mixed`  
  context: server, location, if  
  > Defines a server or location as a pubsub endpoint. For long-polling, GETs subscribe. and POSTs publish. For Websockets, publishing data on a connection does not yield a channel metadata response. Without additional configuration, this turns a location into an echo server.    
  [more details](#pubsub-endpoint)  

- **nchan_subscriber** `[ websocket | eventsource | longpoll | intervalpoll | chunked | multipart-mixed | http-raw-stream ]`  
  arguments: 0 - 5  
  default: `websocket eventsource longpoll chunked multipart-mixed`  
  context: server, location, if  
  legacy name: push_subscriber  
  > Defines a server or location as a channel subscriber endpoint. This location represents a subscriber's interface to a channel's message queue. The queue is traversed automatically, starting at the position defined by the `nchan_subscriber_first_message` setting.    
  >  The value is a list of permitted subscriber types.    
  [more details](#subscriber-endpoints)  

- **nchan_subscriber_channel_id**  
  arguments: 1 - 7  
  default: `(none)`  
  context: server, location, if  
  > Channel id for subscriber location. Can have up to 4 values to subscribe to up to 4 channels.    

- **nchan_subscriber_compound_etag_message_id**  
  arguments: 1  
  default: `off`  
  context: server, location, if  
  > Override the default behavior of using both `Last-Modified` and `Etag` headers for the message id.    
  > Enabling this option packs the entire message id into the `Etag` header, and discards  
  > `Last-Modified` and `If-Modified-Since` headers.    
  [more details]()  

- **nchan_subscriber_first_message** `[ oldest | newest | <number> ]`  
  arguments: 1  
  default: `oldest`  
  context: server, location, if  
  > Controls the first message received by a new subscriber. 'oldest' starts at the oldest available message in a channel's message queue, 'newest' waits until a message arrives. If a number `n` is specified, starts at `n`th message from the oldest. (`-n` starts at `n`th from now). 0 is equivalent to 'newest'.    

- **nchan_subscriber_http_raw_stream_separator** `<string>`  
  arguments: 1  
  default: `\n`  
  context: server, location, if  
  > Message separator string for the http-raw-stream subscriber. Automatically terminated with a newline character.    

- **nchan_subscriber_last_message_id**  
  arguments: 1 - 5  
  default: `$http_last_event_id $arg_last_event_id`  
  context: server, location, if  
  > If `If-Modified-Since` and `If-None-Match` headers are absent, set the message id to the first non-empty of these values. Used primarily as a workaround for the inability to set the first `Last-Message-Id` of a web browser's EventSource object.     

- **nchan_subscriber_message_id_custom_etag_header**  
  arguments: 1  
  default: `(none)`  
  context: server, location, if  
  > Use a custom header instead of the Etag header for message ID in subscriber responses. This setting is a hack, useful when behind a caching proxy such as Cloudflare that under some conditions (like using gzip encoding) swallow the Etag header.    

- **nchan_subscriber_timeout** `<number> (seconds)`  
  arguments: 1  
  default: `0 (none)`  
  context: http, server, location, if  
  legacy name: push_subscriber_timeout  
  > Maximum time a subscriber may wait for a message before being disconnected. If you don't want a subscriber's connection to timeout, set this to 0. When possible, the subscriber will get a response with a `408 Request Timeout` status; otherwise the subscriber will simply be disconnected.    

- **nchan_websocket_ping_interval** `<number> (seconds)`  
  arguments: 1  
  default: `0 (none)`  
  context: server, location, if  
  > Interval for sending websocket ping frames. Disabled by default.    

- **nchan_authorize_request** `<url>`  
  arguments: 1  
  context: server, location, if  
  > Send GET request to internal location (which may proxy to an upstream server) for authorization of a publisher or subscriber request. A 200 response authorizes the request, a 403 response forbids it.    
  [more details](https://nchan.slact.net/details#request-authorization)  

- **nchan_subscribe_request** `<url>`  
  arguments: 1  
  context: server, location, if  
  > Send GET request to internal location (which may proxy to an upstream server) after subscribing. Disabled for longpoll and interval-polling subscribers.    
  [more details](https://nchan.slact.net/details#subsribe-and-unsubscribe-callbacks)  

- **nchan_unsubscribe_request** `<url>`  
  arguments: 1  
  context: server, location, if  
  > Send GET request to internal location (which may proxy to an upstream server) after unsubscribing. Disabled for longpoll and interval-polling subscribers.    
  [more details](https://nchan.slact.net/details#subsribe-and-unsubscribe-callbacks)  

- **nchan_max_reserved_memory** `<size>`  
  arguments: 1  
  default: `32M`  
  context: http  
  legacy name: push_max_reserved_memory  
  > The size of the shared memory chunk this module will use for message queuing and buffering.    
  [more details](#memory-storage)  

- **nchan_message_buffer_length** `[ <number> | <variable> ]`  
  arguments: 1  
  default: `10`  
  context: http, server, location  
  legacy names: push_max_message_buffer_length, push_message_buffer_length  
  > Publisher configuration setting the maximum number of messages to store per channel. A channel's message buffer will retain a maximum of this many most recent messages. An Nginx variable can also be used to set the buffer length dynamically.    

- **nchan_message_timeout** `[ <time> | <variable> ]`  
  arguments: 1  
  default: `1h`  
  context: http, server, location  
  legacy name: push_message_timeout  
  > Publisher configuration setting the length of time a message may be queued before it is considered expired. If you do not want messages to expire, set this to 0. Note that messages always expire from oldest to newest, so an older message may prevent a newer one with a shorter timeout from expiring. An Nginx variable can also be used to set the timeout dynamically.    

- **nchan_redis_idle_channel_cache_timeout** `<time>`  
  arguments: 1  
  default: `30s`  
  context: http, server, location  
  > A Redis-stored channel and its messages are removed from memory (local cache) after this timeout, provided there are no local subscribers.    

- **nchan_redis_pass**  
  arguments: 1  
  context: http, server, location  
  > Use an upstream config block for Redis servers.    
  [more details](https://nchan.slact.net/details#using-redis)  

- **nchan_redis_ping_interval**  
  arguments: 1  
  default: `4m`  
  context: http, server, location  
  > Send a keepalive command to redis to keep the Nchan redis clients from disconnecting. Set to 0 to disable.    

- **nchan_redis_server**  
  arguments: 1  
  context: upstream  
  > Used in upstream { } blocks to set redis servers.    
  [more details](https://nchan.slact.net/details#using-redis)  

- **nchan_redis_url**  
  arguments: 1  
  default: `127.0.0.1:6379`  
  context: http, server, location  
  > The path to a redis server, of the form 'redis://:password@hostname:6379/0'. Shorthand of the form 'host:port' or just 'host' is also accepted.    
  [more details](https://nchan.slact.net/details#using-redis)  

- **nchan_store_messages** `[ on | off ]`  
  arguments: 1  
  default: `on`  
  context: http, server, location, if  
  legacy name: push_store_messages  
  > Publisher configuration. "`off`" is equivalent to setting `nchan_message_buffer_length 0`, which disables the buffering of old messages. Using this setting is not recommended when publishing very quickly, as it may result in missed messages.    

- **nchan_use_redis** `[ on | off ]`  
  arguments: 1  
  default: `off`  
  context: http, server, location  
  > Use redis for message storage at this location.    
  [more details](https://nchan.slact.net/details#using-redis)  

- **nchan_access_control_allow_origin** `<string>`  
  arguments: 1  
  default: `*`  
  context: http, server, location  
  > Set the [Cross-Origin Resource Sharing (CORS)](https://developer.mozilla.org/en-US/docs/Web/HTTP/Access_control_CORS) `Access-Control-Allow-Origin` header to this value. If the publisher or subscriber request's `Origin` header does not match this value, respond with a `403 Forbidden`.    

- **nchan_channel_group** `<string>`  
  arguments: 1  
  default: `(none)`  
  context: server, location, if  
  legacy name: push_channel_group  
  > Because settings are bound to locations and not individual channels, it is useful to be able to have channels that can be reached only from some locations and never others. That's where this setting comes in. Think of it as a prefix string for the channel id.    

- **nchan_subscribe_existing_channels_only** `[ on | off ]`  
  arguments: 1  
  default: `off`  
  context: http, server, location  
  legacy name: push_authorized_channels_only  
  > Whether or not a subscriber may create a channel by sending a request to a subscriber location. If set to on, a publisher must send a POST or PUT request before a subscriber can request messages on the channel. Otherwise, all subscriber requests to nonexistent channels will get a 403 Forbidden response.    

- **nchan_channel_event_string** `<string>`  
  arguments: 1  
  default: `"$nchan_channel_event $nchan_channel_id"`  
  context: server, location, if  
  > Contents of channel event message    

- **nchan_channel_events_channel_id**  
  arguments: 1  
  context: server, location, if  
  > Channel id where `nchan_channel_id`'s events should be sent. Events like subscriber enqueue/dequeue, publishing messages, etc. Useful for application debugging. The channel event message is configurable via nchan_channel_event_string. The channel group for events is hardcoded to 'meta'.    
  [more details](https://nchan.slact.net/details#channel-events)  

- **nchan_stub_status**  
  arguments: 0  
  context: location  
  > Similar to Nginx's stub_status directive, requests to an `nchan_stub_status` location get a response with some vital Nchan statistics. This data does not account for information from other Nchan instances, and monitors only local connections, published messages, etc.    
  [more details](https://nchan.slact.net/details#nchan_stub_status)  

- **nchan_max_channel_id_length** `<number>`  
  arguments: 1  
  default: `512`  
  context: http, server, location  
  legacy name: push_max_channel_id_length  
  > Maximum permissible channel id length (number of characters). Longer ids will be truncated.    

- **nchan_max_channel_subscribers** `<number>`  
  arguments: 1  
  default: `0 (unlimited)`  
  context: http, server, location  
  legacy name: push_max_channel_subscribers  
  > Maximum concurrent subscribers to the channel on this Nchan server. Does not include subscribers on other Nchan instances when using a shared Redis server.    

- **nchan_channel_timeout**  
  arguments: 1  
  context: http, server, location  
  legacy name: push_channel_timeout  
  > Amount of time an empty channel hangs around. Don't mess with this setting unless you know what you are doing!    

- **nchan_storage_engine** `[ memory | redis ]`  
  arguments: 1  
  default: `memory`  
  context: http, server, location  
  > Development directive to completely replace default storage engine. Don't use unless you are an Nchan developer.    

## Contribute
Please support this project with a donation to keep me warm through the winter. I accept bitcoin at 15dLBzRS4HLRwCCVjx4emYkxXcyAPmGxM3 . Other donation methods can be found at https://nchan.slact.net
