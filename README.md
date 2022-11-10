<img class="logo" alt="NCHAN" src="https://nchan.io/github-logo.png" />

https://nchan.io

Nchan is a scalable, flexible pub/sub server for the modern web, built as a module for the [Nginx](http://nginx.org) web server. It can be configured as a standalone server, or as a shim between your application and hundreds, thousands, or millions of live subscribers. It can buffer messages in memory, on-disk, or via [Redis](http://redis.io). All connections are handled asynchronously and distributed among any number of worker processes. It can also scale to many Nginx servers with [Redis](http://redis.io).

Messages are [published](#publisher-endpoints) to channels with HTTP `POST` requests or Websocket, and [subscribed](#subscriber-endpoint) also through [Websocket](#websocket), [long-polling](#long-polling), [EventSource](#eventsource) (SSE), old-fashioned [interval polling](#interval-polling), [and](#http-chunked-transfer) [more](#http-multipart-mixed).

In a web browser, you can use Websocket or EventSource natively, or the [NchanSubscriber.js](https://github.com/slact/nchan.js) wrapper library. It supports Long-Polling, EventSource, and resumable Websockets, and has a few other added convenience options. It's also available on [NPM](https://www.npmjs.com/package/nchan).

## Features
 - RESTful, HTTP-native [API](#publishing-messages).
 - Supports [Websocket](#websocket), [EventSource (Server-Sent Events)](#eventsource), [Long-Polling](#long-polling) and other HTTP-based subscribers.
 - Per-channel configurable message buffers with no-repeat, no-loss message delivery guarantees.
 - Subscribe to [hundreds of channels](#channel-multiplexing) over a single subscriber connection.
 - HTTP request [callbacks and hooks](#hooks-and-callbacks) for easy integration.
 - Introspection with [channel events](#channel-events) and [url for monitoring performance statistics](#nchan_stub_status-stats).
 - Channel [group](#channel-groups) usage [accounting and limits](#limits-and-accounting).
 - Fast, nonblocking [shared-memory local message storage](#memory-storage) and optional, slower, persistent storage with [Redis](#redis).
 - Horizontally scalable (using [Redis](#redis)).
 - Auto-failover and [high availability](#high-availability) with no single point of failure using [Redis Cluster](#redis-cluster).

## Status and History

The latest Nchan release is 1.3.5 (October 27, 2022) ([changelog](https://nchan.io/changelog)).

The first iteration of Nchan was written in 2009-2010 as the [Nginx HTTP Push Module](https://pushmodule.slact.net), and was vastly refactored into its present state in 2014-2016.

#### Upgrade from Nginx HTTP Push Module

Although Nchan is backwards-compatible with all Push Module configuration directives, some of the more unusual and rarely used settings have been disabled and will be ignored (with a warning). See the [upgrade page](https://nchan.io/upgrade) for a detailed list of changes and improvements, as well as a full list of incompatibilities.


## Does it scale?

<img class="benchmark_graph" alt="benchmarking internal subscriber response times" src="https://nchan.io/img/benchmark_internal_total.png" />

Yes it does. Like Nginx, Nchan can easily handle as much traffic as you can throw at it. I've tried to benchmark it, but my benchmarking tools are much slower than Nchan. The data I've gathered is on how long Nchan itself takes to respond to every subscriber after publishing a message -- this excludes TCP handshake times and internal HTTP request parsing. Basically, it measures how Nchan scales assuming all other components are already tuned for scalability. The graphed data are averages of 5 runs with 50-byte messages.

With a well-tuned OS and network stack on commodity server hardware, expect to handle upwards of 300K concurrent subscribers per second at minimal CPU load. Nchan can also be scaled out to multiple Nginx instances using the [Redis storage engine](#nchan_use_redis), and that too can be scaled up beyond a single-point-of-failure by using [Redis Cluster](#redis-cluster).


## Install

#### Download Packages
 - [Arch Linux](https://archlinux.org): [nginx-mod-nchan](https://aur.archlinux.org/packages/nginx-mod-nchan/) and [nginx-mainline-mod-nchan](https://aur.archlinux.org/packages/nginx-mainline-mod-nchan/) are available in the Arch User Repository.
 - Mac OS X: a [homebrew](http://brew.sh) package is available. `brew tap denji/nginx; brew install nginx-full --with-nchan-module`
 - [Debian](https://www.debian.org/): A dynamic module build is available in the Debian package repository: [libnginx-mod-nchan](https://packages.debian.org/sid/libnginx-mod-nchan).  
 Additionally, you can use the pre-built static module packages [nginx-common.deb](https://nchan.io/download/nginx-common.deb) and [nginx-extras.deb](https://nchan.io/download/nginx-extras.deb). Download both and install them with `dpkg -i`, followed by `sudo apt-get -f install`.
 - [Ubuntu](http://www.ubuntu.com/):  [nginx-common.ubuntu.deb](https://nchan.io/download/nginx-common.ubuntu.deb) and [nginx-extras.ubuntu.deb](https://nchan.io/download/nginx-extras.ubuntu.deb). Download both and install them with `dpkg -i`, followed by `sudo apt-get -f install`. Who knows when Ubuntu will add Nchan to their repository?...
 - [Fedora](https://fedoraproject.org): Dynamic module builds for Nginx > 1.10.0 are available: [nginx-mod-nchan.x86_64.rpm](https://nchan.io/download/nginx-mod-nchan.x86-64.rpm), [nginx-mod-nchan.src.rpm](https://nchan.io/download/nginx-mod-nchan.src.rpm). 
 - [Heroku](https://heroku.com): A buildpack for compiling Nchan into Nginx is available: [nchan-buildpack](https://github.com/andjosh/nchan-buildpack). A one-click, readily-deployable app is also available: [nchan-heroku](https://github.com/andjosh/nchan-heroku).
 - A statically compiled binary and associated linux nginx installation files are also [available as a tarball](https://nchan.io/download/nginx-nchan-latest.tar.gz).


#### Build From Source
Grab the latest copy of Nginx from [nginx.org](http://nginx.org). Grab the latest Nchan source from [github](https://github.com/slact/nchan/releases). Follow the instructions for [building Nginx](https://www.nginx.com/resources/wiki/start/topics/tutorials/install/#source-releases), except during the `configure` stage, add
```
./configure --add-module=path/to/nchan ...
```

If you're using Nginx  > 1.9.11, you can build Nchan as a [dynamic module](https://www.nginx.com/blog/dynamic-modules-nginx-1-9-11/) with `--add-dynamic-module=path/to/nchan`

Run `make`, then `make install`.

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

You can now publish messages to channels by `POST`ing data to `/pub?id=channel_id` , and subscribe by pointing Websocket, EventSource, or [NchanSubscriber.js](https://github.com/slact/nchan.js) to `sub/?id=channel_id`. It's that simple.

But Nchan is very flexible and highly configurable. So, of course, it can get a lot more complicated...

### Conceptual Overview

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


## Publisher Endpoints

Publisher endpoints are Nginx config *locations* with the [*`nchan_publisher`*](#nchan_publisher) directive.

Messages can be published to a channel by sending HTTP **POST** requests with the message contents to the *publisher endpoint* locations. You can also publish messages through a **Websocket** connection to the same location.

```nginx
  location /pub {
    #example publisher location
    nchan_publisher;
    nchan_channel_id foo;
    nchan_channel_group test;
    nchan_message_buffer_length 50;
    nchan_message_timeout 5m;
  }
```

<!-- tag:publisher -->

### Publishing Messages

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

 {"messages": 5, "requested": 18, "subscribers": 0, "last_message_id": "1450755280:0" }
```

Websocket publishers also receive the same responses when publishing, with the encoding determined by the *`Accept`* header present during the handshake.

The response code for an HTTP request is *`202` Accepted* if no subscribers are present at time of publication, or *`201` Created* if at least 1 subscriber was present.

Metadata can be added to a message when using an HTTP POST request for publishing. A `Content-Type` header will be associated as the message's content type (and output to Long-Poll, Interval-Poll, and multipart/mixed subscribers). A `X-EventSource-Event` header can also be used to associate an EventSource `event:` line value with a message.

### Other Publisher Endpoint Actions

**HTTP `GET`** requests return channel information without publishing a message. The response code is `200` if the channel exists, and `404` otherwise:  
```console
> curl --request POST --data "test message" http://127.0.0.2:80/pub
  ...

> curl -v --request GET -H "Accept: text/json" http://127.0.0.2:80/pub

 {"messages": 1, "requested": 7, "subscribers": 0, "last_message_id": "1450755421:0" }
```


**HTTP `DELETE`** requests delete a channel and end all subscriber connections. Like the `GET` requests, this returns a `200` status response with channel info if the channel existed, and a `404` otherwise.

### How Channel Settings Work

*A channel's configuration is set to the that of its last-used publishing location.*
So, if you want a channel to behave consistently, and want to publish to it from multiple locations, *make sure those locations have the same configuration*.

You can also can use differently-configured publisher locations to dynamically update a channel's message buffer settings. This can be used to erase messages or to scale an existing channel's message buffer as desired.

## Subscriber Endpoints

Subscriber endpoints are Nginx config *locations* with the [*`nchan_subscriber`*](#nchan_subscriber) directive.

Nchan supports several different kinds of subscribers for receiving messages: [*Websocket*](#websocket), [*EventSource*](#eventsource) (Server Sent Events),  [*Long-Poll*](#long-polling), [*Interval-Poll*](#interval-polling). [*HTTP chunked transfer*](#http-chunked-transfer), and [*HTTP multipart/mixed*](#http-multipart-mixed).

```nginx
  location /sub {
    #example subscriber location
    nchan_subscriber;
    nchan_channel_id foo;
    nchan_channel_group test;
    nchan_subscriber_first_message oldest;
  }
```

<!-- tag:subscriber -->

- ### Long-Polling
  The tried-and-true server-push method supported by every browser out there.  
  Initiated by sending an HTTP `GET` request to a channel subscriber endpoint.  
  The long-polling subscriber walks through a channel's message queue via the built-in cache mechanism of HTTP clients, namely with the "`Last-Modified`" and "`Etag`" headers. Explicitly, to receive the next message for given a long-poll subscriber response, send a request with the "`If-Modified-Since`" header set to the previous response's "`Last-Modified`" header, and "`If-None-Match`" likewise set to the previous response's "`Etag`" header.  
  Sending a request without a "`If-Modified-Since`" or "`If-None-Match`" headers returns the oldest message in a channel's message queue, or waits until the next published message, depending on the value of the `nchan_subscriber_first_message` config directive.  
  A message's associated content type, if present, will be sent to this subscriber with the `Content-Type` header.
  <!-- tag:subscriber-longpoll -->
  
- ### Interval-Polling
  Works just like long-polling, except if the requested message is not yet available, immediately responds with a `304 Not Modified`.
  Nchan cannot automatically distinguish between long-poll and interval-poll subscriber requests, so long-polling must be disabled for a subscriber location if you wish to use interval-polling.

- ### Websocket
  Bidirectional communication for web browsers. Part of the [HTML5 spec](http://www.w3.org/TR/2014/REC-html5-20141028/single-page.html). Nchan supports the latest protocol version 13 ([RFC 6455](https://tools.ietf.org/html/rfc6455)).  
  Initiated by sending a websocket handshake to the desired subscriber endpoint location.  
  If the websocket connection is closed by the server, the `close` frame will contain the HTTP response code and status line describing the reason for closing the connection. Server-initiated keep-alive pings can be configured with the [`nchan_websocket_ping_interval`](#nchan_websocket_ping_interval) config directive.
  Messages are delivered to subscribers in `text` websocket frames, except if a message's `content-type` is "`application/octet-stream`" -- then it is delivered in a `binary` frame.
  <br />
  Websocket subscribers can use the custom `ws+meta.nchan` subprotocol to receive message metadata with messages, making websocket connections resumable. Messages received with this subprotocol are of the form
  <pre>
  id: message_id
  content-type: message_content_type
  \n
  message_data
  </pre>   
  The `content-type:` line may be omitted.
  <br />
  #### Websocket Publisher
  Messages published through a websocket connection can be forwarded to an upstream application with the [`nchan_publisher_upstream_request`](#nchan_publisher_upstream_request) config directive.   
  Messages published in a binary frame are automatically given the `content-type` "`application/octet-stream`".
  #### Permessage-deflate
  Nchan version 1.1.8 and above supports the [permessage-deflate protocol extension](https://tools.ietf.org/html/rfc7692). Messages are deflated once when they are published, and then can be broadcast to any number of compatible websocket subscribers. Message deflation is enabled by setting the [`nchan_deflate_message_for_websocket on;`](#nchan_deflate_message_for_websocket) directive in a publisher location.
  <br />
  The deflated data is stored alongside the original message in memory, or, if large enough, on disk. This means more [shared memory](#nchan_shared_memory_size) is necessary when using `nchan_deflate_message_for_websocket`.
  <br />
  Deflation parameters (speed, memory use, strategy, etc.), can be tweaked using the [`nchan_permessage_deflate_compression_window`](#nchan_permessage_deflate_compression_window), [`nchan_permessage_deflate_compression_level`](#nchan_permessage_deflate_compression_level),
  [`nchan_permessage_deflate_compression_strategy`](#nchan_permessage_deflate_compression_strategy), and 
  [`nchan_permessage_deflate_compression_window`](#nchan_permessage_deflate_compression_window) settings.
  <br />
  Nchan also supports the (deprecated) [perframe-deflate extension](https://tools.ietf.org/html/draft-tyoshino-hybi-websocket-perframe-deflate-06) still in use by Safari as `x-webkit-perframe-deflate`.
  <br />
  <!-- tag:subscriber-websocket -->
  
- ### EventSource
  Also known as [Server-Sent Events](https://en.wikipedia.org/wiki/Server-sent_events) or SSE, it predates Websockets in the [HTML5 spec](http://www.w3.org/TR/2014/REC-html5-20141028/single-page.html), and is a [very simple protocol](http://www.w3.org/TR/eventsource/#event-stream-interpretation).  
  Initiated by sending an HTTP `GET` request to a channel subscriber endpoint with the "`Accept: text/event-stream`" header.    
  Each message `data: ` segment will be prefaced by the message `id: `.  
  To resume a closed EventSource connection from the last-received message, one *should* start the connection with the "`Last-Event-ID`" header set to the last message's `id`.  
  Unfortunately, browsers [don't support setting](http://www.w3.org/TR/2011/WD-eventsource-20111020/#concept-event-stream-last-event-id) this header for an `EventSource` object, so by default the last message id is set either from the "`Last-Event-Id`" header or the `last_event_id` url query string argument.  
  This behavior can be configured via the [`nchan_subscriber_last_message_id`](#nchan_subscriber_last_message_id) config.  
  A message's `content-type` will not be received by an EventSource subscriber, as the protocol makes no provisions for this metadata.
  A message's associated `event` type, if present, will be sent to this subscriber with the `event:` line.  
  <!-- tag:subscriber-eventsource -->
  
- ### HTTP [multipart/mixed](http://www.w3.org/Protocols/rfc1341/7_2_Multipart.html#z0)
  The `multipart/mixed` MIMEtype was conceived for emails, but hey, why not use it for HTTP? It's easy to parse and includes metadata with each message.  
  Initiated by including an `Accept: multipart/mixed` header.  
  The response headers and the unused "preamble" portion of the response body are sent right away, with the boundary string generated randomly for each subscriber.  Each subsequent message will be sent as one part of the multipart message, and will include the message time and tag (`Last-Modified` and `Etag`) as well as the optional `Content-Type` headers.  
  Each message is terminated with the next multipart message's boundary **without a trailing newline**. While this conforms to the multipart spec, it is unusual as multipart messages are defined as *starting*, rather than ending with a boundary.  
  A message's associated content type, if present, will be sent to this subscriber with the `Content-Type` header.
  <!-- tag:subscriber-multipart -->
  
- ### HTTP Raw Stream
  A simple subscription method similar to the [streaming subscriber](https://github.com/wandenberg/nginx-push-stream-module/blob/master/docs/directives/subscribers.textile#push_stream_subscriber) of the [Nginx HTTP Push Stream Module](https://github.com/wandenberg/nginx-push-stream-module). Messages are appended to the response body, separated by a newline or configurable by `nchan_subscriber_http_raw_stream_separator`.
  <!-- tag:subscriber-rawstream -->

- ### HTTP [Chunked Transfer](http://www.w3.org/Protocols/rfc2616/rfc2616-sec3.html#sec3.6.1)
  This subscription method uses the `chunked` `Transfer-Encoding` to receive messages.   
  Initiated by explicitly including `chunked` in the [`TE` header](http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.39):  
  `TE: chunked` (or `TE: chunked;q=??` where the qval > 0)  
  The response headers are sent right away, and each message will be sent as an individual chunk. Note that because a zero-length chunk terminates the transfer, **zero-length messages will not be sent** to the subscriber.  
  Unlike the other subscriber types, the `chunked` subscriber cannot be used with http/2 because it disallows chunked encoding.
  <!-- tag:subscriber-chunked -->

## PubSub Endpoint  

PubSub endpoints are Nginx config *locations* with the [*`nchan_pubsub`*](#nchan_pubsub) directive.

A combination of *publisher* and *subscriber* endpoints, this location treats all HTTP `GET`
requests as subscribers, and all HTTP `POST` as publishers. Channels cannot be deleted through a pubsub endpoing with an HTTP `DELETE` request.

One simple use case is an echo server:

```nginx
  location = /pubsub {
    nchan_pubsub;
    nchan_channel_id foo;
    nchan_channel_group test;
  }
```

A more interesting setup may set different publisher and subscriber channel ids:

```nginx
  location = /pubsub {
    nchan_pubsub;
    nchan_publisher_channel_id foo;
    nchan_subscriber_channel_id bar;
    nchan_channel_group test;
  }
```

Here, subscribers will listen for messages on channel `foo`, and publishers will publish messages to channel `bar`. This can be useful when setting up websocket proxying between web clients and your application.
<!-- tag:pubsub -->

## The Channel ID

So far the examples have used static channel ids, which is not very useful. In practice, the channel id can be set to any nginx *variable*, such as a querystring argument, a header value, or a part of the location url:

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

I recommend using the last option, a channel id derived from the request URL via a regular expression. It makes things nice and RESTful.

<!-- tag:channel-id -->

### Channel Multiplexing

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

It is also possible to publish to multiple channels with a single request as well as delete multiple channels with a single request, with similar configuration:

```nginx
  location ~ /multipub/(\w+)/(\w+)$ {
    nchan_publisher;
    nchan_channel_id "$1" "$2" "another_channel";
    #POST /multipub/foo/bar will publish to:
    # channels 'foo', 'bar', 'another_channel'
    #DELETE /multipub/foo/bar will delete:
    # channels 'foo', 'bar', 'another_channel'
  }
```

When a channel is deleted, all of its messages are deleted, and all of its subscribers' connection are closed -- including ones subscribing through a multiplexed location. For example, suppose a subscriber is subscribed to channels "foo" and "bar" via a single multiplexed connection. If "foo" is deleted, the connection is closed, and the subscriber therefore loses the "bar" subscription as well.

See the [Channel Security](#securing-channels) section about using good IDs and keeping private channels secure.

<!-- tag:channel-multiplexing -->

### Channel Groups

Channels can be associated with groups to avoid channel ID conflicts:

```nginx
  location /test_pubsub {
    nchan_pubsub;
    nchan_channel_group "test";
    nchan_channel_id "foo";
  }
  
  location /pubsub {
    nchan_pubsub;
    nchan_channel_group "production";
    nchan_channel_id "foo";
    #same channel id, different channel group. Thus, different channel.
  }
  
  location /flexgroup_pubsub {
    nchan_pubsub;
    nchan_channel_group $arg_group;
    nchan_channel_id "foo";
    #group can be set with request variables too
  }
```

#### Limits and Accounting

Groups can be used to track aggregate channel usage, as well as set limits on the number of channels, subscribers, stored messages, memory use, etc:

```nginx
  #enable group accounting
  nchan_channel_group_accounting on;
  
  location ~ /pubsub/(\w+)$ {
    nchan_pubsub;
    nchan_channel_group "limited";
    nchan_channel_id $1;
  }
  
  location ~ /prelimited_pubsub/(\w+)$ {
    nchan_pubsub;
    nchan_channel_group "limited";
    nchan_channel_id $1;
    nchan_group_max_subscribers 100;
    nchan_group_max_messages_memory 50M;
  }
  
  location /group {
    nchan_channel_group limited;
    nchan_group_location;
    nchan_group_max_channels $arg_max_channels;
    nchan_group_max_messages $arg_max_messages;
    nchan_group_max_messages_memory $arg_max_messages_mem;
    nchan_group_max_messages_disk $arg_max_messages_disk;
    nchan_group_max_subscribers $arg_max_subs;
  }
```

Here, `/group` is an `nchan_group_location`, which is used for accessing and modifying group data. To get group data, send a `GET` request to a `nchan_group_location`:

```sh
>  curl http://localhost/group

channels: 10
subscribers: 0
messages: 219
shared memory used by messages: 42362 bytes
disk space used by messages: 0 bytes
limits:
  max channels: 0
  max subscribers: 0
  max messages: 0
  max messages shared memory: 0
  max messages disk space: 0  
```

By default, the data is returned in human-readable plaintext, but can also be formatted as JSON, XML, or YAML:

```sh
>  curl -H "Accept: text/json" http://localhost/group

{
  "channels": 21,
  "subscribers": 40,
  "messages": 53,
  "messages_memory": 19941,
  "messages_disk": 0,
  "limits": {
    "channels": 0,
    "subscribers": 0,
    "messages": 0,
    "messages_memory": 0,
    "messages_disk": 0
  }
}
```

The data in the response are for the single Nchan instance only, regardless of whether Redis is used. A limit of 0 means 'unlimited'.

Limits can be set per-location, as with the above `/prelimited_pubsub/...` location, or with a POST request to the `nchan_group_location`:
```sh
>  curl -X POST "http://localhost/group?max_channels=15&max_subs=1000&max_messages_disk=0.5G"

channels: 0
subscribers: 0
messages: 0
shared memory used by messages: 0 bytes
disk space used by messages: 0 bytes
limits:
  max channels: 15
  max subscribers: 1000
  max messages: 0
  max messages shared memory: 0
  max messages disk space: 536870912

```

Limits are only applied locally, regardless of whether Redis is enabled. 
If a publisher or subscriber request exceeds a group limit, Nchan will respond to it with a `403 Forbidden` response.

<!-- tag:group -->

## Hooks and Callbacks

<!-- tag:hook -->
  
### Request Authorization

This feature, configured with [`nchan_authorize_request`](#nchan_authorize_request), behaves just like the Nginx [http_auth_request module](http://nginx.org/en/docs/http/ngx_http_auth_request_module.html#auth_request_set).

Consider the configuration:
```nginx
  upstream my_app {
    server 127.0.0.1:8080;
  }
  location = /auth {
    proxy_pass http://my_app/pubsub_authorize;
    proxy_pass_request_body off;
    proxy_set_header Content-Length "";
    proxy_set_header X-Subscriber-Type $nchan_subscriber_type;
    proxy_set_header X-Publisher-Type $nchan_publisher_type;
    proxy_set_header X-Prev-Message-Id $nchan_prev_message_id;
    proxy_set_header X-Channel-Id $nchan_channel_id;
    proxy_set_header X-Original-URI $request_uri;
    proxy_set_header X-Forwarded-For $remote_addr;
  }
  
  location ~ /pubsub/auth/(\w+)$ {
    nchan_channel_id $1;
    nchan_authorize_request /auth;
    nchan_pubsub;
    nchan_channel_group test;
  }
```

Here, any request to the location `/pubsub/auth/<...>` will need to be authorized by your application (`my_app`). Nginx will generate a `GET /pubsub_authorize` request to the application, with additional headers set by the `proxy_set_header` directives. Note that Nchan-specific variables are available for this authorization request. Once your application receives this request, it should decide whether or not to authorize the subscriber. This can be done based on a forwarded session cookie, IP address, or any set of parameters of your choosing. If authorized, it should respond with an empty `200 OK` response.  
All non-`2xx` response codes (such as `403 Forbidden`) are interpreted as authorization failures. In this case, the failing response is proxied to the client. 

Note that Websocket and EventSource clients will only try to authorize during the initial handshake request, whereas Long-Poll and Interval-Poll subscribers will need to be authorized each time they request the next message, which may flood your application with too many authorization requests.

<!-- commands: nchan_authorize_request -->

### Subscriber Presence

Subscribers can notify an application when they have subscribed and unsubscribed to a channel using the [`nchan_subscribe_request`](#nchan_subscribe_request)
and [`nchan_unsubscribe_request`](#nchan_unsubscribe_request) settings. 
These should point to Nginx locations configured to forward requests to an upstream proxy (your application):

```nginx
  location ~ /sub/(\w+)$ {
    nchan_channel_id $1;
    nchan_subscribe_request /upstream/sub;
    nchan_unsubscribe_request /upstream/unsub;
    nchan_subscriber;
    nchan_channel_group test;
  }

  location = /upstream/unsub {
    proxy_pass http://127.0.0.1:9292/unsub;
    proxy_ignore_client_abort on;  #!!!important!!!!
    proxy_set_header X-Subscriber-Type $nchan_subscriber_type;
    proxy_set_header X-Channel-Id $nchan_channel_id;
    proxy_set_header X-Original-URI $request_uri;
  } 
  location = /upstream/sub {
    proxy_pass http://127.0.0.1:9292/sub;
    proxy_set_header X-Subscriber-Type $nchan_subscriber_type;
    proxy_set_header X-Message-Id $nchan_message_id;
    proxy_set_header X-Channel-Id $nchan_channel_id;
    proxy_set_header X-Original-URI $request_uri;
  } 
```

In order for `nchan_unsubscribe_request` to work correctly, the location it points to must have `proxy_ignore_client_abort on;`. Otherwise, suddenly aborted subscribers may not trigger an unsubscribe request.

Note that the subscribe/unsubscribe hooks are **disabled for long-poll and interval-poll clients**, because they would trigger these hooks each time they receive a message.

<!-- commands: nchan_subscribe_request nchan_unsubscribe_request -->

### Message Forwarding

Messages can be forwarded to an upstream application before being published using the `nchan_publisher_upstream_request` setting:

```nginx
  location ~ /pub/(\w+)$ {
    #publisher endpoint
    nchan_channel_id $1;
    nchan_pubsub;
    nchan_publisher_upstream_request /upstream_pub;
  }
  
  location = /upstream_pub {
    proxy_pass http://127.0.0.1:9292/pub;
    proxy_set_header X-Publisher-Type $nchan_publisher_type;
    proxy_set_header X-Prev-Message-Id $nchan_prev_message_id;
    proxy_set_header X-Channel-Id $nchan_channel_id;
    proxy_set_header X-Original-URI $request_uri;
  } 
```
With this configuration, incoming messages are first `POST`ed to `http://127.0.0.1:9292/pub`.
The upstream response code determines how publishing will proceed:
  - `304 Not Modified` publishes the message as received, without modifification.
  - `204 No Content` discards the message
  - `200 OK` is used for modifying the message. Instead of the original incoming message, the message contained in this HTTP response is published.

There are two main use cases for `nchan_publisher_upstream_request`: forwarding incoming data from Websocket publishers to an application, and mutating incoming messages.

<!-- commands: nchan_publisher_upstream_request -->

## Storage

Nchan can stores messages in memory, on disk, or via Redis. Memory storage is much faster, whereas Redis has additional overhead as is considerably slower for publishing messages, but offers near unlimited scalability for broadcast use cases with far more subscribers than publishers.

### Memory Storage

This default storage method uses a segment of shared memory to store messages and channel data. Large messages as determined by Nginx's caching layer are stored on-disk. The size of the memory segment is configured with `nchan_shared_memory_size`. Data stored here is not persistent, and is lost if Nginx is restarted or reloaded.

<!-- tag:memstore -->

### Redis

[Redis](http://redis.io) can be used to add **data persistence** and **horizontal scalability**, **failover** and **high availability** to your Nchan setup. 

<!-- tag:redis -->

#### Connecting to a Redis Server
To connect to a single Redis master server, use an `upstream` with `nchan_redis_server` and `nchan_redis_pass` settings:

```nginx
http {
  upstream my_redis_server {
    nchan_redis_server 127.0.0.1;
  }
  server {
    listen 80;
    
    location ~ /redis_sub/(\w+)$ {
      nchan_subscriber;
      nchan_channel_id $1;
      nchan_redis_pass my_redis_server;
    }
    location ~ /redis_pub/(\w+)$ {
      nchan_redis_pass my_redis_server;
      nchan_publisher;
      nchan_channel_id $1;
    }
  }
} 
```

All servers with the above configuration connecting to the same redis server share channel and message data.

Channels that don't use Redis can be configured side-by-side with Redis-backed channels, provided the endpoints never overlap. (This can be ensured, as above, by setting separate `nchan_channel_group`s.). Different locations can also connect to different Redis servers.

Nchan can work with a single Redis master. It can also auto-discover and use Redis slaves to balance PUBSUB traffic.

<!-- commands: nchan_redis_server nchan_redis_pass -->

#### Redis Cluster
Nchan also supports using Redis Cluster, which adds scalability via sharding channels among cluster nodes. Redis cluster also provides **automatic failover**, **high availability**, and eliminates the single point of failure of one shared Redis server. It is configured and used like so:

```nginx
http {
  upstream redis_cluster {
    nchan_redis_server redis://127.0.0.1:7000;
    nchan_redis_server redis://127.0.0.1:7001;
    nchan_redis_server redis://127.0.0.1:7002;
    # you don't need to specify all the nodes, they will be autodiscovered
    # however, it's recommended that you do specify at least a few master nodes.
  }
  server {
    listen 80;
    
    location ~ /sub/(\w+)$ {
      nchan_subscriber;
      nchan_channel_id $1;
      nchan_redis_pass redis_cluster;
    }
    location ~ /pub/(\w+)$ {
      nchan_publisher;
      nchan_channel_id $1;
      nchan_redis_pass redis_cluster;
    }
  }
} 
```

<!-- commands: nchan_redis_server nchan_redis_pass -->

##### High Availability
Redis Cluster connections are designed to be resilient and try to recover from errors. Interrupted connections will have their commands queued until reconnection, and Nchan will publish any messages it successfully received while disconnected. Nchan is also adaptive to cluster modifications. It will add new nodes and remove them as needed.

All Nchan servers sharing a Redis server or cluster should have their times synchronized (via ntpd or your favorite ntp daemon). Failure to do so may result in missed or duplicate messages.

##### Failover Recovery
Starting with version 1.3.0, Nchan will attempt to recover from cluster node failures, keyslot errors, and cluster epoch changes without disconnecting from the entire cluster. It will attempt to do this until [`nchan_redis_cluster_max_failing_time`](#nchan_redis_cluster_max_failing_time) is exceeded. Additionally, [recovery attempt delays](#nchan_redis_cluster_recovery_delay) have configurable [jitter](#nchan_redis_cluster_recovery_delay_jitter), [exponential backoff](#nchan_redis_cluster_recovery_delay_backoff), and [maximum](#nchan_redis_cluster_recovery_delay_max) values.

#### Using Redis securely

Redis servers can be connected to via TLS by using the [`nchan_redis_ssl`](#nchan_redis_ssl) config setting in an `upstream` block, or by using the `rediss://`  schema for the server URLs.

A password and optional username for the `AUTH` command can be set by the [`nchan_redis_username`](#nchan_redis_username) and [`nchan_redis_password`](#nchan_redis_password) config settings in an `upstream` block, or by using the `redis://<username>:<password>@hostname` server URL schema.

Note that autodiscovered Redis nodes inherit their parent's SSL, username, and password settings.

#### Tweaks and Optimizations

As of version 1.2.0, Nchan uses Redis slaves to load-balance PUBSUB traffic. By default, there is an equal chance that a channel's PUBSUB subscription will go to any master or slave. The [`nchan_redis_subscribe_weights`](#nchan_redis_subscribe_weights) setting is available to fine-tune this load-balancing.

Also from 1.2.0 onward, [`nchan_redis_optimize_target`](#nchan_redis_optimize_target) can be used to prefer optimizing Redis slaves for CPU or bandwidth. For heavy publishing loads, the tradeoff is very roughly 35% replication bandwidth per slave to 30% CPU load on slaves.

#### Performance Statistics

Redis command statistics were added in version 1.3.5. These provide total number of times different Redis commands were run on, and the total amount of time they took. The stats are for a given Nchan server, *not* all servers connected to a Redis upstream. They are grouped by each upstream, and totaled per node.

```nginx
http {
  upstream my_redis_cluster {
    nchan_redis_server 127.0.0.1;
  }
  
  server {
    #[...]
    
    location ~ /nchan_redis_cluster_stats$ {
      nchan_redis_upstream_stats my_redis_cluster;
    }
  }

```

To get the stats, send a GET request to the stats location.

```console
  curl http://localhost/nchan_redis_cluster_stats
```

The response is JSON of the form:

```js
{
  "upstream": "redis_cluster",
  "nodes": [
    {
      "address"        : "127.0.0.1:7000",
      "id"             : "f13d71b1d14d8bf92b72cebee61421294e95dc72",
      "command_totals" : {
        "connect"    : {
          "msec"     : 357,
          "times"    : 5
        },
        "pubsub_subscribe": {
          "msec"     : 749,
          "times"    : 37
        },
        "pubsub_unsubsribe": {
          "msec"     : 332,
          "times"    : 37
        }
        /*[...]*/
      }
    },
    {
      "address"        : "127.0.0.1:7001",
      "id"             : "b768ecb4152912bed6dc927e8f70284191a79ed7",
      "command_totals" : {
        "connect"    : {
          "msec"     : 4281,
          "times"    : 5
        },
        "pubsub_subscribe": {
          "msec"     : 309,
          "times"    : 33
        },
        "pubsub_unsubsribe": {
          "msec"     : 307,
          "times"    : 30
        },
        /*[...]*/
      },
    }
    /*[...]*/
  ]
}
```

For brevity, the entire `command_totals` hash is omitted in this documentation.

<!-- commands: nchan_redis_upstream_stats nchan_redis_upstream_stats_disconnected_timeout nchan_redis_upstream_stats_enabled -->

## Introspection

There are several ways to see what's happening inside Nchan. These are useful for debugging application integration and for measuring performance.

### Channel Events

Channel events are messages automatically published by Nchan when certain events occur in a channel. These are very useful for debugging the use of channels. However, they carry a significant performance overhead and should be used during development, and not in production.

Channel events are published to special 'meta' channels associated with normal channels. Here's how to configure them:

```nginx
location ~ /pubsub/(.+)$ {
  nchan_pubsub;
  nchan_channel_id $1;
  nchan_channel_events_channel_id $1; #enables channel events for this location
}

location ~ /channel_events/(.+) {
  #channel events subscriber location
  nchan_subscriber;
  nchan_channel_group meta; #"meta" is a SPECIAL channel group
  nchan_channel_id $1;
}
```

Note the `/channel_events/...` location has a *special* `nchan_channel_group`, `meta`. This group is reserved for accessing "channel events channels", or"metachannels".

Now, say I subscribe to `/channel_events/foo` I will refer to this as the channel events subscriber.

Let's see what this channel events subscriber receives when I publish messages to 

Subscribing to `/pubsub/foo` produces the channel event
```
subscriber_enqueue foo
```

Publishing a message to `/pubsub/foo`:
```
channel_publish foo
```

Unsubscribing from `/pubsub/foo`:
```
subscriber_dequeue foo
```

Deleting `/pubsub/foo` (with HTTP `DELETE /pubsub/foo`):
```
channel_delete foo
```

The event string itself is configirable with [nchan_channel_event_string](#nchan_channel_event_string). By default, it is set to `$nchan_channel_event $nchan_channel_id`. 
This string can use any Nginx and [Nchan variables](/#variables).


### nchan_stub_status Stats

Like Nginx's [stub_status](https://nginx.org/en/docs/http/ngx_http_stub_status_module.html),
`nchan_stub_status` is used to get performance metrics.

```nginx
  location /nchan_stub_status {
    nchan_stub_status;
  }
```

Sending a GET request to this location produces the response:

```text
total published messages: 1906
stored messages: 1249
shared memory used: 1824K
channels: 80
subscribers: 90
redis pending commands: 0
redis connected servers: 0
redis unhealthy upstreams: 0
total redis commands sent: 0
total interprocess alerts received: 1059634
interprocess alerts in transit: 0
interprocess queued alerts: 0
total interprocess send delay: 0
total interprocess receive delay: 0
nchan version: 1.1.5
```

Here's what each line means, and how to interpret it:
  - `total published messages`: Number of messages published to all channels through this Nchan server.
  - `stored messages`: Number of messages currently buffered in memory
  - `shared memory used`: Total shared memory used for buffering messages, storing channel information, and other purposes. This value should be comfortably below `nchan_shared_memory_size`.
  - `channels`: Number of channels present on this Nchan server.
  - `subscribers`: Number of subscribers to all channels on this Nchan server.
  - `redis pending commands`: Number of commands sent to Redis that are awaiting a reply. May spike during high load, especially if the Redis server is overloaded. Should tend towards 0.
  - `redis connected servers`: Number of redis servers to which Nchan is currently connected.
  - `redis unhealthy upstreams`: Number of redis upstreams (individual server or cluster mode) that are currently not usable for publishing and subscribing.
  - `total redis commands sent`: Total number of commands this Nchan instance sent to Redis.
  - `total interprocess alerts received`: Number of interprocess communication packets transmitted between Nginx workers processes for Nchan. Can grow at 100-10000 per second at high load.
  - `interprocess alerts in transit`: Number of interprocess communication packets in transit between Nginx workers. May be nonzero during high load, but should always tend toward 0 over time.
  - `interprocess queued alerts`: Number of interprocess communication packets waiting to be sent. May be nonzero during high load, but should always tend toward 0 over time.
  - `total interprocess send delay`: Total amount of time interprocess communication packets spend being queued if delayed. May increase during high load.
  - `total interprocess receive delay`: Total amount of time interprocess communication packets spend in transit if delayed. May increase during high load.
  - `nchan_version`: current version of Nchan. Available for version 1.1.5 and above.

Additionally, when there is at least one `nchan_stub_status` location, this data is also available [through variables](#variables).
  
## Securing Channels

### Securing Publisher Endpoints
Consider the use case of an application where authenticated users each use a private, dedicated channel for live updates. The configuration might look like this:

```nginx
http {
  server {
    #available only on localhost
    listen  127.0.0.1:8080;
    location ~ /pub/(\w+)$ {
      nchan_publisher;
      nchan_channel_group my_app_group;
      nchan_channel_id $1;
    }
  }
  
  server {
    #available to the world
    listen 80;
    
    location ~ /sub/(\w+)$ {
      nchan_subscriber;
      nchan_channel_group my_app_group;
      nchan_channel_id $1;
    }
  }
}

```

Here, the subscriber endpoint is available on a public-facing port 80, and the publisher endpoint is only available on localhost, so can be accessed only by applications residing on that machine. Another way to limit access to the publisher endpoint is by using the allow/deny settings:

```nginx

  server {
    #available to the world
    listen 80; 
    location ~ /pub/(\w+)$ {
      allow 127.0.0.1;
      deny all;
      nchan_publisher;
      nchan_channel_group my_app_group;
      nchan_channel_id $1;
    }
```

Here, only the local IP 127.0.0.1 is allowed to use the publisher location, even though it is defined in a non-localhost server block.

### Keeping a Channel Private

A Channel ID that is meant to be private should be treated with the same care as a session ID token. Considering the above use case of one-channel-per-user, how can we ensure that only the authenticated user, and no one else, is able to access his channel? 

First, if you intend on securing the channel contents, you must use TLS/SSL:

```nginx 
http {
  server {
    #available only on localhost
    listen  127.0.0.1:8080;
    #...publisher endpoint config
  }
  server {
    #available to the world
    listen 443 ssl;
    #SSL config goes here
    location ~ /sub/(\w+)$ {
      nchan_subscriber;
      nchan_channel_group my_app_group;
      nchan_channel_id $1;
    }
  }
}
```

Now that you have a secure connection between the subscriber client and the server, you don't need to worry about the channel ID or messages being passively intercepted. This is a minimum requirement for secure message delivery, but it is not sufficient. 

You must also take care to do at least one of the following:
  - [Generate good, high-entropy Channel IDs](#good-ids).
  - [Authorize all subscribers with the `nchan_authorize_request` config directive](#request-authorization).
  - [Authorize subscribers and hide channel IDs with the "`X-Accel-Redirect`" mechanism](#x-accel-redirect).
  
#### Good IDs

An ID that can be guessed is an ID that can be hijacked. If you are not authenticating subscribers (as described below), a channel ID should be impossible to guess. Use at least 128 bits of entropy to generate a random token, associate it with the authenticated user, and share it only with the user's client. Do not reuse tokens, just as you would not reuse session IDs.

#### X-Accel-Redirect

This feature uses the [X-Accel feature](https://www.nginx.com/resources/wiki/start/topics/examples/x-accel) of Nginx upstream proxies to perform an internal request to a subscriber endpoint.
It allows a subscriber client to be authenticated by your application, and then redirected by nginx internally to a location chosen by your application (such as a publisher or subscriber endpoint). This makes it possible to have securely authenticated clients that are unaware of the channel id they are subscribed to.

Consider the following configuration:
```nginx 
upstream upstream_app {
  server 127.0.0.1:8080;
}

server {
  listen 80; 
  
  location = /sub_upstream {
    proxy_pass http://upstream_app/subscriber_x_accel_redirect;
    proxy_set_header X-Forwarded-For $remote_addr;
  }
  
  location ~ /sub/internal/(\w+)$ {
    internal; #this location only accessible for internal nginx redirects
    nchan_subscriber;
    nchan_channel_id $1;
    nchan_channel_group test;
  }
}
```

As commented, `/sub/internal/` is inaccessible from the outside:
```console
> curl  -v  http://127.0.0.1/sub/internal/foo
  
  < HTTP/1.1 404 Not Found
  < Server: nginx/1.9.5
  <
  <html>
  <head><title>404 Not Found</title></head>
  <body bgcolor="white">
  <center><h1>404 Not Found</h1></center>
  <hr><center>nginx/1.9.5</center>
  </body>
  </html>
```

But if a request is made to `/sub_upstream`, it gets forwarded to your application (`my_app`) on port 8080 with the url `/subscriber_x_accel_redirect`.
Note that you can set any forwarded headers here like any [`proxy_pass`](http://nginx.org/en/docs/http/ngx_http_proxy_module.html#proxy_pass) Nginx location, 
but unlike the case with `nchan_authorize_request`, Nchan-specific variables are not available.

Now, your application must be set up to handle the request to `/subscriber_x_accel_redirect`. You should make sure the client is properly authenticated (maybe using a session cookie), and generate an associated channel id. If authentication fails, respond with a normal `403 Forbidden` response. You can also pass extra information about the failure in the response body and headers.

If your application successfully authenticates the subscriber request, you now need to instruct Nginx to issue an internal redirect to `/sub/internal/my_channel_id`.
This is accomplished by responding with an empty `200 OK` response that includes two headers:
- `X-Accel-Redirect: /sub/internal/my_channel_id`
- `X-Accel-Buffering: no`

In the presence of these headers, Nginx will not forward your app's response to the client, and instead will *internally* redirect to `/sub/internal/my_channel_id`. 
This will behave as if the client had requested the subscriber endpoint location directly.

Thus using X-Accel-Redirect it is possible to both authenticate all subscribers *and* keep channel IDs completely hidden from subscribers.

This method is especially useful for EventSource and Websocket subscribers. Long-Polling subscribers will need to be re-authenticated for every new message, which may flood your application with too many authentication requests.

### Revoking Channel Authorization

In some cases, you may want to revoke a particular subscriber's authorization for a given channel (e.g., if the user's permissions are changed). If the channel is unique to the subscriber, this is simply accomplished by deleting the channel. The same can be achieved for shared channels by subscribing each subscriber to both the shared channel and a subscriber-specific channel via a multiplexed connection. Deleting the subscriber-specific channel will terminate the subscriber''s connection, thereby also terminating their subscription to the shared channel. Consider the following configuration:

```nginx
location ~ /sub/(\w+) {
  nchan_subscriber;
  nchan_channel_id shared_$1 user_$arg_userid;
  nchan_authorize_request /authorize;
}

location /pub/user {
  nchan_publisher;
  nchan_channel_id user_$arg_userid;
}
```

A request to `/sub/foo?userid=1234` will subscribe to channels "shared_foo" and "user_1234" via a multiplexed connection. If you later send a `DELETE` request to `/pub/user?userid=1234`, this subscriber will be disconnected and therefore unsubscribed from both "user_1234" and "shared_foo".
  
## Variables

Nchan makes several variables usabled in the config file:
 
- `$nchan_channel_id`  
  The channel id extracted from a publisher or subscriber location request. For multiplexed locations, this is the first channel id in the list.

- `$nchan_channel_id1`, `$nchan_channel_id2`, `$nchan_channel_id3`, `$nchan_channel_id4`  
  As above, but for the nth channel id in multiplexed channels.

- `$nchan_subscriber_type`  
  For subscriber locations, this variable is set to the subscriber type (websocket, longpoll, etc.).

- `$nchan_channel_subscriber_last_seen`  
  For publisher locations, this variable is set to the timestamp for the last connected subscriber.
  
- `$nchan_channel_subscriber_count`  
  For publisher locations, this variable is set to the number of subscribers in the published channel.
  
- `$nchan_channel_message_count`  
  For publisher locations, this variable is set to the number of messages buffered in the published channel.
  
- `$nchan_publisher_type`  
  For publisher locations, this variable is set to the subscriber type (http or websocket).
  
- `$nchan_prev_message_id`, `$nchan_message_id`  
  The current and previous (if applicable) message id for publisher request or subscriber response.

- `$nchan_channel_event`  
  For channel events, this is the event name. Useful when configuring `nchan_channel_event_string`.

- `$nchan_version`  
  Current Nchan version. Available since 1.1.5.
  
Additionally, `nchan_stub_status` data is also exposed as variables. These are available only when `nchan_stub_status` is enabled on at least one location:

- `$nchan_stub_status_total_published_messages`  
- `$nchan_stub_status_stored_messages`  
- `$nchan_stub_status_shared_memory_used`  
- `$nchan_stub_status_channels`  
- `$nchan_stub_status_subscribers`  
- `$nchan_stub_status_redis_pending_commands`  
- `$nchan_stub_status_redis_connected_servers`  
- `$nchan_stub_status_redis_unhealthy_upstreams`  
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

- **nchan_deflate_message_for_websocket** `[ on | off ]`  
  arguments: 1  
  default: `off`  
  context: server, location  
  > Store a compressed (deflated) copy of the message along with the original to be sent to websocket clients supporting the permessage-deflate protocol extension    

- **nchan_eventsource_event**  
  arguments: 1  
  default: `(none)`  
  context: server, location, if  
  > Set the EventSource `event:` line to this value. When used in a publisher location, overrides the published message's `X-EventSource-Event` header and associates the message with the given value. When used in a subscriber location, overrides all messages' associated `event:` string with the given value.    

- **nchan_eventsource_ping_comment**  
  arguments: 1  
  default: `(empty)`  
  context: server, location, if  
  > Set the EventSource comment `: ...` line for periodic pings from server to client. Newlines are not allowed. If empty, no comment is sent with the ping.    

- **nchan_eventsource_ping_data**  
  arguments: 1  
  default: `(empty)`  
  context: server, location, if  
  > Set the EventSource `data:` line for periodic pings from server to client. Newlines are not allowed. If empty, no data is sent with the ping.    

- **nchan_eventsource_ping_event**  
  arguments: 1  
  default: `ping`  
  context: server, location, if  
  > Set the EventSource `event:` line for periodic pings from server to client. Newlines are not allowed. If empty, no event type is sent with the ping.    

- **nchan_eventsource_ping_interval** `<number> (seconds)`  
  arguments: 1  
  default: `0 (none)`  
  context: server, location, if  
  > Interval for sending ping messages to EventSource subscribers. Disabled by default.    

- **nchan_longpoll_multipart_response** `[ off | on | raw ]`  
  arguments: 1  
  default: `off`  
  context: server, location, if  
  > when set to 'on', enable sending multiple messages in a single longpoll response, separated using the multipart/mixed content-type scheme. If there is only one available message in response to a long-poll request, it is sent unmodified. This is useful for high-latency long-polling connections as a way to minimize round-trips to the server. When set to 'raw', sends multiple messages using the http-raw-stream message separator.    

- **nchan_permessage_deflate_compression_level** `[ 0-9 ]`  
  arguments: 1  
  default: `6`  
  context: http  
  > Compression level for the `deflate` algorithm used in websocket's permessage-deflate extension. 0: no compression, 1: fastest, worst, 9: slowest, best    

- **nchan_permessage_deflate_compression_memlevel** `[ 1-9 ]`  
  arguments: 1  
  default: `8`  
  context: http  
  > Memory level for the `deflate` algorithm used in websocket's permessage-deflate extension. How much memory should be allocated for the internal compression state. 1 - minimum memory, slow and reduces compression ratio; 9 - maximum memory for optimal speed    

- **nchan_permessage_deflate_compression_strategy** `[ default | filtered | huffman-only | rle | fixed ]`  
  arguments: 1  
  default: `default`  
  context: http  
  > Compression strategy for the `deflate` algorithm used in websocket's permessage-deflate extension. Use 'default' for normal data, For details see [zlib's section on copression strategies](http://zlib.net/manual.html#Advanced)    

- **nchan_permessage_deflate_compression_window** `[ 9-15 ]`  
  arguments: 1  
  default: `10`  
  context: http  
  > Compression window for the `deflate` algorithm used in websocket's permessage-deflate extension. The base two logarithm of the window size (the size of the history buffer). The bigger the window, the better the compression, but the more memory used by the compressor.    

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
  [more details](#message-forwarding)  

- **nchan_pubsub** `[ http | websocket | eventsource | longpoll | intervalpoll | chunked | multipart-mixed | http-raw-stream ]`  
  arguments: 0 - 6  
  default: `http websocket eventsource longpoll chunked multipart-mixed`  
  context: server, location, if  
  > Defines a server or location as a pubsub endpoint. For long-polling, GETs subscribe. and POSTs publish. For Websockets, publishing data on a connection does not yield a channel metadata response. Without additional configuration, this turns a location into an echo server.    
  [more details](#pubsub-endpoint)  

- **nchan_subscribe_request** `<url>`  
  arguments: 1  
  context: server, location, if  
  > Send GET request to internal location (which may proxy to an upstream server) after subscribing. Disabled for longpoll and interval-polling subscribers.    
  [more details](#subscriber-presence)  

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
  > Message separator string for the http-raw-stream subscriber. Automatically terminated with a newline character if not explicitly set to an empty string.    

- **nchan_subscriber_info**  
  arguments: 0  
  context: location  
  > A subscriber location for debugging the state of subscribers on a given channel. The subscribers of the channel specified by `nchan_channel_id` evaluate `nchan_subscriber_info_string` and send it back to the requested on this location. This is useful to see where subscribers are in an Nchan cluster, as well as debugging subscriber connection issues.    

- **nchan_subscriber_info_string**  
  arguments: 1  
  default: `$nchan_subscriber_type $remote_addr:$remote_port $http_user_agent $server_name $request_uri $pid`  
  context: server, location  
  > this string is evaluated by each subscriber on a given channel and sent to the requester of a `nchan_subscriber_info` location    

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

- **nchan_unsubscribe_request** `<url>`  
  arguments: 1  
  context: server, location, if  
  > Send GET request to internal location (which may proxy to an upstream server) after unsubscribing. Disabled for longpoll and interval-polling subscribers.    
  [more details](#subscriber-presence)  

- **nchan_websocket_client_heartbeat** `<heartbeat_in> <heartbeat_out>`  
  arguments: 2  
  default: `none (disabled)`  
  context: server, location, if  
  > Most browser Websocket clients do not allow manually sending PINGs to the server. To overcome this limitation, this setting can be used to set up a PING/PONG message/response connection heartbeat. When the client sends the server message *heartbeat_in* (PING), the server automatically responds with *heartbeat_out* (PONG).    

- **nchan_websocket_ping_interval** `<number> (seconds)`  
  arguments: 1  
  default: `0 (none)`  
  context: server, location, if  
  > Interval for sending websocket ping frames. Disabled by default.    

- **nchan_access_control_allow_credentials**  
  arguments: 1  
  default: `on`  
  context: http, server, location, if  
  > When enabled, sets the [Cross-Origin Resource Sharing (CORS)](https://developer.mozilla.org/en-US/docs/Web/HTTP/Access_control_CORS) `Access-Control-Allow-Credentials` header to `true`.    

- **nchan_access_control_allow_origin** `<string>`  
  arguments: 1  
  default: `$http_origin`  
  context: http, server, location, if  
  > Set the [Cross-Origin Resource Sharing (CORS)](https://developer.mozilla.org/en-US/docs/Web/HTTP/Access_control_CORS) `Access-Control-Allow-Origin` header to this value. If the incoming request's `Origin` header does not match this value, respond with a `403 Forbidden`. Multiple origins can be provided in a single argument separated with a space.    

- **nchan_authorize_request** `<url>`  
  arguments: 1  
  context: server, location, if  
  > Send GET request to internal location (which may proxy to an upstream server) for authorization of a publisher or subscriber request. A 200 response authorizes the request, a 403 response forbids it.    
  [more details](#request-authorization)  

- **nchan_channel_group** `<string>`  
  arguments: 1  
  default: `(none)`  
  context: server, location, if  
  legacy name: push_channel_group  
  > The accounting and security group a channel belongs to. Works like a prefix string to the channel id. Can be set with nginx variables.    

- **nchan_channel_group_accounting**  
  arguments: 1  
  default: `off`  
  context: server, location  
  > Enable tracking channel, subscriber, and message information on a per-channel-group basis. Can be used to place upper limits on channel groups.    

- **nchan_group_location** `[ get | set | delete | off ]`  
  arguments: 0 - 3  
  default: `get set delete`  
  context: location  
  > Group information and configuration location. GET request for group info, POST to set limits, DELETE to delete all channels in group.    

- **nchan_group_max_channels** `<number>`  
  arguments: 1  
  default: `0 (unlimited)`  
  context: location  
  > Maximum number of channels allowed in the group.    

- **nchan_group_max_messages** `<number>`  
  arguments: 1  
  default: `0 (unlimited)`  
  context: location  
  > Maximum number of messages allowed for all the channels in the group.    

- **nchan_group_max_messages_disk** `<number>`  
  arguments: 1  
  default: `0 (unlimited)`  
  context: location  
  > Maximum amount of disk space allowed for the messages of all the channels in the group.    

- **nchan_group_max_messages_memory** `<number>`  
  arguments: 1  
  default: `0 (unlimited)`  
  context: location  
  > Maximum amount of shared memory allowed for the messages of all the channels in the group.    

- **nchan_group_max_subscribers** `<number>`  
  arguments: 1  
  default: `0 (unlimited)`  
  context: location  
  > Maximum number of subscribers allowed for the messages of all the channels in the group.    

- **nchan_max_channel_id_length** `<number>`  
  arguments: 1  
  default: `1024`  
  context: http, server, location  
  legacy name: push_max_channel_id_length  
  > Maximum permissible channel id length (number of characters). This settings applies to ids before they may be split by the `nchan_channel_id_split_delimiter` Requests with a channel id that is too long will receive a `403 Forbidden` response.    

- **nchan_max_channel_subscribers** `<number>`  
  arguments: 1  
  default: `0 (unlimited)`  
  context: http, server, location  
  legacy name: push_max_channel_subscribers  
  > Maximum concurrent subscribers to the channel on this Nchan server. Does not include subscribers on other Nchan instances when using a shared Redis server.    

- **nchan_subscribe_existing_channels_only** `[ on | off ]`  
  arguments: 1  
  default: `off`  
  context: http, server, location  
  legacy name: push_authorized_channels_only  
  > Whether or not a subscriber may create a channel by sending a request to a subscriber location. If set to on, a publisher must send a POST or PUT request before a subscriber can request messages on the channel. Otherwise, all subscriber requests to nonexistent channels will get a 403 Forbidden response.    

- **nchan_message_buffer_length** `[ <number> | <variable> ]`  
  arguments: 1  
  default: `10`  
  context: http, server, location  
  legacy names: push_max_message_buffer_length, push_message_buffer_length  
  > Publisher configuration setting the maximum number of messages to store per channel. A channel's message buffer will retain a maximum of this many most recent messages. An Nginx variable can also be used to set the buffer length dynamically.    

- **nchan_message_temp_path** `<path>`  
  arguments: 1  
  default: `<client_body_temp_path>`  
  context: http  
  > Large messages are stored in temporary files in the `client_body_temp_path` or the `nchan_message_temp_path` if the former is unavailable. Default is the built-in default `client_body_temp_path`    

- **nchan_message_timeout** `[ <time> | <variable> ]`  
  arguments: 1  
  default: `1h`  
  context: http, server, location  
  legacy name: push_message_timeout  
  > Publisher configuration setting the length of time a message may be queued before it is considered expired. If you do not want messages to expire, set this to 0. Note that messages always expire from oldest to newest, so an older message may prevent a newer one with a shorter timeout from expiring. An Nginx variable can also be used to set the timeout dynamically.    

- **nchan_redis_accurate_subscriber_count**  
  arguments: 1  
  default: `off`  
  context: upstream  
  > When disabled, use fast but potentially inaccurate subscriber counts. These may become inaccurate if Nginx workers exit uncleanly or are terminated. When enabled, use a slightly slower but completely accurate subscriber count. Defaults to 'off' for legacy reasons, but will be enabled by default in the future.    

- **nchan_redis_cluster_check_interval_backoff** `<floating point> >= 0, ratio of current delay`  
  arguments: 1  
  default: `2 (increase delay by 200% each try)`  
  context: upstream  
  > Add an exponentially increasing delay to the Redis cluster check interval. `Delay[n] = (Delay[n-1] + jitter) * (nchan_redis_cluster_check_interval_backoff + 1)`.    

- **nchan_redis_cluster_check_interval_jitter** `<floating point> >= 0, (0 to disable)`  
  arguments: 1  
  default: `0.2 (20% of interval value)`  
  context: upstream  
  > Introduce random jitter to Redis cluster check interval, where the range is `±(cluster_check_interval * nchan_redis_cluster_check_interval_jitter) / 2`.    

- **nchan_redis_cluster_check_interval_max** `<time> (0 to disable)`  
  arguments: 1  
  default: `30s`  
  context: upstream  
  > Maximum Redis cluster check interval after backoff and jitter.    

- **nchan_redis_cluster_check_interval_min** `<time>`  
  arguments: 1  
  default: `1s (0 to disable)`  
  context: upstream  
  > When connected to a cluster, periodically check the cluster state and layout via a random master node.    

- **nchan_redis_cluster_connect_timeout**  
  arguments: 1  
  default: `15s`  
  context: upstream  
  > Redis cluster connection timeout.    

- **nchan_redis_cluster_max_failing_time**  
  arguments: 1  
  default: `30s`  
  context: upstream  
  > Maximum time a Redis cluster can be in a failing state before Nchan disconnects from it. During this time, Nchan will try to recover from a cluster or node failure without disconnecting the entire cluster.    

- **nchan_redis_cluster_recovery_delay** `<time>`  
  arguments: 1  
  default: `100ms`  
  context: upstream  
  > After a cluster recovery failure, wait this long to try again.    

- **nchan_redis_cluster_recovery_delay_backoff** `<floating point> >= 0, ratio of current delay`  
  arguments: 1  
  default: `0.5 (increase delay by 50% each try)`  
  context: upstream  
  > Add an exponentially increasing delay to Redis cluster recovery retries. `Delay[n] = (Delay[n-1] + jitter) * (nchan_redis_cluster_recovery_delay_backoff + 1)`.    

- **nchan_redis_cluster_recovery_delay_jitter** `<floating point> >= 0, (0 to disable)`  
  arguments: 1  
  default: `0.5 (50% of delay value)`  
  context: upstream  
  > Introduce random jitter to Redis cluster recovery retry time, where the range is `±(recovery_delay * nchan_redis_cluster_recovery_delay_jitter) / 2`.    

- **nchan_redis_cluster_recovery_delay_max** `<time> (0 to disable)`  
  arguments: 1  
  default: `2s`  
  context: upstream  
  > Maximum Redis cluster recovery delay after backoff and jitter.    

- **nchan_redis_command_timeout** `<time> (0 to leave unlimited)`  
  arguments: 1  
  default: `5s`  
  context: upstream  
  > If a Redis server exceeds this time to produce a command reply, it is considered unhealthy and is disconnected.    

- **nchan_redis_connect_timeout**  
  arguments: 1  
  default: `10s`  
  context: upstream  
  > Redis server connection timeout.    

- **nchan_redis_discovered_ip_range_blacklist** `<CIDR range>`  
  arguments: 1 - 7  
  context: upstream  
  > do not attempt to connect to **autodiscovered** nodes with IPs in the specified ranges. Useful for blacklisting private network ranges for clusters and Redis slaves. NOTE that this blacklist applies only to autodiscovered nodes, and not ones specified in the upstream block    

- **nchan_redis_idle_channel_cache_timeout** `<time>`  
  arguments: 1  
  default: `30s`  
  context: http, server, location  
  > A Redis-stored channel and its messages are removed from memory (local cache) after this timeout, provided there are no local subscribers.    

- **nchan_redis_namespace** `<string>`  
  arguments: 1  
  context: http, server, upstream, location  
  > Prefix all Redis keys with this string. All Nchan-related keys in redis will be of the form "nchan_redis_namespace:*" . Default is empty.    

- **nchan_redis_nostore_fastpublish** `[ on | off ]`  
  arguments: 1  
  default: `off`  
  context: http, server, upstream  
  > Increases publishing capacity by 2-3x for Redis nostore mode at the expense of inaccurate subscriber counts in the publisher response.    

- **nchan_redis_optimize_target** `[ cpu | bandwidth ]`  
  arguments: 1  
  default: `bandwidth`  
  context: upstream  
  > This tweaks whether [effect replication](https://redis.io/commands/eval#replicating-commands-instead-of-scripts) is enabled. This setting is obsolete, as effect replication is now always enabled to support other features    

- **nchan_redis_pass** `<upstream-name>`  
  arguments: 1  
  context: http, server, location  
  > Use an upstream config block for Redis servers.    
  [more details](#connecting-to-a-redis-server)  

- **nchan_redis_password**  
  arguments: 1  
  default: `<none>`  
  context: upstream  
  > Set Redis password for AUTH command. All servers in the upstream block will use this password _unless_ a different password is specified by a server URL.    

- **nchan_redis_ping_interval**  
  arguments: 1  
  default: `4m`  
  context: http, server, upstream, location  
  > Send a keepalive command to redis to keep the Nchan redis clients from disconnecting. Set to 0 to disable.    

- **nchan_redis_reconnect_delay** `<time>`  
  arguments: 1  
  default: `500ms`  
  context: upstream  
  > After a connection failure, wait this long before trying to reconnect to Redis.    

- **nchan_redis_reconnect_delay_backoff** `<floating point> >= 0 (0 to disable)`  
  arguments: 1  
  default: `0.5 (increase delay by 50% each try)`  
  context: upstream  
  > Add an exponentially increasing delay to Redis connection retries. `Delay[n] = (Delay[n-1] + jitter) * (nchan_redis_reconnect_delay_backoff + 1)`.    

- **nchan_redis_reconnect_delay_jitter** `<floating point> >= 0 (0 to disable)`  
  arguments: 1  
  default: `0.1 (10% of delay value)`  
  context: upstream  
  > Introduce random jitter to Redis reconnection time, where the range is `±(reconnect_delay * nchan_redis_reconnect_delay_jitter) / 2`.    

- **nchan_redis_reconnect_delay_max** `<time> (0 to disable)`  
  arguments: 1  
  default: `10s`  
  context: upstream  
  > Maximum Redis reconnection delay after backoff and jitter.    

- **nchan_redis_retry_commands**  
  arguments: 1  
  default: `on`  
  context: upstream  
  > Allow Nchan to retry some Redis commands on keyslot errors and cluster unavailability. Queuing up a lot of commands while the cluster is unavailable may lead to excessive memory use, but it can also defer commands during transient failures.    

- **nchan_redis_retry_commands_max_wait** `<time> (0 to leave unlimited)`  
  arguments: 1  
  default: `500ms`  
  context: upstream  
  > When `nchan_redis_retry_commands` is on, the maximum time a command will stayed queued to be retried.    

- **nchan_redis_server** `<redis-url>`  
  arguments: 1  
  context: upstream  
  > Used in upstream { } blocks to set redis servers. Redis url is in the form 'redis://:password@hostname:6379/0'. Shorthands 'host:port' or 'host' are permitted.    
  [more details](#connecting-to-a-redis-server)  

- **nchan_redis_ssl** `[ on | off ]`  
  arguments: 1  
  default: `off`  
  context: upstream  
  > Enables SSL/TLS for all connections to Redis servers in this upstream block. When enabled, no unsecured connections are permitted    

- **nchan_redis_ssl_ciphers**  
  arguments: 1  
  default: `<system default>`  
  context: upstream  
  > Acceptable ciphers when using TLS for Redis connections    

- **nchan_redis_ssl_client_certificate**  
  arguments: 1  
  context: upstream  
  > Path to client certificate when using TLS for Redis connections    

- **nchan_redis_ssl_client_certificate_key**  
  arguments: 1  
  context: upstream  
  > Path to client certificate key when using TLS for Redis connections    

- **nchan_redis_ssl_server_name**  
  arguments: 1  
  context: upstream  
  > Server name to verify (CN) when using TLS for Redis connections    

- **nchan_redis_ssl_trusted_certificate**  
  arguments: 1  
  context: upstream  
  > Trusted certificate (CA) when using TLS for Redis connections    

- **nchan_redis_ssl_trusted_certificate_path**  
  arguments: 1  
  default: `<system default>`  
  context: upstream  
  > Trusted certificate (CA) when using TLS for Redis connections. Defaults to the system's SSL cert path unless nchan_redis_ssl_trusted_certificate is set    

- **nchan_redis_ssl_verify_certificate** `[ on | off ]`  
  arguments: 1  
  default: `on`  
  context: upstream  
  > Should the server certificate be verified when using TLS for Redis connections? Useful to disable when testing with a self-signed server certificate.    

- **nchan_redis_storage_mode** `[ distributed | backup | nostore ]`  
  arguments: 1  
  default: `distributed`  
  context: http, server, upstream, location  
  > The mode of operation of the Redis server. In `distributed` mode, messages are published directly to Redis, and retrieved in real-time. Any number of Nchan servers in distributed mode can share the Redis server (or cluster). Useful for horizontal scalability, but suffers the latency penalty of all message publishing going through Redis first.  
  >   
  > In `backup` mode, messages are published locally first, then later forwarded to Redis, and are retrieved only upon channel initialization. Only one Nchan server should use a Redis server (or cluster) in this mode. Useful for data persistence without sacrificing response times to the latency of a round-trip to Redis.  
  >   
  > In `nostore` mode, messages are published as in `distributed` mode, but are not stored. Thus Redis is used to broadcast messages to many Nchan instances with no delivery guarantees during connection failure, and only local in-memory storage. This means that there are also no message delivery guarantees for subscribers switching from one Nchan instance to another connected to the same Redis server or cluster. Nostore mode increases Redis publishing capacity by an order of magnitude.    

- **nchan_redis_subscribe_weights** `master=<integer> slave=<integer>`  
  arguments: 1 - 2  
  default: `master=1 slave=1`  
  context: upstream  
  > Determines how subscriptions to Redis PUBSUB channels are distributed between master and slave nodes. The higher the number, the more likely that each node of that type will be chosen for each new channel. The weights for slave nodes are cumulative, so an equal 1:1 master:slave weight ratio with two slaves would have a 1/3 chance of picking a master, and 2/3 chance of picking one of the slaves. The weight must be a non-negative integer.    

- **nchan_redis_upstream_stats** `<upstream_name>`  
  arguments: 1  
  default: `(none)`  
  context: server, location  
  > Defines a location as redis statistics endpoint. GET requests to this location produce a JSON response with detailed listings of total Redis command times and number of calls, broken down by node and command type. Useful for making graphs about Redis performance. Can be set with nginx variables.    

- **nchan_redis_upstream_stats_disconnected_timeout**  
  arguments: 1  
  default: `5m`  
  context: upstream  
  > Keep stats for disconnected nodes around for this long. Useful for tracking stats for nodes that have intermittent connectivity issues.    

- **nchan_redis_upstream_stats_enabled** `[ on | off ]`  
  arguments: 1  
  default: `<on> if at least 1 redis stats location is configured, otherwise <off>`  
  context: upstream  
  > Gather Redis node command timings for this upstream    

- **nchan_redis_url** `<redis-url>`  
  arguments: 1  
  default: `127.0.0.1:6379`  
  context: http, server, location  
  > Use of this command is discouraged in favor of upstreams blocks with (`nchan_redis_server`)[#nchan_redis_server]. The path to a redis server, of the form 'redis://:password@hostname:6379/0'. Shorthand of the form 'host:port' or just 'host' is also accepted.    
  [more details](#connecting-to-a-redis-server)  

- **nchan_redis_username**  
  arguments: 1  
  default: `<none>`  
  context: upstream  
  > Set Redis username for AUTH command (available when using ACLs on the Redis server). All servers in the upstream block will use this username _unless_ a different username is specified by a server URL.    

- **nchan_shared_memory_size** `<size>`  
  arguments: 1  
  default: `128M`  
  context: http  
  legacy names: push_max_reserved_memory, nchan_max_reserved_memory  
  > Shared memory slab pre-allocated for Nchan. Used for channel statistics, message storage, and interprocess communication.    
  [more details](#memory-storage)  

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
  > Use of this command is discouraged in favor of (`nchan_redis_pass`)[#nchan_redis_pass]. Use Redis for message storage at this location.    
  [more details](#connecting-to-a-redis-server)  

- **nchan_channel_event_string** `<string>`  
  arguments: 1  
  default: `"$nchan_channel_event $nchan_channel_id"`  
  context: server, location, if  
  > Contents of channel event message    

- **nchan_channel_events_channel_id**  
  arguments: 1  
  context: server, location, if  
  > Channel id where `nchan_channel_id`'s events should be sent. Events like subscriber enqueue/dequeue, publishing messages, etc. Useful for application debugging. The channel event message is configurable via nchan_channel_event_string. The channel group for events is hardcoded to 'meta'.    
  [more details](#channel-events)  

- **nchan_stub_status**  
  arguments: 0  
  context: location  
  > Similar to Nginx's stub_status directive, requests to an `nchan_stub_status` location get a response with some vital Nchan statistics. This data does not account for information from other Nchan instances, and monitors only local connections, published messages, etc.    
  [more details](#nchan_stub_status)  

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
Please support this project with a donation to keep me warm through the winter. I accept bitcoin at 15dLBzRS4HLRwCCVjx4emYkxXcyAPmGxM3 . Other donation methods can be found at https://nchan.io
