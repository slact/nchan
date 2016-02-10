<img class="logo" alt="NCHAN" src="https://nchan.slact.net/github-logo.png" />

https://nchan.slact.net

Nchan is a scalable, flexible pub/sub server for the modern web, built as a module for the [Nginx](http://nginx.org) web server. It can be configured as a standalone server, or as a shim between your application and tens, thousands, or millions of live subscribers. It can buffer messages in memory, on-disk, or via [Redis](http://redis.io). All connections are handled asynchronously and distributed among any number of worker processes. It can also scale to many nginx server instances with [Redis](http://redis.io).

Messages are [published](#publisher-endpoints) to channels with HTTP `POST` requests or websockets, and [subscribed](#subscriber-endpoint) also through websockets, [long-polling](#long-polling), [EventSource](#eventsource) (SSE), old-fashioned [interval polling](#interval-polling), [and](#http-chunked-transfer) [more](#http-multipart-mixed). Each subscriber can listen to [up to 255 channels](#channel-multiplexing) per connection, and can be optionally [authenticated](https://nchan.slact.net/details#authenticate-with-nchan_authorize_request) via a custom application url. An [events](#nchan_channel_event_string) [meta channel](#nchan_channel_events_channel_id) is also available for debugging.

## Status and History

The latest Nchan release is v0.99.3 (February 10, 2016) ([changelog](https://nchan.slact.net/changelog)). This is a *beta* release. There may be some bugs but Nchan is already stable and well-tested.

The first iteration of Nchan was written in 2009-2010 as the [Nginx HTTP Push Module](https://pushmodule.slact.net), and was vastly refactored into its present state in 2014-2016. The present release is in the **testing** phase. The core features and old functionality are thoroughly tested and stable. Some of the new functionality, specifically *redis storage and channel events are still experimental* and may be a bit buggy.

Please help make the entire codebase ready for production use! Report any quirks, bugs, leaks, crashes, or larvae you find.

#### Upgrade from Nginx HTTP Push Module

Although Nchan is backwards-compatible with all Push Module configuration directives, some of the more unusual and rarely used settings have been disabled and will be ignored (with a warning). See the [upgrade page](https://nchan.slact.net/upgrade) for a detailed list of changes and improvements, as well as a full list of incompatibilities.


## Does it scale?

<img class="benchmark_graph" alt="benchmarking internal subscriber response times" src="https://nchan.slact.net/img/benchmark_internal_total.png" />

Yes it does. Like Nginx, Nchan can easily handle as much traffic as you can throw at it. I've tried to benchmark it, but my benchmarking tools are much slower than Nchan. The data I've gathered is on how long Nchan itself takes to respond to every subscriber after publishing a message -- this excludes TCP handshake times and internal HTTP request parsing. Basically, it measures how Nchan scales assuming all other components are already tuned for scalability. The graphed data are averages of 5 runs with 50-byte messages.

With a well-tuned OS and network stack on commodity server hardware, expect to handle upwards of 300K concurrent subscribers per second at minimal CPU load. Nchan can also be scaled out to multiple Nginx instances using the [Redis storage engine](#nchan_use_redis).

Currently, Nchan's performance is limited by available memory bandwidth. This can be improved significantly in future versions with fewer allocations and the use of contiguous memory pools. Please consider supporting Nchan to speed up the work of memory cache optimization.

## Download

#### Packages
 - [Arch Linux](https://archlinux.org): [nginx-nchan](https://aur.archlinux.org/packages/nginx-nchan/) and [nginx-nchan-git](https://aur.archlinux.org/packages/nginx-nchan-git/) are available in the Arch User Repository.  
 - Mac OS X: a [homebrew](http://brew.sh) package is available. `brew tap homebrew/nginx; brew install nginx-full --with-nchan-module`
 - [Debian](https://www.debian.org/) and [Ubuntu](http://www.ubuntu.com/): [nginx-common](https://nchan.slact.net/download/nginx-common.deb) and [nginx-extras](https://nchan.slact.net/download/nginx-extras.deb), compiled with Nchan. Download both and install them with `dpkg -i`. These packages should soon be available directly from the Debian repository.
 - [Ubuntu](http://www.ubuntu.com/): For Ubuntu 'trusty' (14.04 LTS), install `nginx-common` from the repo, then download [nginx-extras.ubuntu.deb](https://nchan.slact.net/download/nginx-extras.ubuntu.deb). This build is based on nginx 1.4, which is the version bundled with ubuntu `trusty`, and is rather outdated. You can also use the debian packages.
 - [Fedora](https://fedoraproject.org): A 64-bit binary rpm and a source rpm are available: [nginx-nchan.x86_64.rpm](https://nchan.slact.net/download/nginx-nchan.x86-64.rpm), [ngx-nchan.src.rpm](https://nchan.slact.net/download/nginx-nchan.src.rpm). 
 - A statically compiled binary and associated linux nginx installation files are also [available as a tarball](https://nchan.slact.net/download/nginx-nchan-latest.tar.gz).


#### From Source
Grab the latest copy of Nginx from [nginx.org](http://nginx.org). Grab the latest Nchan source from [github](https://github.com/slact/nchan/releases). Follow the instructions for [building Nginx](https://www.nginx.com/resources/wiki/start/topics/tutorials/install/#source-releases), except during the `configure` stage, add
```
./configure --add-module=path/to/nchan ...
```
Run `make`, `make install`, and enjoy. (Caution, contents may be hot.)

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

#### Subscriber Endpoint

Subscriber endpoints are Nginx config *locations* with the [*`nchan_subscriber`*](#nchan_subscriber) directive.

Nchan supports several different kinds of subscribers for receiving messages: [*Websocket*](#websocket), [*EventSource*](#eventsource) (Server Sent Events),  [*Long-Poll*](#long-polling), [*Interval-Poll*](#interval-polling). [*HTTP chunked transfer*](#http-chunked-transfer), and [*HTTP multipart/mixed*](#http-multipart-mixed).

- ##### Long-Polling
  The tried-and-true server-push method supported by every browser out there.  
  Initiated by sending an HTTP `GET` request to a channel subscriber endpoint.  
  The long-polling subscriber walks through a channel's message queue via the built-in cache mechanism of HTTP clients, namely with the "`Last-Modified`" and "`Etag`" headers. Explicitly, to receive the next message for given a long-poll subscriber response, send a request with the "`If-Modified-Since`" header set to the previous response's "`Last-Modified`" header, and "`If-None-Match`" likewise set to the previous response's "`Etag`" header.  
  Sending a request without a "`If-Modified-Since`" or "`If-None-Match`" headers returns the oldest message in a channel's message queue, or waits until the next published message, depending on the value of the `nchan_subscriber_first_message` config directive.  
  A message's associated content type, if present, will be sent to this subscriber with the `Content-Type` header.
  
- ##### Interval-Polling
  Works just like long-polling, except if the requested message is not yet available, immediately responds with a `304 Not Modified`.
  There is no way to differentiate between long-poll and interval-poll subscriber requests, so long-polling must be disabled for a subscriber location if you wish to use interval-polling.

- ##### Websocket
  Bidirectional communication for web browsers. Part of the [HTML5 spec](http://www.w3.org/TR/2014/REC-html5-20141028/single-page.html). Nchan supports the latest protocol version 13 ([RFC 6455](https://tools.ietf.org/html/rfc6455)).  
  Initiated by sending a websocket handshake to the desired subscriber endpoint location.  
  If the websocket connection is closed by the server, the `close` frame will contain the HTTP response code and status line describing the reason for closing the connection. Server-initiated keep-alive pings can be configured with the [`nchan_websocket_ping_interval`](#nchan_websocket_ping_interval) config directive. Websocket extensions and subprotocols are not yet supported.  
  Messages published through a websocket connection can be forwarded to an upstream application with the [`nchan_publisher_upstream_request`](nchan_publisher_upstream_request) config directive.
  
- ##### EventSource
  Also known as [Server-Sent Events](https://en.wikipedia.org/wiki/Server-sent_events) or SSE, it predates Websockets in the [HTML5 spec](http://www.w3.org/TR/2014/REC-html5-20141028/single-page.html), and is a [very simple protocol](http://www.w3.org/TR/eventsource/#event-stream-interpretation).  
  Initiated by sending an HTTP `GET` request to a channel subscriber endpoint with the "`Accept: text/event-stream`" header.    
  Each message `data: ` segment will be prefaced by the message `id: `.  
  To resume a closed EventSource connection from the last-received message, one *should* start the connection with the "`Last-Event-ID`" header set to the last message's `id`.  
  Unfortunately, browsers [don't support setting](http://www.w3.org/TR/2011/WD-eventsource-20111020/#concept-event-stream-last-event-id) this header for an `EventSource` object, so by default the last message id is set either from the "`Last-Event-Id`" header or the `last_event_id` url query string argument.  
  This behavior can be configured via the [`nchan_subscriber_last_message_id`](#nchan_subscriber_last_message_id) config.  
  A message's associated `event` type, if present, will be sent to this subscriber with the `event:` line.
  
- ##### HTTP [multipart/mixed](http://www.w3.org/Protocols/rfc1341/7_2_Multipart.html#z0)
  The `multipart/mixed` MIMEtype was conceived for emails, but hey, why not use it for HTTP? It's easy to parse and includes metadata with each message.  
  Initiated by including an `Accept: multipart/mixed` header.  
  The response headers and the unused "preamble" portion of the response body are sent right away, with the boundary string generated randomly for each subscriber.  Each subsequent message will be sent as one part of the multipart message, and will include the message time and tag (`Last-Modified` and `Etag`) as well as the optional `Content-Type` headers.  
  Each message is terminated with the next multipart message's boundary **without a trailing newline**. While this conforms to the multipart spec, it is unusual as multipart messages are defined as *starting*, rather than ending with a boundary.  
  A message's associated content type, if present, will be sent to this subscriber with the `Content-Type` header.

- ##### HTTP [Chunked Transfer](http://www.w3.org/Protocols/rfc2616/rfc2616-sec3.html#sec3.6.1)
  This subscription method uses the `chunked` `Transfer-Encoding` to receive messages.   
  Initiated by explicitly including `chunked` in the [`TE` header](http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.39):  
  `TE: chunked` (or `TE: chunked;q=??` where the qval > 0)  
  The response headers are sent right away, and each message will be sent as an individual chunk. Note that because a zero-length chunk terminates the transfer, **zero-length messages will not be sent** to the subscriber.  
  Unlike the other subscriber types, the `chunked` subscriber cannot be used with http/2 because it dissallows chunked encoding.
  
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

#### Channel Multiplexing

Any subscriber location can be an endpoint for up to 255 channels. Messages published to all the specified channels will be delivered in-order to the subscriber. There are two ways to enable multiplexing:

Up to 7 channel ids can be specified for the `nchan_channel_id` or `nchan_channel_subscriber_id` config directive:

```nginx
  location ~ /multisub/(\w+)/(\w+)$ {
    nchan_subscriber;
    nchan_channel_id "$1" "$2" "common_channel";
    #GET /multisub/foo/bar will be subscribed to:
    # channels 'foo', 'bar', and 'common_channel',
    #and will received messages from all of the above.
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
    #and will received messages from all of the above.
  }
```

`DELETE` requests on any channel are forwarded to relevant multi-channel subscribers, and their connections are terminated.

Publishing to multiple channels with a single request is also possible, with similar configuration:

```nginx
  location ~ /multipub/(\w+)/(\w+)$ {
    nchan_publisher;
    nchan_channel_id "$1" "$2" "another_channel";
  }
```
			    

## Configuration Directives

- **nchan_channel_id**  
  arguments: 1 - 7  
  default: `(none)`  
  context: server, location, if  
  > Channel id for a publisher or subscriber location. Can have up to 4 values to subscribe to up to 4 channels.    

- **nchan_channel_id_split_delimiter**  
  arguments: 1  
  default: `(none)`  
  context: server, location, if  
  > Split the channel id into several ids for multiplexing using the delimiter string provided.    

- **nchan_eventsource_event**  
  arguments: 1  
  default: `(none)`  
  context: server, location, if  
  > Set the EventSource `event:` line to this value. When used in a publisher location, overrides the published message's `X-EventSource-Event` header and associates the message with the given value. When used in a subscriber location, overrides all messages' associated `event:` string with the given value.    

- **nchan_longpoll_multipart_response**  
  arguments: 1  
  default: `off`  
  context: server, location, if  
  > Enable sending multiple messages in a single longpoll response, separated using the multipart/mixed content-type scheme. If there is only one available message in response to a long-poll request, it is sent unmodified. This is useful for high-latency long-polling connections as a way to minimize round-trips to the server.    

- **nchan_publisher** `[ http | websocket ]`  
  arguments: 0 - 2  
  default: `http websocket`  
  context: server, location, if  
  legacy name: push_publisher  
  > Defines a server or location as a publisher endpoint. Requests to a publisher location are treated as messages to be sent to subscribers. See the protocol documentation for a detailed description.    

- **nchan_publisher_channel_id**  
  arguments: 1 - 7  
  default: `(none)`  
  context: server, location, if  
  > Channel id for publisher location.    

- **nchan_publisher_upstream_request** `<url>`  
  arguments: 1  
  context: server, location, if  
  > Send POST request to internal location (which may proxy to an upstream server) with published message in the request body. Useful for bridging websocket publishers with HTTP applications, or for transforming message via upstream application before publishing to a channel.    
  > The upstream response code determine how publishing will proceed. A `200 OK` will publish the message from the upstream response's body. A `304 Not Modified` will publish the message as it was received from the publisher. A `204 No Content` will result in the message not being published.    

- **nchan_pubsub** `[ http | websocket | eventsource | longpoll | intervalpoll | chunked | multipart-mixed ]`  
  arguments: 0 - 6  
  default: `http websocket eventsource longpoll chunked multipart-mixed`  
  context: server, location, if  
  > Defines a server or location as a pubsub endpoint. For long-polling, GETs subscribe. and POSTs publish. For Websockets, publishing data on a connection does not yield a channel metadata response. Without additional configuration, this turns a location into an echo server.    

- **nchan_subscriber** `[ websocket | eventsource | longpoll | intervalpoll | chunked | multipart-mixed ]`  
  arguments: 0 - 5  
  default: `websocket eventsource longpoll chunked multipart-mixed`  
  context: server, location, if  
  legacy name: push_subscriber  
  > Defines a server or location as a channel subscriber endpoint. This location represents a subscriber's interface to a channel's message queue. The queue is traversed automatically, starting at the position defined by the `nchan_subscriber_first_message` setting.    
  >  The value is a list of permitted subscriber types.    

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

- **nchan_subscriber_first_message** `[ oldest | newest ]`  
  arguments: 1  
  default: `oldest`  
  context: server, location, if  
  > Controls the first message received by a new subscriber. 'oldest' returns the oldest available message in a channel's message queue, 'newest' waits until a message arrives.    

- **nchan_subscriber_last_message_id**  
  arguments: 1 - 5  
  default: `$http_last_event_id $arg_last_event_id`  
  context: server, location, if  
  > If `If-Modified-Since` and `If-None-Match` headers are absent, set the message id to the first non-empty of these values. Used primarily as a workaround for the inability to set the first `Last-Message-Id` of a web browser's EventSource object.     

- **nchan_subscriber_timeout** `<number> (seconds)`  
  arguments: 1  
  default: `0 (none)`  
  context: http, server, location, if  
  legacy name: push_subscriber_timeout  
  > The length of time a subscriber's long-polling connection can last before it's timed out. If you don't want subscriber's connection to timeout, set this to 0. Applicable only if a `nchan_subscriber` is present in this or a child context.    

- **nchan_websocket_ping_interval** `<number> (seconds)`  
  arguments: 1  
  default: `0 (none)`  
  context: server, location, if  
  > Interval for sending websocket ping frames. Disabled by default.    

- **nchan_access_control_allow_origin** `<string>`  
  arguments: 1  
  default: `*`  
  context: http, server, location  
  > Set the [Cross-Origin Resource Sharing (CORS)](https://developer.mozilla.org/en-US/docs/Web/HTTP/Access_control_CORS) `Access-Control-Allow-Origin` header to this value. If the publisher or subscriber request's `Origin` header does not match this value, respond with a `403 Forbidden`.    

- **nchan_authorize_request** `<url>`  
  arguments: 1  
  context: server, location, if  
  > Send GET request to internal location (which may proxy to an upstream server) for authorization of a publisher or subscriber request. A 200 response authorizes the request, a 403 response forbids it.    

- **nchan_max_reserved_memory** `<size>`  
  arguments: 1  
  default: `32M`  
  context: http  
  legacy name: push_max_reserved_memory  
  > The size of the shared memory chunk this module will use for message queuing and buffering.    

- **nchan_message_buffer_length** `<number>`  
  arguments: 1  
  default: `10`  
  context: http, server, location  
  legacy names: push_max_message_buffer_length, push_message_buffer_length  
  > The maximum number of messages to store per channel. A channel's message buffer will retain a maximum of this many most recent messages.    

- **nchan_message_timeout** `<time>`  
  arguments: 1  
  default: `1h`  
  context: http, server, location  
  legacy name: push_message_timeout  
  > The length of time a message may be queued before it is considered expired. If you do not want messages to expire, set this to 0. Applicable only if a nchan_publisher is present in this or a child context.    

- **nchan_redis_url**  
  arguments: 1  
  default: `127.0.0.1:6379`  
  context: http  
  > The path to a redis server, of the form 'redis://:password@hostname:6379/0'. Shorthand of the form 'host:port' or just 'host' is also accepted.    

- **nchan_store_messages** `[ on | off ]`  
  arguments: 1  
  default: `on`  
  context: http, server, location, if  
  legacy name: push_store_messages  
  > Whether or not message queuing is enabled. "Off" is equivalent to the setting nchan_channel_buffer_length 0    

- **nchan_use_redis** `[ on | off ]`  
  arguments: 1  
  default: `off`  
  context: http, server, location  
  > Use redis for message storage at this location.    

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
  > Whether or not a subscriber may create a channel by sending a request to a push_subscriber location. If set to on, a publisher must send a POST or PUT request before a subscriber can request messages on the channel. Otherwise, all subscriber requests to nonexistent channels will get a 403 Forbidden response.    

- **nchan_channel_event_string** `<string>`  
  arguments: 1  
  default: `$nchan_channel_event $nchan_channel_id`  
  context: server, location, if  
  > Contents of channel event message    

- **nchan_channel_events_channel_id**  
  arguments: 1  
  context: server, location, if  
  > Channel id where `nchan_channel_id`'s events should be sent. Events like subscriber enqueue/dequeue, publishing messages, etc. Useful for application debugging. The channel event message is configurable via nchan_channel_event_string. The channel group for events is hardcoded to 'meta'.    

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
  > Maximum concurrent subscribers.    

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
