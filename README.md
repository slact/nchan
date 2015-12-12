<img class="logo" alt="NCHAN" src="https://raw.githubusercontent.com/slact/nchan/master/nchan_logo.png" />

https://nchan.slact.net

Nchan is a scalable, flexible pub/sub server for the modern web, built as a module for the [Nginx](http://nginx.org) web server. It can be configured as a standalone server, or as a shim between your application and tens, thousands, or millions of live subscribers. It can buffer messages in memory, on-disk, or via [Redis](http://redis.io). All connections are handled asynchronously and distributed among any number of worker processes. It can also scale to many nginx server instances with [Redis](http://redis.io).

Messages are published to channels with HTTP `POST` requests or websockets, and subscribed also through websockets, long-polling, EventSource (SSE), old-fashioned interval polling, and more. Any location can be a subscriber endpoint for up to 4 channels. Each subscriber can be optionally authenticated via a custom application url, and an events meta channel is available for debugging.

## Status and History

The latest Nchan release is v0.904 (December 7, 2015) ([changelog](https://nchan.slact.net/changelog)). This is an *alpha* release. It is suitable for testing and experimentation. A production-ready release will be available soon.

The first iteration of Nchan was written in 2009-2010 as the [Nginx HTTP Push Module](https://pushmodule.slact.net), and was vastly refactored into its present state in 2014-2015. The present release is in the **testing** phase. The core features and old functionality are thoroughly tested and stable. Some of the new functionality, specifically *redis storage and channel events are still experimental* and may be a bit buggy, and the rest is somewhere in between.

Nchan is already very fast (parsing regular expressions within nginx uses more CPU cycles than all of the nchan code), but there is also a lot of room left for improvement. This release focuses on *correctness* and *stability*, with further optimizations (like zero-copy message publishing) planned for later.

Please help make the entire codebase ready for production use! Report any quirks, bugs, leaks, crashes, or larvae you find.

#### Upgrade from Nginx HTTP Push Module

Although Nchan is *mostly* backwards-compatible with Push Module configuration directives, some of the more unusual and rarely used settings have been removed. See the [upgrade page](https://nchan.slact.net/upgrade) for a detailed list of changes and improvements, as well as a full list of incompatibilities.

#### What's happening right now?

I'm currently stress-testing the stability of the Redis storage engine. Also, this thing's going to need some benchmarks. Who doesn't love a good graph?

## Download

#### Pre-built packages
 - For [Arch Linux](https://archlinux.org): [nginx-nchan](https://aur.archlinux.org/packages/nginx-nchan/) and [nginx-nchan-git](https://aur.archlinux.org/packages/nginx-nchan-git/) are available in the Arch User Repository.  
 - [Debian](https://www.debian.org/) and [Ubuntu](http://www.ubuntu.com/) : A statically compiled 64-bit [.deb package is available](https://nchan.slact.net/download/nginx-nchan-latest.deb).
 - The same statically compiled binary and associated linux nginx installation files are also [available as a tarball](https://nchan.slact.net/download/nginx-nchan-latest.tar.gz).


#### From Source
Grab the latest copy of Nginx from [nginx.org](http://nginx.org). Grab the latest Nchan source from [github](https://github.com/slact/nchan). Follow the instructions for [building Nginx](https://www.nginx.com/resources/wiki/start/topics/tutorials/install/#source-releases), except during the `configure` stage, add
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

Publisher endpoints are Nginx config *locations* with the *`nchan_publisher`* directive.

Messages can be published to a channel by sending HTTP **POST** requests with the message contents to the *publisher endpoint* locations. You can also publish messages through a **Websocket** connection to the same location.

##### Publishing Messages

Requests and websocket messages are responded to with information about the channel at time of message publication. Here's an example from publishing with `curl`:

```console
>  curl --request POST --data "test message" http://127.0.0.1:80/pub

 queued messages: 5
 last requested: 4 sec. ago (-1=never)
 active subscribers: 0
```

The response can be in plaintext (as above), JSON, or XML, based on the request's *`Accept`* header:

```console
> curl --request POST --data "test message" -H "Accept: text/json" http://127.0.0.2:80/pub

 {"messages": 2, "requested": 4, "subscribers": 0 }
```

Websocket publishers also receive the same responses when publishing, with the encoding determined by the *`Accept`* header present during the handshake.

The response code for an HTTP request is *`202` Accepted* if no subscribers are present at time of publication, or *`201` Created* if at least 1 subscriber was present.

##### Other Publisher Endpoint Actions

**HTTP `GET`** requests return channel information without publishing a message. The response code is `200` if the channel exists, and `404` otherwise:  
```console
> curl --request POST --data "test message" http://127.0.0.2:80/pub
  ...

> curl -v --request GET -H "Accept: text/json" http://127.0.0.2:80/pub

  {"messages": 1, "requested": 7, "subscribers": 0 }
```


**HTTP `DELETE`** requests delete a channel. Like the `GET` requests, this returns a `200` status response with channel info if the channel existed, and a `404` otherwise.

#### Subscriber Endpoint

Subscriber endpoints are Nginx config *locations* with the *`nchan_subscriber`* directive.

Nchan supports several different kinds of subscribers for receiving messages: *Websocket*, *EventSource* (Server Sent Events),  *Long-Poll*, and *Interval-Poll*.

- ##### Long-Polling
  The tried-and-true server-push method supported by every browser out there.  
  Initiated by sending an HTTP `GET` request to a channel subscriber endpoint.  
  The long-polling subscriber walks through a channel's message queue via the built-in cache mechanism of HTTP clients, namely with the "`Last-Modified`" and "`Etag`" headers. Explicitly, to receive the next message for given a long-poll subscriber response, send a request with the "`If-Modified-Since`" header set to the previous response's "`Last-Modified`" header, and "`If-None-Match`" likewise set to the previous response's "`Etag`" header.  
  Sending a request without a "`If-Modified-Since`" or "`If-None-Match`" headers returns the oldest message in a channel's message queue, or waits until the next published message, depending on the value of the `nchan_subscriber_first_message` config directive.
  
- ##### Interval-Polling
  Works just like long-polling, except if the requested message is not yet available, immediately responds with a `304 Not Modified`.
  There is no way to differentiate between long-poll and interval-poll subscriber requests, so long-polling must be disabled for a subscriber location if you wish to use interval-polling.

- ##### Websocket
  Bidirectional communication for web browsers. Part of the [HTML5 spec](http://www.w3.org/TR/2014/REC-html5-20141028/single-page.html). Nchan supports the latest protocol version 13 ([RFC 6455](https://tools.ietf.org/html/rfc6455)).   
  Initiated by sending a websocket handshake to the desired subscriber endpoint location.  
  If the websocket connection is closed by the server, the `close` frame will contain the HTTP response code and status line describing the reason for closing the connection.  
  Websocket extensions and subprotocols are not yet supported, nor are server-side keep-alive pings.
  
- ##### EventSource
  Also known as [Server-Sent Events](https://en.wikipedia.org/wiki/Server-sent_events) or SSE, it predates Websockets in the [HTML5 spec](http://www.w3.org/TR/2014/REC-html5-20141028/single-page.html), and is a [very simple protocol](http://www.w3.org/TR/eventsource/#event-stream-interpretation).  
  Initiated by sending an HTTP `GET` request to a channel subscriber endpoint with the "`Accept: text/event-stream`" header.    
  Each message `data: ` segment will be prefaced by the message `id: `.  
  To resume a closed EventSource connection from the last-received message, initiate the connection with the "`Last-Event-ID`" header set to the last message's `id`.
  
- ##### HTTP [Chunked Transfer](http://www.w3.org/Protocols/rfc2616/rfc2616-sec3.html#sec3.6.1)
  This subscription method uses the `chunked` `Transfer-Encoding` to receive messages.   
  Initiated by explicitly including `chunked` in the [`TE` header](http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.39):  
  `TE: chunked` (or `TE: chunked;q=??` where the qval > 0)  
  The response headers are sent right away, and each message will be sent as an individual chunk. Note that because a zero-length chunk terminates the transfer, **zero-length messages will not be sent** to the subscriber.
  
- ##### HTTP [multipart/mixed](http://www.w3.org/Protocols/rfc1341/7_2_Multipart.html#z0)
  The `multipart/mixed` MIMEtype was conceived for emails, but hey, why not use it for HTTP? It's easy to parse and includes metadata with each message.  
  Initiated by including an `Accept: multipart/mixed` header.  
  The response headers and the unused "preamble" portion of the response body are sent right away, with the boundary string generated randomly for each subscriber.  Each subsequent message will be sent as one part of the multipart message, and will include the message time and tag (`Last-Modified` and `Etag`) as well as the optional `Content-Type` headers.  
  Each messages is terminated with the next multipart message's boundary **without a trailing newline**. While this conforms to the multipart spec, it is unusual as multipart messages are defined as *starting*, rather than ending with a boundary.


#### PubSub Endpoint  

PubSub endpoints are Nginx config *locations* with the *`nchan_pubsub`* directive.

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

Any subscriber location can be an endpoint for up to 4 channels. Messages published to all the specified channels will be delivered in-order to the subscriber. This is configured by specifying multiple channel ids for the `nchan_channel_id` or `nchan_channel_subscriber_id` config directive:

```nginx
  location ~ /multisub/(\w+)/(\w+)$ {
    nchan_subscriber;
    nchan_channel_id "$1" "$2" "common_channel";
    #GET /multisub/foo/bar will be subscribed to:
    # channels 'foo', 'bar', and 'common_channel',
    #and will received messages from all of the above.
  }
```

Publishing to multiple channels from one location is not supported.

## Configuration Directives

- **nchan_channel_id**  
  arguments: 1 - 4  
  default: `(none)`  
  context: server, location, if  
  > Channel id for a publisher or subscriber location. Can have up to 4 values to subscribe to up to 4 channels.    

- **nchan_publisher** `[ http | websocket ]`  
  arguments: 0 - 2  
  default: `http websocket`  
  context: server, location, if  
  legacy name: push_publisher  
  > Defines a server or location as a publisher endpoint. Requests to a publisher location are treated as messages to be sent to subscribers. See the protocol documentation for a detailed description.    

- **nchan_publisher_channel_id**  
  arguments: 1  
  default: `(none)`  
  context: server, location, if  
  > Channel id for publisher location.    

- **nchan_pubsub** `[ http | websocket | eventsource | longpoll | intervalpoll | chunked ]`  
  arguments: 0 - 5  
  default: `http websocket eventsource longpoll chunked`  
  context: server, location, if  
  > Defines a server or location as a pubsub endpoint. For long-polling, GETs subscribe. and POSTs publish. For Websockets, publishing data on a connection does not yield a channel metadata response. Without additional configuration, this turns a location into an echo server.    

- **nchan_subscriber** `[ websocket | eventsource | longpoll | intervalpoll | chunked ]`  
  arguments: 0 - 4  
  default: `websocket eventsource longpoll chunked`  
  context: server, location, if  
  legacy name: push_subscriber  
  > Defines a server or location as a channel subscriber endpoint. This location represents a subscriber's interface to a channel's message queue. The queue is traversed automatically, starting at the position defined by the `nchan_subscriber_first_message` setting.    
  >  The value is a list of permitted subscriber types.    

- **nchan_subscriber_channel_id**  
  arguments: 1 - 4  
  default: `(none)`  
  context: server, location, if  
  > Channel id for subscriber location. Can have up to 4 values to subscribe to up to 4 channels.    

- **nchan_subscriber_first_message** `[ oldest | newest ]`  
  arguments: 1  
  default: `oldest`  
  context: server, location, if  
  > Controls the first message received by a new subscriber. 'oldest' returns the oldest available message in a channel's message queue, 'newest' waits until a message arrives.    

- **nchan_subscriber_timeout** `<number>`  
  arguments: 1  
  default: `0 (none)`  
  context: http, server, location, if  
  legacy name: push_subscriber_timeout  
  > The length of time a subscriber's long-polling connection can last before it's timed out. If you don't want subscriber's connection to timeout, set this to 0. Applicable only if a push_subscriber is present in this or a child context.    

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
