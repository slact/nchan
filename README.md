![NCHAN](https://raw.githubusercontent.com/slact/nchan/master/nchan_logo.png)

Nchan is a scalable, flexible pub/sub server for the modern web, built on top of the Nginx web server. It can be configured as a standalone server, or a a shim between your application and tens, thousands, or millions of live subscribers. It can buffer messages in memory, on-disk, or via Redis. All connections are handled asynchronously and distributed among any number of worker processes. It can also scale to many nginx server instances with Redis.

Messages are published to channels with HTTP POST requests and websockets, and subscribed also through websockets, long-polling, EventSource (SSE), or old-fashioned interval polling. Each subscriber can be optionally authenticated via a custom application url, and an events meta channel is available for debugging.

##Status and History

**This document is being actively developed.**

The first iteration of Nchan was written in 2009-2010 as the Nginx HTTP Push Module. It was vastly refactored in 2014-2015, and here we are today. The present release is in the **testing** phase. The core features and old functionality are thoroughly tested and stable. Some of new functionality, specifically *redis storage and channel events are still experimental* and may be a bit buggy, and the rest is somewhere in between.

Nchan is already very fast (parsing regular expressions within nginx uses more CPU cycles than all of the nchan code), but there is also a lot of room left for improvement. This release focuses on *correctness* and *stability*, with further optimizations (like zero-copy message publishing) planned for later.

Please help make the entire codebase ready for production use! Report any quirks, bugs, leaks, crashes, or larvae you find.

##Getting Started

###Download
For now, grab the code from github.

###Build and Install
For now, build a recent Nginx version with 
```
./configure --add-module=path/to/nchan ...
make 
```
##Usage

Nchan can be configured as a shim between your application and subscribers, a standalone pub/sub server for web clients, or as websocket proxy for your application. There are many other use cases, but for now I will focus on the above.

###The Basics 

The basic unit of most pub/sub solutions is the messaging *channel*. Nchan is no different. Publishers send messages to channels with a certain *channel id*, and subscribers subscribed to those channels receive them. Pretty simple, right? 

Well... the trouble is that nginx configuration does not deal with channels, publishers, and subscribers. Rather, it has several sections for incominf requests to match agains *server* and *location* sections. Consider this very simple nginx config:

```nginx




```

##Configuration Directives

- **nchan_channel_id**  
  default: `(none)`  
  context: server, location, if  
  > Channel id for a publisher or subscriber location. Can have up to 4 values to subscribe to up to 4 channels.    

- **nchan_publisher**  
  context: server, location, if  
  legacy name: push_publisher  
  > Defines a server or location as a message publisher. Requests to a publisher location are treated as messages to be sent to subscribers. See the protocol documentation for a detailed description.    

- **nchan_publisher_channel_id**  
  default: `(none)`  
  context: server, location, if  
  > Channel id for publisher location.    

- **nchan_pubsub**  
  default: `(none)`  
  context: server, location, if  
  > Defines a server or location as a publisher and subscriber endpoint. For long-polling, GETs subscribe. and POSTS publish. For Websockets, publishing data on a connection does not yield a channel metadata response. Without additional configuration, this turns a location into an echo server.    

- **nchan_subscriber** `[ any | websocket | eventsource | longpoll | intervalpoll ]`  
  default: `any (websocket|eventsource|longpoll)`  
  context: server, location, if  
  legacy name: push_subscriber  
  > Defines a server or location as a subscriber. This location represents a subscriber's interface to a channel's message queue. The queue is traversed automatically via caching information request headers (If-Modified-Since and If-None-Match), beginning with the oldest available message. Requests for upcoming messages are handled in accordance with the setting provided. See the protocol documentation for a detailed description.    

- **nchan_subscriber_channel_id**  
  default: `(none)`  
  context: server, location, if  
  > Channel id for subscriber location. Can have up to 4 values to subscribe to up to 4 channels.    

- **nchan_subscriber_concurrency** `[ last | first | broadcast ]`  
  context: http, server, location, if  
  legacy name: push_subscriber_concurrency  
  > Controls how multiple subscriber requests to a channel (identified by some common ID) are handled.The values work as follows:  
  >       - broadcast: any number of concurrent subscriber requests may be held.  
  >       - last: only the most recent subscriber request is kept, all others get a 409 Conflict response.  
  >       - first: only the oldest subscriber request is kept, all others get a 409 Conflict response.    

- **nchan_subscriber_first_message** `[ oldest | newest ]`  
  default: `newest`  
  context: server, location, if  
  > Controls the first message received by a new subscriber. 'oldest' returns the oldest available message in a channel's message queue, 'newest' waits until a message arrives    

- **nchan_subscriber_timeout** `[ <number> ]`  
  default: `0 (none)`  
  context: http, server, location, if  
  legacy name: push_subscriber_timeout  
  > The length of time a subscriber's long-polling connection can last before it's timed out. If you don't want subscriber's connection to timeout, set this to 0. Applicable only if a push_subscriber is present in this or a child context.    

- **nchan_authorize_request** `[ <url> ]`  
  context: server, location, if  
  > send GET request to internal location (which may proxy to an upstream server) for authorization of ap ublisher or subscriber request. A 200 response authorizes the request, a 403 response forbids it.    

- **nchan_delete_oldest_received_message** `[ on | off ]`  
  default: `off`  
  context: http, server, location  
  legacy name: push_delete_oldest_received_message  
  > When enabled, as soon as the oldest message in a channel's message queue has been received by a subscriber, it is deleted -- provided there are more than push_min_message_buffer_length messages in the channel's message buffer. Recommend avoiding this directive as it violates subscribers' assumptions of GET request idempotence.    

- **nchan_max_message_buffer_length** `[ <number> ]`  
  default: `10`  
  context: http, server, location  
  legacy name: push_max_message_buffer_length  
  > The maximum number of messages to store per channel. A channel's message buffer will retain at most this many most recent messages.    

- **nchan_max_reserved_memory** `[ <size> ]`  
  default: `32M`  
  context: http  
  legacy name: push_max_reserved_memory  
  > The size of the shared memory chunk this module will use for message queuing and buffering.    

- **nchan_message_buffer_length** `[ <number> ]`  
  default: `*none*`  
  context: http, server, location  
  legacy name: push_message_buffer_length  
  > The exact number of messages to store per channel. Sets both nchan_max_message_buffer_length and nchan_min_message_buffer_length to this value.    

- **nchan_message_timeout** `[ <time> ]`  
  default: `1h`  
  context: http, server, location  
  legacy name: push_message_timeout  
  > The length of time a message may be queued before it is considered expired. If you do not want messages to expire, set this to 0. Applicable only if a nchan_publisher is present in this or a child context.    

- **nchan_min_message_buffer_length** `[ <number> ]`  
  default: `1`  
  context: http, server, location  
  legacy name: push_min_message_buffer_length  
  > The minimum number of messages to store per channel. A channel's message  buffer will retain at least this many most recent messages.    

- **nchan_redis_url**  
  default: `127.0.0.1:6379`  
  context: http  
  > The path to a redis server, of the form 'redis://:password@hostname:6379/0'. Shorthand of the form 'host:port' or just 'host' is also accepted.    

- **nchan_store_messages** `[ on | off ]`  
  default: `on`  
  context: http, server, location, if  
  legacy name: push_store_messages  
  > Whether or not message queuing is enabled. "Off" is equivalent to the setting nchan_channel_buffer_length 0    

- **nchan_use_redis** `[ on | off ]`  
  default: `off`  
  context: http, server, location  
  > Use redis for message storage at this location.    

- **nchan_authorized_channels_only** `[ on | off ]`  
  default: `off`  
  context: http, server, location  
  legacy name: push_authorized_channels_only  
  > Whether or not a subscriber may create a channel by making a request to a push_subscriber location. If set to on, a publisher must send a POST or PUT request before a subscriber can request messages on the channel. Otherwise, all subscriber requests to nonexistent channels will get a 403 Forbidden response.    

- **nchan_channel_group** `[ <string> ]`  
  default: `(none)`  
  context: server, location, if  
  legacy name: push_channel_group  
  > Because settings are bound to locations and not individual channels, it is useful to be able to have channels that can be reached only from some locations and never others. That's where this setting comes in. Think of it as a prefix string for the channel id.    

- **nchan_channel_event_string** `[ <string> ]`  
  default: `$nchan_channel_event $nchan_channel_id`  
  context: server, location, if  
  > Contents of channel event message    

- **nchan_channel_events_channel_id**  
  context: server, location, if  
  > Channel id where `nchan_channel_id`'s events should be sent. Things like subscriber enqueue/dequeue, publishing messages, etc. Useful for application debugging. The channel event message is configurable via nchan_channel_event_string. The channel group for events is hardcoded to 'meta'.    

- **nchan_max_channel_id_length** `[ <number> ]`  
  default: `512`  
  context: http, server, location  
  legacy name: push_max_channel_id_length  
  > Maximum permissible channel id length (number of characters). Longer ids will be truncated.    

- **nchan_max_channel_subscribers** `[ <number> ]`  
  default: `0 (unlimited)`  
  context: http, server, location  
  legacy name: push_max_channel_subscribers  
  > Maximum concurrent subscribers.    

- **nchan_channel_timeout**  
  context: http, server, location  
  legacy name: push_channel_timeout  

- **nchan_storage_engine**  
  context: http, server, location  
  legacy name: push_storage_engine  
  > development directive to completely replace default storage engine. Don't use unless you know what you're doing    

##Contribute
Please support this project with a donation to keep me warm through the winter. I accept bitcoin at 1NHPMyqSanG2BC21Twqi8Pf1pXXgbPuLdJ . Other donation methods can be found at https://nchan.slact.net
