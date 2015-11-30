![NCHAN](https://raw.githubusercontent.com/slact/nchan/master/nchan_logo.png)

Nchan is a scalable, flexible pub/sub server for the modern web, built on top of the Nginx web server. It can be configured as a standalone server, or a a shim between your application and tens, thousands, or millions of live subscribers. It can buffer messages in memory, on-disk, or via Redis. All connections are handled asynchronously and distributed among any number of worker processes. It can also scale to many nginx server instances with Redis.

Messages are published to channels with HTTP POST requests and websockets, and subscribed also through websockets, long-polling, EventSource (SSE), or old-fashioned interval polling. Each subscriber can be optionally authenticated via a custom application url, and a meta channel events channel is available for debugging.

##Status and History

The first iteration of Nchan was written in 2009-2010 as the Nginx HTTP Push Module. It was vastly refactored in 2014-2015, and here we are today. The present release is in testing phase, with the core features and old functionality being quite thoroughly tested and stable. Some of new functionality specifically redis storage and channel events are still experimental and may still be a bit buggy, and the rest is somewhere in between.

Please help make the entire codebase ready for production use! Please report any quirks, bugs, or larvae you find.

--
##Configuration Directives

<div class="config">

</div>