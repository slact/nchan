![NCHAN](https://raw.githubusercontent.com/slact/nchan/master/nchan_logo.png)

Nchan is a scalable, flexible pub/sub server for the modern web, built on top of the Nginx web server. It can be configured as a standalone server, or a a shim between your application and tens, thousands, or millions of live subscribers. It can buffer messages in memory, on-disk, or via Redis. All connections are handled asynchronously and distributed among any number of worker processes. It can also scale to many nginx server instances with Redis.

Messages can be published to channels simply with HTTP POST requests and websockets, and subscribed also through websockets, long-polling, EventSource (SSE), or old-fashioned interval polling.

...