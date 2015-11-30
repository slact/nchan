![NCHAN](https://raw.githubusercontent.com/slact/nchan/master/nchan_logo.png)

Nchan is a scalable, flexible pub/sub server for the modern web, built on top of the Nginx web server. It can be configured as a standalone server, or a a shim between your application and tens, thousands, or millions of live subscribers. It can buffer messages in memory, on-disk, or via Redis. All connections are handled asynchronously and distributed among any number of worker processes. It can also scale to many nginx server instances with Redis.

Messages are published to channels with HTTP POST requests and websockets, and subscribed also through websockets, long-polling, EventSource (SSE), or old-fashioned interval polling. Each subscriber can be optionally authenticated via a custom application url, and an events meta channel is available for debugging.

##Documentation
This document is being actively developed.

##Status and History

The first iteration of Nchan was written in 2009-2010 as the Nginx HTTP Push Module. It was vastly refactored in 2014-2015, and here we are today. The present release is in the **testing** phase. The core features and old functionality are thoroughly tested and stable. Some of new functionality, specifically *redis storage and channel events are still experimental* and may still be a bit buggy, and the rest is somewhere in between.

Please help make the entire codebase ready for production use! Report any quirks, bugs, leaks, crashes, or larvae you find.

##Configuration Directives



##Contribute
Please support this project with a donation to keep me warm through the winter. I accept bitcoin at 1NHPMyqSanG2BC21Twqi8Pf1pXXgbPuLdJ . Other donation methods can be found at https://nchan.slact.net
