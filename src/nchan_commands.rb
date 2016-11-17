CfCmd.new do
  
  nchan_channel_id [:srv, :loc, :if],
      :nchan_set_pubsub_channel_id,
      :loc_conf,
      args: 1..7,
      alt: [ :nchan_pubsub_channel_id ],
      
      group: "pubsub",
      default: "(none)",
      tags: [ 'channel-id' ],
      info: "Channel id for a publisher or subscriber location. Can have up to 4 values to subscribe to up to 4 channels.",
      uri: "#the-channel-id"
  
  nchan_publisher_channel_id [:srv, :loc, :if],
      :nchan_set_pub_channel_id,
      :loc_conf,
      args: 1..7,
      alt: [ :nchan_pub_channel_id ],
      
      group: "pubsub",
      tags: [ 'pubsub', 'channel-id' ],
      default: "(none)",
      info: "Channel id for publisher location."
  
  nchan_publisher_upstream_request [:srv, :loc, :if],
      :ngx_http_set_complex_value_slot,
      [:loc_conf, :publisher_upstream_request_url],
      
      group: "pubsub",
      tags: [ 'publisher', 'hook' ],
      value: "<url>",
      uri: "/details#message-publishing-callbacks",
      info: <<-EOS.gsub(/^ {8}/, '')
        Send POST request to internal location (which may proxy to an upstream server) with published message in the request body. Useful for bridging websocket publishers with HTTP applications, or for transforming message via upstream application before publishing to a channel.  
        The upstream response code determines how publishing will proceed. A `200 OK` will publish the message from the upstream response's body. A `304 Not Modified` will publish the message as it was received from the publisher. A `204 No Content` will result in the message not being published.
      EOS
      
  
  nchan_channel_id_split_delimiter [:srv, :loc, :if],
      :ngx_conf_set_str_slot,
      [:loc_conf, :channel_id_split_delimiter],
      
      group: "pubsub",
      tags: [ 'channel-id', 'channel-multiplexing' ],
      default: "(none)",
      info: "Split the channel id into several ids for multiplexing using the delimiter string provided.",
      uri: "#channel-multiplexing"
  
  nchan_subscriber_channel_id [:srv, :loc, :if],
      :nchan_set_sub_channel_id,
      :loc_conf,
      args: 1..7,
      alt: [ :nchan_sub_channel_id ],
      group: "pubsub",
      tags: ["pubsub", 'channel-id'],
      
      default: "(none)",
      info: "Channel id for subscriber location. Can have up to 4 values to subscribe to up to 4 channels."
  
  nchan_pubsub [:srv, :loc, :if],
      :nchan_pubsub_directive,
      :loc_conf,
      args: 0..6,
      
      group: "pubsub",
      tags: [ 'publisher', 'subscriber', 'pubsub' ],
      value: ["http", "websocket", "eventsource", "longpoll", "intervalpoll", "chunked", "multipart-mixed", "http-raw-stream"],
      default: ["http", "websocket", "eventsource", "longpoll", "chunked", "multipart-mixed"],
      info: "Defines a server or location as a pubsub endpoint. For long-polling, GETs subscribe. and POSTs publish. For Websockets, publishing data on a connection does not yield a channel metadata response. Without additional configuration, this turns a location into an echo server.",
      uri: '#pubsub-endpoint'
  
  
  nchan_longpoll_multipart_response [:srv, :loc, :if],
      :nchan_set_longpoll_multipart,
      [:loc_conf, :longpoll_multimsg],
      args: 1,
      
      group: "pubsub",
      tags: ['subscriber-longpoll'],
      default: "off",
      value: ["off", "on", "raw"],
      info: "when set to 'on', enable sending multiple messages in a single longpoll response, separated using the multipart/mixed content-type scheme. If there is only one available message in response to a long-poll request, it is sent unmodified. This is useful for high-latency long-polling connections as a way to minimize round-trips to the server. When set to 'raw', sends multiple messages using the http-raw-stream message separator."
  
  nchan_eventsource_event [:srv, :loc, :if],
      :ngx_conf_set_str_slot,
      [:loc_conf, :eventsource_event],
      args: 1,
      
      group: "pubsub",
      tags: ['subscriber-eventsource'],
      default: "(none)",
      info: "Set the EventSource `event:` line to this value. When used in a publisher location, overrides the published message's `X-EventSource-Event` header and associates the message with the given value. When used in a subscriber location, overrides all messages' associated `event:` string with the given value."
      
  
  nchan_subscriber [:srv, :loc, :if],
      :nchan_subscriber_directive,
      :loc_conf,
      args: 0..5,
      legacy: "push_subscriber",
      
      group: "pubsub",
      tags: ['subscriber'],
      value: ["websocket", "eventsource", "longpoll", "intervalpoll", "chunked", "multipart-mixed", "http-raw-stream"],
      default: ["websocket", "eventsource", "longpoll", "chunked", "multipart-mixed"],
      info: "Defines a server or location as a channel subscriber endpoint. This location represents a subscriber's interface to a channel's message queue. The queue is traversed automatically, starting at the position defined by the `nchan_subscriber_first_message` setting.  \n The value is a list of permitted subscriber types." ,
      uri: "#subscriber-endpoints"
  
  nchan_subscriber_compound_etag_message_id [:srv, :loc, :if], 
      :ngx_conf_set_flag_slot,
      [:loc_conf, :msg_in_etag_only],
      args: 1,
      
      group: "pubsub",
      tags: ['subscriber'],
      default: "off",
      uri: "",
      info: <<-EOS.gsub(/^ {8}/, '')
        Override the default behavior of using both `Last-Modified` and `Etag` headers for the message id.  
        Enabling this option packs the entire message id into the `Etag` header, and discards
        `Last-Modified` and `If-Modified-Since` headers.
      EOS
      
  nchan_subscriber_message_id_custom_etag_header [:srv, :loc, :if], 
      :ngx_conf_set_str_slot, 
      [:loc_conf, :custom_msgtag_header],
      args: 1,
      
      group: "pubsub",
      tags: ['subscriber'],
      default: "(none)",
      info: <<-EOS.gsub(/^ {8}/, '')
        Use a custom header instead of the Etag header for message ID in subscriber responses. This setting is a hack, useful when behind a caching proxy such as Cloudflare that under some conditions (like using gzip encoding) swallow the Etag header.
      EOS
  
  nchan_subscriber_last_message_id [:srv, :loc, :if], 
      :nchan_subscriber_last_message_id,
      :loc_conf,
      args: 1..5,
      
      group: "pubsub",
      tags: ['subscriber'],
      default: ["$http_last_event_id", "$arg_last_event_id"],
      info: "If `If-Modified-Since` and `If-None-Match` headers are absent, set the message id to the first non-empty of these values. Used primarily as a workaround for the inability to set the first `Last-Message-Id` of a web browser's EventSource object. "
  
  nchan_subscriber_http_raw_stream_separator [:srv, :loc, :if],
      :nchan_set_raw_subscriber_separator,
      [:loc_conf, :subscriber_http_raw_stream_separator],
      args: 1,
      
      group: "pubsub",
      tags: ['subscriber-rawstream'],
      value: "<string>",
      default: "\\n",
      info: "Message separator string for the http-raw-stream subscriber. Automatically terminated with a newline character."
  
  nchan_subscriber_first_message [:srv, :loc, :if],
      :nchan_subscriber_first_message_directive,
      :loc_conf,
      args: 1,
      
      group: "pubsub",
      tags: ['subscriber'],
      value: ["oldest", "newest", "<number>"],
      default: "oldest",
      info: "Controls the first message received by a new subscriber. 'oldest' starts at the oldest available message in a channel's message queue, 'newest' waits until a message arrives. If a number `n` is specified, starts at `n`th message from the oldest. (`-n` starts at `n`th from now). 0 is equivalent to 'newest'."
  
  
  #nchan_subscriber_concurrency [:main, :srv, :loc, :if],
  #    :nchan_set_subscriber_concurrency,
  #    [:loc_conf, :subscriber_concurrency],
  #    legacy: "push_subscriber_concurrency",
  #    
  #    group: "pubsub",
  #    value: [ :last, :first, :broadcast ],
  #    info: "Controls how multiple subscriber requests to a channel (identified by some common ID) are handled.The values work as follows:
  #    - broadcast: any number of concurrent subscriber requests may be held.
  #    - last: only the most recent subscriber request is kept, all others get a 409 Conflict response.
  #    - first: only the oldest subscriber request is kept, all others get a 409 Conflict response."
  
  nchan_websocket_ping_interval [:srv, :loc, :if],
      :ngx_conf_set_sec_slot,
      [:loc_conf, :websocket_ping_interval],
      
      group: "pubsub",
      tags: ['subscriber-websocket'],
      value: "<number> (seconds)",
      default: "0 (none)",
      info: "Interval for sending websocket ping frames. Disabled by default."
  
  nchan_publisher [:srv, :loc, :if],
      :nchan_publisher_directive,
      :loc_conf,
      args: 0..2,
      legacy: "push_publisher",
      
      group: "pubsub",
      tags: ['publisher'],
      value: ["http", "websocket"],
      default: ["http", "websocket"],
      info: "Defines a server or location as a publisher endpoint. Requests to a publisher location are treated as messages to be sent to subscribers. See the protocol documentation for a detailed description.",
      uri: "#publisher-endpoints"
  
  nchan_subscriber_timeout [:main, :srv, :loc, :if],
      :ngx_conf_set_sec_slot,
      [:loc_conf, :subscriber_timeout],
      legacy: "push_subscriber_timeout",
      
      group: "pubsub",
      tags: ['subscriber'],
      value: "<number> (seconds)",
      default: "0 (none)",
      info: "Maximum time a subscriber may wait for a message before being disconnected. If you don't want a subscriber's connection to timeout, set this to 0. When possible, the subscriber will get a response with a `408 Request Timeout` status; otherwise the subscriber will simply be disconnected."
      
  
  nchan_authorize_request [:srv, :loc, :if], 
      :ngx_http_set_complex_value_slot,
      [:loc_conf, :authorize_request_url],
      
      group: "security",
      tags: ['publisher', 'subscriber', 'hook'],
      value: "<url>",
      info: "Send GET request to internal location (which may proxy to an upstream server) for authorization of a publisher or subscriber request. A 200 response authorizes the request, a 403 response forbids it.",
      uri: "/details#request-authorization"
  
  nchan_subscribe_request [:srv, :loc, :if], 
      :ngx_http_set_complex_value_slot,
      [:loc_conf, :subscribe_request_url],
      
      group: "pubsub",
      tags: ['subscriber', 'hook'],
      value: "<url>",
      info: "Send GET request to internal location (which may proxy to an upstream server) after subscribing. Disabled for longpoll and interval-polling subscribers.",
      uri: "/details#subsribe-and-unsubscribe-callbacks"
  
  nchan_unsubscribe_request [:srv, :loc, :if], 
      :ngx_http_set_unsubscribe_request_url,
      [:loc_conf, :unsubscribe_request_url],
      
      group: "pubsub",
      tags: ['subscriber', 'hook'],
      value: "<url>",
      info: "Send GET request to internal location (which may proxy to an upstream server) after unsubscribing. Disabled for longpoll and interval-polling subscribers.",
      uri: "/details#subsribe-and-unsubscribe-callbacks"
  
  nchan_store_messages [:main, :srv, :loc, :if],
      :nchan_store_messages_directive,
      :loc_conf,
      legacy: "push_store_messages",
      
      group: "storage",
      tags: ['publisher'],
      value: [:on, :off],
      default: :on,
      info: "Publisher configuration. \"`off`\" is equivalent to setting `nchan_message_buffer_length 0`, which disables the buffering of old messages. Using this setting is not recommended when publishing very quickly, as it may result in missed messages."
    
  nchan_max_reserved_memory [:main],
      :ngx_conf_set_size_slot,
      [:main_conf, :shm_size],
      legacy: "push_max_reserved_memory",
      
      group: "storage",
      tags: ['memstore'],
      value: "<size>",
      default: "32M",
      info: "The size of the shared memory chunk this module will use for message queuing and buffering.",
      uri: "#memory-storage"
    
  nchan_redis_url [:main, :srv, :loc],
      :ngx_conf_set_redis_url,
      [:loc_conf, :"redis.url"],
      
      group: "storage",
      tags: ['redis'],
      default: "127.0.0.1:6379",
      info: "The path to a redis server, of the form 'redis://:password@hostname:6379/0'. Shorthand of the form 'host:port' or just 'host' is also accepted.",
      uri: "/details#using-redis"
  
  nchan_redis_pass [:main, :srv, :loc],
      :ngx_conf_set_redis_upstream_pass,
      [:loc_conf, :"redis"],
      
      group: "storage",
      tags: ['publisher', 'subscriber', 'redis'],
      info: "Use an upstream config block for Redis servers.",
      uri: "/details#using-redis"
  
  nchan_redis_pass_inheritable [:main, :srv, :loc],
      :ngx_conf_set_flag_slot,
      [:loc_conf, :"redis.upstream_inheritable"],
      
      undocumented: true,
      group: "debug"
  
  nchan_redis_publish_msgpacked_max_size [:main],
      :ngx_conf_set_size_slot,
      [:main_conf, :redis_publish_message_msgkey_size],
      undocumented: true,
      group: "storage"
  
  nchan_redis_server [:upstream],
      :ngx_conf_upstream_redis_server,
      :loc_conf,
      group: "storage",
      tags: ['redis'],
      info: "Used in upstream { } blocks to set redis servers.",
      uri: "/details#using-redis"
  
  nchan_use_redis [:main, :srv, :loc],
      :ngx_conf_enable_redis,
      [:loc_conf, :"redis.url_enabled"],
      
      group: "storage",
      tags: ['redis', 'publisher', 'subscriber'],
      value: [ :on, :off ],
      default: :off,
      info: "Use redis for message storage at this location.", 
      uri: "/details#using-redis"
  
  nchan_redis_ping_interval [:main, :srv, :loc],
      :ngx_conf_set_sec_slot,
      [:loc_conf, :"redis.ping_interval"],
      
      group: "storage",
      tags: ['redis'],
      default: "4m",
      info: "Send a keepalive command to redis to keep the Nchan redis clients from disconnecting. Set to 0 to disable."
  
  nchan_redis_fakesub_timer_interval [:main],
      :ngx_conf_set_msec_slot,
      [:main_conf, :redis_fakesub_timer_interval],
      
      group: "tweak",
      undocumented: true,
      default: "100ms"
  
  nchan_redis_idle_channel_cache_timeout [:main, :srv, :loc],
      :ngx_conf_set_sec_slot,
      [:loc_conf, :redis_idle_channel_cache_timeout],
      
      group: "storage",
      tags: ['redis'],
      value: "<time>",
      default: "30s",
      info: "A Redis-stored channel and its messages are removed from memory (local cache) after this timeout, provided there are no local subscribers."
  
  nchan_message_timeout [:main, :srv, :loc], 
      :nchan_set_message_timeout, 
      [:loc_conf, :message_timeout],
      legacy: "push_message_timeout",
      
      group: "storage",
      tags: ['publisher'],
      value: ["<time>", "<variable>"],
      default: "1h",
      info: "Publisher configuration setting the length of time a message may be queued before it is considered expired. If you do not want messages to expire, set this to 0. Note that messages always expire from oldest to newest, so an older message may prevent a newer one with a shorter timeout from expiring. An Nginx variable can also be used to set the timeout dynamically."
  
  nchan_message_buffer_length [:main, :srv, :loc],
      :nchan_set_message_buffer_length,
      [:loc_conf, :max_messages],
      legacy: [ "push_max_message_buffer_length", "push_message_buffer_length" ],
      alt: ["nchan_message_max_buffer_length"],
      
      group: "storage",
      tags: ['publisher'],
      value: ["<number>", "<variable>"],
      default: 10,
      info: "Publisher configuration setting the maximum number of messages to store per channel. A channel's message buffer will retain a maximum of this many most recent messages. An Nginx variable can also be used to set the buffer length dynamically."
  
  nchan_subscribe_existing_channels_only [:main, :srv, :loc],
      :ngx_conf_set_flag_slot, 
      [:loc_conf, :subscribe_only_existing_channel],
      legacy: "push_authorized_channels_only",
      
      group: "security",
      tags: ['subscriber'],
      value: [:on, :off],
      default: :off,
      info: "Whether or not a subscriber may create a channel by sending a request to a subscriber location. If set to on, a publisher must send a POST or PUT request before a subscriber can request messages on the channel. Otherwise, all subscriber requests to nonexistent channels will get a 403 Forbidden response."
  
  nchan_access_control_allow_origin [:main, :srv, :loc], 
      :ngx_conf_set_str_slot,
      [:loc_conf, :allow_origin],
      args: 1,
      
      group: "security",
      value: "<string>",
      tags: ['publisher', 'subscriber'],
      default: "*",
      info: "Set the [Cross-Origin Resource Sharing (CORS)](https://developer.mozilla.org/en-US/docs/Web/HTTP/Access_control_CORS) `Access-Control-Allow-Origin` header to this value. If the publisher or subscriber request's `Origin` header does not match this value, respond with a `403 Forbidden`."
  
  nchan_channel_group [:srv, :loc, :if], 
      :ngx_conf_set_str_slot, 
      [:loc_conf, :channel_group],
      legacy: "push_channel_group",
      
      group: "security",
      tags: ['publisher', 'subscriber', 'channel-events'],
      value: "<string>",
      default: "(none)",
      info: "Because settings are bound to locations and not individual channels, it is useful to be able to have channels that can be reached only from some locations and never others. That's where this setting comes in. Think of it as a prefix string for the channel id."
  
  nchan_channel_events_channel_id [:srv, :loc, :if],
      :nchan_set_channel_events_channel_id,
      :loc_conf,
      args: 1,
      
      group: "meta",
      tags: ['publisher', 'subscriber', 'channel-events'],
      uri: "/details#channel-events",
      info: "Channel id where `nchan_channel_id`'s events should be sent. Events like subscriber enqueue/dequeue, publishing messages, etc. Useful for application debugging. The channel event message is configurable via nchan_channel_event_string. The channel group for events is hardcoded to 'meta'."
  
  nchan_stub_status [:loc],
      :nchan_stub_status_directive,
      :loc_conf,
      args: 0,
      
      group: "meta",
      info: "Similar to Nginx's stub_status directive, requests to an `nchan_stub_status` location get a response with some vital Nchan statistics. This data does not account for information from other Nchan instances, and monitors only local connections, published messages, etc.",
      uri: "/details#nchan_stub_status"
  
  nchan_channel_event_string [:srv, :loc, :if], 
      :ngx_http_set_complex_value_slot,
      [:loc_conf, :channel_event_string],
      
      group: "meta",
      tags: ['publisher', 'subscriber', 'channel-events'],
      value: "<string>",
      default: "\"$nchan_channel_event $nchan_channel_id\"",
      info: "Contents of channel event message"
  
  nchan_max_channel_id_length [:main, :srv, :loc],
      :ngx_conf_set_num_slot,
      [:loc_conf, :max_channel_id_length],
      legacy: "push_max_channel_id_length",
      
      group: "security",
      tags: ['publisher', 'subscriber', 'channel-id' ],
      value: "<number>",
      default: 512,
      info: "Maximum permissible channel id length (number of characters). Longer ids will be truncated."
  
  nchan_max_channel_subscribers [:main, :srv, :loc],
      :ngx_conf_set_num_slot,
      [:loc_conf, :max_channel_subscribers],
      legacy: "push_max_channel_subscribers",
      
      group: "security",
      tags: ['subscriber'],
      value: "<number>",
      default: "0 (unlimited)",
      info: "Maximum concurrent subscribers to the channel on this Nchan server. Does not include subscribers on other Nchan instances when using a shared Redis server."
  
  nchan_channel_timeout [:main, :srv, :loc],
      :ngx_conf_set_sec_slot,
      [:loc_conf, :channel_timeout],
      legacy: "push_channel_timeout",
      
      group: "development",
      info: "Amount of time an empty channel hangs around. Don't mess with this setting unless you know what you are doing!"
  
  nchan_storage_engine [:main, :srv, :loc],
      :nchan_set_storage_engine, 
      [:loc_conf, :storage_engine],
      
      group: "development",
      value: ["memory", "redis"],
      default: "memory",
      info: "Development directive to completely replace default storage engine. Don't use unless you are an Nchan developer."
  
  push_min_message_buffer_length [:srv, :loc, :if],
      :nchan_ignore_obsolete_setting,
      :loc_conf,
      undocumented: true,
      group: "obsolete"
  
  push_subscriber_concurrency [:srv, :loc, :if],
      :nchan_ignore_subscriber_concurrency,
      :loc_conf,
      undocumented: true,
      group: "obsolete"
  
end
