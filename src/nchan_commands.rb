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
      uri: "#message-forwarding",
      info: <<-EOS.gsub(/^ {8}/, '')
        Send POST request to internal location (which may proxy to an upstream server) with published message in the request body. Useful for bridging websocket publishers with HTTP applications, or for transforming message via upstream application before publishing to a channel.  
        The upstream response code determines how publishing will proceed. A `200 OK` will publish the message from the upstream response's body. A `304 Not Modified` will publish the message as it was received from the publisher. A `204 No Content` will result in the message not being published.
      EOS
  
      
  nchan_deflate_message_for_websocket [:srv, :loc],
      :nchan_set_message_compression_slot,
      :loc_conf,
      args: 1,
      
      group: "pubsub",
      tags: ["publisher", 'subscriber-websocket'],
      value: ['on', 'off'],
      default: "off",
      info: "Store a compressed (deflated) copy of the message along with the original to be sent to websocket clients supporting the permessage-deflate protocol extension"
      
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
      alt: :nchan_sub_channel_id,
      group: "pubsub",
      tags: ["pubsub", 'channel-id'],
      
      default: "(none)",
      info: "Channel id for subscriber location. Can have up to 4 values to subscribe to up to 4 channels."
  
  nchan_pubsub [:srv, :loc, :if],
      :nchan_pubsub_directive,
      :loc_conf,
      args: 0..6,
      alt: :nchan_pubsub_location,
      
      group: "pubsub",
      tags: [ 'publisher', 'subscriber', 'pubsub' ],
      value: ["http", "websocket", "eventsource", "longpoll", "intervalpoll", "chunked", "multipart-mixed", "http-raw-stream"],
      default: ["http", "websocket", "eventsource", "longpoll", "chunked", "multipart-mixed"],
      info: "Defines a server or location as a pubsub endpoint. For long-polling, GETs subscribe. and POSTs publish. For Websockets, publishing data on a connection does not yield a channel metadata response. Without additional configuration, this turns a location into an echo server.",
      uri: '#pubsub-endpoint'
  
  nchan_subscriber_info [:loc],
      :nchan_subscriber_info_directive,
      :loc_conf,
      args: 0,
      
      group: "pubsub",
      tags: ["subscriber", "debug"],
      info: "A subscriber location for debugging the state of subscribers on a given channel. The subscribers of the channel specified by `nchan_channel_id` evaluate `nchan_subscriber_info_string` and send it back to the requested on this location. This is useful to see where subscribers are in an Nchan cluster, as well as debugging subscriber connection issues."
  
  nchan_subscriber_info_string [:srv, :loc],
      :ngx_http_set_complex_value_slot,
      [:loc_conf, :subscriber_info_string],
      args: 1,
      
      group: "pubsub",
      tags: ["subscriber", "debug"],
      default: "$nchan_subscriber_type $remote_addr:$remote_port $http_user_agent $server_name $request_uri $pid",
      info: "this string is evaluated by each subscriber on a given channel and sent to the requester of a `nchan_subscriber_info` location"
  
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
  
  nchan_eventsource_ping_comment [:srv, :loc, :if],
      :ngx_conf_set_str_slot_no_newlines,
      [:loc_conf, :"eventsource_ping.comment"],
      args: 1,
      
      group: "pubsub",
      tags: ['subscriber-eventsource'],
      default: "(empty)",
      info: "Set the EventSource comment `: ...` line for periodic pings from server to client. Newlines are not allowed. If empty, no comment is sent with the ping."
  
  nchan_eventsource_ping_event [:srv, :loc, :if],
      :ngx_conf_set_str_slot_no_newlines,
      [:loc_conf, :"eventsource_ping.event"],
      args: 1,
      
      group: "pubsub",
      tags: ['subscriber-eventsource'],
      default: "ping",
      info: "Set the EventSource `event:` line for periodic pings from server to client. Newlines are not allowed. If empty, no event type is sent with the ping."
  
  nchan_eventsource_ping_data [:srv, :loc, :if],
      :ngx_conf_set_str_slot_no_newlines,
      [:loc_conf, :"eventsource_ping.data"],
      args: 1,
      
      group: "pubsub",
      tags: ['subscriber-eventsource'],
      default: "(empty)",
      info: "Set the EventSource `data:` line for periodic pings from server to client. Newlines are not allowed. If empty, no data is sent with the ping."
  
    nchan_eventsource_ping_interval [:srv, :loc, :if],
      :ngx_conf_set_sec_slot,
      [:loc_conf, :"eventsource_ping.interval"],
      
      group: "pubsub",
      tags: ['subscriber-eventsource'],
      value: "<number> (seconds)",
      default: "0 (none)",
      info: "Interval for sending ping messages to EventSource subscribers. Disabled by default."
  
  nchan_subscriber [:srv, :loc, :if],
      :nchan_subscriber_directive,
      :loc_conf,
      args: 0..5,
      legacy: "push_subscriber",
      alt: :nchan_subscriber_location,
      
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
      info: "Message separator string for the http-raw-stream subscriber. Automatically terminated with a newline character if not explicitly set to an empty string."
  
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
  
  nchan_websocket_client_heartbeat [:srv, :loc, :if],
      :nchan_websocket_heartbeat_directive,
      [:loc_conf, :websocket_heartbeat],
      args: 2,
      
      group: "pubsub",
      tags:['subscriber-websocket'],
      value: "<heartbeat_in> <heartbeat_out>",
      default: "none (disabled)",
      info: "Most browser Websocket clients do not allow manually sending PINGs to the server. To overcome this limitation, this setting can be used to set up a PING/PONG message/response connection heartbeat. When the client sends the server message *heartbeat_in* (PING), the server automatically responds with *heartbeat_out* (PONG)."
  
  nchan_publisher [:srv, :loc, :if],
      :nchan_publisher_directive,
      :loc_conf,
      args: 0..2,
      legacy: "push_publisher",
      alt: :nchan_publisher_location,
      
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
      uri: "#request-authorization"
  
  nchan_subscribe_request [:srv, :loc, :if], 
      :ngx_http_set_complex_value_slot,
      [:loc_conf, :subscribe_request_url],
      
      group: "pubsub",
      tags: ['subscriber', 'hook'],
      value: "<url>",
      info: "Send GET request to internal location (which may proxy to an upstream server) after subscribing. Disabled for longpoll and interval-polling subscribers.",
      uri: "#subscriber-presence"
  
  nchan_unsubscribe_request [:srv, :loc, :if], 
      :ngx_http_set_unsubscribe_request_url,
      [:loc_conf, :unsubscribe_request_url],
      
      group: "pubsub",
      tags: ['subscriber', 'hook'],
      value: "<url>",
      info: "Send GET request to internal location (which may proxy to an upstream server) after unsubscribing. Disabled for longpoll and interval-polling subscribers.",
      uri: "#subscriber-presence"
  
  nchan_message_temp_path [:main],
      :ngx_conf_set_path_slot,
      [:main_conf, :message_temp_path],
      
      group: "storage",
      tags: ["memstore"],
      value: "<path>",
      default: "<client_body_temp_path>",
      info: "Large messages are stored in temporary files in the `client_body_temp_path` or the `nchan_message_temp_path` if the former is unavailable. Default is the built-in default `client_body_temp_path`"
      
  
  nchan_store_messages [:main, :srv, :loc, :if],
      :nchan_store_messages_directive,
      :loc_conf,
      legacy: "push_store_messages",
      
      group: "storage",
      tags: ['publisher'],
      value: [:on, :off],
      default: :on,
      info: "Publisher configuration. \"`off`\" is equivalent to setting `nchan_message_buffer_length 0`, which disables the buffering of old messages. Using this setting is not recommended when publishing very quickly, as it may result in missed messages."
    
  nchan_shared_memory_size [:main],
      :nchan_conf_set_size_slot,
      [:main_conf, :shm_size],
      legacy: ["push_max_reserved_memory", "nchan_max_reserved_memory"],
      
      group: "storage",
      tags: ['memstore'],
      value: "<size>",
      default: "128M",
      info: "Shared memory slab pre-allocated for Nchan. Used for channel statistics, message storage, and interprocess communication.",
      uri: "#memory-storage"
  
  nchan_permessage_deflate_compression_level [:main],
      :nchan_conf_deflate_compression_level_directive,
      :main_conf,
      
      group: "pubsub",
      tags: ['subscriber-websocket'],
      value: ["0-9"],
      default: "6",
      info: "Compression level for the `deflate` algorithm used in websocket's permessage-deflate extension. 0: no compression, 1: fastest, worst, 9: slowest, best"
  
  nchan_permessage_deflate_compression_strategy [:main],
      :nchan_conf_deflate_compression_strategy_directive,
      :main_conf,
      
      group: "pubsub",
      tags: ['subscriber-websocket'],
      value: ["default", "filtered", "huffman-only", "rle", "fixed"],
      default: "default",
      info: "Compression strategy for the `deflate` algorithm used in websocket's permessage-deflate extension. Use 'default' for normal data, For details see [zlib's section on copression strategies](http://zlib.net/manual.html#Advanced)"
  
  nchan_permessage_deflate_compression_window [:main],
      :nchan_conf_deflate_compression_window_directive,
      :main_conf,
      
      group: "pubsub",
      tags: ['subscriber-websocket'],
      value: ["9-15"],
      default: "10",
      info: "Compression window for the `deflate` algorithm used in websocket's permessage-deflate extension. The base two logarithm of the window size (the size of the history buffer). The bigger the window, the better the compression, but the more memory used by the compressor."
  
  nchan_permessage_deflate_compression_memlevel [:main],
      :nchan_conf_deflate_compression_memlevel_directive,
      :main_conf,
      
      group: "pubsub",
      tags: ['subscriber-websocket'],
      value: ["1-9"],
      default: "8",
      info: "Memory level for the `deflate` algorithm used in websocket's permessage-deflate extension. How much memory should be allocated for the internal compression state. 1 - minimum memory, slow and reduces compression ratio; 9 - maximum memory for optimal speed"
  
  nchan_redis_url [:main, :srv, :loc],
      :ngx_conf_set_redis_url,
      [:loc_conf, :"redis.url"],
      
      group: "storage",
      tags: ['redis'],
      value: "<redis-url>",
      default: "127.0.0.1:6379",
      info: "Use of this command is discouraged in favor of upstreams blocks with (`nchan_redis_server`)[#nchan_redis_server]. The path to a redis server, of the form 'redis://:password@hostname:6379/0'. Shorthand of the form 'host:port' or just 'host' is also accepted.",
      uri: "#connecting-to-a-redis-server"
  
  nchan_redis_pass [:main, :srv, :loc],
      :ngx_conf_set_redis_upstream_pass,
      [:loc_conf, :"redis"],
      
      group: "storage",
      tags: ['publisher', 'subscriber', 'redis'],
      value: "<upstream-name>",
      info: "Use an upstream config block for Redis servers.",
      uri: "#connecting-to-a-redis-server"
  
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
  
  nchan_redis_discovered_ip_range_blacklist [:upstream],
      :ngx_conf_set_redis_ip_blacklist,
      :srv_conf,
      args: 1..7,
      group: "storage",
      tags: ['redis'],
      value: "<CIDR range>",
      info: "do not attempt to connect to **autodiscovered** nodes with IPs in the specified ranges. Useful for blacklisting private network ranges for clusters and Redis slaves. NOTE that this blacklist applies only to autodiscovered nodes, and not ones specified in the upstream block"
  
  nchan_redis_server [:upstream],
      :ngx_conf_upstream_redis_server,
      :loc_conf,
      group: "storage",
      tags: ['redis'],
      value: "<redis-url>",
      info: "Used in upstream { } blocks to set redis servers. Redis url is in the form 'redis://:password@hostname:6379/0'. Shorthands 'host:port' or 'host' are permitted.",
      uri: "#connecting-to-a-redis-server"
  
  nchan_redis_storage_mode [:main, :srv, :upstream, :loc], 
      :ngx_conf_set_redis_storage_mode_slot,
      [:loc_conf, :"redis.storage_mode"],
      
      group: "storage",
      tags: ['redis'],
      value: ["distributed", "backup", "nostore"],
      default: "distributed",
      info: <<-EOS.gsub(/^ {8}/, '')
        The mode of operation of the Redis server. In `distributed` mode, messages are published directly to Redis, and retrieved in real-time. Any number of Nchan servers in distributed mode can share the Redis server (or cluster). Useful for horizontal scalability, but suffers the latency penalty of all message publishing going through Redis first.
        
        In `backup` mode, messages are published locally first, then later forwarded to Redis, and are retrieved only upon chanel initialization. Only one Nchan server should use a Redis server (or cluster) in this mode. Useful for data persistence without sacrificing response times to the latency of a round-trip to Redis.
        
        In `nostore` mode, messages are published as in `distributed` mode, but are not stored. Thus Redis is used to broadcast messages to many Nchan instances with no delivery guarantees during connection failure, and only local in-memory storage. This means that there are also no message delivery guarantees for subscribers switching from one Nchan instance to another connected to the same Redis server or cluster. Nostore mode increases Redis publishing capacity by an order of magnitude.
      EOS
  
  nchan_redis_nostore_fastpublish [:main, :srv, :upstream],
      :ngx_conf_set_flag_slot,
      [:loc_conf, :"redis.nostore_fastpublish"],
      
      group: "storage",
      tags: ['redis', 'publisher'],
      value: [:on, :off],
      default: :off,
      info: "Increases publishing capacity by 2-3x for Redis nostore mode at the expense of inaccurate subscriber counts in the publisher response."
  
  nchan_redis_username [:upstream],
      :ngx_conf_set_str_slot,
      [:srv_conf, :"redis.username"],
      
      group: "storage",
      tags: ['redis'],
      default: "<none>",
      info: "Set Redis username for AUTH command (available when using ACLs on the Redis server). All servers in the upstream block will use this username _unless_ a different username is specified by a server URL."
  
  nchan_redis_password [:upstream],
      :ngx_conf_set_str_slot,
      [:srv_conf, :"redis.password"],
      
      group: "storage",
      tags: ['redis'],
      default: "<none>",
      info: "Set Redis password for AUTH command. All servers in the upstream block will use this password _unless_ a different password is specified by a server URL."
  
  nchan_redis_ssl [:upstream],
      :ngx_conf_set_flag_slot,
      [:srv_conf, :"redis.tls.enabled"],
      alt: :nchan_redis_tls,
      
      group: "storage",
      tags: ['redis', 'ssl'],
      value: [:on, :off],
      default: :off,
      info: "Enables SSL/TLS for all connections to Redis servers in this upstream block. When enabled, no unsecured connections are permitted"
  
  nchan_redis_ssl_client_certificate [:upstream],
      :ngx_conf_set_str_slot,
      [:srv_conf, :"redis.tls.client_certificate"],
      alt: :nchan_redis_tls_client_certificate,
      
      group: "storage",
      tags: ['redis', 'ssl'],
      info: "Path to client certificate when using TLS for Redis connections"
  
  nchan_redis_ssl_client_certificate_key [:upstream],
      :ngx_conf_set_str_slot,
      [:srv_conf, :"redis.tls.client_certificate_key"],
      alt: :nchan_redis_tls_client_certificate,
      
      group: "storage",
      tags: ['redis', 'ssl'],
      info: "Path to client certificate key when using TLS for Redis connections"
  
  nchan_redis_ssl_server_name [:upstream],
      :ngx_conf_set_str_slot,
      [:srv_conf, :"redis.tls.server_name"],
      alt: :nchan_redis_tls_server_name,
      
      group: "storage",
      tags: ['redis', 'ssl'],
      info: "Server name to verify (CN) when using TLS for Redis connections"
  
  nchan_redis_ssl_trusted_certificate [:upstream],
      :ngx_conf_set_str_slot,
      [:srv_conf, :"redis.tls.trusted_certificate"],
      alt: :nchan_redis_tls_trusted_certificate,
      
      group: "storage",
      tags: ['redis', 'ssl'],
      info: "Trusted certificate (CA) when using TLS for Redis connections"
      
  nchan_redis_ssl_trusted_certificate_path [:upstream],
      :ngx_conf_set_str_slot,
      [:srv_conf, :"redis.tls.trusted_certificate_path"],
      alt: :nchan_redis_tls_trusted_certificate_path,
      
      group: "storage",
      default: "<system default>",
      tags: ['redis', 'ssl'],
      info: "Trusted certificate (CA) when using TLS for Redis connections. Defaults tothe system's SSL cert path unless nchan_redis_ssl_trusted_certificate is set"
  
  nchan_redis_ssl_ciphers [:upstream],
      :ngx_conf_set_str_slot,
      [:srv_conf, :"redis.tls.ciphers"],
      alt: :nchan_redis_tls_ciphers,
      
      group: "storage",
      default: "<system default>",
      tags: ['redis', 'ssl'],
      info: "Acceptable cipers when using TLS for Redis connections"
  
  nchan_redis_ssl_verify_certificate [:upstream],
      :ngx_conf_set_flag_slot,
      [:srv_conf, :"redis.tls.ciphers"],
      alt: :nchan_redis_tls_verify_certificate,
      
      group: "storage",
      value: [:on, :off],
      default: :on,
      tags: ['redis', 'ssl'],
      info: "Should the server certificate be verified when using TLS for Redis connections? Useful to disable when testing with a self-signed server certificate."
  
  nchan_use_redis [:main, :srv, :loc],
      :ngx_conf_enable_redis,
      [:loc_conf, :"redis.url_enabled"],
      
      group: "storage",
      tags: ['redis', 'publisher', 'subscriber'],
      value: [ :on, :off ],
      default: :off,
      info: "Use of this command is discouraged in favor of (`nchan_redis_pass`)[#nchan_redis_pass]. Use Redis for message storage at this location.",
      uri: "#connecting-to-a-redis-server"
  
  nchan_redis_ping_interval [:main, :srv, :upstream, :loc],
      :ngx_conf_set_sec_slot,
      [:loc_conf, :"redis.ping_interval"],
      
      group: "storage",
      tags: ['redis'],
      default: "4m",
      info: "Send a keepalive command to redis to keep the Nchan redis clients from disconnecting. Set to 0 to disable."
  
  nchan_redis_cluster_check_interval [:main, :srv, :upstream, :loc],
      :ngx_conf_set_sec_slot,
      [:loc_conf, :"redis.cluster_check_interval"],
      
      group: "storage",
      tags: ['redis'],
      default: "5s",
      info: "Send a CLUSTER INFO command to each connected Redis node to see if the cluster config epoch has changed. Sent only when in Cluster mode and if any other command that may result in a MOVE error has not been sent in the configured time."
  
  nchan_redis_wait_after_connecting [:main, :srv, :loc],
      :nchan_ignore_obsolete_setting,
      :loc_conf,
      
      group: "obsolete",
      tags: ['redis'],
      uncocumented: true
  
  nchan_redis_connect_timeout [:upstream],
      :ngx_conf_set_msec_slot,
      [:srv_conf, :"redis.connect_timeout"],
      
      group: "storage",
      tags: ['redis'],
      default: "600ms",
      info: "Redis server connection timeout."
  
  nchan_redis_subscribe_weights [:upstream],
      :ngx_conf_set_redis_subscribe_weights,
      :srv_conf,
      args: 1..2,
      
      group: "storage",
      tags: ['redis'],
      value: "master=<integer> slave=<integer>",
      default: "master=1 slave=1",
      info: "Determines how subscriptions to Redis PUBSUB channels are distributed between master and slave nodes. The higher the number, the more likely that each node of that type will be chosen for each new channel. The weights for slave nodes are cumulative, so an equal 1:1 master:slave weight ratio with two slaves would have a 1/3 chance of picking a master, and 2/3 chance of picking one of the slaves. The weight must be a non-negative integer."
  
  nchan_redis_optimize_target [:upstream],
      :ngx_conf_set_redis_optimize_target,
      :srv_conf,
      
      group: "storage",
      tags: ['redis'],
      value: [:cpu, :bandwidth],
      default: "cpu",
      info: "This tweaks whether [effect replication](https://redis.io/commands/eval#replicating-commands-instead-of-scripts) is enabled. Optimizing for CPU usage enables effect replication, costing additional bandwidth (between 1.2 and 2 times more) between all master->slave links. Optimizing for bandwidth increases CPU load on slaves, but keeps outgoing bandwidth used for replication the same as the incoming bandwidth on Master."
  
  nchan_redis_namespace [:main, :srv, :upstream, :loc], 
      :ngx_conf_set_str_slot,
      [:loc_conf, :"redis.namespace"],
      args: 1,
      post_handler: "ngx_conf_process_redis_namespace_slot",
      
      group: "storage",
      value: "<string>",
      tags: ['redis'],
      info: "Prefix all Redis keys with this string. All Nchan-related keys in redis will be of the form \"nchan_redis_namespace:*\" . Default is empty."
  
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
      alt: :nchan_message_max_buffer_length,
      
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
  
  nchan_access_control_allow_origin [:main, :srv, :loc, :if], 
      :ngx_http_set_complex_value_slot,
      [:loc_conf, :allow_origin],
      args: 1,
      
      group: "security",
      value: "<string>",
      tags: ['publisher', 'subscriber'],
      default: "$http_origin",
      info: "Set the [Cross-Origin Resource Sharing (CORS)](https://developer.mozilla.org/en-US/docs/Web/HTTP/Access_control_CORS) `Access-Control-Allow-Origin` header to this value. If the incoming request's `Origin` header does not match this value, respond with a `403 Forbidden`. Multiple origins can be provided in a single argument separated with a space."

  nchan_access_control_allow_credentials [:main, :srv, :loc, :if], 
      :ngx_conf_set_flag_slot,
      [:loc_conf, :allow_credentials],
      args: 1,
      
      group: "security",
      tags: ['publisher', 'subscriber'],
      default: "on",
      info: "When enabled, sets the [Cross-Origin Resource Sharing (CORS)](https://developer.mozilla.org/en-US/docs/Web/HTTP/Access_control_CORS) `Access-Control-Allow-Credentials` header to `true`."
  
  
  nchan_channel_group [:srv, :loc, :if], 
      :ngx_http_set_complex_value_slot, 
      [:loc_conf, :channel_group],
      legacy: "push_channel_group",
      
      group: "security",
      tags: ['publisher', 'subscriber', 'channel-events', 'group'],
      value: "<string>",
      default: "(none)",
      info: "The accounting and security group a channel belongs to. Works like a prefix string to the channel id. Can be set with nginx variables."
  
  nchan_channel_group_accounting [:srv, :loc], 
      :ngx_conf_set_flag_slot,
      [:loc_conf, "group.enable_accounting"],
      
      group: "security",
      tags: ['group', 'security'],
      default: "off",
      info: "Enable tracking channel, subscriber, and message information on a per-channel-group basis. Can be used to place upper limits on channel groups."
  
  nchan_group_location [:loc], 
      :nchan_group_directive, 
      [:loc_conf],
      args: 0..3,
      
      group: "security",
      tags: ['group'],
      value: ["get", "set", "delete", "off"],
      default: ["get", "set", "delete"],
      info: "Group information and configuration location. GET request for group info, POST to set limits, DELETE to delete all channels in group."
  
  nchan_group_max_channels [:loc], 
      :ngx_http_set_complex_value_slot, 
      [:loc_conf, "group.max_channels"],
      
      group: "security",
      tags: ['group', 'security'],
      value: "<number>",
      default: "0 (unlimited)",
      info: "Maximum number of channels allowed in the group."
  
  nchan_group_max_messages [:loc], 
      :ngx_http_set_complex_value_slot, 
      [:loc_conf, "group.max_messages"],
      
      group: "security",
      tags: ['group', 'security'],
      value: "<number>",
      default: "0 (unlimited)",
      info: "Maximum number of messages allowed for all the channels in the group."
  
  nchan_group_max_messages_memory [:loc], 
      :ngx_http_set_complex_value_slot, 
      [:loc_conf, "group.max_messages_shm_bytes"],
      
      group: "security",
      tags: ['group', 'security'],
      value: "<number>",
      default: "0 (unlimited)",
      info: "Maximum amount of shared memory allowed for the messages of all the channels in the group."
  
  nchan_group_max_messages_disk [:loc], 
      :ngx_http_set_complex_value_slot, 
      [:loc_conf, "group.max_messages_file_bytes"],
      
      group: "security",
      tags: ['group', 'security'],
      value: "<number>",
      default: "0 (unlimited)",
      info: "Maximum amount of disk space allowed for the messages of all the channels in the group."
  
  nchan_group_max_subscribers [:loc], 
      :ngx_http_set_complex_value_slot, 
      [:loc_conf, "group.max_subscribers"],
      
      group: "security",
      tags: ['group', 'security'],
      value: "<number>",
      default: "0 (unlimited)",
      info: "Maximum number of subscribers allowed for the messages of all the channels in the group."
  
  nchan_channel_events_channel_id [:srv, :loc, :if],
      :nchan_set_channel_events_channel_id,
      :loc_conf,
      args: 1,
      
      group: "meta",
      tags: ['publisher', 'subscriber', 'channel-events', 'introspection'],
      uri: "#channel-events",
      info: "Channel id where `nchan_channel_id`'s events should be sent. Events like subscriber enqueue/dequeue, publishing messages, etc. Useful for application debugging. The channel event message is configurable via nchan_channel_event_string. The channel group for events is hardcoded to 'meta'."
  
  nchan_stub_status [:loc],
      :nchan_stub_status_directive,
      :loc_conf,
      args: 0,
      
      group: "meta",
      tags: ['introspection'],
      info: "Similar to Nginx's stub_status directive, requests to an `nchan_stub_status` location get a response with some vital Nchan statistics. This data does not account for information from other Nchan instances, and monitors only local connections, published messages, etc.",
      uri: "#nchan_stub_status"
  
  nchan_channel_event_string [:srv, :loc, :if], 
      :ngx_http_set_complex_value_slot,
      [:loc_conf, :channel_event_string],
      
      group: "meta",
      tags: ['publisher', 'subscriber', 'channel-events', 'introspection'],
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
      default: 1024,
      info: "Maximum permissible channel id length (number of characters). This settings applies to ids before they may be split by the `nchan_channel_id_split_delimiter` Requests with a channel id that is too long will receive a `403 Forbidden` response."
  
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
  
  nchan_benchmark [:loc],
      :nchan_benchmark_directive,
      [:loc_conf],
      group: "development",
      undocumented: true,
      args: 0
  nchan_benchmark_time [:loc],
      :ngx_conf_set_sec_slot,
      [:loc_conf, "benchmark.time"],
      group: "development",
      undocumented: true
  nchan_benchmark_messages_per_channel_per_minute [:loc],
      :ngx_conf_set_num_slot,
      [:loc_conf, "benchmark.msgs_per_minute"],
      group: "development",
      undocumented: true
  nchan_benchmark_message_padding_bytes [:loc],
      :ngx_conf_set_num_slot,
      [:loc_conf, "benchmark.msg_padding"],
      group: "development",
      undocumented: true
  nchan_benchmark_channels [:loc],
      :ngx_conf_set_num_slot,
      [:loc_conf, "benchmark.channels"],
      group: "development",
      undocumented: true
  nchan_benchmark_subscribers_per_channel [:loc],
      :ngx_conf_set_num_slot,
      [:loc_conf, "benchmark.subscribers_per_channel"],
      group: "development",
      undocumented: true
  nchan_benchmark_subscriber_distribution [:loc],
      :nchan_benchmark_subscriber_distribution_directive,
      [:loc_conf, "benchmark.subscriber_distribution"],
      group: "development",
      undocumented: true
  nchan_benchmark_publisher_distribution [:loc],
      :nchan_benchmark_publisher_distribution_directive,
      [:loc_conf, "benchmark.publisher_distribution"],
      group: "development",
      undocumented: true

  
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
