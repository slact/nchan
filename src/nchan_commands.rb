CfCmd.new do
  
  nchan_channel_id [:srv, :loc, :if],
      :nchan_set_pubsub_channel_id,
      :loc_conf,
      args: 1..4,
      alt: [ :nchan_pubsub_channel_id ],
      
      group: "pubsub",
      default: "(none)",
      info: "Channel id for a publisher or subscriber location. Can have up to 4 values to subscribe to up to 4 channels."
  
  nchan_publisher_channel_id [:srv, :loc, :if],
      :nchan_set_pub_channel_id,
      :loc_conf,
      args: 1,
      alt: [ :nchan_pub_channel_id ],
      
      group: "pubsub",
      default: "(none)",
      info: "Channel id for publisher location."
  
  nchan_subscriber_channel_id [:srv, :loc, :if],
      :nchan_set_sub_channel_id,
      :loc_conf,
      args: 1..4,
      alt: [ :nchan_sub_channel_id ],
      group: "pubsub",
          
      default: "(none)",
      info: "Channel id for subscriber location. Can have up to 4 values to subscribe to up to 4 channels."
  
  nchan_pubsub [:srv, :loc, :if],
      :nchan_pubsub_directive,
      :loc_conf,
      args: 0..5,
      
      group: "pubsub",
      default: "(none)",
      info: "Defines a server or location as a publisher and subscriber endpoint. For long-polling, GETs subscribe. and POSTs publish. For Websockets, publishing data on a connection does not yield a channel metadata response. Without additional configuration, this turns a location into an echo server."
  
  nchan_subscriber [:srv, :loc, :if],
      :nchan_subscriber_directive,
      :loc_conf,
      args: 0..4,
      legacy: "push_subscriber",
      
      group: "pubsub",
      value: ["any", "websocket", "eventsource", "longpoll", "intervalpoll"],
      default: "any (websocket|eventsource|longpoll)",
      info: "Defines a server or location as a subscriber. This location represents a subscriber's interface to a channel's message queue. The queue is traversed automatically via caching information request headers (If-Modified-Since and If-None-Match), beginning with the oldest available message. Requests for upcoming messages are handled in accordance with the setting provided. See the protocol documentation for a detailed description."
  
    nchan_subscriber_first_message [:srv, :loc, :if],
      :nchan_subscriber_first_message_directive,
      :loc_conf,
      args: 1,
      
      group: "pubsub",
      value: ["oldest", "newest"],
      default: "newest",
      info: "Controls the first message received by a new subscriber. 'oldest' returns the oldest available message in a channel's message queue, 'newest' waits until a message arrives."
  
  nchan_subscriber_concurrency [:main, :srv, :loc, :if],
      :nchan_set_subscriber_concurrency,
      [:loc_conf, :subscriber_concurrency],
      legacy: "push_subscriber_concurrency",
      
      group: "pubsub",
      value: [ :last, :first, :broadcast ],
      info: "Controls how multiple subscriber requests to a channel (identified by some common ID) are handled.The values work as follows:
      - broadcast: any number of concurrent subscriber requests may be held.
      - last: only the most recent subscriber request is kept, all others get a 409 Conflict response.
      - first: only the oldest subscriber request is kept, all others get a 409 Conflict response."
  
  nchan_publisher [:srv, :loc, :if],
      :nchan_publisher_directive,
      :loc_conf,
      args: 0,
      legacy: "push_publisher",
      
      group: "pubsub",
      info: "Defines a server or location as a message publisher. Requests to a publisher location are treated as messages to be sent to subscribers. See the protocol documentation for a detailed description."
  
  nchan_subscriber_timeout [:main, :srv, :loc, :if],
      :ngx_conf_set_sec_slot,
      [:loc_conf, :subscriber_timeout],
      legacy: "push_subscriber_timeout",
      
      group: "pubsub",
      value: "<number>",
      default: "0 (none)",
      info: "The length of time a subscriber's long-polling connection can last before it's timed out. If you don't want subscriber's connection to timeout, set this to 0. Applicable only if a push_subscriber is present in this or a child context."
  
  nchan_authorize_request [:srv, :loc, :if], 
      :ngx_http_set_complex_value_slot,
      [:loc_conf, :authorize_request_url],
      
      group: "security",
      value: "<url>",
      info: "Send GET request to internal location (which may proxy to an upstream server) for authorization of a publisher or subscriber request. A 200 response authorizes the request, a 403 response forbids it."
  
  nchan_store_messages [:main, :srv, :loc, :if],
      :nchan_store_messages_directive,
      :loc_conf,
      legacy: "push_store_messages",
      
      group: "storage",
      value: [:on, :off],
      default: :on,
      info: "Whether or not message queuing is enabled. \"Off\" is equivalent to the setting nchan_channel_buffer_length 0"
    
  nchan_max_reserved_memory [:main],
      :ngx_conf_set_size_slot,
      [:main_conf, :shm_size],
      legacy: "push_max_reserved_memory",
      
      group: "storage",
      value: "<size>",
      default: "32M",
      info: "The size of the shared memory chunk this module will use for message queuing and buffering."
    
  nchan_redis_url [:main],
      :ngx_conf_set_str_slot,
      [:main_conf, :redis_url],
      
      group: "storage",
      default: "127.0.0.1:6379",
      info: "The path to a redis server, of the form 'redis://:password@hostname:6379/0'. Shorthand of the form 'host:port' or just 'host' is also accepted."
  
  nchan_use_redis [:main, :srv, :loc],
      :ngx_conf_set_flag_slot,
      [:loc_conf, :use_redis],
      
      group: "storage",
      value: [ :on, :off ],
      default: :off,
      info: "Use redis for message storage at this location."
  
  nchan_message_timeout [:main, :srv, :loc], 
      :ngx_conf_set_sec_slot, 
      [:loc_conf, :buffer_timeout],
      legacy: "push_message_timeout",
      
      group: "storage",
      value: "<time>",
      default: "1h",
      info: "The length of time a message may be queued before it is considered expired. If you do not want messages to expire, set this to 0. Applicable only if a nchan_publisher is present in this or a child context."
  
  nchan_min_message_buffer_length [:main, :srv, :loc],
      :ngx_conf_set_num_slot,
      [:loc_conf, :min_messages],
      legacy: "push_min_message_buffer_length",
      
      group: "storage",
      value: "<number>",
      default: 1,
      info: "The minimum number of messages to store per channel. A channel's message  buffer will retain at least this many most recent messages."
  
  nchan_max_message_buffer_length [:main, :srv, :loc],
      :ngx_conf_set_num_slot,
      [:loc_conf, :max_messages],
      legacy: "push_max_message_buffer_length",
      
      group: "storage",
      value: "<number>",
      default: 10,
      info: "The maximum number of messages to store per channel. A channel's message buffer will retain a maximum of this many most recent messages."
  
    nchan_message_buffer_length [:main, :srv, :loc],
      :nchan_set_message_buffer_length,
      :loc_conf,
      legacy: "push_message_buffer_length",
      
      group: "storage",
      value: "<number>",
      default: "none",
      info: "The exact number of messages to store per channel. Sets both nchan_max_message_buffer_length and nchan_min_message_buffer_length to this value."
  
  nchan_authorized_channels_only [:main, :srv, :loc],
      :ngx_conf_set_flag_slot, 
      [:loc_conf, :authorize_channel],
      legacy: "push_authorized_channels_only",
      
      group: "security",
      value: [:on, :off],
      default: :off,
      info: "Whether or not a subscriber may create a channel by sending a request to a push_subscriber location. If set to on, a publisher must send a POST or PUT request before a subscriber can request messages on the channel. Otherwise, all subscriber requests to nonexistent channels will get a 403 Forbidden response."
  
  nchan_channel_group [:srv, :loc, :if], 
      :ngx_conf_set_str_slot, 
      [:loc_conf, :channel_group],
      legacy: "push_channel_group",
      
      group: "security",
      value: "<string>",
      default: "(none)",
      info: "Because settings are bound to locations and not individual channels, it is useful to be able to have channels that can be reached only from some locations and never others. That's where this setting comes in. Think of it as a prefix string for the channel id."
  
  nchan_channel_events_channel_id [:srv, :loc, :if],
      :nchan_set_channel_events_channel_id,
      :loc_conf,
      args: 1,
      
      group: "meta",
      info: "Channel id where `nchan_channel_id`'s events should be sent. Events like subscriber enqueue/dequeue, publishing messages, etc. Useful for application debugging. The channel event message is configurable via nchan_channel_event_string. The channel group for events is hardcoded to 'meta'."
  
  nchan_channel_event_string [:srv, :loc, :if], 
      :ngx_http_set_complex_value_slot,
      [:loc_conf, :channel_event_string],
      
      group: "meta",
      value: "<string>",
      default: "$nchan_channel_event $nchan_channel_id",
      info: "Contents of channel event message"
  
  nchan_max_channel_id_length [:main, :srv, :loc],
      :ngx_conf_set_num_slot,
      [:loc_conf, :max_channel_id_length],
      legacy: "push_max_channel_id_length",
      
      group: "security",
      value: "<number>",
      default: 512,
      info: "Maximum permissible channel id length (number of characters). Longer ids will be truncated."
  
  nchan_max_channel_subscribers [:main, :srv, :loc],
      :ngx_conf_set_num_slot,
      [:loc_conf, :max_channel_subscribers],
      legacy: "push_max_channel_subscribers",
      
      group: "security",
      value: "<number>",
      default: "0 (unlimited)",
      info: "Maximum concurrent subscribers."
  
  nchan_channel_timeout [:main, :srv, :loc],
      :ngx_conf_set_sec_slot,
      [:loc_conf, :channel_timeout],
      legacy: "push_channel_timeout",
      
      group: "development",
      info: "Amount of time an empty channel hangs around. Don't mess with this setting unless you know what you are doing!"
  
  nchan_storage_engine [:main, :srv, :loc],
      :nchan_set_storage_engine, 
      [:loc_conf, :storage_engine],
      legacy: "push_storage_engine",
      
      group: "development",
      info: "Development directive to completely replace default storage engine. Don't use unless you know what you're doing"
end