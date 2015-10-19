CfCmd.new do
  nchan_message_timeout [:main, :srv, :loc], 
      :ngx_conf_set_sec_slot, 
      [:loc_conf, :buffer_timeout],
      legacy: "push_message_timeout"
  
  nchan_max_reserved_memory [:main],
      :ngx_conf_set_size_slot,
      [:main_conf, :shm_size],
      legacy: "push_max_reserved_memory"
  
  nchan_min_message_buffer_length [:main, :srv, :loc],
      :ngx_conf_set_num_slot,
      [:loc_conf, :min_messages],
      legacy: "push_min_message_buffer_length"
  
  nchan_max_message_buffer_length [:main, :srv, :loc],
      :ngx_conf_set_num_slot,
      [:loc_conf, :max_messages],
      legacy: "push_max_message_buffer_length"
  
  nchan_message_buffer_length [:main, :srv, :loc],
      :nchan_set_message_buffer_length,
      :loc_conf,
      legacy: "push_message_buffer_length"
  
  nchan_delete_oldest_received_message [:main, :srv, :loc],
      :ngx_conf_set_flag_slot,
      [:loc_conf, :delete_oldest_received_message],
      legacy: "push_delete_oldest_received_message"
  
  nchan_publisher [:srv, :loc],
      :nchan_publisher,
      :loc_conf,
      args: 0,
      legacy: "push_publisher"
  
  nchan_subscriber [:srv, :loc],
      :nchan_subscriber,
      [:loc_conf, :subscriber_poll_mechanism],
      args: 0..1,
      legacy: "push_subscriber"
  
  nchan_subscriber_concurrency [:main, :srv, :loc],
      :nchan_set_subscriber_concurrency,
      [:loc_conf, :subscriber_concurrency],
      legacy: "push_subscriber_concurrency"
  
  nchan_subscriber_timeout [:main, :srv, :loc],
      :ngx_conf_set_sec_slot,
      [:loc_conf, :subscriber_timeout],
      legacy: "push_subscriber_timeout"
  
  nchan_authorized_channels_only [:main, :srv, :loc],
      :ngx_conf_set_flag_slot, 
      [:loc_conf, :authorize_channel],
      legacy: "push_authorized_channels_only"
  
  nchan_store_messages [:main, :srv, :loc],
      :nchan_store_messages_directive,
      :loc_conf,
      legacy: "push_store_messages"
  
  nchan_channel_group [:srv, :loc], 
      :ngx_conf_set_str_slot, 
      [:loc_conf, :channel_group],
      legacy: "push_channel_group"
  
  nchan_max_channel_id_length [:main, :srv, :loc],
      :ngx_conf_set_num_slot,
      [:loc_conf, :max_channel_id_length],
      legacy: "push_max_channel_id_length"
  
  nchan_max_channel_subscribers [:main, :srv, :loc],
      :ngx_conf_set_num_slot,
      [:loc_conf, :max_channel_subscribers],
      legacy: "push_max_channel_subscribers"
  
  nchan_ignore_queue_on_no_cache [:main, :srv, :loc],
      :ngx_conf_set_flag_slot,
      [:loc_conf, :ignore_queue_on_no_cache],
      legacy: "push_ignore_queue_on_no_cache"
  
  nchan_channel_timeout [:main, :srv, :loc],
      :ngx_conf_set_sec_slot,
      [:loc_conf, :channel_timeout],
      legacy: "push_channel_timeout"
  
  nchan_storage_engine [:main, :srv, :loc],
      :nchan_set_storage_engine, 
      [:loc_conf, :storage_engine],
      legacy: "push_storage_engine"
end