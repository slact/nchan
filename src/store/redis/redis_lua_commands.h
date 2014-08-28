//don't edit this please, it was auto-generated

typedef struct {
  char *delete;

  char *get_last_message;

  //input:  keys: [], values: [channel_id, msg_time, msg_tag]
  //output: result_code, message
  // result_code can be: 200 - ok, 404 - not found, 410 - gone
  char *get_message;

  //input:  keys: [], values: [channel_id, time, message, content_type, msg_ttl]
  //output: message_tag, channel_hash
  char *publish;

  char *subscribe;

} nhpm_redis_lua_scripts_t;

static nhpm_redis_lua_scripts_t nhpm_rds_lua_scripts = {
  //delete
  "local id = ARGV[1]\n"
  "local keys= {\n"
  "  'channel:'..id,\n"
  "  'channel:messages:'..id,\n"
  "  'channel:last_message:'..id\n"
  "}\n"
  "\n"
  "return redis.call('DEL', unpack(keys))\n"
  "",

  //get_last_message
  " \n",

  //get_message
  "--input:  keys: [], values: [channel_id, msg_time, msg_tag]\n"
  "--output: result_code, message\n"
  "-- result_code can be: 200 - ok, 404 - not found, 410 - gone\n"
  "local id, time, tag = ARGV[1], tonumber(ARGV[2]), tonumber(ARGV[3])\n"
  "local key={\n"
  "  time_offset=   'pushmodule:message_time_offset',\n"
  "  last_message= 'channel:msg:%s:'..id, --not finished yet\n"
  "  message=      ('channel:msg:%s:%s:%s'):format(time, tag, id),\n"
  "  channel=      'channel:'..id,\n"
  "  messages=     'channel:messages:'..id\n"
  "}\n"
  "\n"
  "local channel = redis.call('HGETALL', key.channel)\n"
  "if channel == nil then\n"
  "  return {404, nil}\n"
  "end\n"
  "\n"
  "if channel.prev_message ==  then\n"
  "  \n"
  "end\n",

  //publish
  "--input:  keys: [], values: [channel_id, time, message, content_type, msg_ttl]\n"
  "--output: message_tag, channel_hash\n"
  "\n"
  "local id=ARGV[1]\n"
  "local time=tonumber(ARGV[2])\n"
  "local msg={\n"
  "  id=nil,\n"
  "  data= ARGV[3],\n"
  "  content_type=ARGV[4],\n"
  "  ttl= tonumber(ARGV[5]),\n"
  "  time= time,\n"
  "  tag=  0,\n"
  "  last_message=nil,\n"
  "  oldest_message =nil\n"
  "}\n"
  "\n"
  "if type(msg.content_type)=='string' and msg.content_type:find(':') then\n"
  "  return {err='Message content-type cannot contain \":\" character.'}\n"
  "end\n"
  "\n"
  "\n"
  "-- sets all fields for a hash from a dictionary\n"
  "local hmset = function (key, dict)\n"
  "  if next(dict) == nil then return nil end\n"
  "  local bulk = {}\n"
  "  for k, v in pairs(dict) do\n"
  "    table.insert(bulk, k)\n"
  "    table.insert(bulk, v)\n"
  "  end\n"
  "  return redis.call('HMSET', key, unpack(bulk))\n"
  "end\n"
  "local echo=function(val)\n"
  "  redis.call('ECHO', val)\n"
  "end\n"
  "\n"
  "local key={\n"
  "  time_offset=   'pushmodule:message_time_offset',\n"
  "  last_message= 'channel:msg:%s:'..id, --not finished yet\n"
  "  message=      'channel:msg:%s:'..id, --not finished yet\n"
  "  channel=      'channel:'..id,\n"
  "  messages=     'channel:messages:'..id,\n"
  "  pubsub=       'channel:pubsub:'..id\n"
  "}\n"
  "\n"
  "local channel = redis.call('HGETALL', key.channel)\n"
  "if channel~=nil and channel.current_message ~= nil then\n"
  "  key.last_message=key.last_message:format(channel.current_message)\n"
  "else\n"
  "  channel={}\n"
  "  key.last_message=nil\n"
  "end\n"
  "\n"
  "--set new message id\n"
  "local last_time, last_tag\n"
  "if key.last_message then\n"
  "  last_time, last_tag = redis.call('HMGET', key.last_message, 'time', 'tag')\n"
  "  if tonumber(last_time)==msg.time then\n"
  "    msg.tag=tonumber(last_tag)+1\n"
  "  end\n"
  "end\n"
  "msg.id=('%i:%i'):format(msg.time, msg.tag)\n"
  "key.message=key.message:format(msg.id)\n"
  "\n"
  "msg.prev=channel.current_message\n"
  "\n"
  "--update channel\n"
  "if channel.ttl then\n"
  "  redis.call('HMSET', key.channel, 'current_message', msg.id, 'prev_message', msg.prev, 'time', msg.time)\n"
  "else\n"
  "  channel.ttl = msg.ttl\n"
  "  redis.call('HMSET', key.channel, 'current_message', msg.id, 'prev_message', msg.prev, 'time', msg.time, 'ttl', msg.ttl)\n"
  "end\n"
  "\n"
  "--write message\n"
  "hmset(key.message, msg)\n"
  "\n"
  "--make sure the time offset key is set\n"
  "local time_offset=tonumber(redis.call('GET', key.time_offset))\n"
  "if time_offset == nil then\n"
  "  redis.call('SET', key.time_offset, time)\n"
  "end\n"
  "\n"
  "local msg_score  = ('%i.%05i'):format(msg.time-time_offset, msg.tag)\n"
  "redis.call('ZADD', key.messages, msg_score, msg.id)\n"
  "\n"
  "--remove old messages from zset\n"
  "local num_messages_removed= redis.call('ZREMRANGEBYSCORE', key.messages, '-inf', time-time_offset-channel.ttl)\n"
  "redis.call('ECHO', \"removed \" .. num_messages_removed .. \"messages\")\n"
  "\n"
  "--set expiration times for all the things\n"
  "redis.call('EXPIRE', key.message, channel.ttl)\n"
  "redis.call('EXPIRE', key.channel, channel.ttl)\n"
  "redis.call('EXPIRE', key.messages, channel.ttl)\n"
  "redis.call('EXPIRE', key.pubsub,  channel.ttl)\n"
  "\n"
  "--publish message\n"
  "--might there be a more efficient way?\n"
  "redis.call('PUBLISH', key.pubsub, ('%i:%i:%s:%s'):format(msg.time, msg.tag, msg.content_type, msg.data))\n"
  "\n"
  "return { msg.tag, {ttl=(channel or msg).ttl, time=(channel or msg).time, subscribers=channel.subscribers or 0} }\n",

  //subscribe
  "local foo=\"bar\"\n"
  "local bar=\"baz\"\n"
  "return 11\n"
  ""
};

static nhpm_redis_lua_scripts_t nhpm_rds_lua_hashes = {
  "e50f84ac41fab66a2bc7e54111f21bcfe54f5b24",
  "b858cb282617fb0956d960215c8e84d1ccf909c6",
  "b5a0cd653b844c8e6a0d7da5e0c4498efdd4b831",
  "3e4fc31fda33bbf7e03d49eb32042a8f4ab6bccb",
  "4c4b4270fea171f083da79fc68075c728fa3dc91"
};

static nhpm_redis_lua_scripts_t nhpm_rds_lua_script_names = {
  "delete",
  "get_last_message",
  "get_message",
  "publish",
  "subscribe",
};

