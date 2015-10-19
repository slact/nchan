--input:  keys: [], values: [channel_id, time, message, content_type, msg_ttl, max_msg_buf_size]
--output: message_tag, channel_hash {ttl, time_last_seen, subscribers, messages}

local id=ARGV[1]
local time=tonumber(ARGV[2])
local msg={
  id=nil,
  data= ARGV[3],
  content_type=ARGV[4],
  ttl= tonumber(ARGV[5]),
  time= time,
  tag= 0
}
local store_at_most_n_messages = ARGV[6]
if store_at_most_n_messages == nil or store_at_most_n_messages == "" then
  return {err="Argument 6, max_msg_buf_size, can't be empty"}
end

local enable_debug=true
local dbg = (function(on)
  if on then return function(...) 
    local arg, cur = {...}, nil
    for i = 1, #arg do
      arg[i]=tostring(arg[i])
    end
    redis.call('echo', table.concat(arg))
  end; else
    return function(...) return; end
  end
end)(enable_debug)

if type(msg.content_type)=='string' and msg.content_type:find(':') then
  return {err='Message content-type cannot contain ":" character.'}
end

dbg(' #######  PUBLISH   ######## ')

-- sets all fields for a hash from a dictionary
local hmset = function (key, dict)
  if next(dict) == nil then return nil end
  local bulk = {}
  for k, v in pairs(dict) do
    table.insert(bulk, k)
    table.insert(bulk, v)
  end
  return redis.call('HMSET', key, unpack(bulk))
end

local tohash=function(arr)
  if type(arr)~="table" then
    return nil
  end
  local h = {}
  local k=nil
  for i, v in ipairs(arr) do
    if k == nil then
      k=v
    else
      h[k]=v; k=nil
    end
  end
  return h
end

local key={
  time_offset=  'nchan:message_time_offset',
  last_message= nil,
  message=      'channel:msg:%s:'..id, --not finished yet
  channel=      'channel:'..id,
  messages=     'channel:messages:'..id,
  subscribers=  'channel:subscribers:'..id,
  subscriber_id='channel:next_subscriber_id:'..id, --integer
}

local channel_pubsub = 'channel:pubsub:'..id

local new_channel
local channel
if redis.call('EXISTS', key.channel) ~= 0 then
  channel=tohash(redis.call('HGETALL', key.channel))
end

if channel~=nil then
  dbg("channel present")
  if channel.current_message ~= nil then
    dbg("channel current_message present")
    key.last_message=('channel:msg:%s:%s'):format(channel.current_message, id)
  else
    dbg("channel current_message absent")
    key.last_message=nil
  end
  new_channel=false
else
  dbg("channel missing")
  channel={}
  new_channel=true
  key.last_message=nil
end

--set new message id
if key.last_message then
  local lastmsg = redis.call('HMGET', key.last_message, 'time', 'tag')
  local lasttime, lasttag = tonumber(lastmsg[1]), tonumber(lastmsg[2])
  dbg("New message id: last_time ", lasttime, " last_tag ", lasttag, " msg_time ", msg.time)
  if lasttime==msg.time then
    msg.tag=lasttag+1
  end
end
msg.id=('%i:%i'):format(msg.time, msg.tag)

key.message=key.message:format(msg.id)
if redis.call('exists', key.message) ~= 0 then
  return {err=("Message for channel %s id %s already exists"):format(id, msg.id)}
end

msg.prev=channel.current_message
if key.last_message then
  redis.call('HSET', key.last_message, 'next', msg.id)
end

--update channel
redis.call('HSET', key.channel, 'current_message', msg.id)
if msg.prev then
  redis.call('HSET', key.channel, 'prev_message', msg.prev)
end
if msg.time then
  redis.call('HSET', key.channel, 'time', msg.time)
end
if not channel.ttl then
  channel.ttl=msg.ttl
  redis.call('HSET', key.channel, 'ttl', channel.ttl)
end

if not channel.max_stored_messages then
  channel.max_stored_messages = store_at_most_n_messages
  redis.call('HSET', key.channel, 'max_stored_messages', store_at_most_n_messages)
  dbg("channel.max_stored_messages was not set, but is now ", store_at_most_n_messages)
else
  channel.max_stored_messages =tonumber(channel.max_stored_messages)
  dbg("channel.mas_stored_messages == " , channel.max_stored_messages)
end

--write message
hmset(key.message, msg)


--check old entries
local oldestmsg=function(list_key, old_fmt)
  local old, oldkey
  local n, del=0,0
  while true do
    n=n+1
    old=redis.call('lindex', list_key, -1)
    if old then
      oldkey=old_fmt:format(old)
      local ex=redis.call('exists', oldkey)
      if ex==1 then
        return oldkey
      else
        redis.call('rpop', list_key)
        del=del+1
      end 
    else
      break
    end
  end
end

local max_stored_msgs = tonumber(redis.call('HGET', key.channel, 'max_stored_messages')) or -1

if max_stored_msgs < 0 then --no limit
  oldestmsg(key.messages, 'channel:msg:%s:'..id)
  redis.call('LPUSH', key.messages, msg.id)
elseif max_stored_msgs > 0 then
  local stored_messages = tonumber(redis.call('LLEN', key.messages))
  redis.call('LPUSH', key.messages, msg.id)
  if stored_messages > max_stored_msgs then
    local oldmsgid = redis.call('RPOP', key.messages)
    redis.call('DEL', 'channel:msg:'..id..':'..oldmsgid)
  end
  oldestmsg(key.messages, 'channel:msg:%s:'..id)
end


--set expiration times for all the things
for i, k in pairs(key) do
  if i ~= 'last_message' then
    redis.call('EXPIRE', k, channel.ttl)
  end
end

--publish message
local unpacked

if #msg.data < 5*1024 then
  unpacked= {
    "msg",
    msg.time,
    tonumber(msg.tag),
    msg.data,
    msg.content_type
  }
else
  unpacked= {
    "msgkey",
    msg.time,
    tonumber(msg.tag),
    key.message
  }
end

local msgpacked = cmsgpack.pack(unpacked)

dbg(("Stored message with id %i:%i => %s"):format(msg.time, msg.tag, msg.data))

--now publish to the efficient channel
if redis.call('PUBSUB','NUMSUB', channel_pubsub)[2] > 0 then
  msgpacked = cmsgpack.pack(unpacked)
  redis.call('PUBLISH', channel_pubsub, msgpacked)
end

local num_messages = redis.call('llen', key.messages)

dbg("channel ", id, " ttl: ",channel.ttl, ", subscribers: ", channel.subscribers, ", messages: ", num_messages)
return { msg.tag, {tonumber(channel.ttl or msg.ttl), tonumber(channel.time or msg.time), tonumber(channel.subscribers or 0), tonumber(num_messages)}, new_channel}