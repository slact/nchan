--input:  keys: [], values: [channel_id, time, message, content_type, msg_ttl, subscriber_channel]
--output: message_tag, channel_hash {ttl, time_last_seen, subscribers}

local id=ARGV[1]
local time=tonumber(ARGV[2])
local msg={
  id=nil,
  data= ARGV[3],
  content_type=ARGV[4],
  ttl= tonumber(ARGV[5]),
  time= time,
  tag=  0,
  last_message=nil,
  oldest_message =nil
}
local enable_debug=true
local dbg = (function(on)
if on then
  return function(...)
  redis.call('echo', table.concat({...}))
end
  else
    return function(...)
    return
    end
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
  time_offset=  'pushmodule:message_time_offset',
  last_message= nil,
  message=      'channel:msg:%s:'..id, --not finished yet
  channel=      'channel:'..id,
  messages=     'channel:messages:'..id,
  subscribers=  'channel:subscribers:'..id
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
  dbg("last_time"..lasttime.." last_tag" ..lasttag.." msg_time"..msg.time)
  if lasttime==msg.time then
    msg.tag=lasttag+1
  end
end
msg.id=('%i:%i'):format(msg.time, msg.tag)
key.message=key.message:format(msg.id)

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
oldestmsg(key.messages, 'channel:msg:%s:'..id)
--update message list
redis.call('LPUSH', key.messages, msg.id)

--set expiration times for all the things
redis.call('EXPIRE', key.message, channel.ttl)
redis.call('EXPIRE', key.channel, channel.ttl)
redis.call('EXPIRE', key.messages, channel.ttl)
--redis.call('EXPIRE', key.subscribers,  channel.ttl)

--publish message
local msgpacked = cmsgpack.pack({time=msg.time, tag=msg.tag, content_type=msg.content_type, data=msg.data, channel=id})

local subscribers = redis.call('SMEMBERS', key.subscribers)
if subscribers and #subscribers > 0 then
  for k,channel_key in pairs(subscribers) do
    --not efficient, but useful for a few short-term subscriptions
    redis.call('PUBLISH', channel_key, msgpacked)
  end
  --clear short-term subscriber list
  redis.call('DEL', key.subscribers)
end
--now publish to the efficient channel
if redis.call('PUBSUB','NUMSUB', channel_pubsub)[2] > 0 then
  redis.call('PUBLISH', channel_pubsub, ('%i:%i:%s:%s'):format(msg.time, msg.tag, msg.content_type, msg.data))
end


return { msg.tag, {channel.ttl or msg.ttl, channel.time or msg.time, channel.subscribers or 0}, new_channel}