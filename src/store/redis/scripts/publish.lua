local id=ARGV[1]
local time=tonumber(ARGV[2])
local msg={
  id="%s:%s",
  data= ARGV[3],
  content_type=ARGV[4],
  ttl= ARGV[5],
  time= time,
  tag=  0,
  prev_message=nil,
  old_message =nil
}

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
local echo=function(val)
  redis.call('ECHO', val)
end

local key={
  time_offset=  KEYS[1],
  last_message= 'channel:msg:%s:'..id, --not finished yet
  message=      'channel:msg:%s:'..id, --not finished yet
  channel=      'channel:'..id,
  messages=     'channel:messages:'..id,
  pubsub=       'channel:pubsub:'..id
}

local channel = redis.call('HGETALL', key.channel)
if channel~=nil and channel.last_message ~=nil then
  key.last_message=key.last_message:format(channel.last_message)
else
  channel={}
  key.last_message=nil
end

--set new message id
local last_time, last_tag
if key.last_message then
  last_time, last_tag = redis.call('HMGET', key.last_message, 'time', 'tag')
  if tonumber(last_time)==msg.time then
    msg.tag=tonumber(last_tag)+1
  end
end
msg.id=msg.id:format(last_time, last_tag)
msg.last_message=channel.last_message
key.message=key.message:format(msg.id)

--update channel
redis.call("HMSET", key.channel, "last_message", msg.id, "time", msg.time)

msg.prev_msg=channel.last_message
--write message
hmset(key.message, msg)
--set new message ttl, which must expire before or at the same time as the previous
cedis.call('EXPIRE', key.message, msg.ttl)

--make sure the time offset key is set
local time_offset=tonumber(redis.call('GET', key.time_offset))
if time_offset == nil then
  redis.call('SET', key.time_offset, time)
end

local msg_score  = ('%i.%05i'):format(msg.time-time_offset, msg.tag)
redis.call("ZADD", key.messages, msg_score, msg.id)

return { msg.tag, channel }
