--input:  keys: [], values: [channel_id, time, message, content_type, msg_ttl]
--output: message_tag, channel_hash

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

if type(msg.content_type)=='string' and msg.content_type:find(':') then
  return {err='Message content-type cannot contain ":" character.'}
end


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
  pubsub=       'channel:pubsub:'..id
}

local new_channel
local channel
if redis.call('EXISTS', key.channel) ~= 0 then
  channel=tohash(redis.call('HGETALL', key.channel))
end

if channel~=nil then
  echo("channel present")
  if channel.current_message ~= nil then
    echo("channel current_message present")
    key.last_message=('channel:msg:%s:%s'):format(channel.current_message, id)
  else
    echo("channel current_message absent")
    key.last_message=nil
  end
  new_channel=false
else
  echo("channel missing")
  channel={}
  new_channel=true
  key.last_message=nil
end

--set new message id
if key.last_message then
  local lastmsg = redis.call('HMGET', key.last_message, 'time', 'tag')
  local lasttime, lasttag = tonumber(lastmsg[1]), tonumber(lastmsg[2])
  echo("last_time"..lasttime.." last_tag" ..lasttag.." msg_time"..msg.time)
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
redis.call('EXPIRE', key.pubsub,  channel.ttl)

--publish message
--might there be a more efficient way?
redis.call('PUBLISH', key.pubsub, ('%i:%i:%s:%s'):format(msg.time, msg.tag, msg.content_type, msg.data))

return { msg.tag, {ttl=(channel or msg).ttl, time=(channel or msg).time, subscribers=channel.subscribers or 0}, new=new_channel }