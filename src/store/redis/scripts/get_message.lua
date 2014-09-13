--input:  keys: [], values: [channel_id, msg_time, msg_tag, no_msgid_order, subscriber_channel]
--output: result_code, msg_time, msg_tag, message, content_type,  channel_subscriber_count
-- no_msgid_order: 'FILO' for oldest message, 'FIFO' for most recent
-- result_code can be: 200 - ok, 404 - not found, 410 - gone, 418 - not yet available
local id, time, tag, subscribe_if_current = ARGV[1], tonumber(ARGV[2]), tonumber(ARGV[3])
local no_msgid_order=ARGV[4]
local subscriber_channel = ARGV[5]
local msg_id
if time and tag then
  msg_id=("%s:%s"):format(time, tag)
end

local key={
  time_offset=   'pushmodule:message_time_offset',
  next_message= 'channel:msg:%s:'..id, --not finished yet
  message=      'channel:msg:%s:%s', --not done yet
  channel=      'channel:'..id,
  messages=     'channel:messages:'..id,
  pubsub=       'channel:subscribers:'..id
}


redis.call('ECHO', ' #######  GET_MESSAGE ######## ')

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

local dbg = function(msg)
  local enable
  enable=true
  if enable then
    redis.call('echo', msg)
  end
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
      --dbg(k.."="..v)
      h[k]=v; k=nil
    end
  end
  return h
end

if no_msgid_order ~= 'FIFO' then
  no_msgid_order = 'FILO'
end

local channel = tohash(redis.call('HGETALL', key.channel))
if channel == nil then
  dbg("CHANNEL NOT FOUND")
  return {404, nil}
end

if msg_id==nil or #msg_id==0 then
  dbg("no msg id given, ord="..no_msgid_order)
  if no_msgid_order == 'FIFO' then --most recent message
    dbg("get most recent")
    msg_id=channel.current_message
  elseif no_msgid_order == 'FILO' then --oldest message
    dbg("get oldest")
    msg_id=oldestmsg(key.messages, ('channel:msg:%s:'..id))
  end
  if msg_id == nil then
    return {404, nil}
  else
    local msg=tohash(redis.call('HGETALL', msg_id))
    if subscriber_channel and #subscriber_channel>0 then
      --unsubscribe from this channel.
      redis.call('SREM',  key.pubsub, subscriber_hannel)
    end
    return {200, msg.time, msg.tag, msg.data, msg.content_type}
  end
else
  key.message=key.message:format(msg_id, id)

  if msg_id and channel.current_message == msg_id then
    dbg("MESSAGE NOT READY")
    if subscriber_channel and #subscriber_channel>0 then
      --subscribe to this channel.
      redis.call('SADD',  key.pubsub, subscriber_channel)
    end
    return {418, nil}
  end

  local msg=tohash(redis.call('HGETALL', key.message))

  if next(msg) == nil then -- no such message. it might've expired, or maybe it was never there
    dbg("MESSAGE NOT FOUND")
    --subscribe if necessary
    if subscriber_channel and #subscriber_channel>0 then
      --subscribe to this channel.
      redis.call('SADD',  key.pubsub, subscriber_channel)
    end
    return {404, nil}
  end

  local next_msg, next_msgtime, next_msgtag
  if not msg.next then --this should have been taken care of by the channel.current_message check
    dbg("NEXT MESSAGE KEY NOT PRESENT. ERROR, ERROR!")
    return {404, nil}
  else
    dbg("NEXT MESSAGE KEY PRESENT: " .. msg.next)
    key.next_message=key.next_message:format(msg.next)
    if redis.call('EXISTS', key.next_message)~=0 then
      local ntime, ntag, ndata, ncontenttype=unpack(redis.call('HMGET', key.next_message, 'time', 'tag', 'data', 'content_type'))
      return {200, tonumber(ntime), tonumber(ntag), ndata, ncontenttype}
    else
      dbg("NEXT MESSAGE NOT FOUND")
      return {404, nil}
    end
  end
end