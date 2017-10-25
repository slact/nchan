--input:  keys: [], values: [namespace, channel_id, msg_time, msg_tag, no_msgid_order, create_channel_ttl]
--output: result_code, msg_ttl, msg_time, msg_tag, prev_msg_time, prev_msg_tag, message, content_type, eventsource_event, compression_type, channel_subscriber_count
-- no_msgid_order: 'FILO' for oldest message, 'FIFO' for most recent
-- create_channel_ttl - make new channel if it's absent, with ttl set to this. 0 to disable.
-- result_code can be: 200 - ok, 404 - not found, 410 - gone, 418 - not yet available
local ns, id, time, tag, subscribe_if_current = ARGV[1], ARGV[2], tonumber(ARGV[3]), tonumber(ARGV[4])
local no_msgid_order=ARGV[5]
local create_channel_ttl=tonumber(ARGV[6]) or 0
local msg_id
if time and time ~= 0 and tag then
  msg_id=("%s:%s"):format(time, tag)
end

if redis.replicate_commands then
  redis.replicate_commands()
end

-- This script has gotten big and ugly, but there are a few good reasons 
-- to keep it big and ugly. It needs to do a lot of stuff atomically, and 
-- redis doesn't do includes. It could be generated pre-insertion into redis, 
-- but then error messages become less useful, complicating debugging. If you 
-- have a solution to this, please help.
local ch=('%s{channel:%s}'):format(ns, id)
local msgkey_fmt=ch..':msg:%s'
local key={
  next_message= msgkey_fmt, --hash
  message=      msgkey_fmt, --hash
  channel=      ch, --hash
  messages=     ch..':messages', --list
--  pubsub=       ch..':subscribers:', --set
}

--local dbg = function(...) redis.call('echo', table.concat({...})); end

redis.call('echo', ' #######  GET_MESSAGE ######## ')

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
      --dbg(list_key, " is empty")
      break
    end
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
local new_channel = false
if next(channel) == nil then
  if create_channel_ttl==0 then
    return {404, nil}
  end
  redis.call('HSET', key.channel, 'time', time)
  redis.call('EXPIRE', key.channel, create_channel_ttl)
  channel = {time=time}
  new_channel = true
end

local subs_count = tonumber(channel.subscribers)

if msg_id==nil then
  local found_msg_key
  if new_channel then
    --dbg("new channel")
    return {418, "", "", "", "", subs_count}
  else
    --dbg("no msg id given, ord="..no_msgid_order)
    
    if no_msgid_order == 'FIFO' then --most recent message
      --dbg("get most recent")
      found_msg_key=channel.current_message
    elseif no_msgid_order == 'FILO' then --oldest message
      --dbg("get oldest")
      found_msg_key=oldestmsg(key.messages, msgkey_fmt)
    end
    
    if found_msg_key == nil then
      --we await a message
      return {418, "", "", "", "", subs_count}
    else
      local msg=tohash(redis.call('HGETALL', found_msg_key))
      if not next(msg) then --empty
        return {404, "", "", "", "", subs_count}
      else
        --dbg(("found msg %s:%s  after %s:%s"):format(tostring(msg.time), tostring(msg.tag), tostring(time), tostring(tag)))
        local ttl = redis.call('TTL', found_msg_key)
        return {200, ttl, tonumber(msg.time) or "", tonumber(msg.tag) or "", tonumber(msg.prev_time) or "", tonumber(msg.prev_tag) or "", msg.data or "", msg.content_type or "", msg.eventsource_event or "", tonumber(msg.compression or 0), subs_count}
      end
    end
  end
else
  if msg_id and channel.current_message == msg_id
   or not channel.current_message then
    return {418, "", "", "", "", subs_count}
  end

  key.message=key.message:format(msg_id)
  local msg=tohash(redis.call('HGETALL', key.message))

  if next(msg) == nil then -- no such message. it might've expired, or maybe it was never there
    --dbg("MESSAGE NOT FOUND")
    return {404, nil}
  end

  local next_msg, next_msgtime, next_msgtag
  if not msg.next then --this should have been taken care of by the channel.current_message check
    --dbg("NEXT MESSAGE KEY NOT PRESENT. ERROR, ERROR!")
    return {404, nil}
  else
    --dbg("NEXT MESSAGE KEY PRESENT: " .. msg.next)
    key.next_message=key.next_message:format(msg.next)
    if redis.call('EXISTS', key.next_message)~=0 then
      local ntime, ntag, prev_time, prev_tag, ndata, ncontenttype, neventsource_event, ncompression=unpack(redis.call('HMGET', key.next_message, 'time', 'tag', 'prev_time', 'prev_tag', 'data', 'content_type', 'eventsource_event', 'compression'))
      local ttl = redis.call('TTL', key.next_message)
      --dbg(("found msg2 %i:%i  after %i:%i"):format(ntime, ntag, time, tag))
      return {200, ttl, tonumber(ntime) or "", tonumber(ntag) or "", tonumber(prev_time) or "", tonumber(prev_tag) or "", ndata or "", ncontenttype or "", neventsource_event or "", ncompression or 0, subs_count}
    else
      --dbg("NEXT MESSAGE NOT FOUND")
      return {404, nil}
    end
  end
end
