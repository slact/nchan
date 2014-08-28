--input:  keys: [], values: [channel_id, msg_time, msg_tag]
--output: result_code, msg_time, msg_tag, message
-- result_code can be: 200 - ok, 404 - not found, 410 - gone, 418 - not yet available
local id, time, tag, subscribe_if_current = ARGV[1], tonumber(ARGV[2]), tonumber(ARGV[3])
local msg_id=("%s:%s"):format(time, tag)
local key={
  time_offset=   'pushmodule:message_time_offset',
  next_message= 'channel:msg:%s:'..id, --not finished yet
  message=      ('channel:msg:%s:%s'):format(msg_id, id),
  channel=      'channel:'..id,
  messages=     'channel:messages:'..id
}
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
      dbg(k.."="..v)
      h[k]=v; k=nil
    end
  end
  return h
end

local channel = tohash(redis.call('HGETALL', key.channel))
if channel == nil then
  dbg("CHANNEL NOT FOUND")
  return {404, nil}
elseif channel.current_message == msg_id then
  dbg("MESSAGE NOT READY")
  return {418, nil}
end

local msg=tohash(redis.call('HGETALL', key.message))

if msg==nil then -- no such message. it might've expired, or maybe it was never there...
  dbg("MESSAGE NOT FOUND")
  return {404, nil}
end

local next_msg, next_msgtime, next_msgtag
if not msg.next then --this should have been taken care of by the channel.current_message check
  dbg("NEXT MESSAGE KEY NOT PRESENT")
  return {404, nil}
else
  key.next_message=key.next_message:format(msg.next)
  if redis.call('EXISTS', key.next_message)~=0 then
    local ntime, ntag, ndata=unpack(redis.call('HMGET', key.next_message, 'time', 'tag', 'data'))
    return {200, ntime, ntag, ndata}
  else
    dbg("NEXT MESSAGE NOT FOUND")
    return {404, nil}
  end
end