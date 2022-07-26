--input: keys: [],  values: [ namespace, channel_id, publish_command, use_accurate_subscriber_count ]
--output: channel_hash {ttl, time_last_seen, subscribers, messages} or nil
-- delete this channel and all its messages
local ns = ARGV[1]
local id = ARGV[2]
local publish_command = ARGV[3]
local use_accurate_subscriber_count = tonumber(ARGV[4])~=0
local ch = ('%s{channel:%s}'):format(ns, id)
local key_msg=    ch..':msg:%s' --not finished yet
local key_channel=ch
local messages=   ch..':messages'
local subscribers=ch..':subscribers'
local pubsub=     ch..':pubsub'
local subscriber_counts = ch..':subscriber_counts'

redis.replicate_commands()

redis.call('echo', ' ####### DELETE #######')
local num_messages = 0
--delete all the messages right now mister!
local msg
while true do
  msg = redis.call('LPOP', messages)
  if msg then
    num_messages = num_messages + 1
    redis.call('DEL', key_msg:format(msg))
  else
    break
  end
end

local del_msgpack =cmsgpack.pack({"alert", "delete channel", id})
for k,channel_key in pairs(redis.call('SMEMBERS', subscribers)) do
  redis.call('PUBLISH', channel_key, del_msgpack)
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

local channel = nil
local subscriber_count = nil
if redis.call('EXISTS', key_channel) ~= 0 then
  channel = tohash(redis.call('hgetall', key_channel))
  --leave some crumbs behind showing this channel was just deleted
  redis.call('setex', ch..":deleted", 5, 1)
  
  if use_accurate_subscriber_count then
    local sub_counts = tohash(redis.call("HGETALL", subscriber_counts))
    subscriber_count = 0
    for k, v in pairs(sub_counts) do
      v = tonumber(v)
      local res = redis.call("PUBSUB", "NUMSUB", k)
      if tonumber(res[2]) >= 1 and v > 0 then
        subscriber_count = subscriber_count + tonumber(v)
      else
        redis.call("HDEL", subscriber_counts, k)
      end
    end
  else
    subscriber_count = tonumber(channel.fake_subscribers) or tonumber(channel.subscribers)
  end
  
end

redis.call('DEL', key_channel, messages, subscribers, subscriber_counts)
redis.call('PUBLISH', pubsub, del_msgpack)

if channel then
  return {
    tonumber(channel.ttl) or 0,
    tonumber(channel.last_seen_fake_subscriber) or 0,
    subscriber_count or 0,
    channel.current_message or "",
    tonumber(num_messages)
  }
else
  return nil
end
