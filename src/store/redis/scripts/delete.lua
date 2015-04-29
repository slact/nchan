--input: keys: [],  values: [ channel_id ]
--output: channel_hash {ttl, time_last_seen, subscribers, messages} or nil
-- delete this channel and all its messages
local id = ARGV[1]
local key_msg=    'channel:msg:%s:'..id --not finished yet
local key_channel='channel:'..id
local messages=   'channel:messages:'..id
local subscribers='channel:subscribers:'..id
local pubsub=     'channel:pubsub:'..id

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

dbg(' ####### DELETE #######')
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

local nearly_departed = nil
if redis.call('EXISTS', key_channel) ~= 0 then
  nearly_departed = redis.call('hmget', key_channel, 'ttl', 'time_last_seen', 'subscribers')
  for i = 1, #nearly_departed do
    nearly_departed[i]=tonumber(nearly_departed[i]) or 0
  end
  
  --leave some crumbs behind showing this channel was just deleted
  redis.call('setex', "channel:deleted:"..id, 5, 1)
  
  table.insert(nearly_departed, num_messages)
end

redis.call('DEL', key_channel, messages, subscribers)

if redis.call('PUBSUB','NUMSUB', pubsub)[2] > 0 then
  redis.call('PUBLISH', pubsub, del_msgpack)
end

return nearly_departed