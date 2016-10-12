--input: keys: [],  values: [ channel_id ]
--output: channel_hash {ttl, time_last_seen, subscribers, messages} or nil
-- delete this channel and all its messages
local id = ARGV[1]
local ch = ('{channel:%s}'):format(id)
local key_msg=    ch..':msg:%s' --not finished yet
local key_channel=ch
local messages=   ch..':messages'
local subscribers=ch..':subscribers'
local pubsub=     ch..':pubsub'

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

local nearly_departed = nil
if redis.call('EXISTS', key_channel) ~= 0 then
  nearly_departed = redis.call('hmget', key_channel, 'ttl', 'time_last_seen', 'subscribers', 'fake_subscribers', 'current_message')
  if(nearly_departed[4]) then
    --replace subscribers count with fake_subscribers
    nearly_departed[3]=nearly_departed[4]
    table.remove(nearly_departed, 4)
  end
  for i = 1, 4 do
    nearly_departed[i]=tonumber(nearly_departed[i]) or 0
  end
  if type(nearly_departed[5]) ~= "string" then
    nearly_departed[5]=""
  end
  
  --leave some crumbs behind showing this channel was just deleted
  redis.call('setex', ch..":deleted", 5, 1)
  
  table.insert(nearly_departed, num_messages)
end

redis.call('DEL', key_channel, messages, subscribers)

if redis.call('PUBSUB','NUMSUB', pubsub)[2] > 0 then
  redis.call('PUBLISH', pubsub, del_msgpack)
end

return nearly_departed
