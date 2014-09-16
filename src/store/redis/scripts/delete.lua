--input: keys: [],  values: [ channel_id ]
-- delete this channel and all its messages
local id = ARGV[1]
local key_msg=    'channel:msg:%s:'..id --not finished yet
local key_channel='channel:'..id
local messages=   'channel:messages:'..id
local subscribers='channel:subscribers:'..id
local pubsub=     'channel:pubsub:'..id
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
dbg(' ####### DELETE #######')
--delete all the messages right now mister!
local msg
while true do
  msg = redis.call('LPOP', messages)
  if msg then
    redis.call('DEL', key_msg:format(msg))
  else
    break
  end
end

local del_msg="delete:" .. id
for k,channel_key in pairs(redis.call('SMEMBERS', subscribers)) do
  redis.call('PUBLISH', channel_key, del_msg)
end

local r= redis.call('DEL', key_channel, messages, subscribers)

if redis.call('PUBSUB','NUMSUB', pubsub)[2] > 0 then
  redis.call('PUBLISH', pubsub, "delete")
end

return r