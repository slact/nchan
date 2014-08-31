--input: keys: [],  values: [ channel_id ]
-- delete this channel and all its messages
local id = ARGV[1]
local key_msg=    'channel:msg:%s:'..id --not finished yet
local key_channel='channel:'..id
local messages=   'channel:messages:'..id
local pubsub=     'channel:pubsub:'..id

--delete all the messages right now mister!
local msg
while true do
  msg = redis.call('lpop', id)
  if msg then
    redis.call('del', key_msg:format(msg))
  else
    break
  end
end

--TODO: publish delete channel message maybe

redis.call('del', key_channel, messages, pubsub)

return redis.call('DEL', key_channel, messages, pubsub)
