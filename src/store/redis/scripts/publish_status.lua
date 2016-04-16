--input:  keys: [], values: [channel_id, status_code]
--output: current_subscribers

redis.call('echo', ' ####### PUBLISH STATUS ####### ')
--local dbg = function(...) redis.call('echo', table.concat({...})); end
local id=ARGV[1]
local code=tonumber(ARGV[2])
if code==nil then
  return {err="non-numeric status code given, bailing!"}
end

local pubmsg = "status:"..code
local subs_key = 'channel:subscribers:'..id
local chan_key = 'channel:'..id

for k,channel_key in pairs(redis.call('SMEMBERS', subs_key)) do
  --not efficient, but useful for a few short-term subscriptions
  redis.call('PUBLISH', channel_key, pubmsg)
end
--clear short-term subscriber list
redis.call('DEL', subs_key)
--now publish to the efficient channel
redis.call('PUBLISH', channel_pubsub, pubmsg)
return redis.call('HGET', chan_key, 'subscribers') or 0
