--input:  keys: [], values: [namespace, channel_id, status_code]
--output: current_subscribers

redis.call('echo', ' ####### PUBLISH STATUS ####### ')
--local dbg = function(...) redis.call('echo', table.concat({...})); end
local ns, id=ARGV[1], ARGV[2]
local code=tonumber(ARGV[3])
if code==nil then
  return {err="non-numeric status code given, bailing!"}
end

local pubmsg = "status:"..code
local ch = ('%s{channel:%s}'):format(ns, id)
local subs_key = ch..':subscribers'
local chan_key = ch
--local channel_pubsub = ch..':pubsub'

for k,channel_key in pairs(redis.call('SMEMBERS', subs_key)) do
  --not efficient, but useful for a few short-term subscriptions
  redis.call('PUBLISH', channel_key, pubmsg)
end
--clear short-term subscriber list
redis.call('DEL', subs_key)
--now publish to the efficient channel
--what?... redis.call('PUBLISH', channel_pubsub, pubmsg)
return redis.call('HGET', chan_key, 'subscribers') or 0
