--input: keys: [], values: [namespace, channel_id, subscriber_id, empty_ttl]
-- 'subscriber_id' is an existing id
-- 'empty_ttl' is channel ttl when without subscribers. 0 to delete immediately, -1 to persist, >0 ttl in sec
--output: subscriber_id, num_current_subscribers

local ns, id, sub_id, empty_ttl = ARGV[1], ARGV[2], ARGV[3], tonumber(ARGV[4]) or 20

--local dbg = function(...) redis.call('echo', table.concat({...})); end

redis.call('echo', ' ######## SUBSCRIBER UNREGISTER SCRIPT ####### ')
local ch=('%s{channel:%s}'):format(ns, id)
local keys = {
  channel =     ch,
  messages =    ch..':messages',
  subscribers = ch..':subscribers',
  subscriber_counts = ch..':subscriber_counts'
}

local setkeyttl=function(ttl)
  for i,v in pairs(keys) do
    if ttl > 0 then
      redis.call('expire', v, ttl)
    elseif ttl < 0 then
      redis.call('persist', v)
    else
      redis.call('del', v)
    end
  end
end

local sub_count = 0

local res = redis.pcall('EXISTS', keys.channel)
if type(res) == "table" and res["err"] then
  return {err = ("CLUSTER KEYSLOT ERROR. %i %s"):format(empty_ttl, id)}
end

if res ~= 0 then
   sub_count = redis.call('hincrby', keys.channel, 'subscribers', -1)

  if sub_count == 0 and tonumber(redis.call('LLEN', keys.messages)) == 0 then
    setkeyttl(empty_ttl)
  elseif sub_count < 0 then
    return {err="Subscriber count for channel " .. id .. " less than zero: " .. sub_count}
  end
else
  --dbg("channel ", id, " already gone")
end

return {sub_id, sub_count}
