--input: keys: [], values: [channel_id, subscriber_id, empty_ttl]
-- 'subscriber_id' is an existing id
-- 'empty_ttl' is channel ttl when without subscribers. 0 to delete immediately, -1 to persist, >0 ttl in sec
--output: subscriber_id, num_current_subscribers

local id, sub_id, empty_ttl = ARGV[1], ARGV[2], tonumber(ARGV[3]) or 20

local enable_debug=true
local dbg = (function(on)
if on then return function(...) redis.call('echo', table.concat({...})); end
  else return function(...) return; end end
end)(enable_debug)

dbg(' ######## SUBSCRIBER UNREGISTER SCRIPT ####### ')

local keys = {
  channel =     'channel:'..id,
  messages =    'channel:messages:'..id,
  subscribers = 'channel:subscribers:'..id,
  subscriber_id='channel:next_subscriber_id:'..id --integer
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
if redis.call('EXISTS', keys.channel) ~= 0 then
   sub_count = redis.call('hincrby', keys.channel, 'subscribers', -1)

  if sub_count == 0 then
    setkeyttl(empty_ttl)
  elseif sub_count < 0 then
    return {err="Subscriber count for channel " .. id .. " less than zero: " .. sub_count}
  end
else
  dbg("channel ", id, " already gone")
end

return {sub_id, sub_count}