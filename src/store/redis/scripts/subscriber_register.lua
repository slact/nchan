--input: keys: [], values: [channel_id, subscriber_id, channel_empty_ttl, active_ttl, concurrency]
--  'subscriber_id' can be '-' for new id, or an existing id
--  'active_ttl' is channel ttl with non-zero subscribers. -1 to persist, >0 ttl in sec
--  'concurrency' can be 'FIFO', 'FILO', or 'broadcast'
--output: subscriber_id, num_current_subscribers

local id, sub_id, active_ttl, concurrency = ARGV[1], ARGV[2], tonumber(ARGV[3]) or 20, ARGV[4]

local enable_debug=true
local dbg = (function(on)
if on then return function(...) redis.call('echo', table.concat({...})); end
  else return function(...) return; end end
end)(enable_debug)

dbg(' ######## SUBSCRIBER REGISTER SCRIPT ####### ')

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
    else
      redis.call('persist', v)
    end
  end
end

--[[
local check_concurrency_in = function(i, id)
  if concurrency == "FIFO" then
    return i==1 and id or "DENY"
  end
  return id
end

if concurrency == "FILO" then
  --kick out old subscribers
  
  
end
]]

local sub_count

if sub_id == "-" then
  sub_id =tonumber(redis.call('INCR', keys.subscriber_id))
  sub_count=redis.call('hincrby', keys.channel, 'subscribers', 1)
else
  sub_count=redis.call('hget', keys.channel, 'subscribers')
end
setkeyttl(active_ttl)

dbg("id= ", tostring(sub_id), "count= ", tostring(sub_count))

return {sub_id, sub_count}