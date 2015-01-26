--input: keys: [], values: [channel_id, channel_empty_ttl, channel_active_ttl, action, concurrency, subscriber_id, [...]]
--output: num_current_subscribers, subscriber_id, [...]
--  input takes any number of subscriber ids. If no id available, use "-"
-- 'action' can be one of 'subscribe', 'unsubscribe'
-- 'concurrency' can be one of 'FIFO', 'FILO', and 'broadcast' . relevant only for 'subscribe' action                                                  

local id = ARGV[1]
local channel_empty_ttl = tonumber(ARGV[2]) or 20
local channel_active_ttl = tonumber(ARGV[3]) or 0
local action = ARGV[4]
local concurrency = ARGV[5]
local sub_ids = {}
for i=6, #ARGV do
  table.insert(sub_ids, ARGV[i])
end

local enable_debug=true
local dbg = (function(on)
if on then return function(...) redis.call('echo', table.concat({...})); end
  else return function(...) return; end end
end)(enable_debug)

dbg(' ######## SUBSCRIBER SCRIPT ####### ')

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

local check_concurrency_in = function(i, id)
  if concurrency == "FIFO" then
    return i==1 and id or "DENY"
  end
  return id
end

if concurrency == "FILO" then
  --kick out old subscribers
  
  
end

local sub_count
if action == 'subscribe' then
  sub_count= redis.call('hincrby', keys.channel, 'subscribers', #sub_ids)
  
  for i, v in ipairs(sub_ids) do
    if v == "-" then
      v = tonumber(redis.call('INCR', keys.subscriber_id))
      sub_ids[i]=check_concurrency_in(i, v)
    end
  end
  
  if sub_count > 0 and sub_count == #sub_ids then
    setkeyttl(channel_active_ttl)
  end
  
  
else --unsubscribe
  sub_count= redis.call('hincrby', keys.channel, 'subscribers', -#sub_ids)
  
  for i, v in ipairs(sub_ids) do
    if v == "-" then
      return {err="Trying to remove id-less subscriber for channel " .. id}
    end
  end
  
  if sub_count == 0 and #sub_ids > 0 then
    setkeyttl(channel_active_ttl)
  elseif sub_count < 0  and #sub_ids > 0 then
    return {err="Subscriber count for channel " .. id .. " less than zero: " .. sub_count}
  end
end

return {sub_count, unpack(sub_ids)}