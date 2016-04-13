--input: keys: [], values: [channel_id, subscriber_id, active_ttl]
--  'subscriber_id' can be '-' for new id, or an existing id
--  'active_ttl' is channel ttl with non-zero subscribers. -1 to persist, >0 ttl in sec
--output: subscriber_id, num_current_subscribers

local id, sub_id, active_ttl, concurrency = ARGV[1], ARGV[2], tonumber(ARGV[3]) or 20, ARGV[4]

local dbg = function(...) redis.call('echo', table.concat({...})); end

dbg(' ######## SUBSCRIBER REGISTER SCRIPT ####### ')

local keys = {
  channel =     'channel:'..id,
  messages =    'channel:messages:'..id,
  subscribers = 'channel:subscribers:'..id
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

local sub_count

if sub_id == "-" then
  sub_id = tonumber(redis.call('HINCRBY', keys.channel, "last_subscriber_id", 1))
  sub_count=redis.call('hincrby', keys.channel, 'subscribers', 1)
else
  sub_count=redis.call('hget', keys.channel, 'subscribers')
end
setkeyttl(active_ttl)

dbg("id= ", tostring(sub_id), "count= ", tostring(sub_count))

return {sub_id, sub_count}
