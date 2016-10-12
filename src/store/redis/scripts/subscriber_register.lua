--input: keys: [], values: [channel_id, subscriber_id, active_ttl]
--  'subscriber_id' can be '-' for new id, or an existing id
--  'active_ttl' is channel ttl with non-zero subscribers. -1 to persist, >0 ttl in sec
--output: subscriber_id, num_current_subscribers, next_keepalive_time

local id, sub_id, active_ttl, concurrency = ARGV[1], ARGV[2], tonumber(ARGV[3]) or 20, ARGV[4]

--local dbg = function(...) redis.call('echo', table.concat({...})); end

redis.call('echo', ' ######## SUBSCRIBER REGISTER SCRIPT ####### ')
local ch=("{channel:%s}"):format(id)
local keys = {
  channel =     ch,
  messages =    ch..':messages:',
  subscribers = ch..':subscribers'
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

local random_safe_next_ttl = function(ttl)
  return math.floor(ttl/2 + ttl/2.1 * math.random())
end

local sub_count

if sub_id == "-" then
  sub_id = tonumber(redis.call('HINCRBY', keys.channel, "last_subscriber_id", 1))
  sub_count=tonumber(redis.call('hincrby', keys.channel, 'subscribers', 1))
else
  sub_count=tonumber(redis.call('hget', keys.channel, 'subscribers'))
end

local next_keepalive 
local actual_ttl = tonumber(redis.call('ttl', keys.channel))
if actual_ttl < active_ttl then
  setkeyttl(active_ttl)
  next_keepalive = random_safe_next_ttl(active_ttl)
else
  next_keepalive = random_safe_next_ttl(actual_ttl)
end

return {sub_id, sub_count, next_keepalive}
