--input: keys: [], values: [namespace, channel_id, subscriber_id, active_ttl_msec, ttl_safety_margin_msec, time, want_channel_settings]
--  'subscriber_id' can be '-' for new id, or an existing id
--  'active_ttl_msec' is channel ttl with non-zero subscribers. -1 to persist, >0 ttl in msec
--  'ttl_safety_margin_msec' is number of seconds before TTL that Nchan issues a keepalive recheck
--output: subscriber_id, num_current_subscribers, next_keepalive_time, channel_buffer_length
--  'channel_buffer_length' is returned only if want_channel_settings is 1

local ns, id, sub_id, active_ttl, ttl_safety_margin, time = ARGV[1], ARGV[2], ARGV[3], tonumber(ARGV[4]), tonumber(ARGV[5]), tonumber(ARGV[6])
local want_channel_settings = tonumber(ARGV[6]) == 1

--local dbg = function(...) redis.call('echo', table.concat({...})); end

redis.call('echo', ' ######## SUBSCRIBER REGISTER SCRIPT ####### ')
local ch=("%s{channel:%s}"):format(ns, id)
local keys = {
  channel =     ch,
  messages =    ch..':messages',
  subscribers = ch..':subscribers',
  subscriber_counts = ch..':subscriber_counts'
}

local setkeyttl=function(ttl)
  for i,v in pairs(keys) do
    if ttl > 0 then
      redis.call('PEXPIRE', v, ttl)
    else
      redis.call('PERSIST', v)
    end
  end
end

local sub_count

if sub_id == "-" then
  sub_id = tonumber(redis.call('HINCRBY', keys.channel, "last_subscriber_id", 1))
  sub_count=tonumber(redis.call('hincrby', keys.channel, 'subscribers', 1))
else
  sub_count=tonumber(redis.call('hget', keys.channel, 'subscribers'))
end
if time then
  redis.call('hset', keys.channel, "last_seen_subscriber", time)
end

local next_keepalive
local actual_ttl = tonumber(redis.call('PTTL', keys.channel))
local safe_ttl = active_ttl + ttl_safety_margin
if actual_ttl < safe_ttl then
  setkeyttl(safe_ttl)
  next_keepalive = active_ttl
else
  next_keepalive = (actual_ttl - ttl_safety_margin > 0) and (actual_ttl - ttl_safety_margin) or math.ceil(actual_ttl / 2)
end


local ret = {sub_id, sub_count, next_keepalive}
if want_channel_settings then
  local max_messages = tonumber(redis.call('hget', keys.channel, 'max_stored_messages'))
  table.insert(ret, max_messages)
end

return ret
