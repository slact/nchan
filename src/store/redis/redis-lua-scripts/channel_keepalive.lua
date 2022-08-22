--input:  keys: [], values: [namespace, channel_id, ttl_msec, ttl_safety_margin_msec]
-- ttl_msec is for when there are no messages but at least 1 subscriber.
--output: seconds until next keepalive is expected, or -1 for "let it disappear"
redis.call('ECHO', ' ####### CHANNEL KEEPALIVE ####### ')
local ns=ARGV[1]
local id=ARGV[2]
local ttl=tonumber(ARGV[3])
local ttl_safety_margin=tonumber(ARGV[4])
if not ttl then
  return {err="Invalid channel keepalive TTL (3rd arg)"}
end

--safe ttl is a bit greater than the expected ttl, so that Nchan can update it before it expires
local safe_ttl = ttl + ttl_safety_margin

local ch = ('%s{channel:%s}'):format(ns, id)
local key= {
  channel=   ch, --hash
  messages=  ch..':messages', --list
  subscribers = ch..':subscribers', --list
  subscriber_counts = ch..':subscriber_counts' --hash
}

local subs_count = tonumber(redis.call('HGET', key.channel, "subscribers")) or 0
local msgs_count = tonumber(redis.call('LLEN', key.messages)) or 0
local actual_ttl = tonumber(redis.call('PTTL',  key.channel))


if subs_count <= 0 then
  return -1
end

if msgs_count > 0 and actual_ttl > safe_ttl then
  local return_ttl =  actual_ttl - ttl_safety_margin > 0 and actual_ttl - ttl_safety_margin or math.ceil(actual_ttl / 2)
  return return_ttl
end

--refresh ttl
redis.call('PEXPIRE', key.channel, safe_ttl);
redis.call('PEXPIRE', key.messages, safe_ttl);
redis.call('PEXPIRE', key.subscribers, safe_ttl);
redis.call('PEXPIRE', key.subscriber_counts, safe_ttl);

return ttl

