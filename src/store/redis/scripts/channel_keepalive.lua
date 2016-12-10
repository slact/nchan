--input:  keys: [], values: [namespace, channel_id, ttl]
-- ttl is for when there are no messages but at least 1 subscriber.
--output: seconds until next keepalive is expected, or -1 for "let it disappear"
redis.call('ECHO', ' ####### CHANNEL KEEPALIVE ####### ')
local ns=ARGV[1]
local id=ARGV[2]
local ttl=tonumber(ARGV[3])
if not ttl then
  return {err="Invalid channel keepalive TTL (2nd arg)"}
end

local random_safe_next_ttl = function(ttl)
  return math.floor(ttl/2 + ttl/2.1 * math.random())
end
local ch = ('%s{channel:%s}'):format(ns, id)
local key={
  channel=   ch, --hash
  messages=  ch..':messages', --list
}
  
local subs_count = tonumber(redis.call('HGET', key.channel, "subscribers")) or 0
local msgs_count = tonumber(redis.call('LLEN', key.messages)) or 0
local actual_ttl = tonumber(redis.call('TTL',  key.channel))

if subs_count > 0 then
  if msgs_count > 0 and actual_ttl > ttl then
    return random_safe_next_ttl(actual_ttl)
  end
  --refresh ttl
  redis.call('expire', key.channel, ttl);
  redis.call('expire', key.messages, ttl);
  return random_safe_next_ttl(ttl)
else
  return -1
end
