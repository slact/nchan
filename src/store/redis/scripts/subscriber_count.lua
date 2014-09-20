--input:  keys: [], values: [channel_id, subscriber_delta, channel_empty_ttl, channel_active_ttl]
--output: current_subscribers
local id = ARGV[1]
local key = 'channel:'..id
local subscriber_delta = tonumber(ARGV[2])
local channel_empty_ttl = tonumber(ARGV[3]) or 20
local channel_active_ttl = tonumber(ARGV[4]) or 0

local enable_debug=true
local dbg = (function(on)
  if on then return function(...) redis.call('echo', table.concat({...})); end
  else return function(...) return; end end
end)(enable_debug)

dbg(' ######## SUBSCRIBER COUNT ####### ')
dbg('active ttl:', type(channel_active_ttl), " ", tostring(channel_active_ttl), " empty ttl:",type(channel_empty_ttl), " ", tostring(channel_empty_ttl))


if not subscriber_delta or subscriber_delta == 0 then
  return {err="subscriber_delta is not a number or is 0: " .. type(ARGV[2]) .. " " .. tostring(ARGV[2])}
end
if redis.call('exists', key) == 0 then
  return {err=("%srementing subscriber count for nonexistent channel %s"), subscriber_delta > 0 and "inc" or "dec", id}
end

local keys={'channel:'..id, 'channel:messages:'..id, 'channel:subscribers:'..id}
local setkeyttl=function(ttl)
  for i,v in ipairs(keys) do
    if ttl > 0 then
      redis.call('expire', v, ttl)
    else
      redis.call('persist', v)
    end
  end
end

local count= redis.call('hincrby', key, 'subscribers', subscriber_delta)
if count == 0 and subscriber_delta < 0 then
  dbg("this channel now has no subscribers")
  setkeyttl(channel_empty_ttl)
elseif count > 0 and count - subscriber_delta == 0 then
  dbg("just added subscribers")
  setkeyttl(channel_active_ttl)
elseif count<0 then
  return {err="Subscriber count for channel " .. id .. " less than zero: " .. count}
end

return count
