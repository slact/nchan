--input: keys: [],  values: [ channel_id ]
--output: channel_hash {ttl, time_last_seen, subscribers, messages} or nil
-- finds and return the info hash of a channel, or nil of channel not found
local id = ARGV[1]
local key_channel='channel:'..id

local enable_debug=true
local dbg = (function(on)
  if on then return function(...) redis.call('echo', table.concat({...})); end
  else return function(...) return; end end
end)(enable_debug)

dbg(' #######  FIND_CHANNEL ######## ')

if redis.call('EXISTS', key_channel) ~= 0 then
  local ch = redis.call('hmget', key_channel, 'ttl', 'time_last_seen', 'subscribers', 'fake_subscribers')
  if(ch[4]) then
    --replace subscribers count with fake_subscribers
    ch[3]=ch[4]
    table.remove(ch, 4)
  end
  for i = 1, #ch do
    ch[i]=tonumber(ch[i]) or 0
  end
  table.insert(ch, redis.call('llen', "channel:messages:"..id))
  return ch
else
  return nil
end