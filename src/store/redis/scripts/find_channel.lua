--input: keys: [],  values: [ channel_id ]
--output: channel_hash {ttl, time_last_seen, subscribers, messages} or nil
-- finds and return the info hash of a channel, or nil of channel not found
local id = ARGV[1]
local chk = ('{channel:%s}'):format(id)
local key_channel =  chk
local key_messages = chk..':messages'

redis.call('echo', ' #######  FIND_CHANNEL ######## ')

if redis.call('EXISTS', key_channel) ~= 0 then
  local ch = redis.call('hmget', key_channel, 'ttl', 'time_last_seen', 'subscribers', 'fake_subscribers', 'current_message')
  if(ch[4]) then
    --replace subscribers count with fake_subscribers
    ch[3]=ch[4]
    table.remove(ch, 4)
  end
  for i = 1, 4 do
    ch[i]=tonumber(ch[i]) or 0
  end
  if type(ch[5]) ~= "string" then
    ch[5]=""
  end
  
  if redis.call("TYPE", key_messages)['ok'] == 'list' then
    local oldest_msgid;
    while true do
      oldest_msgid = redis.call('LRANGE', key_messages, -1, -1)[1]
      if redis.call('EXISTS', (chk..':msg:%s'):format(oldest_msgid)) ~= 0 then
        --key exists.
        break
      else
        redis.breakpoint()
        redis.call('RPOP', key_messages)
      end
    end
    table.insert(ch, tonumber(redis.call('llen', chk..":messages")))
  else
    table.insert(ch, 0)
  end
  
  return ch
else
  return nil
end
