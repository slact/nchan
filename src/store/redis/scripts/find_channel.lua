--input: keys: [],  values: [ channel_id ]
--output: channel_hash {ttl, time_last_seen, subscribers, messages} or nil
-- finds and return the info hash of a channel, or nil of channel not found
local id = ARGV[1]
local channel_key = ('{channel:%s}'):format(id)
local messages_key = channel_key..':messages'

redis.call('echo', ' #######  FIND_CHANNEL ######## ')

--check old entries
local oldestmsg=function(list_key, old_fmt)
  local old, oldkey
  local n, del=0,0
  while true do
    n=n+1
    old=redis.call('lindex', list_key, -1)
    if old then
      oldkey=old_fmt:format(old)
      local ex=redis.call('exists', oldkey)
      if ex==1 then
        return oldkey
      else
        redis.call('rpop', list_key)
        del=del+1
      end
    else
      break
    end
  end
end

if redis.call('EXISTS', channel_key) ~= 0 then
  local ch = redis.call('hmget', channel_key, 'ttl', 'time_last_seen', 'subscribers', 'fake_subscribers', 'current_message')
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
  
  if redis.call("TYPE", messages_key)['ok'] == 'list' then
    oldestmsg(messages_key, channel_key ..':msg:%s')
    table.insert(ch, tonumber(redis.call('llen', messages_key)))
  else
    table.insert(ch, 0)
  end
  
  return ch
else
  return nil
end
