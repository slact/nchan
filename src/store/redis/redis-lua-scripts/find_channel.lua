--input: keys: [],  values: [ namespace, channel_id, use_accurate_subscriber_count ]
--output: channel_hash {ttl, time_last_seen, subscribers, last_channel_id, messages} or nil
-- finds and return the info hash of a channel, or nil of channel not found
local ns = ARGV[1]
local id = ARGV[2]
local use_accurate_subscriber_count = tonumber(ARGV[3]) ~= 0

local channel_key = ('%s{channel:%s}'):format(ns, id)
local messages_key = channel_key..':messages'
local subscriber_counts = channel_key..':subscriber_counts'

redis.replicate_commands()

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

local tohash=function(arr)
  if type(arr)~="table" then
    return nil
  end
  local h = {}
  local k=nil
  for i, v in ipairs(arr) do
    if k == nil then
      k=v
    else
      --dbg(k.."="..v)
      h[k]=v; k=nil
    end
  end
  return h
end

if redis.call('EXISTS', channel_key) ~= 0 then
  local ch = tohash(redis.call('hgetall', channel_key))
    
  local msgs_count
  if redis.call("TYPE", messages_key)['ok'] == 'list' then
    oldestmsg(messages_key, channel_key ..':msg:%s')
    msgs_count = tonumber(redis.call('llen', messages_key))
  else
    msgs_count = 0
  end
  
  local subscriber_count
  if use_accurate_subscriber_count then
    local sub_counts = tohash(redis.call("HGETALL", subscriber_counts))
    subscriber_count = 0
    for k, v in pairs(sub_counts) do
      v = tonumber(v)
      local res = redis.call("PUBSUB", "NUMSUB", k)
      if tonumber(res[2]) >= 1 and v > 0 then
        subscriber_count = subscriber_count + tonumber(v)
      else
        redis.call("HDEL", subscriber_counts, k)
      end
    end
  else
    subscriber_count = tonumber(ch.fake_subscribers) or tonumber(ch.subscribers)
  end
  
  return {
    tonumber(ch.ttl) or 0,
    tonumber(ch.last_seen_fake_subscriber) or 0,
    tonumber(ch.fake_subscribers or ch.subscribers) or 0,
    ch.current_message or "",
    msgs_count
  }
else
  return nil
end
