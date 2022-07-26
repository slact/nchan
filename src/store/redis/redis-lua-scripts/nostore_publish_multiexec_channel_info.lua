--input: keys: [],  values: [ namespace, channel_id, use_accurate_subscriber_count ]
--output: {subscribers_count, last_subscriber_seed}, or nil of channel not found

local ns = ARGV[1]
local id = ARGV[2]
local use_accurate_subscriber_count = tonumber(ARGV[3]) ~= 0

local channel_key = ('%s{channel:%s}'):format(ns, id)
local subscriber_counts = channel_key..':subscriber_counts'

redis.replicate_commands()

redis.call('echo', ' #######  NOSTORE_PUBLISH_MULTIEXEC_CHANNEL_INFO ######## ')

if redis.call('exists', channel_key) ~= 1 then
  return {0, 0}
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

local last_seen = tonumber(redis.call('HGET', channel_key, 'last_seen_fake_subscriber'))

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
  subscriber_count = tonumber(redis.call('HGET', channel_key, 'fake_subscribers'))
end


return {last_seen or 0, subscriber_count or 0}
