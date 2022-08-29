--input:  keys: [], values: [namespace, channel_id, number, time, nginx_worker_id]
--output: -none-
  
redis.call('echo', ' ####### ADD_FAKESUBS ####### ')
local ns = ARGV[1]
local id = ARGV[2]
local num = tonumber(ARGV[3])
local time = tonumber(ARGV[4])
local ngx_worker_id = ARGV[5]

if num==nil then
  return {err="fakesub number not given"}
end

local chan_key = ('%s{channel:%s}'):format(ns, id)
local subs_key = ('%s{channel:%s}:subscriber_counts'):format(ns, id)

local res = redis.pcall('EXISTS', chan_key)
if type(res) == "table" and res["err"] then
  return {err = ("CLUSTER KEYSLOT ERROR. %i %s"):format(num, id)}
end

local exists = res == 1

local old_current_count = 0

if exists or (not exists and num > 0) then
  old_current_count = redis.call('HINCRBY', chan_key, 'fake_subscribers', num)
  if time then
    redis.call('HSET', chan_key, 'last_seen_fake_subscriber', time)
  end
  if not exists then
    redis.call('EXPIRE', chan_key, 5) --something small
  end
end

local res = redis.pcall('EXISTS', subs_key)
if type(res) == "table" and res["err"] then
  return {err = ("CLUSTER KEYSLOT ERROR. %i %s"):format(num, id)}
end
redis.call('HINCRBY', subs_key, ngx_worker_id, num)
local subs_key_pttl = tonumber(redis.call('PTTL',  subs_key))
if subs_key_pttl < 0 then
  subs_key_pttl = redis.call('PTTL', chan_key)
  if subs_key_pttl <= 0 then
    --60 seconds to get your shit together
    subs_key_pttl = 60
  end
  redis.call('PEXPIRE', subs_key, subs_key_pttl)
end

return nil
