--input:  keys: [], values: [channel_id, number]
--output: current_fake_subscribers
  
redis.call('echo', ' ####### FAKESUBS ####### ')
local id=ARGV[1]
local num=tonumber(ARGV[2])
if num==nil then
  return {err="fakesub number not given"}
end

local chan_key = 'channel:'..id
local exists = false
if redis.call('EXISTS', chan_key) == 1 then
  exists = true
end

local cur = 0

if exists or (not exists and num > 0) then
  cur = redis.call('HINCRBY', chan_key, 'fake_subscribers', num)
  if not exists then
    redis.call('EXPIRE', chan_key, 5) --something small
  end
end

return cur
