--input:  keys: [], values: [channel_id, number]
--output: current_fake_subscribers
  
redis.call('echo', ' ####### FAKESUBS ####### ')
local id=ARGV[1]
local num=tonumber(ARGV[2])
if num==nil then
  return {err="fakesub number not given"}
end

local chan_key = ('{channel:%s}'):format(id)

local res = redis.pcall('EXISTS', chan_key)
if type(res) == "table" and res["err"] then
  return {err = ("CLUSTER KEYSLOT ERROR. %i %s"):format(num, id)}
end

local exists = res == 1

local cur = 0

if exists or (not exists and num > 0) then
  cur = redis.call('HINCRBY', chan_key, 'fake_subscribers', num)
  if not exists then
    redis.call('EXPIRE', chan_key, 5) --something small
  end
end

return cur
