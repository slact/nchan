local id = ARGV[1]
local keys= {
  'channel:'..id,
  'channel:messages:'..id,
  'channel:last_message:'..id
}

return redis.call('DEL', unpack(keys))
