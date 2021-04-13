--input:  keys: [unique_request_id_key], values: []
--output: next_unique_request_id_integer
local key = KEYS[1]

redis.call("ECHO", "###### GET SUBSCRIBER INFO ID ##########")

local resp = redis.pcall("INCR", key)
local val
if type(resp) ~= "number" then
  redis.call("SET", key, "0")
  val = redis.call("INCR", key)
else
  val = resp
end

redis.call("ECHO", "val: " .. val)

return val
