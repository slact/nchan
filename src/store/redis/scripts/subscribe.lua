--input:  keys: [], values: [channel_id, subscriber_delta]
--output: current_subscribers
local id = ARGV[1]
local key = 'channel:'..id
local subscriber_delta = tonumber(ARGV[2])
if not subscriber_delta or subscriber_delta == 0 then
  return {err="subscriber_delta is not a number or is 0: " .. type(ARGV[2]) .. " " .. tostring(ARGV[2])}
end

