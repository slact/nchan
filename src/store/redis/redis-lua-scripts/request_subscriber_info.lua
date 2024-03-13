--input: keys: [],  values: [ namespace, channel_id, info_response_id ]
--output: -nothing-

local ns = ARGV[1]
local channel_id = ARGV[2]
local response_id = tonumber(ARGV[3])
local pubsub = ('%s{channel:%s}:pubsub'):format(ns, channel_id)

redis.call('echo', ' ####### REQUEST_SUBSCRIBER_INFO #######')

local alert_msgpack =cmsgpack.pack({"alert", "subscriber info", response_id})

redis.call('PUBLISH', pubsub, alert_msgpack)

return true
