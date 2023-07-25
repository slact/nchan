--input: keys: [],  values: [ namespace, channel_id, publish_cmd, info_response_id ]
--output: -nothing-

local ns = ARGV[1]
local channel_id = ARGV[2]
local publish_cmd = ARGV[3]
local response_id = tonumber(ARGV[4])
local pubsub = ('%s{channel:%s}:pubsub'):format(ns, channel_id)

redis.call('echo', ' ####### REQUEST_SUBSCRIBER_INFO #######')

local alert_msgpack =cmsgpack.pack({"alert", "subscriber info", response_id})

redis.call(publish_cmd, pubsub, alert_msgpack)

return true
