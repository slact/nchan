--input:  keys: [message_key], values: []
--output: msg_ttl, msg_time, msg_tag, prev_msg_time, prev_msg_tag, message, content_type, eventsource_event, compression, channel_subscriber_count
local key = KEYS[1]

local ttl = redis.call('TTL', key)
local time, tag, prev_time, prev_tag, data, content_type, es_event, compression = unpack(redis.call('HMGET', key, 'time', 'tag', 'prev_time', 'prev_tag', 'data', 'content_type', 'eventsource_event', 'compression'))

return {ttl, time, tag, prev_time or 0, prev_tag or 0, data or "", content_type or "", es_event or "", tonumber(compression or 0)}
