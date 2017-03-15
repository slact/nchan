--input: keys: [], values: [namespace, sub_id_hash_lookup_key, subscriber_id, worker_id]
--output: was_subscriber_unregistered? (1/0)

local ns, hash_key, id, worker_id = ARGV[1], ARGV[2], ARGV[3], ARGV[4]
local dbg = function(...) redis.call('echo', table.concat({...}, " ")); end

dbg(' ######## SUBSCRIBER ID UNREGISTER SCRIPT ####### ')
local subhash_key = ns .. hash_key

local oldsub_worker_id = redis.call('HGET', subhash_key, id)
if oldsub_worker_id == worker_id then
  -- this is our subscriber. delete it.
  redis.call('HDEL', subhash_key, id)
  dbg("yeah!")
  return 1
else
  dbg("mismatch:", oldsub_worker_id, worker_id)
  return 0
  --another worker's subscriber, or id not registered. don't touch it
end
