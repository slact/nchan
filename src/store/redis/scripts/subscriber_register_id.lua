--input: keys: [], values: [namespace, sub_id_hash_lookup_key, subscriber_id, worker_key, prefer_new_on_collision]

--output: subscriber_id_registered_ok (1 or 0)

local ns, hash_key, id, worker_id, evict_old_on_collision = ARGV[1], ARGV[2], ARGV[3], ARGV[4]=="1"

--local dbg = function(...) redis.call('echo', table.concat({...})); end

redis.call('echo', ' ######## SUBSCRIBER ID REGISTER SCRIPT ####### ')
local subhash_key = ns .. hash_key

local oldsub_worker_id = redis.call('hget', subhash_key, id)
if oldsub_worker_id then
  local numsub = redis.call('PUBSUB','NUMSUB', oldsub_worker_id)[2]
  if tonumber(numsub) > 0 then
    --looks like an active worker id
    if oldsub_worker_id == worker_id then
      redis.call('echo', "weird... same worker_id " .. worker_id .. "for subscriber id " .. id .. ". this should not happen, but it's not too fatal, really...")
      return 1
    elseif evict_old_on_collision then
      --kick out the old one
      local unpacked = {
        "evict_subscriber",
        id,
        oldsub_worker_id
      }
      redis.call('PUBLISH', channel_pubsub, cmsgpack.pack(unpacked))
      -- don't return
    else --deny new
      return 0
    end
  end
  --otherwise, it looks like the previous id is from a nonexistent worker (at least a not-currently-active worker...)
end

redis.call('hset', subhash_key, id, worker_id)
return 1
