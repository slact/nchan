local lrc = require "lredis.cqueues"
local cqueues = require "cqueues"
local Json = require "cjson"
local HDRHistogram = require "hdrhistogram"
local ut = require("bench.util")
local pp = require "pprint"

function deepcopy(orig)
    local orig_type = type(orig)
    local copy
    if orig_type == 'table' then
        copy = {}
        for orig_key, orig_value in next, orig, nil do
            copy[deepcopy(orig_key)] = deepcopy(orig_value)
        end
        setmetatable(copy, deepcopy(getmetatable(orig)))
    else -- number, string, boolean, etc
        copy = orig
    end
    return copy
end

return function(cq, arg)
  cq:wrap(function()
    --local hdrh = newHistogram()
    
    local pubs = {}
    local slaves = {}
    local num_slaves = 0
    local num_subs = 0
    
    local redis = lrc.connect(arg.redis)
    local redisListener = lrc.connect(arg.redis)
    
    local started_publishing
    local maybeStartPublishing = function()
      local slave_config = deepcopy(arg.config)
      if started_publishing then return end
      local init = {}
      --pp("publish pls", slave_config)
      for i, cf in pairs(slave_config) do
        for slave_id, slave in pairs(slaves) do
          local subs = cf.n > slave.max_subscribers and slave.max_subscribers or cf.n
          cf.n = cf.n - subs
          
          --print("signal slave", slave_id, slave.max_subscribers, subs, cf.n)
          if subs > 0 then
            table.insert(init, function()
              redis:call("publish", "benchi:sub:"..slave_id, Json.encode({
                action="start",
                url=cf.sub,
                n=subs
              }))
            end)
          end
          
          if cf.n == 0 then
            --print("cf.n == 0")
            break
          end
        end
        
        if cf.n > 0 then
          print("Not enough slaves/subscribers yet to subscribe to channel, still need at least " .. cf.n .. " subscribers.")
          return
        end
      end
      
      for i, v in ipairs(init) do
        v()
      end
      started_publishing = true
      print( "Start the thing!" )
    end
  
    local getSlaveData = function(slave_id)
      local data = redis:hgetall("benchi:subs:".. slave_id)
      data.max_subscribers = tonumber(data.max_subscribers)
      
      local numsub = redis:call("pubsub", "numsub", "benchi:sub:".. slave_id)
      assert(numsub[1]=="benchi:sub:".. slave_id)
      if numsub[2] == "1" or numsub[2] > 0 then
        if not slaves[slave_id] then
          num_slaves = num_slaves + 1
        else
          num_subs = slaves[slave_id].max_subscribers
        end
        slaves[slave_id] = data
        num_subs = num_subs + data.max_subscribers
        --pp("SLAVES", slave_id, slaves)
        maybeStartPublishing()
      else
        redis:call("del", "benchi:subs:"..slave_id)
        redis:call("srem", "benchi:subs", slave_id)
      end

    end
    
    local sub_ids = redis:call("smembers", "benchi:subs")
    for i,v in ipairs(sub_ids) do
      getSlaveData(v)
    end
    
    cq:wrap(function() 
      redisListener:subscribe("benchi")
      while true do
        local item = redisListener:get_next()
        if item == nil then break end
        if item[1] == "message" then
          local data = Json.decode(item[3])
          if data.action == "slave-waiting" then
            getSlaveData(data.id)
          elseif data.action == "stats" then
            local hdr_incoming = HDRHistogram.unserialize(data.hdr)
            hdrh:add(hdr_incoming)
            print(hdrh:latency_stats())
          end
        end
      end
    end)
  end)
end
