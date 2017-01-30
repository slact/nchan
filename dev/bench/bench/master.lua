local lrc = require "lredis.cqueues"
local cqueues = require "cqueues"
local Json = require "cjson"
local HDRHistogram = require "hdrhistogram"
local ut = require("bench.util")
local pp = require "pprint"
local mm = require "mm"
local Pub = require "bench.publisher"

local function deepcopy(orig)
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
    local hdrh = ut.newHistogram()
    
    local pubs = {}
    local slaves = {}
    local num_slaves = 0
    local num_subs = 0
    local slave_config = deepcopy(arg.config)
    
    local redis = lrc.connect(arg.redis)
    local redisListener = lrc.connect(arg.redis)
    
    local active_slaves = {}
    
    local wait_for_ready_slaves

    local publish = function()
      local now = ut.now
      for _, cf in pairs(slave_config) do
        local pub 
        if cf.transport ==  "dummy" then
          pub = {post = function(self, msg)
            if type(msg)=="function" then 
              msg = msg()
            end
            redis:call("publish", "benchi:dummy:"..cf.sub, tostring(msg))
            return {status=200}
          end}
        else
          pub = Pub {url = cf.pub}
        end
        table.insert(pubs, pub)
      end
      
      --publishing loop
      while true do
        local resp, err, errno 
        for _, pub in ipairs(pubs) do
          resp, err, errno = pub:post(now)
          if not resp then
            print("publishing error", err, errno)
            --print(resp.status)
            --print(resp.body)
          end
        end
        cqueues.sleep(0.5)
      end
    end
    
    local markSlaveReady = function(slave_id)
      if active_slaves[slave_id] ~= false then
        error("received unexpected slave-ready message for slave id " .. tostring(slave_id))
      end
      
      active_slaves[slave_id]=true
      for _, v in pairs(active_slaves) do
        if not v then return end
      end
      print("all slaves ready")
      return cq:wrap(publish)
    end
    
    local maybeStartPublishing = function()
      if wait_for_ready_slaves then return end
      local init = {}
      for _, cf in pairs(slave_config) do
        for slave_id, slave in pairs(slaves) do
          local subs = cf.n > slave.max_subscribers and slave.max_subscribers or cf.n
          cf.n = cf.n - subs
          
          print("slave", slave_id, slave.max_subscribers, subs, cf.n)
          if subs > 0 then
            init[slave_id] = function()
              redis:call("publish", "benchi:slave:"..slave_id, Json.encode({
                action="start",
                url=cf.sub,
                transport=cf.transport,
                n=subs
              }))
            end
          end
          
          if cf.n == 0 then
            break
          end
        end
        
        if cf.n > 0 then
          print("Not enough slaves/subscribers yet to subscribe to channel, still need at least " .. cf.n .. " subscribers.")
          return
        end
      end
      
      for slave_id, v in pairs(init) do
        v()
        active_slaves[slave_id]=false
      end
      wait_for_ready_slaves = true
      print( "Wait for slaves to be ready..." )
    end
  
    local getSlaveData = function(slave_id)
      local data = redis:hgetall("benchi:slave:".. slave_id)
      data.max_subscribers = tonumber(data.max_subscribers)
      
      local numsub = redis:call("pubsub", "numsub", "benchi:slave:".. slave_id)
      assert(numsub[1]=="benchi:slave:".. slave_id)
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
        redis:call("del", "benchi:slave:"..slave_id)
        redis:call("srem", "benchi:slave", slave_id)
      end

    end
    
    redisListener:subscribe("benchi")
    
    local sub_ids = redis:call("smembers", "benchi:slaves")
    for _,v in ipairs(sub_ids) do
      getSlaveData(v)
    end
    
    cq:wrap(function()
      while true do
        local item = redisListener:get_next()
        if item == nil then break end
        if item[1] == "message" then
          local data = Json.decode(item[3])
          if data.action == "slave-waiting" then
            getSlaveData(data.id)
          elseif data.action == "slave-ready" then
            markSlaveReady(data.id)
          elseif data.action == "slave-histogram" then
            local hdr_incoming = HDRHistogram.unserialize(data.histogram)
            hdrh:merge(hdr_incoming)
            print(("count: %d, avg: %f, min: %f, max: %f"):format(hdrh:count(), hdrh:mean(), hdrh:min(), hdrh:max()))
            print(hdrh:latency_stats())
          end
        end
      end
    end)
  end)
end
