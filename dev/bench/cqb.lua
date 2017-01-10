#!/usr/bin/luajit
local cqueues = require "cqueues"
local signal = require "cqueues.signal"
local thread = require "cqueues.thread"
local lrc = require "lredis.cqueues"
local websocket = require "http.websocket"
local request = require "http.request"
local argparse = require "argparse"
local Json = require "cjson"
local HDRHistogram = require "hdrhistogram"
local pp = require('pprint')
local ut = require("bench.util")

local parser = argparse("script", "thing thinger.")
parser:option("--redis", "Redis orchestration server url.", "127.0.0.1:6379")
parser:option("--config", "Pub/sub config lua file.", "config.lua")
parser:option("--subs", "max subscribers (for slave).", 25000)
parser:option("--threads", "number of threads (for slave).", 4)
parser:mutex(
  parser:flag("--master", "Be master"),
  parser:flag("--slave", "Be slave")
)

local opt = parser:parse(args)
opt.slave_threads = tonumber(opt.slave_threads)
opt.subscribers = tonumber(opt.subscribers)

local cq=cqueues.new()

ut.accessorize(cq)

function beMaster(arg)
  cq:wrap(function()
    --local hdrh = newHistogram()
    
    local pubs = {}
    local slaves = {}
    local num_slaves = 0
    local num_subs = 0
    
    local redis = lrc.connect(arg.redis)
    local redisListener = lrc.connect(arg.redis)
    
    local maybeStartPublishing = function()
      local init = {}
      for i, cf in pairs(arg.config) do
        local n = cf.n
        for slave_id, slave in pairs(slaves) do
          local subs = n > slave.max_subscribers and slave.max_subscribers or n
          n = n - subs
          
          table.insert(init, function()
            redis:call("publish", "benchi:sub:"..slave_id, Json.encode({
              action="start",
              url=cf.sub,
              n=subs
            }))
          end)
          
          if n == 0 then
            break
          end
        end
        
        if n > 0 then
          print("Not enough slaves/subscribers yet to subscribe to channel, still need at least " .. n .. " subscribers.")
          return
        end
      end
      
      for i, v in ipairs(init) do
        v()
      end
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
        maybeStartPublishing()
      else
        redis:call("del", "benchi:subs:"..slave_id)
        redis:call("srem", "benchi:subs", slave_id)
      end
      
      pp(data)
      pp(numsub)
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
          pp("DECODE2", item)
          local data = Json.decode(item[3])
          if data.action == "sub-waiting" then
            getSlaveData(data.id)
          elseif data.action == "stats" then
            local hdr_incoming = HDRHistogram.unserialize(data.hdr)
            hdrh:add(hdr_incoming)
            p(hdrh:latency_stats())
          end
        end
      end
    end)
  end)
end

if not opt.slave and not opt.master then
  print("Role setting missing, assuming --slave")
  opt.slave = true
end 

if opt.slave then
  local opt_json = Json.encode(opt)
  for i=1,tonumber(opt.threads) do
    cq:newThread(function(con, threadnum, opt_json)
      local cqueues = require "cqueues"
      local Json = require "cjson"
      local ut = require("bench.util")
      local cq = cqueues.new()
      ut.accessorize(cq)
      local opt = Json.decode(opt_json)
      local slave = require "bench.slave"
      cq:wrap(function()
        slave(cq, opt)
        print("started slave thread " .. threadnum)
      end)
      assert(cq:loop())
    end, opt_json)
  end
end

if opt.master then
  local conf_chunk, err = loadfile(opt.config)
  if conf_chunk then
    opt.config = conf_chunk()
  else
    print("Config not found at " .. opt.config ..".")
    os.exit(1)
  end
  beMaster(opt)
end


cq:handleSignal({"SIGINT", "SIGTERM"}, function()
  print("outta here")
  os.exit(1)
end)

while true do
  local ok, err, eh, huh = cq:step()
  if not ok then
    pp(ret, err, eh, huh)
  end
end
for err in cq:errors() do
  print(err)
end
