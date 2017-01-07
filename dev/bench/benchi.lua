#!/usr/bin/luvit
_G.require = require
package.path=package.path:gsub("/local", "")
package.cpath=package.cpath:gsub("/local", "")

local argparse = require "argparse"
local timer = require 'timer'
local Json = require "cjson"
local Thread = require "thread"
local posix = require "posix"

local parser = argparse("script", "thing thinger.")
parser:option("--redis", "Redis orchestration server url.", "redis://127.0.0.1:6379")
parser:option("--subscribers", "Subscriber mode: Max. subscribers per thread. Publisher mode: Subscribers needed per channel", 10000)
parser:option("--slave-threads", "Max. slave subscriber threads", 2)
parser:flag("--master", "Be master")
parser:flag("--slave", "Be slave")


--parser:option("--q", "Server url for publishing or subscribing")

table.remove(args, 1) --luvit places the interpreter at index [0], unlike lua5.1's [-1]
local opt = parser:parse(args)
opt.slave_threads = tonumber(opt.slave_threads)
opt.subscribers = tonumber(opt.subscribers)

local time = (function()
  local gettimeofday = require("posix.sys.time").gettimeofday
  local last_tick
  local cached_time_msec
  
  return function()
    local tick = uv.now()
    if tick ~= last_tick then
      local tv = gettimeofday()
      last_tick = tick
      cached_time_msec = tv.tv_sec * 1000 + tv.tv_usec/1000
      --print(tv.tv_sec, tv.tv_usec, cached_time_msec)
    end
    return cached_time_msec
  end
end)()

local channel = tostring(math.random(100000))

--pub:connect()

--[[
local subs={}

timer.setTimeout(10000, function()
  timer.setInterval(500, function()
    --print("send!!")  
    pub:post(time())
  end)
end)

timer.setInterval(5000, function()
  print(hdr:latency_stats())
  print(("count: %d min: %f max: %f avg: %f stddev: %f\n"):format(hdr:count(), hdr:min(), hdr:max(), hdr:mean(), hdr:stddev()))
end)

]]

function newHistogram()
  local hdrhistogram = require "hdrhistogram"
  return hdrhistogram.new(1,360000,3, {multiplier=0.001, unit="ms"})
end

function beSlave(arg, max_subscribers)
  local Json = require "cjson"
  local hdrhistogram = require "hdrhistogram"
  local Redis = require "redis-callback-client"
  local Subscriber = setfenv(loadfile "lib/sub.lua", _G)()
  
  local hdr = newHistogram()
  local timer = require 'timer'
  
  local id = math.random(100000000)
  local subskey = ("benchi:subs:%d"):format(id)
  
  local redis = Redis(arg.redis)
    :send("hmset", subskey, {id=id, max_subscribers=arg.subscribers})
    :send("sadd", "benchi:subs", id)
    :send("publish", "benchi", Json.encode({action="sub-waiting", id=id}))
  
  local subs = {}
  local running = nil
  
  local redisListener = Redis(arg.redis):subscribe("benchi:sub:"..id, function(msg)
    local data = Json.decode(msg)
    if data.action == "start" then
      for i=1,tonumber(data.n) do
        local sub = Subscriber:new(data.url, data.transport)
        sub:connect()
        sub:on("message", function(msg)
          local msec_sent = tonumber(msg)
          --p(msg)
          local msec_wait = time() - msec_sent
          hdr:record(msec_wait)
          --p(msec_wait)
        end)
        sub:on("ready", function()
          p("" .. i .. " ready")
        end)
        table.insert(subs, sub)
      end
    elseif data.action == "stop" then
      for i,sub in ipairs(subs) do
        sub:disconnect()
      end
    elseif data.action == "quit" then
      print("asked to quit")
      for i,sub in ipairs(subs) do
        sub:disconnect()
      end
      redis:disconnect()
      redisListener:disconnect()
    end
  end)
  
  timer.setInterval(10000, function()
    if running then
      redis:publish("benchi", Json.encode {
        action="stats",
        slave_id=id,
        hdr=hdr.serialize()
      })
      hdr:reset()
    end
  end)
  
end



function beMaster(arg)
  local channel = "foo"
  
  local Publisher = setfenv(loadfile "lib/pub.lua", _G)()
  
  
  local pub = Publisher("http://localhost:8082/pub/" .. channel)
  local Json = require "cjson"
  local Redis = require "redis-callback-client"
  
  local redis = Redis(arg.redis)
  local redisListener = Redis(arg.redis)
  
  local HDRHistogram = require "hdrhistogram"
  local hdrh = newHistogram()
  
  local slaves = {}
  
  local maybeStartPublishing = function()
    local numsubs = tonumber(arg.subscribers)
    
    p("maybe?")
    
    if numsubs then
      local possible = 0
      for i,slave in pairs(slaves) do
        possible = possible + tonumber(slave.max_subscribers) 
      end
      if possible < tonumber(arg.subscribers) then
        p("not enough subscriber slaves available")
        return
      end
    end
    
    for id, slave in pairs(slaves) do
      redis:send("publish", "benchi:sub:"..id, Json.encode({
        action="subscribe",
        url=arg.url,
        n=slave.max_subscribers
      }))
    end
  end
  
  local getSlaveData = function(slave_id)
    local parseSlaveData = coroutine.wrap(function(err, data)
      if err then error(err) end
      local d = data
      
      err, data = coroutine.yield()
      p("getslavedata", slave_id, err, data)
      if err then error(err) end
      assert(data[1]=="benchi:sub:".. slave_id)
      if data[2] == "1" or data[2] > 0 then
        slaves[d.id]=d
        maybeStartPublishing()
      else
        redis:send("del", "benchi:subs:"..slave_id):send("srem", "benchi:subs", slave_id)
      end
    end)
    redis:send("multi")
      :send("hgetall", ("benchi:subs:".. slave_id), parseSlaveData)
      :send("pubsub", "numsub", "benchi:sub:".. slave_id, parseSlaveData)
    :send("exec")
  end
  
  redis:send("smembers", "benchi:subs", function(err, data)
    if err then error(err) end
    p("wuuh", err, data)
    for i,v in ipairs(data) do
      getSlaveData(v)
    end
  end)
  
  redisListener:subscribe("benchi", function(msg)
    local data = Json.decode(msg)
    if data.action == "sub-waiting" then
      getSlaveData(data.id)
    elseif data.action == "stats" then
      local hdr_incoming = HDRHistogram.unserialize(data.hdr)
      hdrh:add(hdr_incoming)
      p(hdrh:latency_stats())
    end
  end)
  

end

if opt.slave then
  --[[for i=1,opt.slave_threads do
    local pid = posix.fork()
    if pid == -1 then
      error("fork failed")
    elseif pid == 0 then --i am fork.
      beSlave(opt)
    end
  end
  ]]
  beSlave(opt)
  p("we wuz slave n shiet")
end

if opt.master then
  beMaster(opt)
  p("we wuz master n shiet")
end

--[[if opt.slave then
  for i=1, opt.slave_threads do
    p("be slave thread " .. i)
    Thread.start(beSlave, json_opt)
  end
end
]]
