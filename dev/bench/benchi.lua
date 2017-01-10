#!/usr/bin/luvit
_G.require = require
package.path=package.path:gsub("/local", "")
package.cpath=package.cpath:gsub("/local", "")

local uv = require "uv"
local argparse = require "argparse"
local timer = require 'timer'
local Json = require "cjson"
local Thread = require "thread"
local posix = require "posix"
local Redis = require "redis-callback-client"
local HDRHistogram = require "hdrhistogram"
local url = require "url"
_G.url = url

local parser = argparse("script", "thing thinger.")
parser:option("--redis", "Redis orchestration server url.", "redis://127.0.0.1:6379")
parser:option("--config", "Pub/sub config lua file.", "config.lua")
parser:option("--subs", "max subscribers (for slave).", 25000)
parser:mutex(
  parser:flag("--master", "Be master"),
  parser:flag("--slave", "Be slave")
)

--parser:option("--q", "Server url for publishing or subscribing")

table.remove(args, 1) --luvit places the interpreter at index [0], unlike lua5.1's [-1]
local opt = parser:parse(args)
opt.slave_threads = tonumber(opt.slave_threads)
opt.subscribers = tonumber(opt.subscribers)

p(opt)

if opt.master then
  local conf_str, err = loadfile(opt.config)
  if conf_str then
    opt.config = setfenv(conf_str, _G)()
  else
    print("Config not found at " .. opt.config ..".")
    os.exit(1)
  end
  
end

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
  return HDRHistogram.new(1,360000,3, {multiplier=0.001, unit="ms"})
end

function beSlave(arg)

  local Subscriber = setfenv(loadfile "lib/luvit/sub.lua", _G)()
  
  local hdr = newHistogram()
  local timer = require 'timer'
  
  local id = math.random(100000000)
  local subskey = "benchi:subs:"..id

  local subs = {}
  local running = nil

  local redis = Redis(arg.redis)
  
  local atonce = 500
  
  local redisListener = Redis(arg.redis):subscribe("benchi:sub:"..id, function(msg)
    local data = Json.decode(msg)
    if data.action == "start" then
      local ready_num = 0
      local failed_num = 0
      local pending_num = 0
      local remaining_num = tonumber(data.n)
      
      local interval_id
      interval_id = timer.setInterval(1000, function()
        print(("Subs for %s: ready: %d, failed: %d, pending: %d, still needed: %d"):format(data.url, ready_num, failed_num, pending_num, remaining_num))
        if ready_num + failed_num == tonumber(data.n) then
          timer.clearInterval(interval_id)
        end
      end)
      
      local tryConnect
      tryConnect = function()
        p("tryConnect some stuff")
        if pending_num < atonce then
          
          local try_num = remaining_num - pending_num
          if(try_num > atonce) then try_num = atonce end
          
          p(remaining_num, pending_num, try_num)
          
          for i=1,try_num do
            local sub = Subscriber:new(data.url, data.transport)
            table.insert(subs, sub)
            local index = #subs
            
            pending_num = pending_num + 1
            sub:connect()
            sub:on("message", function(msg)
              local msec_sent = tonumber(msg)
              --p(msg)
              local msec_wait = time() - msec_sent
              hdr:record(msec_wait)
              --p(msec_wait)
            end)
            sub:on("ready", function()
              ready_num = ready_num + 1
              pending_num = pending_num - 1
              remaining_num = remaining_num - 1
            end)
            sub:on("error", function(err)
              assert(sub == subs[index])
              subs[index]=nil
              failed_num = failed_num + 1
              pending_num = pending_num - 1
              print(("Sub %i connection error: %s"):format(i, err))
            end)
          end
        end
        
        if remaining_num > 0 then
          timer.setTimeout(100, tryConnect)
        end
      end
      
      tryConnect()

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
  
  redisListener:send("ping", function(err, ok)
    redis:send("hmset", subskey, {id=id, max_subscribers=arg.subs})
    redis:send("sadd", "benchi:subs", id)
    redis:send("publish", "benchi", Json.encode({action="sub-waiting", id=id}))
  end)
  
  timer.setInterval(10000, function()
    if running then
      redis:send("publish", "benchi", Json.encode {
        action="stats",
        slave_id=id,
        hdr=hdr.serialize()
      })
      hdr:reset()
    end
  end)
  
  print("Started slave id " .. id .. " (max subs: " .. arg.subs .. ")")
end



function beMaster(arg)
  local Publisher = setfenv(loadfile "lib/luvit/pub.lua", _G)()
  
  local redis = Redis(arg.redis)
  local redisListener = Redis(arg.redis)
  
  local hdrh = newHistogram()
  
  
  local pubs = {}
  local slaves = {}
  local num_slaves = 0
  local num_subs = 0
  
  local maybeStartPublishing = function()
    local init = {}
    for i, cf in pairs(arg.config) do
      local n = cf.n
      for slave_id, slave in pairs(slaves) do
        local subs = n > slave.max_subscribers and slave.max_subscribers or n
        n = n - subs
        
        table.insert(init, function()
          redis:send("publish", "benchi:sub:"..slave_id, Json.encode({
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
    local parseSlaveData = coroutine.wrap(function(err, data)
      if err then error(err) end
      local d = data
      d.max_subscribers = tonumber(d.max_subscribers)
      err, data = coroutine.yield()
      if err then error(err) end
      assert(data[1]=="benchi:sub:".. slave_id)
      if data[2] == "1" or data[2] > 0 then
        if not slaves[d.id] then
          num_slaves = num_slaves + 1
        else
          num_subs = slaves[d.id].max_subscribers
        end
        slaves[d.id] = d
        num_subs = num_subs + d.max_subscribers
        maybeStartPublishing()
      else
        redis:send("del", "benchi:subs:"..slave_id)
        redis:send("srem", "benchi:subs", slave_id)
      end
    end)
    redis:send("multi")
      :send("hgetall", ("benchi:subs:".. slave_id), parseSlaveData)
      :send("pubsub", "numsub", "benchi:sub:".. slave_id, parseSlaveData)
    :send("exec")
  end
  
  redis:send("smembers", "benchi:subs", function(err, data)
    if err then error(err) end
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

p(opt)
if opt.slave then
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
local sigint = uv.new_signal()
uv.signal_start(sigint, "sigint", function(signal)
  print("got " .. signal .. ", shutting down")
  os.exit(1)
end)
