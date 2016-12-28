--[[lit-meta
name = "slact/redis-callback-client"
version = "0.0.5"
description = "A full-featured callback-based Redis client for Luvit"
tags = {"redis"}
license = "MIT"
author = { name = "slact" }
homepage = "https://github.com/slact/luvit-redis-callback-client"
dependencies = {
  "creationix/redis-codec",
  "creationix/sha1",
}
]]

local redisCodec = require 'redis-codec'
local sha1 = require "sha1"
local regex = require "rex"
local net = require 'net'
local Emitter = require("core").Emitter

local parseUrl = function(url)
  local m = {regex.match(url, "(redis://)?(:([^?]+)@)?([\\w.-]+)(:(\\d+))?/?(\\d+)?")}
  return {
    host=m[4] or "127.0.0.1",
    port=m[6] and tonumber(m[6]) or 6379,
    password=m[3],
    db=m[7] and tonumber(m[7])
  }
end

return function(url)
  local pubsub = {}
  local callbacks = {}
  local scripts = {}
  
  local socket
  
  local failHard=function(err, ok)
    if(err) then error(err) end
  end
  
  local connect_params=parseUrl(url)
  
  local Redis = Emitter:extend()
  function Redis:send(...)
    local arg = {...}
    if type(arg[#arg]) == "function" then
      --add callback
      table.insert(callbacks, table.remove(arg, #arg))
    else
      table.insert(callbacks, false)
    end
    local cmd = arg[1]:lower()
    if cmd == "multi" then
      socket:cork()
    elseif cmd == "exec" or cmd == "discard" then
      socket:uncork()
    end
    socket:write(redisCodec.encode(arg))
    
    return self
  end
    
  function Redis:subscribe(channel, callback)
    self:send("subscribe", channel, function(err, d)
      --p("subscribe", channel, err, d)
      if d then
        pubsub[channel]=callback
      else
        callback(err, nil)
      end
    end)
    return self
  end
    
  function Redis:unsubscribe(channel)
    self:send("unsubscribe", channel, function(err, d)
      if d then
        pubsub[channel]=nil
      end
    end)
  end
    
  function Redis:disconnect()
    socket:shutdown()
  end
    
  function Redis:loadScript(name, script, callback)
    local src
    scripts[name]=sha1(script)
    self:send("script", "load", script, function(err, data)
      if callback then
        callback(err, data)
      else
        failHard(err, data)
      end
      assert(scripts[name] == data)
    end)
  end
    
  function Redis:runScript(name, keys, args, callback)
    if scripts[name] == false then
      error("script hasn't loaded yet")
    elseif scripts[name] then
      if callback then
        self:send("evalsha", scripts[name], #keys, unpack(keys), unpack(args), callback)
      else
        self:send("evalsha", scripts[name], #keys, unpack(keys), unpack(args))
      end
    else
      error("Unknown Redis script " .. tostring(name))
    end
  end
  
  
  local self = Redis:new()
  
  socket = net.connect(connect_params.port, connect_params.host)
  socket:cork()
  
  if connect_params.password then
    self:send("auth", connect_params.password, function(err, d)
    failHard(err, d)
    if not connect_params.db then self:emit("connect", err, d) end
    end)
  end
  
  if connect_params.db then
      self:send("select", connect_params.db, function(err, d)
      failHard(err, d)
      self:emit("connect", err, d)
    end)
  end
  
  socket:on("connect", function(err, d)
    --p("connected")
    socket:uncork()
    if err then 
      if err == "ECONNREFUSED" then
        error("Cound not connect to Redis at " .. connect_params.host .. ":" .. connect_params.port)
      else
        error(err)
      end
    end
    if not (connect_params.password or connect_params.db) then
      self:emit("connect", err, d)
    end
  end)
  
  socket:on("disconnect", function(err, d)
    self:emit("disconnect", err, d)
  end)
  
  socket:on('data', function(data)
    -- If error, print and close connection
    --p("onData", data)
    
    while #data>0 do
      local d
      d, data = redisCodec.decode(data)
      if type(d)=="table" and d[1]=="message" then
        pubsub[d[2]](d[3])
      elseif callbacks[1] then
        if type(d)=="table" and d.error then
          callbacks[1](d.error, nil)
        else
          callbacks[1](nil, d)
        end
        table.remove(callbacks, 1)
      end
    end
  end)
  return self
end
