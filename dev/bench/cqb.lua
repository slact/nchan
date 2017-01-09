#!/usr/bin/luajit
local cqueues = require "cqueues"
local lrc = require "lredis.cqueues"
local websocket = require "http.websocket"
local request = require "http.request"
local argparse = require "argparse"
local Json = require "cjson"
local HDRHistogram = require "hdrhistogram"


local parser = argparse("script", "thing thinger.")
parser:option("--redis", "Redis orchestration server url.", "redis://127.0.0.1:6379")
parser:option("--config", "Pub/sub config lua file.", "config.lua")
parser:option("--subs", "max subscribers (for slave).", 25000)
parser:option("--threads", "number of threads (for slave).", 4)
parser:mutex(
  parser:flag("--master", "Be master"),
  parser:flag("--slave", "Be slave")
)


function wrapEmitter(obj)
  local evts = setmetatable({}, {
    __index = function(tbl, k)
      local ls = {}
      tbl[k] = ls
      return ls
    end
  })
  function obj:on(event, fn)
    table.insert(evts[event], fn)
    return self
  end
  function obj:off(event, fn)
    for i, v in pairs(evts[event]) do
      if v == fn then
        table.remove(evts[event], fn)
        return self
      end
    end
    
    return self
  end
  function obj:emit(event, ...)
    for i, v in ipairs(evts[event]) do
      v(...)
    end
    return self
  end
  return obj
end

local opt = parser:parse(args)
opt.slave_threads = tonumber(opt.slave_threads)
opt.subscribers = tonumber(opt.subscribers)

local cq=cqueues.new()

local timeout = function(timeout_sec, cb)
  cq:wrap(function()
    cqueues.sleep(timeout_sec)
    cb()
  end)
end

local timeoutRepeat = function(fn)
  cq:wrap(function()
    local n
    while true do
      n = fn()
      if n and n > 0 then
        cqueues.sleep(n)
      else
        break
      end
    end
  end)
end







local time = (function()
  local gettimeofday = require("posix.sys.time").gettimeofday
  return function()
    local tv = gettimeofday()
    return tv.tv_sec * 1000 + tv.tv_usec/1000
    --print(tv.tv_sec, tv.tv_usec, cached_time_msec)
  end
end)()


function newHistogram()
  return HDRHistogram.new(1,360000,3, {multiplier=0.001, unit="ms"})
end

local Subscriber
do
  
  local transport = setmetatable( {
    websocket = function(client, opt)
      local ws = websocket.new_from_uri("ws://localhost:8082/sub/broadcast/foo")
      local coro
      
      return {
        start = function(self)
          coro = cq:wrap(function()
            client.connecting = true
            client:emit("start")
            local ret, err = ws:connect(10)
            client.connecting = nil
            if not ret then
              ws:close()
              client:emit("error", err)
              return
            end
            client.connected = true
            client:emit("connect")
            
            local data, err
            while true do 
              data, err = ws:receive()
              --now fetch messages, yeah?
              if not data then
                client.connected = nil
                ws:close()
                client:emit("error", err)
                return
              else
                client:emit("message", data)
              end
            end
          end)
        end,
        
        stop = function(self)
          client.connected = nil
          ws:close()
          --do something with the coroutine maybe?
        end
      }
    end
  }, {__index = function(t, name)
    return function(...)
      error("Transport " .. name .. " not implemented")
    end
  end})
  
  local mt = {
    __index = {
      connect = function(self)
        self.transport:start()
      end,
      disconnect = function(self)
        self.transport:stop()
      end
    }
  }
  
  Subscriber = function(opt)
    local self = setmetatable(wrapEmitter({}), mt)
    if type(opt) == "string" then
      opt = {url = opt}
    end
    self.url = opt.url
    self.transport_name = opt.transport
    if not self.transport_name then
      local protocol = self.url:match("^(%w+):")
      if protocol == "ws" or protocol == "wss" then
        self.transport_name = "websocket"
      end
    end
    if not self.transport_name then
      error("unspecified transport name")
    end
    self.transport = transport[self.transport_name](self, {url=opt.url})
    return self
  end
end


local needed = 10000
local atonce = 1
local n = {
  connected = 0,
  pending = 0,
  failed = 0,
  msgs = 0
}

local ii=1
timeoutRepeat(function()
  local this_round = needed - n.connected - n.pending
  if n.pending > atonce then
    atonce = math.ceil(atonce / 2)
  elseif atonce < 8000 then
    atonce = math.floor(atonce * 2)
  end
  if this_round > atonce then
    this_round = atonce
  end
  for i=1, this_round do
    local sub = Subscriber("ws://localhost:8082/sub/broadcast/foo")
    
    sub:on("start", function()
      --print("START", sub)
      n.pending = n.pending+1
    end)
    sub.on("error", function(err)
      --print("ERROR", sub, err)
      if sub.connecting then
        n.pending = n.pending-1
      end
      n.failed = n.failed+1
    end)
    sub:on("connect", function()
      --print("connect", sub)
      n.pending = n.pending-1
      n.connected = n.connected + 1
    end)
    sub:on("message", function(msg)
      --print("message", sub)
      n.msgs = n.msgs + 1
    end)
    sub:connect()
  end
  if n.connected == needed then
    return nil
  elseif n.pending > atonce then 
    return 1
  elseif n.connected + n.pending < needed then 
    return 0.1
  end
end)


timeoutRepeat(function()
  print(("connected: %d, pending: %d, atonce: %d, failed: %d, msgs %d"):format(n.connected, n.pending, atonce, n.failed, n.msgs))
  return 1
end)



assert(cq:loop())
for err in cq:errors() do
  print(err)
end
