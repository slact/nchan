#!/usr/bin/luvit

package.path=package.path:gsub("/local", "")
package.cpath=package.cpath:gsub("/local", "")

local argparse = require "argparse"
local timer = require 'timer'
local Redis = require "redis-callback-client"
local httpCodec = require 'http-codec'
local Url = require "url"
local tls = require "tls"
local regex = require "rex"
local net = require 'net'
local Emitter = require("core").Emitter
local utils = require('utils')
local wsCodec = require 'websocket-codec'
local uv = require "uv"
  
local parser = argparse("script", "thing thinger.")
parser:option("--redis", "Redis server url.", "redis://127.0.0.1:6379")
parser:option("--url", "Server url for publishing or subscribing")
--parser:option("--q", "Server url for publishing or subscribing")

table.remove(args, 1) --luvit places the interpreter at index [0], unlike lua5.1's [-1]
local args = parser:parse(args)

local Subscriber = (function()
  local wsEncode = wsCodec.encode 
  
  local transport = {
    websocket = function() 
      local httpEncode = httpCodec.encoder()
      local httpDecode = httpCodec.decoder()
      return {
        connect = function(self)
          self.handshake = coroutine.create(function()
            --p(self.sub.url)
            self.hs_result, self.hs_err = wsCodec.handshake({
              host=self.sub.url.host, 
              port=self.sub.url.port, 
              path=self.sub.url.path
            }, function(req)
              self.sub.sock:write(httpEncode(req))
              local head = coroutine.yield()
              return head
            end)
          end)
          coroutine.resume(self.handshake)
        end,
        
        recv = function(self, data)
          --p(data, self.handshake)
          if self.handshake then
            local head, body = httpDecode(data)
            coroutine.resume(self.handshake, head)
            if not self.hs_result then
              --p(head)
              error("Websocket handshake failed: " .. self.hs_err)
            end
            self.handshake=nil
            self.sub:emit("ready", self.sub)
          else
            local frame
            while #data > 0 do
              frame, data = wsCodec.decode(data)
              if frame.opcode == 1 then --data
                self.sub:emit("message", frame.payload)
              elseif frame.opcode == 8 then --close
                if not self.sent_close_frame then
                  self.sub.sock:write(wsEncode{opcode=8, payload=frame.payload})
                end
                --TERMINATE
              elseif frame.opcode == 9 then --ping
                self.sub.sock:write(wsEncode{opcode=10, payload=frame.payload})
              else
                p(frame)
                error("unhandled websocket frame opcode " .. tostring(frame.opcode))
              end
              
            end
          end
        end,
        
        disconnect = function(self)
        
        end,
      }
    end,
    
    eventsource = function() 
      return {
        connect = function(self)
          local req = {
            method = "GET",
            path = self.sub.url.path,
            {"Host", self.sub.url.host},
            {"Accept", "text/event-stream"}
          }
          --p("yeah self")
          self.handshake = true
          self.sub.sock:write(httpEncode(req))
        end,
        
        recv = function(self, data)
        if self.handshake then
            local head, body = httpDecode(data)
            if head.code == 200 then --ok
              self.handshake = false
            end
            if body then
              self:parseData(body)
            end
          else
              self:parseData(data)
          end
        end,
        
        parseData = function(self, data)
          for line in data:gmatch("[^\n]*\n") do
            p(line)
            if line:match("^:") then --comment
              --ignore
            elseif line == "\n" then
              --meh
            end
          end
        end,
        
        disconnect = function(self)
          
        end
      }
    end
  }
  
  local Subscriber = Emitter:extend()
  
  function Subscriber:initialize(url, subscriber_type)
    self.url = Url.parse(url)
    self.unparsed_url = url
    self.sub_type = subscriber_type
    if self.url.protocol == "wss" or self.url.protocol == "https" then
      self.use_tls = true
      self.url.port = self.url.port or 443
    else
      self.use_tls = false
      self.url.port = self.url.port or 80
    end
    
    if not self.sub_type then
      if self.url.protocol == 'wss' or self.url.protocol == 'ws' then
        self.sub_type = 'websocket'
      else
        self.sub_type = 'eventsource' --default?...
      end
    end
    self.transport = transport[self.sub_type]()
    self.transport.sub = self
  end
  
  function Subscriber:connect()
    local opt = {port = self.url.port, host = self.url.hostname}
    --p(opt)
    self.sock = (self.use_tls and tls or net).connect(opt, function(sock, err) 
      self.transport:connect()
    end)
    --p(self.sock)
    self.sock:on("end", function() p("end") end)
    self.sock:on("finish", function() p("finish") end)
    self.sock:on("_socketEnd", function() p("_socketEnd") end)
    self.sock:on("start", function() p("start") end)
    --self.sock:on("connect", function() p("connect") end)
    --self.sock:on("disconnect", function() p("disconnect") end)
    self.sock:on("data", utils.bind(self.transport.recv, self.transport))

  end
  
  function Subscriber:disconnect()
    self.transport:disconnect()
  end
  
  return Subscriber
end)()

local Publisher = function(raw_url)
  local httpEncode = httpCodec.encoder()
  local httpDecode = httpCodec.decoder()

  local sock
  local url = Url.parse(raw_url)
  local opt = {port = url.port, host = url.hostname}
  local use_tls
  if url.protocol == "https" then
    use_tls = true
    url.port = url.port or 443
  else
    use_tls = false
    url.port = url.port or 80
  end
  
  local self
  local recv = function(data)
    --p(data)
    local head, body, done
    head, data = httpDecode(data)
    body, data = httpDecode(data)
    done, data = httpDecode(data)
    if head.code >= 300 or head.code <200 then
      error("Publisher error code: " .. head.code)
    end
    --p("message sent.")
  end
  self = {
    connect = function(self)
      sock = (use_tls and tls or net).connect(opt)
      sock:on("data", recv)
    end,
    post = function(self, msg, args)
      msg = tostring(msg)
      sock:write(httpEncode({
        method = "POST",
        path = url.path,
        {"Host", url.host},
        {"Content-Length", #msg}
      }))
      sock:write(httpEncode(msg))
    end,
    disconnect = function(self)
      
    end
  }
  
  return self
end


local subs = {}

local timer = require('timer')


local n=0
function onMsg(msg)
  p(msg)
end

local hdrhistogram = require "hdrhistogram"
local hdr = hdrhistogram.new(1,3600000000,3)
p(getmetatable(hdr))

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

local pub = Publisher("http://localhost:8082/pub/" .. channel)
pub:connect()

local subs={}
local sub_url = "ws://localhost:8082/sub/broadcast/"..channel
for i=1,100 do
  local sub = Subscriber:new(sub_url)
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
