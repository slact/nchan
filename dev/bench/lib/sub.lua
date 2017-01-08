local Url = require "url"
local tls = require "tls"
local regex = require "rex"
local net = require 'net'
local Emitter = require("core").Emitter
local utils = require('utils')
local httpCodec = require 'http-codec'
local wsCodec = require 'websocket-codec'

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
              --p(frame)
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
          --p(line)
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
  local opt = {port = self.url.port, host = self.url.hostname, timeout=100}
  --p(opt)
  self.sock = (self.use_tls and tls or net).connect(opt, function(sock, err) 
    self.transport:connect()
  end)
  --p(self.sock)
  self.sock:on("end", function(err) p("end", err) end)
  self.sock:on("timeout", function(err) p("timeout", err) end)
  self.sock:on("finish", function() p("finish") end)
  self.sock:on("_socketEnd", function() p("_socketEnd") end)
  self.sock:on("start", function() p("start") end)
  self.sock:on("connect", function() end)
  self.sock:on("disconnect", function() p("disconnect") end)
  self.sock:on("error", function(err) self:emit("error", err) end)
  self.sock:on("data", utils.bind(self.transport.recv, self.transport))

end

function Subscriber:disconnect()
  self.transport:disconnect()
end

return Subscriber
