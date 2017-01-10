local ut = require("bench.util")
local websocket = require "http.websocket"

local transport = setmetatable( {
  websocket = function(client, opt)
    local ws = websocket.new_from_uri("ws://localhost:8082/sub/broadcast/foo")
    return {
      start = function(self, cq)
        cq:wrap(function()
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
  
return function(opt)
  local self = setmetatable(ut.wrapEmitter({}), mt)
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
