local ut = require("bench.util")
local websocket = require "http.websocket"

local dummies = setmemtatable({}, {__index = function(k, t)
  local empty = {}
  t[k] = empty
  return empty
end})

local transport = setmetatable( {
  dummy = function(client, opt)
    return {
      start = function(self, cq)
        client:emit("start")
        client.connected = true
        local co = cq:wrap(function()
          client:emit("connect")
          local data
          repeat
            data = coroutine.yield()
          until not data
          client:emit("disconnect")
        end)
        dummies[opt.url][self]=co
        self.coroutine = co
        return self
      end,
      
      stop = function(self)
        dummies[opt.url][self]=nil
        return self
      end
    }
  end
  
  websocket = function(client, opt)
    local ws = websocket.new_from_uri(opt.url)
    return {
      start = function(self, cq)
        client:emit("start")
        cq:wrap(function()
          client.connecting = true
          local ret, err = ws:connect(10)
          if not ret then
            ws:close()
            client:emit("error", err)
            client.connecting = nil
            return
          end
          client.connected = true
          client:emit("connect")
          
          local ok, data, err
          while true do 
            ok, data, err = pcall(ws.receive, ws)
            --now fetch messages, yeah?
            if not data or not ok then
              client.connected = nil
              ws:close()
              client:emit("error", ok and err or data)
              client:emit("disconnect")
              return
            else
              client:emit("message", data)
            end
          end
        end)
        return self
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
    connect = function(self, cq)
      self.transport:start(cq)
    end,
    disconnect = function(self)
      self.transport:stop()
    end
  }
}
  
return setmetatable({
  dummyReceive = function(url, msg)
    for _, v in pairs(dummies[url]) do
      coroutine.resume(v, msg)
    end
  end
}, {__call=function(t, opt)
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
end})
