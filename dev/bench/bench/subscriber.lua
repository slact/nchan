local ut = require("bench.util")
local websocket = require "http.websocket"
local cqueues = require "cqueues"
local Promise = require("cqueues.promise")

--local dummies = setmetatable({}, {__index = function(t, k)
--  local empty = {}
--  t[k] = empty
--  return empty
--end})
local dummies = {}

local transport = setmetatable( {
  dummy = function(client, opt)
    return {
      start = function(self, cq)
        client:emit("start")
        client.connected = true
        if not dummies[opt.url] then
          dummies[opt.url] = Promise.new()
        end
        local co = cq:wrap(function()
          client:emit("connect")
          local data
          repeat
            data = dummies[opt.url]:get()
            if not data then
              client.connected = nil
              client:emit("error", nil)
            else
              client:emit("message", data)
            end
          until not data
          client:emit("disconnect")
        end)
        self.coroutine = co
        return self
      end,
      
      stop = function(self)
        if dummies[opt.url] then
          local promise = dummies[opt.url]
          dummies[opt.url] = nil
          promise:set(true, false)
        end
        return self
      end
    }
  end,
  
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
    if dummies[url] then
      local promise = dummies[url]
      dummies[url] =  Promise.new()
      promise:set(true, msg)
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
