local ut = require("bench.util")
local http_request = require "http.request"
local http_client = require "http.client"
local monotime = require "cqueues".monotime
local mm = require "mm"

local function non_final_status(status)
    return status:sub(1, 1) == "1" and status ~= "101"
end

local mt = { __index = {
  connect = function(self)
    local err, errno
    local req = self.request
    local timeout = self.opt.timeout
    
    self.connection, err, errno = http_client.connect({
      host = req.host,
      port = req.port,
      bind = req.bind, 
      tls = req.tls,
      ctx = req.ctx,
      sendname = req.sendname,
      version = req.version
    }, timeout)
    if not self.connection then
      return nil, err, errno
    end
    
    return self
  end,
  
  send = function(self, method, headers, body)
    local req = self.request
    local timeout = self.opt.timeout
    local stream
    
    if not self.connection then self:connect() end
    
    local err, errno
    
    stream, err, errno = self.connection:new_stream()
    if not stream then
      return nil, err, errno
    end
    
    
    if headers and not body then headers, body = nil, headers end
    req.headers:upsert(":method", method:upper() or "GET")
    if headers then
      for h, v in pairs(headers) do
        req.headers:upsert(h, v)
      end
    end
    
    if body then
      local bodytype = type(body)
      if bodytype == "function" then
        -- no chunking, just run the function once and output its contents
        body = body()
      end
      if bodytype ~= "string" then
        body = tostring(body)
      end
      
      req:set_body(body)
    end
    
    local ok, err, errno = stream:write_headers(req.headers, req.body == nil, timeout)
    if not ok then
      self:disconnect()
      return nil, err, errno
    end
    
    if body then
      ok, err, errno = stream:write_body_from_string(body, timeout)
      if not ok then
        print(ok, err, errno)
        self:disconnect()
        return nil, err, errno
      end
    end
    
    local resp_headers
    --now the response
    if not resp_headers or non_final_status(resp_headers:get(":status")) then
      -- Skip through 1xx informational headers.
      -- From RFC 7231 Section 6.2: "A user agent MAY ignore unexpected 1xx responses"
      repeat
        resp_headers, err, errno = stream:get_headers(deadline and (deadline-monotime()))
        if resp_headers == nil then
          self:disconnect()
          return nil, err, errno
        end
      until not non_final_status(resp_headers:get(":status"))
    end
    
    if req.follow_redirects and resp_headers:get(":status"):sub(1,1) == "3" then
      self.request, err, errno = req:handle_redirect(resp_headers)
      if not self.request then
        self:disconnect()
        return nil, err, errno
      end
      return self:send(method, headers, body)
    end
    
    
    local resp_body = stream:get_body_as_string()
    
    self.response = {
      status = tonumber(resp_headers:get(":status")),
      headers = resp_headers,
      body = resp_body
    }
    
    return self.response
  end,
  
  disconnect = function(self)
    --if self.stream then
    --  self.stream:shutdown()
    --  self.stream = nil
    --end
    
    if self.connection then
      self.connection:close()
      self.connection = nil
    end
    return self
  end
}}

for i, method in ipairs({"get", "post", "put", "delete"}) do
  mt.__index[method]=function(self, headers, body)
    return self:send(method, headers, body)
  end
end

return function(opt)
  local self = setmetatable({opt = opt}, mt)
  if opt.url or opt.uri then
    self.request = http_request.new_from_uri(opt.url or opt.uri)
  end
  return self
end
