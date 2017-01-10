local Url = require "url"
local tls = require "tls"
local net = require 'net'
--local Emitter = require("core").Emitter
--local utils = require('utils')
local httpCodec = require 'http-codec'

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

return Publisher
