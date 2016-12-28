--[[lit-meta
  name = "creationix/websocket-client"
  version = "3.0.1"
  description = "A coroutine based client for Websockets"
  tags = {"coro", "websocket"}
  license = "MIT"
  author = { name = "Tim Caswell" }
  homepage = "https://github.com/creationix/redis-luvit"
  dependencies = {
    "luvit/http-codec@2.0.0",
    "creationix/websocket-codec@2.0.0",
    "creationix/coro-net@2.0.0",
  }
]]

local connect = require('coro-net').connect
local websocketCodec = require('websocket-codec')
local httpCodec = require('http-codec')

return function (url, subprotocol, options)
  options = options or {}
  local headers = options.headers or {}

  local protocol, host, port, path = string.match(url, "^(wss?)://([^:/]+):?(%d*)(/?[^#]*)")
  local tls = options.tls
  if protocol == "ws" then
    port = tonumber(port) or 80
  elseif protocol == "wss" then
    port = tonumber(port) or 443
    if not tls then tls = {} end
  else
    error("Sorry, only ws:// or wss:// protocols supported")
  end
  if #path == 0 then path = "/" end

  local read, write, socket, updateDecoder, updateEncoder = assert(connect{
    tls = tls,
    host = host,
    port = port,
    encode = httpCodec.encoder(),
    decode = httpCodec.decoder(),
  })

  -- Perform the websocket handshake
  assert(websocketCodec.handshake({
    host = host,
    path = path,
    protocol = subprotocol
  }, function (req)
    for _ = 1, #headers do
      req[#req + 1] = headers[1]
    end
    write(req)
    local res = read()
    if not res then error("Missing server response") end
    if res.code == 400 then
      -- p { req = req, res = res }
      local reason = read() or res.reason
      error("Invalid request: " .. reason)
    end
    return res
  end))

  -- Upgrade the protocol to websocket
  updateDecoder(websocketCodec.decode)
  updateEncoder(websocketCodec.encode)

  return read, write, socket
end
