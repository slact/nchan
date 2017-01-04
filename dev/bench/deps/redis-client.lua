--[[lit-meta
name = "creationix/redis-client"
version = "1.1.0"
description = "A coroutine based client for Redis"
tags = {"coro", "redis"}
license = "MIT"
author = { name = "Tim Caswell" }
homepage = "https://github.com/creationix/redis-luvit"
dependencies = {
  "creationix/redis-codec@1.0.2",
  "creationix/coro-net@2.2.0",
}
]]

local codec = require('redis-codec')
local connect = require('coro-net').connect

return function (config)
  if not config then config = {} end

  local read, write = assert(connect{
    host = config.host or "localhost",
    port = config.port or 6379,
    encode = codec.encode,
    decode = codec.decode,
  })

  return function (command, ...)
    if not command then return write() end
    write {command, ...}
    local res = read()
    if type(res) == "table" and res.error then
      error(res.error)
    end
    return res
  end
end
