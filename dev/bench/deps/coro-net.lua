--[[lit-meta
  name = "creationix/coro-net"
  version = "2.2.0"
  dependencies = {
    "creationix/coro-channel@2.2.0"
  }
  optionalDependencies = {
    "luvit/secure-socket@1.0.0"
  }
  homepage = "https://github.com/luvit/lit/blob/master/deps/coro-net.lua"
  description = "An coro style client and server helper for tcp and pipes."
  tags = {"coro", "tcp", "pipe", "net"}
  license = "MIT"
  author = { name = "Tim Caswell" }
]]

local uv = require('uv')
local wrapStream = require('coro-channel').wrapStream
local secureSocket -- Lazy required from "secure-socket" on first use.

local function makeCallback(timeout)
  local thread = coroutine.running()
  local timer, done
  if timeout then
    timer = uv.new_timer()
    timer:start(timeout, 0, function ()
      if done then return end
      done = true
      timer:close()
      return assert(coroutine.resume(thread, nil, "timeout"))
    end)
  end
  return function (err, data)
    if done then return end
    done = true
    if timer then timer:close() end
    if err then
      return assert(coroutine.resume(thread, nil, err))
    end
    return assert(coroutine.resume(thread, data or true))
  end
end

local function normalize(options, server)
  local t = type(options)
  if t == "string" then
    options = {path=options}
  elseif t == "number" then
    options = {port=options}
  elseif t ~= "table" then
    assert("Net options must be table, string, or number")
  end
  if options.tls == true then
    options.tls = {}
  end
  if server and options.tls then
    options.tls.server = true
    assert(options.tls.cert, "TLS servers require a certificate")
    assert(options.tls.key, "TLS servers require a key")
  end
  if options.port or options.host then
    options.isTcp = true
    options.host = options.host or "127.0.0.1"
    assert(options.port, "options.port is required for tcp connections")
  elseif options.path then
    options.isTcp = false
  else
    error("Must set either options.path or options.port")
  end
  return options
end

local function connect(options)
  local socket, success, err
  options = normalize(options)
  if options.isTcp then
    success, err = uv.getaddrinfo(options.host, options.port, {
      socktype = options.socktype or "stream",
      family = options.family or "inet",
    }, makeCallback(options.timeout))
    if not success then return nil, err end
    local res
    res, err = coroutine.yield()
    if not res then return nil, err end
    socket = uv.new_tcp()
    socket:connect(res[1].addr, res[1].port, makeCallback(options.timeout))
  else
    socket = uv.new_pipe(false)
    socket:connect(options.path, makeCallback(options.timeout))
  end
  success, err = coroutine.yield()
  if not success then return nil, err end

  local dsocket
  if options.tls then
    if not secureSocket then secureSocket = require('secure-socket') end
    dsocket, err = secureSocket(socket, options.tls)
    if not dsocket then
      return nil, err
    end
  else
    dsocket = socket
  end

  local read, write, updateDecoder, updateEncoder, close =
    wrapStream(dsocket, options.decode, options.encode)

  return read, write, socket, updateDecoder, updateEncoder, close
end

local function createServer(options, onConnect)
  local server
  options = normalize(options, true)
  if options.isTcp then
    server = uv.new_tcp()
    assert(server:bind(options.host, options.port))
  else
    server = uv.new_pipe(false)
    assert(server:bind(options.path))
  end
  assert(server:listen(256, function (err)
    assert(not err, err)
    local socket = options.isTcp and uv.new_tcp() or uv.new_pipe(false)
    server:accept(socket)
    coroutine.wrap(function ()
      local success, failure = xpcall(function ()
        local dsocket
        if options.tls then
          if not secureSocket then secureSocket = require('secure-socket') end
          dsocket = assert(secureSocket(socket, options.tls))
        else
          dsocket = socket
        end

        local read, write, updateDecoder, updateEncoder =
          wrapStream(dsocket, options.decode, options.encode)
        return onConnect(read, write, socket, updateDecoder, updateEncoder)
      end, debug.traceback)
      if not success then
        print(failure)
      end
    end)()
  end))
  return server
end

return {
  makeCallback = makeCallback,
  connect = connect,
  createServer = createServer,
}
