--[[lit-meta
  name = "creationix/coro-channel"
  version = "2.2.1"
  homepage = "https://github.com/luvit/lit/blob/master/deps/coro-channel.lua"
  description = "An adapter for wrapping uv streams as coro-streams and chaining filters."
  tags = {"coro", "adapter"}
  license = "MIT"
  author = { name = "Tim Caswell" }
]]

-- local p = require('pretty-print').prettyPrint

local function makeCloser(socket)
  local closer = {
    read = false,
    written = false,
    errored = false,
  }

  local closed = false

  local function close()
    if closed then return end
    closed = true
    if not closer.readClosed then
      closer.readClosed = true
      if closer.onClose() then
        closer.onClose()
      end
    end
    if not socket:is_closing() then
      socket:close()
    end
  end

  closer.close = close

  function closer.check()
    if closer.errored or (closer.read and closer.written) then
      return close()
    end
  end

  return closer
end

local function makeRead(socket, decode, closer)
  local paused = true

  local queue = {}
  local tindex = 0
  local dindex = 0

  local function dispatch(data)

    -- p("<-", data[1])

    if tindex > dindex then
      local thread = queue[dindex]
      queue[dindex] = nil
      dindex = dindex + 1
      assert(coroutine.resume(thread, unpack(data)))
    else
      queue[dindex] = data
      dindex = dindex + 1
      if not paused then
        paused = true
        assert(socket:read_stop())
      end
    end
  end

  closer.onClose = function ()
    if not closer.read then
      closer.read = true
      return dispatch {nil, closer.errored}
    end
  end

  local buffer = ""
  local function onRead(err, chunk)
    if err then
      closer.errored = err
      return closer.check()
    end
    if not chunk then
      if closer.read then return end
      closer.read = true
      dispatch {}
      return closer.check()
    end
    if not decode then
      return dispatch {chunk}
    end
    buffer = buffer .. chunk
    while true do
      local success, item, extra = pcall(decode, buffer)
      if not success then
        dispatch {nil, item}
        return
      end
      if not extra then return end
      buffer = extra
      dispatch {item}
    end
  end

  local function read()
    if dindex > tindex then
      local data = queue[tindex]
      queue[tindex] = nil
      tindex = tindex + 1
      return unpack(data)
    end
    if paused then
      paused = false
      assert(socket:read_start(onRead))
    end
    queue[tindex] = coroutine.running()
    tindex = tindex + 1
    return coroutine.yield()
  end

  local function updateDecoder(newDecode)
    decode = newDecode
  end
  return read, updateDecoder
end

local function makeWrite(socket, encode, closer)

  local function wait()
    local thread = coroutine.running()
    return function (err)
      assert(coroutine.resume(thread, err))
    end
  end

  local function write(chunk)
    if closer.written then
      return nil, "already shutdown"
    end

    -- p("->", chunk)

    if chunk == nil then
      closer.written = true
      closer.check()
      socket:shutdown(wait())
      return coroutine.yield()
    end

    if encode then
      chunk = encode(chunk)
    end
    local success, err = socket:write(chunk, wait())
    if not success then
      closer.errored = err
      closer.check()
      return nil, err
    end
    err = coroutine.yield()
    return not err, err
  end

  local function updateEncoder(newEncode)
    encode = newEncode
  end

  return write, updateEncoder
end

local function wrapRead(socket, decode)
  local closer = makeCloser(socket)
  closer.written = true
  local read, updateDecoder = makeRead(socket, decode, closer)
  return read, updateDecoder, closer.close
end

local function wrapWrite(socket, encode)
  local closer = makeCloser(socket)
  closer.read = true
  local write, updateEncoder = makeWrite(socket, encode, closer)
  return write, updateEncoder, closer.close
end

local function wrapStream(socket, decode, encode)
  assert(socket
    and socket.write
    and socket.shutdown
    and socket.read_start
    and socket.read_stop
    and socket.is_closing
    and socket.close)

  local closer = makeCloser(socket)
  local read, updateDecoder = makeRead(socket, decode, closer)
  local write, updateEncoder = makeWrite(socket, encode, closer)

  return read, write, updateDecoder, updateEncoder, closer.close

end

local function chain(...)
  local args = {...}
  local nargs = select("#", ...)
  return function (read, write)
    local threads = {} -- coroutine thread for each item
    local waiting = {} -- flag when waiting to pull from upstream
    local boxes = {}   -- storage when waiting to write to downstream
    for i = 1, nargs do
      threads[i] = coroutine.create(args[i])
      waiting[i] = false
      local r, w
      if i == 1 then
        r = read
      else
        function r()
          local j = i - 1
          if boxes[j] then
            local data = boxes[j]
            boxes[j] = nil
            assert(coroutine.resume(threads[j]))
            return unpack(data)
          else
            waiting[i] = true
            return coroutine.yield()
          end
        end
      end
      if i == nargs then
        w = write
      else
        function w(...)
          local j = i + 1
          if waiting[j] then
            waiting[j] = false
            assert(coroutine.resume(threads[j], ...))
          else
            boxes[i] = {...}
            coroutine.yield()
          end
        end
      end
      assert(coroutine.resume(threads[i], r, w))
    end
  end
end

return {
  wrapRead = wrapRead,
  wrapWrite = wrapWrite,
  wrapStream = wrapStream,
  chain = chain,
}
