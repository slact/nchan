#!/usr/bin/luajit
local cqueues = require "cqueues"
local lrc = require "lredis.cqueues"
local websocket = require "http.websocket"
local cq=cqueues.new()

local timeout = function(timeout_sec, cb)
  cq:wrap(function()
    cqueues.sleep(timeout_sec)
    cb()
  end)
end

local timeoutRepeat = function(fn)
  cq:wrap(function()
    local n
    while true do
      n = fn()
      if n and n > 0 then
        cqueues.sleep(n)
      else
        break
      end
    end
  end)
end

local needed = 20000
local atonce = 1
local n = {
  connected  =  0,
  pending = 0,
  failed = 0,
  msgs = 0
}
timeoutRepeat(function()
  local this_round = needed - n.connected - n.pending
  if n.pending > atonce then
    atonce = math.ceil(atonce / 2)
  elseif atonce < 8000 then
    atonce = math.floor(atonce * 2)
  end
  if this_round > atonce then
    this_round = atonce
  end
  for i=1, this_round do
    cq:wrap(function()
      local ws = websocket.new_from_uri("ws://localhost:8082/sub/broadcast/foo")
      n.pending = n.pending + 1
      local ret, err = ws:connect(10)
      n.pending = n.pending - 1
      if not ret then
        n.failed = n.failed + 1
        ws:close()
        print("fail", err)
        return
      end
      n.connected = n.connected + 1
      for _=1, 5 do
          local data, err = ws:receive()
          if not data then
            print("receive  error:", err)
            return
          end
          n.msgs = n.msgs + 1
      end
      ws:close()
    end)
  end
  if n.connected == needed then
    return nil
  elseif n.pending > atonce then 
    return 1
  elseif n.connected + n.pending < needed then 
    return 0.1
  end
end)


timeoutRepeat(function()
  print(("connected: %d, pending: %d,  atonce: %d, failed: %d, msgs %d"):format(n.connected, n.pending, atonce, n.failed, n.msgs))
  return 1
end)



cq:loop()
