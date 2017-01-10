local lrc = require "lredis.cqueues"
local websocket = require "http.websocket"
local cqueues = require "cqueues"
local ut = require("bench.util")
local Json = require "cjson"

local function tryConnectSubscribers(cq, url, needed)
  local atonce = 1
  local connected, pending, failed, msgs = 0,0,0,0

  ut.timeoutRepeat(function()
    local this_round = needed - connected - pending
    if pending > atonce then
      atonce = math.ceil(atonce / 2)
    elseif atonce < 8000 then
      atonce = math.floor(atonce * 2)
    end
    if this_round > atonce then
      this_round = atonce
    end
    for i=1, this_round do
      local sub = Subscriber(url)
      
      sub:on("start", function()
        --print("START", sub)
        pending = pending+1
      end)
      sub.on("error", function(err)
        --print("ERROR", sub, err)
        if sub.connecting then
          ending = pending-1
        end
        failed = failed+1
      end)
      sub:on("connect", function()
        --print("connect", sub)
        pending = pending-1
        connected = connected + 1
      end)
      sub:on("message", function(msg)
        --print("message", sub)
        msgs = msgs + 1
      end)
      sub:connect()
    end
    if connected == needed then
      print("all connected")
      return nil
    elseif pending > atonce then 
      return 1
    elseif connected + pending < needed then 
      return 0.1
    end
  end)
  
  cq:timeoutRepeat(function()
    print(("needed: %d, connected: %d, pending: %d, failed: %d"):format(needed, connected, pending, failed))
    return connected < needed and 1
  end)
end

return function(cq, arg)
  --local hdr = newHistogram()
  local id = math.random(100000000)
  local subskey = "benchi:subs:"..id
  cq:wrap(function()
    local redis = lrc.connect(arg.redis)
    local redisListener = lrc.connect(arg.redis)
    redisListener:subscribe("benchi:sub:"..id)
    
    redis:hmset(subskey, {id=tostring(id), max_subscribers=tostring(arg.subs)})
    redis:call("sadd", "benchi:subs", tostring(id))
    redis:call("publish", "benchi", Json.encode({action="sub-waiting", id=id}))
    
    while true do
      local item = redisListener:get_next()
      if item == nil then break end
      if item[1] == "message" then
        local data = Json.decode(item[3])
        if data.action == "start" then
          tryConnectSubscribers(cq, data.url, data.n)
        elseif data.action == "quit" then
          error("want to quit yeah");
        else
          pp("WEIRD ACTION", data)
        end
      end
    end
  end)
end
