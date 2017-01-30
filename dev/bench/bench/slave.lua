local lrc = require "lredis.cqueues"
local websocket = require "http.websocket"
local cqueues = require "cqueues"
local condition = require "cqueues.condition"
local ut = require("bench.util")
local Json = require "cjson"
local uuid = require "lua_uuid"
local Subscriber = require "bench.subscriber"
local pp = require "pprint"

return function(cq, arg)
  local histogram = ut.newHistogram()
  local id = uuid()
  local slavekey = "benchi:slave:"..id
  
  local dummy_subbed = {}
  
  pp(arg)
  
  local self = ut.wrapEmitter({
    id = id,
    count = {
      pending = 0,
      connected = 0,
      failed = 0,    
      msgs = 0
    },
    
    subscribers = setmetatable({}, {__mode='k'}),
    pending_subscribers = setmetatable({}, {__mode='k'})
  })
  
  self:on("stop", function() 
    if self.redisListener then
      self.redisListener:unsubscribe("benchi:slave:"..id)
    end
  end)
  
  local should_stop
  
  function self:stop(fn)
    should_stop = true
    local need_to_stop = 0
    self.redis:call("publish", "benchi", Json.encode({action="slave-stop", id=id}))
    for _, subs in ipairs {self.pending_subscribers, self.subscribers} do
      for sub, __ in pairs(subs) do
        --print("stop everything for sub " .. tostring(sub))
        sub:disconnect()
        need_to_stop = need_to_stop + 1
      end
    end
    
    if need_to_stop == 0 then
      --nothing needs to be stopped
      print("need to stop nothing")
      self:emit("stop")
    end
  end
  
  local function tryConnectSubscribers(cq, url, transport, needed)
    local atonce = 1
    local delay = 1
    local n = self.count
    n.needed = needed
    
    local function throttle(what)
      local this_round = n.needed - n.connected - n.pending
      if what == "error" then
        atonce = 1
        delay = 5
      elseif not what or what == 0 then 
        if n.pending > atonce then
          atonce = math.ceil(atonce / 2)
          delay = 1
        elseif atonce < 8000 then
          atonce = math.floor(atonce * 2)
          delay = 0.1
        end
      end
      if this_round > atonce then
        this_round = atonce
      end
      cqueues.sleep(delay)
      return this_round
    end
    
    cq:wrap(function()
      while not should_stop do
        cqueues.sleep(10)
        local ser = histogram:serialize()
        histogram:reset()
        self.redis:call("publish", "benchi", Json.encode({action="slave-histogram", id=id, histogram=ser}))
      end
    end)
    
    cq:timeoutRepeat(function()
      if should_stop then
        print("stop this crazy train")
        return nil
      end
      print(("  ...slave %s needs: %d, connected: %d, pending: %d, (atonce: %d), failed: %d"):format(self.id, n.needed, n.connected, n.pending, atonce, n.failed))
      return n.connected < n.needed and 1
    end)
    
    while true do
      local this_round = throttle()
      --print(("!!!!id: %s, needed: %d, thisround: %d, connected: %d, pending: %d, (atonce: %d, delay:%f), failed: %d"):format(self.id, n.needed, this_round, n.connected, n.pending, atonce, delay, n.failed))
      for i=1, this_round do
        local sub = Subscriber({url=url, transport=transport})

        sub:on("start", function()
          n.pending = n.pending+1
          self.pending_subscribers[sub]=true
        end)
        sub:on("error", function(err)
          throttle("error")
          if sub.connecting then
            io.stderr:write(("Connection error: %s\n"):format(err))
            self.pending_subscribers[sub]=nil
            n.pending = n.pending-1
          end
          if should_stop and n.connected == 0 and n.pending == 0 then
            self:emit("stop")
          end
          n.failed = n.failed+1
        end)
        sub:on("disconnect", function()
          assert(self.subscribers[sub])
          self.subscribers[sub]=nil
          n.connected = n.connected - 1
          if should_stop and n.connected == 0 and n.pending == 0 then
            self:emit("stop")
          end
        end)
        sub:on("connect", function()
          --print("connect", sub)
          self.pending_subscribers[sub]=nil
          self.subscribers[sub]=true
          n.pending = n.pending-1
          n.connected = n.connected + 1
        end)
        sub:on("message", function(msg)
          histogram:record(ut.now() - tonumber(msg))
          n.msgs = n.msgs + 1
        end)
        sub:connect(cq)
        if sub.transport_name == "dummy" and not dummy_subbed[url] then
          --ugh
          self.redisListener:subscribe("benchi:dummy:"..url)
          dummy_subbed[url] = true
        end
        
      end
      if n.connected == n.needed then
        print(("Slave %s connected %d subscribers"):format(self.id, n.connected))
        self.redis:call("publish", "benchi", Json.encode({action="slave-ready", id=id}))
        return nil
      elseif should_stop then
        print("stop trying to connect")
        return nil
      end
    end
  end
  
  
  cq:wrap(function()
    self.redis = lrc.connect(arg.redis)
    self.redisListener = lrc.connect(arg.redis)
    self.redisListener:subscribe("benchi:slave:"..id)
    
    self.redis:hmset(slavekey, {id=tostring(id), max_subscribers=tostring(arg.subs)})
    self.redis:call("sadd", "benchi:slaves", tostring(id))
    self.redis:call("publish", "benchi", Json.encode({action="slave-waiting", id=id}))
    
    while true do
      local item = self.redisListener:get_next()
      if item == nil then break end
      if item[1] == "message" then
        if item[2] == slavekey then
          local data = Json.decode(item[3])
          if data.action == "start" then
            cq:wrap(tryConnectSubscribers, cq, data.url, data.transport, data.n)
          elseif data.action == "quit" then
            error("want to quit yeah");
          else
            pp("WEIRD ACTION", data)
          end
        elseif item[2]:match("^benchi:dummy:") then
          local url = item[2]:match("^benchi:dummy:(.*)")
          Subscriber.dummyReceive(url, item[3])
        else
          pp("WEIRD pubsub channel", item[2])
        end
      end
    end
  end)
  
  return self
end
