#!/usr/bin/luajit
local cqueues = require "cqueues"
local signal = require "cqueues.signal"
local lrc = require "lredis.cqueues"
local websocket = require "http.websocket"
local request = require "http.request"
local argparse = require "argparse"
local Json = require "cjson"
local HDRHistogram = require "hdrhistogram"
local pp = require('pprint')


local parser = argparse("script", "thing thinger.")
parser:option("--redis", "Redis orchestration server url.", "redis://127.0.0.1:6379")
parser:option("--config", "Pub/sub config lua file.", "config.lua")
parser:option("--subs", "max subscribers (for slave).", 25000)
parser:option("--threads", "number of threads (for slave).", 4)
parser:mutex(
  parser:flag("--master", "Be master"),
  parser:flag("--slave", "Be slave")
)


function wrapEmitter(obj)
  local evts = setmetatable({}, {
    __index = function(tbl, k)
      local ls = {}
      tbl[k] = ls
      return ls
    end
  })
  function obj:on(event, fn)
    table.insert(evts[event], fn)
    return self
  end
  function obj:off(event, fn)
    for i, v in pairs(evts[event]) do
      if v == fn then
        table.remove(evts[event], fn)
        return self
      end
    end
    
    return self
  end
  function obj:emit(event, ...)
    for i, v in ipairs(evts[event]) do
      v(...)
    end
    return self
  end
  return obj
end

local opt = parser:parse(args)
opt.slave_threads = tonumber(opt.slave_threads)
opt.subscribers = tonumber(opt.subscribers)

local cq=cqueues.new()

function timeout(timeout_sec, cb)
  cq:wrap(function()
    cqueues.sleep(timeout_sec)
    cb()
  end)
end

function timeoutRepeat(fn)
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

function handleSignal(signame, fn)
  if type(signame) == "table" then
    for k, v in pairs(signame) do
      handleSignal(v, fn)
    end
  else
    signal.discard(signal[signame])
    signal.block(signal[signame])
    cq:wrap(function()
      local sig = signal.listen(signal[signame])
      sig:wait()
      fn()
    end)
  end
end

local now = (function()
  local gettimeofday = require("posix.sys.time").gettimeofday
  local monotime = cqueues.monotime
  local tv=gettimeofday()
  local off = monotime()
  local t0=tv.tv_sec + tv.tv_usec/1000000
  
  return function()
    return t0 + (monotime() - off)
  end
end)()


function newHistogram()
  return HDRHistogram.new(1,360000,3, {multiplier=0.001, unit="ms"})
end

local Subscriber
do
  
  local transport = setmetatable( {
    websocket = function(client, opt)
      local ws = websocket.new_from_uri("ws://localhost:8082/sub/broadcast/foo")
      local coro
      
      return {
        start = function(self)
          coro = cq:wrap(function()
            client.connecting = true
            client:emit("start")
            local ret, err = ws:connect(10)
            client.connecting = nil
            if not ret then
              ws:close()
              client:emit("error", err)
              return
            end
            client.connected = true
            client:emit("connect")
            
            local data, err
            while true do 
              data, err = ws:receive()
              --now fetch messages, yeah?
              if not data then
                client.connected = nil
                ws:close()
                client:emit("error", err)
                return
              else
                client:emit("message", data)
              end
            end
          end)
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
      connect = function(self)
        self.transport:start()
      end,
      disconnect = function(self)
        self.transport:stop()
      end
    }
  }
  
  Subscriber = function(opt)
    local self = setmetatable(wrapEmitter({}), mt)
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
  end
end

function tryConnectSubscribers(url, needed)
  local atonce = 1
  local connected, pending, failed, msgs = 0,0,0,0

  timeoutRepeat(function()
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
  
  timeoutRepeat(function()
    print(("needed: %d, connected: %d, pending: %d, failed: %d"):format(needed, connected, pending, failed))
    return connected < needed and 1
  end)
end

function beSlave(arg)
  local hdr = newHistogram()
  local id = math.random(100000000)
  local subskey = "benchi:subs:"..id
  cq:wrap(function()
    local redis = lrc.connect(arg.redis)
    local redisListener = lrc.connect(arg.redis)
    redisListener:subscribe("benchi:sub:"..id)
    
    redis:hmset(subskey, {id=id, max_subscribers=arg.subs})
    redis:call("sadd", "benchi:subs", tostring(id))
    redis:call("publish", "benchi", Json.encode({action="sub-waiting", id=id}))
    
    while true do
      local item = redisListener:get_next()
      if item == nil then break end
      if item[1] == "message" then
        local data = Json.decode(item[3])
        if data.action == "start" then
          tryConnectSubscribers(data.url, data.n)
        elseif data.action == "quit" then
          error("want to quit yeah");
        else
          pp("WEIRD ACTION", data)
        end
      end
    end
  end)
end


function beMaster(arg)
  cq:wrap(function()
    local hdrh = newHistogram()
    
    local pubs = {}
    local slaves = {}
    local num_slaves = 0
    local num_subs = 0
    
    local redis = lrc.connect(arg.redis)
    local redisListener = lrc.connect(arg.redis)
    
    local maybeStartPublishing = function()
      local init = {}
      for i, cf in pairs(arg.config) do
        local n = cf.n
        for slave_id, slave in pairs(slaves) do
          local subs = n > slave.max_subscribers and slave.max_subscribers or n
          n = n - subs
          
          table.insert(init, function()
            redis:call("publish", "benchi:sub:"..slave_id, Json.encode({
              action="start",
              url=cf.sub,
              n=subs
            }))
          end)
          
          if n == 0 then
            break
          end
        end
        
        if n > 0 then
          print("Not enough slaves/subscribers yet to subscribe to channel, still need at least " .. n .. " subscribers.")
          return
        end
      end
      
      for i, v in ipairs(init) do
        v()
      end
      print( "Start the thing!" )
    end
  
    local getSlaveData = function(slave_id)
      local data = redis:hgetall("benchi:subs:".. slave_id)
      data.max_subscribers = tonumber(data.max_subscribers)
      
      local numsub = redis:call("pubsub", "numsub", "benchi:sub:".. slave_id)
      assert(numsub[1]=="benchi:sub:".. slave_id)
      if numsub[2] == "1" or numsub[2] > 0 then
        if not slaves[slave_id] then
          num_slaves = num_slaves + 1
        else
          num_subs = slaves[slave_id].max_subscribers
        end
        slaves[slave_id] = data
        num_subs = num_subs + data.max_subscribers
        maybeStartPublishing()
      else
        redis:call("del", "benchi:subs:"..slave_id)
        redis:call("srem", "benchi:subs", slave_id)
      end
      
      pp(data)
      pp(numsub)
    end
    
    local sub_ids = redis:call("smembers", "benchi:subs")
    for i,v in ipairs(sub_ids) do
      getSlaveData(v)
    end
    
    cq:wrap(function() 
      redisListener:subscribe("benchi")
      while true do
        local item = redisListener:get_next()
        if item == nil then break end
        if item[1] == "message" then
          pp("DECODE2", item)
          local data = Json.decode(item[3])
          if data.action == "sub-waiting" then
            getSlaveData(data.id)
          elseif data.action == "stats" then
            local hdr_incoming = HDRHistogram.unserialize(data.hdr)
            hdrh:add(hdr_incoming)
            p(hdrh:latency_stats())
          end
        end
      end
    end)
  end)
end

if not opt.slave and not opt.master then
  print("Role setting missing, assuming --slave")
  opt.slave = true
end 

if opt.slave then
  beSlave(opt)
end

if opt.master then
  local conf_chunk, err = loadfile(opt.config)
  if conf_chunk then
    opt.config = conf_chunk()
  else
    print("Config not found at " .. opt.config ..".")
    os.exit(1)
  end
  beMaster(opt)
end


handleSignal({"SIGINT", "SIGTERM"}, function()
  print("WHAAAAAAAAAAAAAAAT!!!!")
  os.exit(1)
end)

while true do
  local ok, err, eh, huh = cq:step()
  if not ok then
    pp(ret, err, eh, huh)
  end
end
for err in cq:errors() do
  print(err)
end
