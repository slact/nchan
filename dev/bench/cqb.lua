#!/usr/bin/luajit
local cqueues = require "cqueues"
local thread = require "cqueues.thread"

local argparse = require "argparse"
local Json = require "cjson"
local pp = require('pprint')
local ut = require("bench.util")

local parser = argparse("script", "thing thinger.")
parser:option("--redis", "Redis orchestration server url.", "127.0.0.1:6379")
parser:option("--config", "Pub/sub config lua file.", "config.lua")
parser:option("--subs", "max subscribers (for slave).", 25000)
parser:option("--threads", "number of threads (for slave).", 4)
parser:mutex(
  parser:flag("--master", "Be master"),
  parser:flag("--slave", "Be slave")
)

local opt = parser:parse(args)
opt.slave_threads = tonumber(opt.slave_threads)
opt.subscribers = tonumber(opt.subscribers)

local cq=cqueues.new()

ut.accessorize(cq)

if not opt.slave and not opt.master then
  print("Role setting missing, assuming --slave")
  opt.slave = true
end 

if opt.slave then
  local opt_json = Json.encode(opt)
  for i=1,tonumber(opt.threads) do
    cq:newThread(function(con, threadnum, opt_json)
      local cqueues = require "cqueues"
      local Json = require "cjson"
      local ut = require("bench.util")
      local cq = cqueues.new()
      ut.accessorize(cq)
      local opt = Json.decode(opt_json)
      local slave = require "bench.slave"
      cq:wrap(function()
        slave(cq, opt)
        print("started slave thread " .. threadnum)
      end)
      assert(cq:loop())
    end, opt_json)
  end
end

if opt.master then
  local master = require "bench.master"
  local conf_chunk, err = loadfile(opt.config)
  if conf_chunk then
    opt.config = conf_chunk()
  else
    print("Config not found at " .. opt.config ..".")
    os.exit(1)
  end
  master(cq, opt)
end


cq:handleSignal({"SIGINT", "SIGTERM"}, function()
  print("outta here")
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
