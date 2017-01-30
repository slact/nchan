#!/usr/bin/luajit
local cqueues = require "cqueues"
local thread = require "cqueues.thread"

local argparse = require "argparse"
local Json = require "cjson"
local pp = require('pprint')
local mm = require "mm"
local ut = require("bench.util")

local parser = argparse("script", "thing thinger.")
parser:option("--redis", "Redis orchestration server url.", "127.0.0.1:6379")
parser:option("--config", "Pub/sub config lua file.", "config.lua")
parser:option("--subs", "max subscribers for slave (default: 20000).", 50000)
parser:option("--threads", "number of slave threads (default: 4).", 4)
parser:flag("--master", "Be master")
parser:flag("--slave", "Be slave")


local opt = parser:parse(args)
opt.slave_threads = tonumber(opt.slave_threads)
opt.subscribers = tonumber(opt.subscribers)

local cq=cqueues.new()

ut.accessorize(cq)

if not opt.slave and not opt.master then
  print("Role setting missing, assuming --slave")
  opt.slave = true
end 

local slave_threads = setmetatable({}, {__mode='k'})

if opt.slave then
  local opt_json = Json.encode(opt)
  for i=1,tonumber(opt.threads) do
    local thread, con = cq:newThread(function(con, threadnum, opt_json)
      local cqueues = require "cqueues"
      local Json = require "cjson"
      local ut = require("bench.util")
      --local pp = require "pprint"
      local cq = cqueues.new()
      ut.accessorize(cq)
      local opt = Json.decode(opt_json)
      local Slave = require "bench.slave"
      local slave = Slave(cq, opt)
      print("started slave thread " .. threadnum)
      cq:wrap(function()
        for ln in con:lines() do
          if ln == "exit" then
            slave:on("stop", function()
              print("quit from slave thread "  .. threadnum) --quit stuff
              con:write("exited\n")
            end)
            slave:stop()
          end
        end
      end)
      assert(cq:loop())
    end, opt_json)
    slave_threads[thread] = con
  end
end

if opt.master then
  local master = require "bench.master"
  local conf_chunk, err = loadfile(opt.config)
  if conf_chunk then
    opt.config = conf_chunk()
    pp(opt.config)
  else
    print("Config not found at " .. opt.config ..".")
    os.exit(1)
  end
  master(cq, opt)
end


local force_quit
cq:handleSignal({"SIGINT", "SIGTERM"}, function()
  local exiting = 0
  local exited = 0

  if force_quit then
    print("\nNo more waiting around. Quit!")
    os.exit(1)
  end
  
  print("\nShutting down slaves... ^C again to force-quit.")
  
  for thread, con in pairs(slave_threads) do
    con:write("exit\n")
    exiting = exiting + 1
    cq:wrap(function()
      for ln in con:lines() do
        if ln == "exited" then
          exited = exited + 1
          if exited == exiting then
            print("we're done here")
            os.exit(1)
          end
        end
      end
    end)
  end
  
  if exiting == 0 then
    print("no slave threads. quit now")
    os.exit(1)
  end

  force_quit = 1
end)


assert(cq:loop())
for err in cq:errors() do
  print(err)
end
