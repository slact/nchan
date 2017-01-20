local cqueues = require "cqueues"
local signal = require "cqueues.signal"
local thread = require "cqueues.thread"
local gettimeofday = require("posix.sys.time").gettimeofday
local HDRHistogram = require "hdrhistogram"

return {
  wrapEmitter = function(obj)
    local evts = setmetatable({}, {
      __index = function(tbl, k)
        local ls = {}
        tbl[k] = ls
        return ls
      end
    })
    function obj:on(event, fn)
      assert(self == obj, "table:on() emitter invoked incorrectly")
      table.insert(evts[event], fn)
      return self
    end
    function obj:off(event, fn)
      assert(self == obj, "table:off() emitter invoked incorrectly")
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
  end,
  
  now = (function()
    local monotime = cqueues.monotime
    local tv=gettimeofday()
    local off = monotime()
    local t0=tv.tv_sec + tv.tv_usec/1000000
    
    return function()
      return t0 + (monotime() - off)
    end
  end)(),

  newHistogram = function()
    return HDRHistogram.new(0.001,10, 3, {multiplier=0.001, unit="s"})
  end,
  
  accessorize = function(cq)
    local index = getmetatable(cq).__index
    
    function index:timeoutRepeat(fn)
      self:wrap(function()
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
    
    function index:timeout(timeout_sec, cb)
      self:wrap(function()
        cqueues.sleep(timeout_sec)
        cb()
      end)
    end
    
    do
      local sig_hand_cqs; sig_hand_cqs = setmetatable({}, {__mode='k',  __index = function(t,cq)
        local tt=setmetatable({}, {__index = function(t,signame)
          local tt={}
          t[signame]=tt
          --print(t, signame)
          --signal.discard(signal[signame])
          signal.block(signal[signame])
          cq:wrap(function()
            local sig = signal.listen(signal[signame])
            while true do
              sig:wait()
              for i,v in ipairs(sig_hand_cqs[cq][signame]) do
                v()
              end
              end
            end)
          return tt
        end})
        
        t[cq]=tt
        return tt
      end})
      
      function index:handleSignal(signame, fn)
        if type(signame) == "table" then
          for k, v in pairs(signame) do
            self:handleSignal(v, fn)
          end
        else
          table.insert(sig_hand_cqs[self][signame], fn)
        end
      end
    end
    
    local thread_num = 1
    function index:newThread(fn,  ...)
      local my_num = thread_num
      local thr, con = thread.start(fn, my_num,  ...)
      self:wrap(function()
        local done, err, msg
        repeat
          done, err, msg = thr:join()
        until done
        if err then
          io.stderr:write(("thread %d: %s\n"):format(my_num, tostring(err)))
        end
      end)
      thread_num = thread_num + 1
      return thr, con
    end
    
    return cq
  end
}
