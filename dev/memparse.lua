#!/usr/bin/luajit
local lpty = require "lpty"

local lines_tailed=100000
local filename="errors.log"
local follow=false

local write=io.write

local shm, size_by_label, count_by_label, weird_log
function init()
  print "initialize"
  shm, size_by_label, count_by_label, weird_log={}, {}, {}, {}
  memmap={}
end

function err(str, ...)
  str = str or "Unknown error occurred"
  io.stderr:write(str:format(...))
  io.stderr:write("\n")
end
function printf(str, ...)
  print(str:format(...))
end


local poolsize=0
local poolstart=0
local memmap={}
local membuckets=1024
local bucketsize=0

local resolve_weird_log; do
  local weirdlog={}
  resolve_weird_log=function(line, is_weird)
    local prev = weirdlog[1]
    if prev and prev.ptr == line.ptr and prev.t == line.t and prev.pid ~= line.pid then
      if prev.action == "free"  and line.action == "alloc" then
        --oh, a free happened in a different worker and at the same time?
        --the log was probably written out-of-sequence
        table.remove(weirdlog, 1)
        alloc_raw(line.ptr, line.size, line.lbl, line.t, line.pid)
        free_raw(prev.ptr, prev.t, prev.pid)
        printf("resolved weird free/alloc at %s", line.ptr)
        return false
      elseif prev.action == "alloc" and line.action == "free" then
        --oh, an alloc happened in a different worker and at the same time?
        --the log was probably written out-of-sequence
        table.remove(weirdlog, 1)
        free_raw(line.ptr, line.t, line.pid)
        alloc_raw(prev.ptr, prev.size, prev.lbl, prev.t, prev.pid)
        printf("resolved weird alloc/free at %s", line.ptr)
        return false
      end
    end
    if is_weird then
      table.insert(weirdlog, 1, line)
      return false
    else
      return true
    end
  end
end

local memmap_add
do
  local bucketsize=0
  memmap_add= function(starthex, size, val)
    if not poolsize or not poolstart then return err("poolsize or poolstart not known") end  
    val = val or 1
    local start=tonumber(starthex, 16)
    if not start then return err("starthex was not a hex number") end
    --start should be relative to pool start
    start=start-poolstart
    
    if not size then
      if shm[starthex] then
        size=shm[starthex].size
      else
        err("shm[%s] is nil", starthex)
      end
    end
    
    local bstart = math.floor(start/poolsize * membuckets)
    local bend =   math.floor((start+size)/poolsize * membuckets)
    if bstart == math.huge then
      return err("alloc before shm init")
    end    
    for i=bstart, bend do
      memmap[i]=(memmap[i] or 0)+val
      if memmap[i]<0 then
        err("negative memmap at bucket %s", i)
        memmap[i]=0
      end
    end
  end
end

function alloc(ptr, size, label, time, pid)
  size=tonumber(size)
  local looks_weird=false
  if shm[ptr] ~= nil then
    err("BAD ALLOC AT ptr %s pid %i time %s", ptr, pid, time)
    looks_weird = true
  end
  if resolve_weird_log({action="alloc", ptr=ptr, size=size, lbl=label, t=time, pid=pid}, looks_weird) then
    alloc_raw(ptr, size, label, time, pid)
  end
end
function alloc_raw(ptr, size, label, time, pid)
  shm[ptr]={size=size, label=label, time=time}
  size_by_label[label]=(size_by_label[label] or 0) + size
  count_by_label[label]=(count_by_label[label] or 0) + 1
  memmap_add(ptr, size)
end

function free(ptr, time, pid)
  local looks_weird=false
  local ref=shm[ptr]
  if ref == nil then
    err ("DOUBLE FREE AT ptr %s pid %i time %s", ptr, pid, time)
    looks_weird = true
  end
  if resolve_weird_log({action="free", ptr=ptr, size=nil, lbl=nil, t=time, pid=pid}, looks_weird) then
    free_raw(ptr, time, pid)
  end
end
function free_raw(ptr, time, pid)
  local ref=shm[ptr]
  if ref==nil then
    err("executed double free on ptr %s pid %i time %s", ptr, pid, time)
    return
  end
  memmap_add(ptr, nil, -1)
  size_by_label[ref.label]=size_by_label[ref.label]-ref.size
  count_by_label[ref.label]=(count_by_label[ref.label] or 0) - 1
  shm[ptr]=nil
end

function formatsize(bytes)
  if bytes<1024 then
    return string.format("%ib", bytes)
  elseif bytes > 1024 and bytes < 1048576 then
    return string.format("%.3gKb", bytes/1024)
  else
    return string.format("%.3gMb", bytes/1048576)
  end
end

local alphabet={1,2,3,4,5,6,7,8,9,"A","B","C","D","E","F","G","H","I","J","K","L","M","N","O","P","Q","R","S","T","U","V","W","X","Y","Z"}
function summary()
  local compare=function(a,b)
    return a[2] > b[2]
  end
  local total=0;
  local resort={}
  for k,v in pairs(size_by_label) do table.insert(resort, {k, v}) end
  table.sort(resort, compare)
  for k,v in ipairs(resort) do
    printf("%-40s %-10s %i", v[1], formatsize(v[2]), count_by_label[v[1]])
    total = total + v[2]
  end
  print ("                                       --------      ")
  printf("%-40s %s", "total", formatsize(total))
  
  --memory map
  local n
  for i=0,membuckets do
    n=memmap[i]
    if n==0 or n == nil then
      write("-")
    elseif n<#alphabet then
      write(alphabet[n] or "?")
    else
      write("#")
    end
  end
  print ""
end


function parse_line(str)
  local out_of_memory
  local time,errlevel, pid, msg=str:match("(%d+/%d+/%d+%s+%d+:%d+:%d+)%s+%[(%w+)%]%s+(%d+)#%d+:%s+(.+)")
  if msg ~= nil then
    local ptr, size, label, start

    if errlevel == "crit" then
      print("CRITICAL:", time, pid, msg)
      if msg == "ngx_slab_alloc() failed: no memory" then
        return true
      end
    end
    ptr, size, label = msg:match("shpool alloc addr (%w+) size (%d+) label (.+)")
    if ptr then
      alloc(ptr, size, label, time, pid)
      return ptr
    end

    ptr = msg:match("shpool free addr (%w+)")
    if ptr then
      free(ptr, time, pid)
      return ptr
    end
    
    start, size = msg:match("nchan_shpool start (%w+) size (%d+)")
    if size and start then
      init()
      poolsize=tonumber(size)
      poolstart = tonumber(start, 16)
      printf("shm start %s size %s", start, formatsize(poolsize))
    end
  end
end



----------------------------------------------------------------
--NO FUNCTIONS DEFINED BELOW THIS LINE PLEASE (except lambdas)--

--handle arguments
if arg[1] == "tail" or arg[1] == "follow" then
  follow=true
elseif arg[1] ~= nil then
  filename=arg[1]
end


init()


if follow then
  local lasttime, now=os.time(), nil
  print "follow errors.log"
  
  pty = lpty.new()
  pty:startproc('tail', ("--lines=%s"):format(lines_tailed), "-F", filename)
  
  while true do
    line = pty:readline(false, 1)
    now=os.time()
    if not line then
      --nothing
    elseif line:match('truncated') or
       line:match(("‘%s’ has become inaccessible: No such file or directory"):format(filename)) then
      --reset
      init()
    else
      parse_line(line)
    end
    
    if now - lasttime >= 1 then
      summary()
      lasttime=now
    end
  end
else
  printf("open %s", filename)
  local f = io.open(filename, "r")
  for line in f:lines() do
    if parse_line(line) == true then
      summary()
    end
  end
  f:close()
end


summary()
