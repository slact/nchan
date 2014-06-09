#!/usr/bin/luajit

local filename="errors.log"
local follow=false
if arg[1] == "tail" or arg[1] == "follow" then
  follow=true
elseif arg[1] ~= nil then
  filename=arg[1]
end

local write=io.write

local shm={}
local size_by_label={}
local count_by_label={}
local weird_log={}

local poolsize=0
local memmap={}
local membuckets=1024
local bucketsize=0

function alloc(ptr, size, label, time, pid)
  size=tonumber(size)
  if shm[ptr] ~= nil then
    print("IGNORE WEIRD ALLOC AT", ptr, pid, time)
    table.insert(weird_log, {f="alloc", ptr=ptr, size=size, lbl=label, t=time, pid=pid})
  end
  shm[ptr]={size=size, label=label, time=time}
  size_by_label[label]=(size_by_label[label] or 0) + size
  count_by_label[label]=(count_by_label[label] or 0) + 1
  if bucketsize==0 then
    bucketsize=poolsize/membuckets
  end
  if bucketsize ~= 0 then
    local bstart=math.floor(ptr/poolsize * membuckets)
    local bend=math.floor((ptr+size)/poolsize * membuckets)
    if bend - bstart > 2 then
      print(("large alloc of %i buckets (%s)"):format(bend-bstart, formatsize(size)))
    end
    for i=bstart, bend do
      memmap[i]=(memmap[i] or 0)+1
    end
  end
  
end
function free(ptr, time, pid)
  local ref=shm[ptr]
  if ref == nil then
    print("IGNORE WEIRD FREE AT  ", ptr, pid, time)
    table.insert(weird_log, {f="free", ptr=ptr, size=nil, lbl=nil, t=time, pid=pid})
  else
    size_by_label[ref.label]=size_by_label[ref.label]-ref.size
    count_by_label[ref.label]=(count_by_label[ref.label] or 0) - 1
    shm[ptr]=nil
    if bucketsize ~= 0 then
      local bstart=math.floor(ptr/poolsize*membuckets)
      local bend=math.floor((ptr+ ref.size)/poolsize*membuckets)
      for i=bstart, bend do
        memmap[i]=(memmap[i] or 0)-1
        if memmap[i]<0 then print("memory map error!") end
      end
      
    else
      print("can't make momory map, unknown shm size!")
    end
  end
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

function fragmentation()
  local grow=function(tbl, first, last, step)
    if tbl[n] == nil then
      local i=n
      local grown=0
      while i > 0 do
        if tbl[i]==nil then
          tbl[i]=0
          grown=grown+1
        else
          return grown
        end
        i=i-1
      end
    end
    return 0
  end
  local compare=function(a,b)
    return a[1] < b[1]
  end
  local total=0;
  local resort={}
  for k,v in pairs(shm) do table.insert(resort, {tonumber(v.ptr), v.size}) end
  table.sort(resort, compare)
  
  local step=1024*100
  local memmap={}
  
end

function summary()
  local compare=function(a,b)
    return a[2] > b[2]
  end
  local total=0;
  local resort={}
  for k,v in pairs(size_by_label) do table.insert(resort, {k, v}) end
  table.sort(resort, compare)
  for k,v in ipairs(resort) do
    print(("%-40s %-10s %i"):format(v[1], formatsize(v[2]), count_by_label[v[1]]))
    total = total + v[2]
  end
  print("                                       --------      ")
  print(("%-40s %s" ):format("total", formatsize(total)))
  
  --memory map
  for i=0,membuckets do
    if memmap[i]==0 or memmap[i] == nil then
      write("-")
    elseif memmap[i]<10 then
      write(memmap[i])
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
    local ptr, size, label

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
    
    size = msg:match("ngx_http_push_shpool size (%d+)")
    if size then
      poolsize=tonumber(size)
      print(("%-40s %-10s"):format("shm size", formatsize(poolsize)))
    end
  end
end

if follow then
  local lasttime, now=os.time(), nil
  print "follow errors.log"
  local tailin = io.popen('tail -F '..filename..' 2>&1', 'r')
  for line in tailin:lines() do
    now=os.time()
    if parse_line(line) == true then
      summary()
      lasttime=now
    elseif now - lasttime > 1 then
      summary()
      lasttime=now
    end
  end
else
  print(string.format("open %s", filename))
  local f = io.open(filename, "r")
  local filesize=f:seek("end")
  f:seek("set")
  for line in f:lines() do
    if parse_line(line) == true then
      summary()
    end
  end
  f:close()
end


summary()
