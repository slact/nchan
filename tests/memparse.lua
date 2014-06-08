#!/usr/bin/luajit

local write=io.write

local shm={}
local size_by_label={}
local count_by_label={}

local weird_log={}

function alloc(ptr, size, label, time, pid)
  size=tonumber(size)
  if shm[ptr] ~= nil then
    print("IGNORE WEIRD ALLOC AT", ptr, pid, time)
    table.insert(weird_log, {f="alloc", ptr=ptr, size=size, lbl=label, t=time, pid=pid})
  end
  shm[ptr]={size=size, label=label, time=time}
  size_by_label[label]=(size_by_label[label] or 0) + size
  count_by_label[label]=(count_by_label[label] or 0) + 1
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
  end
end

function formatsize(bytes)
  if bytes<1024 then
    return bytes .. "b"
  elseif bytes > 1024 and bytes < 1048576 then
    return math.floor(bytes/1024) .. "Kb"
  else
    return math.floor(bytes/1048576) .. "Mb"
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
  print("-------------------------------------")
  print(("%-40s %s" ):format("total", formatsize(total)))
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
  end
end



local last_time=os.time()
local f = io.open("errors.log", "r")
local filesize=f:seek("end")
f:seek("set")
for line in f:lines() do
  if parse_line(line) == true then
    summary()
  end
  --os.execute("sleep 0.1")
  if os.time() - last_time > 1 then
    local percent = math.floor((f:seek()/filesize) * 10000)/100
    write(("\r\r\r\r\r\r\r\r\r\r\r\r\rhey%s%%"):format(percent))
    last_time = os.time()
  end
end
f:close()
print "---"
summary()
