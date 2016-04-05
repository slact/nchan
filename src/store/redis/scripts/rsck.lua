--redis-store consistency check

local concat = function(...)
  local arg = {...}
  for i = 1, #arg do
    arg[i]=tostring(arg[i])
  end
  return table.concat(arg, " ")
end
local dbg =function(...) 
  redis.call('echo', concat(...))
end
local errors={}
local err = function(...)
  local msg = concat(...)
  dbg(msg)
  table.insert(errors, msg)
end

local tp=function(t)
  local tt={}
  for i, v in pairs(t) do
    table.insert(tt, tostring(i) .. ": " .. tostring(v))
  end
  return "{" .. table.concat(tt, ", ") .. "}"
end

local tohash=function(arr)
  if type(arr)~="table" then
    return nil
  end
  local h = {}
  local k=nil
  for i, v in ipairs(arr) do
    if k == nil then
      k=v
    else
      h[k]=v; k=nil
    end
  end
  return h
end
local type_is = function(key, _type)
  local t = redis.call("TYPE", key)['ok']
  local type_ok = true
  if type(_type) == "table" then
    type_ok = false
    for i, v in pairs(_type) do
      if v == _type then
        type_ok = true
        break
      end
    end
  elseif t ~= _type then
    err(key, " should be type", type, ", is", t)
    type_ok = false
  end
  return type_ok, t
end

local check_msg = function(chid, msgid, prev_msgid, next_msgid)
  local msgkey = "channel:msg:"..msgid..":"..chid
  local _, t = type_is(msgkey, {"hash", "none"})
  local msg = tohash(redis.call('HGETALL', msgkey))
  local ttl = tonumber(redis.call('TTL', msgkey))
  local n = tonumber(redis.call("HLEN", msgkey))
  if n > 0 and (msg.data == nil or msg.id == nil or msg.time == nil or msg.tag == nil)then
    err("incomplete message (ttl "..ttl..")", msgkey, tp(msg))
    return false
  end
  if t == "hash" and tonumber(ttl) < 0 then
    err("message", msgkey, "ttl =", ttl)
  end
  if ttl ~= -2 then
    if prev_msgid ~= false and msg.prev ~= prev_msgid then
      err("msg", chid, msgid, "prev_message wrong. expected", prev_msgid, "got", msg.prev)
    end
    if next_msgid ~= false and msg.next ~= next_msgid then
      err("msg", chid, msgid, "next_message wrong. expected", next_msgid, "got", msg.next)
    end
  end
end

local check_channel = function(id)
  local key={
    ch = "channel:"..id,
    msgs = "channel:messages:" .. id,
    next_sub_id= "channel:next_subscriber_id:" .. id
  }
  if not type_is(key.ch, "hash") then
    return false
  end
  type_is(key.msgs,{"list", "none"})
  type_is(key.next_sub_id, "string")
  
  local ch = tohash(redis.call('HGETALL', key.ch))
  local len = tonumber(redis.call("HLEN", key.ch))
  local ttl = tonumber(redis.call('TTL',  key.ch))
  if len == 2 then
    err("incomplete channel (ttl " .. ttl ..")", key.ch, tp(ch))
    return false
  end
  
  local msgids = redis.call('LRANGE', key.msgs, 0, -1)
  for i, msgid in ipairs(msgids) do
    check_msg(id, msgid, msgids[i+1], msgids[i-1])
  end
  
  
  if ch.prev_message then
    check_msg(id, ch.prev_message, false, ch.current_message)
  end
  if ch.current_message then
    check_msg(id, ch.current_message, ch.prev_message, false)
  end
  
end

local channel_ids = {}

for i, chkey in ipairs(redis.call("KEYS", "channel:*")) do
  if not chkey:match("^channel:messages:") and
     not chkey:match("^channel:msg:") and 
     not chkey:match("^channel:next_subscriber_id:") then
    table.insert(channel_ids, chkey);
  end
end

dbg("found", #channel_ids, "channels")
for i, chkey in ipairs(channel_ids) do
  local chid = chkey:match("^channel:(.*)")
  check_channel(chid)
end

if errors then
  table.insert(errors, 1, concat(#channel_ids, "channels, found", #errors, "problems"))
  return errors
else
  return concat(#channel_ids, "channels, all ok")
end
