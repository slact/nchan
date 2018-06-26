--redis-store consistency check
local ns = ARGV[1]
if ns and #ns > 0 then
  ns = ns..":"
end

local concat = function(...)
  local arg = {...}
  for i = 1, #arg do
    arg[i]=tostring(arg[i])
  end
  return table.concat(arg, " ")
end
local dbg =function(...) redis.call('echo', concat(...)); end
local errors={}
local err = function(...)
  local msg = concat(...)
  dbg(msg)
  table.insert(errors, msg)
end

local tp=function(t, max_n)
  local tt={}
  for i, v in pairs(t) do
    local val = tostring(v)
    if max_n and #val > max_n then
      val = val:sub(1, max_n) .. "[...]"
    end
    table.insert(tt, tostring(i) .. ": " .. val)
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
local type_is = function(key, _type, description)
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
    err((description or ""), key, "should be type " .. _type .. ", is", t)
    type_ok = false
  end
  return type_ok, t
end

local known_msgs_count=0
local known_msgkeys = {}
local known_channel_keys = {}


local k = {
  channel = function(chid)
    return ("%s{channel:%s}"):format(chid)
  end,
  msg = function (chid, msgid)
    return ("%s:msg:%s"):format(k.channel(chid), msgid)
  end,
  messages = function(chid)
    return k.channel(chid) .. ":messages"
  end
}

local check_msg = function(chid, msgid, prev_msgid, next_msgid, description)
  description = description and "msg (" .. description ..")" or "msg"
  local msgkey = k.msg(chid, msgid)
  if not known_msgkeys[msgkey] then
    known_msgs_count = known_msgs_count + 1
  end
  known_msgkeys[msgkey]=true
  local ok, t = type_is(msgkey, {"hash", "none"}, "message hash")
  if t == "none" then
    --message is missing, but maybe it expired under normal circumstances. 
    --check if any earlier messages are present
    local msgids = redis.call('LRANGE', k.messages(chid), 0, -1)
    local founds = 0
    for i=#msgids, 1, -1 do
      if msgids[i] == msgid then 
        break
      end
      local thismsgkey = k.msg(chid, msgids[i])
      local ttt = redis.call('type', thismsgkey)['ok']
      redis.breakpoint()
      if ttt == "hash" then
        founds = founds + 1
      end
    end
    
    if founds > 0 then
      err("message", msgkey, "missing, with", founds, "prev. msgs in msg list")
    end
    
  end
  local msg = tohash(redis.call('HGETALL', msgkey))
  local ttl = tonumber(redis.call('TTL', msgkey))
  local n = tonumber(redis.call("HLEN", msgkey))
  if n > 0 and (msg.data == nil or msg.id == nil or msg.time == nil or msg.tag == nil)then
    err("incomplete " .. description .."(ttl "..ttl..")", msgkey, tp(msg))
    return false
  end
  if t == "hash" and tonumber(ttl) < 0 then
    err("message", msgkey, "ttl =", ttl)
  end
  if ttl ~= -2 then
    if prev_msgid ~= false and msg.prev ~= prev_msgid then
      err(description, chid, msgid, "prev_message wrong. expected", prev_msgid, "got", msg.prev)
    end
    if next_msgid ~= false and msg.next ~= next_msgid then
      err(description, chid, msgid, "next_message wrong. expected", next_msgid, "got", msg.next)
    end
  end
end

local check_channel = function(id)
  local key={
    ch = k.channel(id),
    msgs = k.messages(id)
  }
  
  local ok, chkey_type = type_is(key.ch, "hash", "channel hash")
  if not ok then
    if chkey_type ~= "none" then
      err("unecpected channel key", key.ch, "type:", chkey_type);
    end
    return false
  end
  local _, msgs_list_type = type_is(key.msgs, {"list", "none"}, "channel messages list")
  
  local ch = tohash(redis.call('HGETALL', key.ch))
  local len = tonumber(redis.call("HLEN", key.ch))
  local ttl = tonumber(redis.call('TTL',  key.ch))
  if not ch.current_message or not ch.time then
    if msgs_list_type == "list" then
      err("incomplete channel (ttl " .. ttl ..")", key.ch, tp(ch))
    end  
  elseif (ch.current_message or ch.prev_message) and msgs_list_type ~= "list" then
    err("channel", key.ch, "has a current_message but no message list")
  end
  
  local msgids = redis.call('LRANGE', key.msgs, 0, -1)
  for i, msgid in ipairs(msgids) do
    check_msg(id, msgid, msgids[i+1], msgids[i-1], "msglist")
  end
  
  if ch.prev_message then
    if redis.call('LINDEX', key.msgs, 1) ~= ch.prev_message then
      err("channel", key.ch, "prev_message doesn't correspond to", key.msgs, "second list element")
    end
    check_msg(id, ch.prev_message, false, ch.current_message, "channel prev_message")
  end
  if ch.current_message then
    if redis.call('LINDEX', key.msgs, 0) ~= ch.current_message then
      err("channel", key.ch, "current_message doesn't correspond to", key.msgs, "first list element")
    end
    check_msg(id, ch.current_message, ch.prev_message, false, "channel current_message")
  end
  
end

local channel_ids = {}

for i, chkey in ipairs(redis.call("KEYS", k.channel("*"))) do
  local msgs_chid_match = chkey:match("^"..k.messages("*"))
  if msgs_chid_match then
    type_is(k.channel(msgs_chid_match), "hash", "channel messages' corresponding hash key")
  elseif not chkey:match(":msg$") then
    table.insert(channel_ids, chkey);
    known_channel_keys[chkey] = true
  end
end

dbg("found", #channel_ids, "channels")
for i, chkey in ipairs(channel_ids) do
  local chid = chkey:match("^" .. k.channel(".*"))
  check_channel(chid)
end

for i, msgkey in ipairs(redis.call("KEYS", k.channel("*")..":msg")) do
  if not known_msgkeys[msgkey] then
    local ok, t = type_is(msgkey, "hash")
    if ok then
      if not redis.call('HGET', msgkey, 'unbuffered') then
        err("orphan message", msgkey, "(ttl: " .. redis.call('TTL', msgkey) .. ")", tp(tohash(redis.call('HGETALL', msgkey)), 15))
      end
    else
      err("orphan message", msgkey, "wrong type", t) 
    end
  end
end

if errors then
  table.insert(errors, 1, concat(#channel_ids, "channels,",known_msgs_count,"messages found", #errors, "problems"))
  return errors
else
  return concat(#channel_ids, "channels,", known_msgs_count, "messages, all ok")
end
