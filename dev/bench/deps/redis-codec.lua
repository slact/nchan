--[[lit-meta
name = "slact/redis-codec"
version = "1.0.3"
description = "Pure Lua codec for RESP (REdis Serialization Protocol) + nils-in-lists-fix"
tags = {"codec", "redis"}
license = "MIT"
author = { name = "Tim Caswell" }
homepage = "https://github.com/slact/redis-luvit"
]]

local function encode(list)
  local len = #list
  local parts = {"*" .. len .. '\r\n'}
  for i = 1, len do
    local str = tostring(list[i])
    parts[i + 1] = "$" .. #str .. "\r\n" .. str .. "\r\n"
  end
  return table.concat(parts)
end

local byte = string.byte
local find = string.find
local sub = string.sub

local function innerDecode(chunk, index)
  if #chunk < 1 then return end
  local first = byte(chunk, index)
  if first == 43 then -- '+' Simple string
    local start = find(chunk, "\r\n", index, true)
    if not start then return end
    return sub(chunk, index + 1, start - 1), start + 2
  elseif first == 45 then -- '-' Error
    local start = find(chunk, "\r\n", index, true)
    if not start then return end
    return {error=sub(chunk, index + 1, start - 1)}, start + 2
  elseif first == 58 then -- ':' Integer
    local start = find(chunk, "\r\n", index, true)
    if not start then return end
    return tonumber(sub(chunk, index + 1, start - 1)), start + 2
  elseif first == 36 then -- '$' Bulk String
    local start = find(chunk, "\r\n", index, true)
    if not start then return end
    local len = tonumber(sub(chunk, index + 1, start - 1))
    if len == -1 then
      return nil, start + 2
    end
    if #chunk < start + 3 + len then return end
    return sub(chunk, start + 2, start + 1 + len), start + 4 + len
  elseif first == 42 then -- '*' List
    local start = find(chunk, "\r\n", index, true)
    if not start then return end
    local len = tonumber(sub(chunk, index + 1, start - 1))
    if len == -1 then
      return nil, start + 2
    end
    local list = {}
    index = start + 2
    for i = 1, len do
      local value
      value, index = innerDecode(chunk, index)
      --if not value then return end
      list[i] = value or false
    end
    return list, index
  else
    local list = {}
    local stop = find(chunk, "\r\n", index, true)
    if not stop then return end
    while index < stop do
      local e = find(chunk, " ", index, true)
      if not e then
        list[#list + 1] = sub(chunk, index, stop - 1)
        break
      end
      list[#list + 1] = sub(chunk, index, e - 1)
      index = e + 1
    end
    return list, stop + 2
  end
end

local function decode(chunk)
  local value, index = innerDecode(chunk, 1)
  if not index then return end
  return value, sub(chunk, index)
end

return {
  encode = encode,
  decode = decode,
}
