local uri="%s://localhost:8082/%s/%s"
local num_chans = 1
local subs_per_channel=30000

local chans = {}

for i=1,num_chans do
  local channel = tostring(math.random(100000))
  table.insert(chans, {
    pub = uri:format("http", "pub", channel),
    sub = uri:format("ws", "sub/broadcast", channel),
    n = subs_per_channel
  })
end

return chans
