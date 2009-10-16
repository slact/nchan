require "httpest"

local request=httpest.request
local sendurl, listenurl = "http://localhost:8089/?channel=%s", "http://localhost:8088/?channel=%s"
local function send(channel, message, callback)
	assert(request{
		url=sendurl:format(channel),
		method="post",
		data=message,
		complete=callback
	})
end

local channeltags = {}
local function listen(channel, callback, headers)
	assert(request{
		url=listenurl:format(channel),
		method="get"
		headers = headers or {
			['if-none-match']=channeltags[channel]['etag'],
			['if-modified-since']=channeltags[channel]['last-modified']
		},
		complete = function(r, status)
			if not r then 
				channeltags[channel]=nil
				return 
			end
				
			
			
		end
	})
	
end

local function testqueuing(channel)
	--part 1: write a bunch of messages.
	local i=0
	local function spawn(resp, status)
		i=i+1
		if req then
			assert(not resp.invalid, "invalid response")
			assert(resp.status==201 or resp.status==202)
		end
		if i<=50 then
			send(channel, (tostring(i) .. " "):rep(20), spawn)
		end
	end
	spawn()
end



testqueuing(12)

httpest.run()