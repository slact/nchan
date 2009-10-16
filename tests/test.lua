require "httpest"
math.randomseed(10)
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
local function listen(channel, callback, headers, timeout)
	if not channeltags[channel] then channeltags[channel] = {} end
	return request{
		url=listenurl:format(channel),
		method="get",
		headers = headers or {
			['if-none-match']=channeltags[channel]['etag'],
			['if-modified-since']=channeltags[channel]['last-modified']
		},
		complete = function(r, status)
			if not r then 
				channeltags[channel]=nil
				return 
			end
			channeltags[channel].etag=r:getheader("etag")
			channeltags[channel]['last-modified']=r:getheader("last-modified")
			if callback then
				return callback(r, status)
			else
				return channel
			end
		end
	}
end

local function testqueuing(channel)
	--part 1: write a bunch of messages.
	local s, i, messages = nil, 0, {}
	local function listener_timeout()
		print("message buffer length is " .. #messages)
		if s then
			httpest.abort_request(s)
		end
		for j, v in ipairs(messages) do
			assert(v==(tostring(i-j) .. " "):rep(20))
		end
	end
	local function listener(resp, status)
		if resp then
			table.insert(messages, 1, resp:getbody())
		end
		httpest.killtimer(listener_timeout)
		s = listen(channel, listener)
		httpest.timer(500, listener_timeout)
	end
	
	local function spawn(resp, status)
		i=i+1
		assert(not status, "fail: " .. (status or "?"))
		if resp then
			assert(not resp.invalid, "invalid response")
			assert(resp.status==201 or resp.status==202)
		end
		if i<=50 then
			send(channel, (tostring(i) .. " "):rep(20), spawn)
		else
			listener()
		end
		if resp then	
			return true
		end
	end
	return spawn
end

for i=1, 5 do
	httpest.addtest("queuing " .. i, testqueuing(math.random()))
end

httpest.run()