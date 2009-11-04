print("The test 'framework' used here is still a touch wonky. A test failure need not mean that things are actually broken...")
require "httpest" --fyi, the test "framework" used here is still wonky.
math.randomseed(os.time())
local request=httpest.request
local sendurl, listenurl = "http://localhost:8082/broadcast/pub?channel=%s", "http://localhost:8082/broadcast/sub?channel=%s"
local function send(channel, message, callback)
	assert(request{
		url=sendurl:format(channel),
		method="post",
		data=message,
		headers={['content-type']="text/foo"},
		complete=function(r, status, sock)
			assert(not status, "fail: " .. (status or "?"))
			assert(r.status==201 or r.status==202, tostring(r))
			if callback then
				callback(r, status, sock)
			end
		end
	})
end

local channeltags = {}
local function listen(channel, callback, timeout, headers)
	if not channeltags[channel] then channeltags[channel] = {} end
	local s
	local subscriber_timeout = function()
		return callback(nil, "timeout", s)
	end
	s = request{
		url=listenurl:format(channel),
		method="get",
		headers = headers or {
			['if-none-match']=channeltags[channel]['etag'],
			['if-modified-since']=channeltags[channel]['last-modified']
		},
		complete = function(r, status, s)
			httpest.killtimer(subscriber_timeout)
			if not r then 
				channeltags[channel]=nil
				return callback(nil, status, s)
			end
			channeltags[channel].etag=r:getheader("etag")
			channeltags[channel]['last-modified']=r:getheader("last-modified")
			if callback then
				return callback(r, status, s)
			else
				return channel
			end
		end
	}
	httpest.timer(timeout or 500, subscriber_timeout)
	return s
end

local function batchsend(channel, times, msg, callback, done)
	send(channel, type(msg)=="function" and msg() or msg, function(...)
		if callback then
			callback(...)
		end
		if times>1 then
			return batchsend(channel, times-1, msg, callback, done)
		else
			return done and done()
		end
	end)
end


local function batchlisten(channel, callback, timeout)
	local function subscriber(r, err, s)
		local result
		if r or err then
			result = callback(r, err, s)
		end
		if (not r and not err) or result then
			return listen(channel, subscriber, timeout)
		end
	end
	return subscriber()
end

local function shortmsg(base)
	return (tostring(base)):rep(3)
end

local function testqueuing(channel, done)
	local s, i, messages = nil, 0, {}
	local function subscriber(resp, status, s)
		if resp then
			table.insert(messages, 1, resp:getbody())
		end
		if status=="timeout" then
			httpest.abort_request(s)
			print("  message buffer length is " .. #messages)
			for j, v in ipairs(messages) do
				assert(v==shortmsg(i-j+1), #v .. "	" .. #shortmsg(i-j+1))
			end
			return nil
		end
		return true
	end
	batchsend(channel, 10, 
		function()
			i=i+1
			return shortmsg(i)
		end, 
		nil,
		function()
			return batchlisten(channel, subscriber, 100) 
		end
	)
end

--queuing
for i=1, 5 do
	local channel = math.random()
	httpest.addtest("queuing " .. i .. "(10 messages)", function() testqueuing(channel) end)
end

--deleting
local channel="deltest"
httpest.addtest("delete", function()
	batchsend(channel, 20, "hey", nil, function()
		request{
			url=sendurl:format(channel),
			method='delete',
			complete=function(r,st,s)
				assert(r.status==200, r.status)
				request{
					url=sendurl:format(channel),
					method='delete',
					complete=function(r,st,s)
						assert(r.status==404)
					end
				}
			end
		}
	end)
end)


--broadcasting
local num, reps, observed = 100, 5, 0
for i=0,reps do
	local channel=math.random()
	httpest.addtest(('broadcast to %s (%s)'):format(num, channel), function()
		local msg = math.random() .. "yesyes"
		for j=1, num do
			batchlisten(channel, function(resp, status, sock)
				if resp then
					httpest.abort_request(sock)
					assert(resp:getbody()==msg, "unexpected message: " .. resp:getbody() .. " (expected " .. msg)
					observed = observed + 1
					print(observed)
					return nil
				end
				return true
			end)
		end
		httpest.timer(2000, function()
			send(channel, msg)
		end)
	end)
end
httpest.timer(5000*reps, function()
	local total = reps*num

	assert(responses==total, ("expected %d responses, got %d"):format(total, observed))
end)

httpest.run()