#!/bin/ruby
require "pry"
code = 200

#\ -w -p 8053

app = proc do |env|
  pp env
  resp = []
  headers = {}
  
  body = env["rack.input"].read
  
  case env["REQUEST_PATH"]
  when "/accel_redirect"  
    chid="foo"
    headers["X-Accel-Redirect"]="/sub/internal/#{chid}"
    headers["X-Accel-Buffering"] = "no"
  when "/subauth"
    #meh
  when "/pub"
    resp << "WEE! + #{body}"
    headers["Content-Length"]=resp.join("").length.to_s
  end
  
  [ code, headers, resp ]
end

run app
