require "pry"
code = 200

#\ -w -p 8053

app = proc do |env|
  chid="foo"
  pp env
  headers = {
    "X-Accel-Redirect" => "/sub/internal/#{chid}",
    "X-Accel-Buffering" => "no"
  }
  
  [ code, headers, [] ]
end

run app
