require "pry"
code = 200

#\ -w -p 8053

app = proc do |env|
  pp env
  [ code, {'Content-Type' => 'text/plain'}, ["fbeefyf"] ]
end

run app
