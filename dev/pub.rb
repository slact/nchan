#!/usr/bin/ruby
require 'securerandom'
require_relative 'pubsub.rb'
require "optparse"
server= "localhost:8082"
msg=false
loop=false
repeat_sec=0.5
content_type=nil
msg_gen = false
on_response=Proc.new {}
method=:POST
runonce=false
accept = nil
timeout = 3
verbose = nil

opt=OptionParser.new do |opts|
  opts.on("-s", "--server SERVER (#{server})", "server and port."){|v| server=v}
  opts.on("-v", "--verbose", "Blabberhttp"){verbose=true}
  opts.on("-l", "--loop [SECONDS]", "re-send message every N seconds (#{repeat_sec})") do |v| 
    loop=true
    repeat_sec=Float(v) unless v.nil?
  end
  opts.on("-M", "--method [#{method}]", "method for request to server"){|v| method= v.upcase.to_sym}
  opts.on("-m", "--message MSG", "publish this message instead of prompting"){|v| msg=v}
  opts.on("-1", "--once", "run once then exit"){runonce=true}
  opts.on("-a", "--accept TYPE", "set Accept header"){|v| accept=v}
  opts.on("-c", "--content-type TYPE", "set content-type for all messages"){|v| content_type=v}
  opts.on("-e",  "--eval RUBY_BLOCK", '{|n| "message #{n}" }'){|v| msg_gen = eval " Proc.new #{v} "}
  opts.on("-d", "--delete", "delete channel via a DELETE request"){method = :DELETE}
  opts.on("-p", "--put", "create channel without submitting message"){method = :PUT}
  opts.on("-t", "--timeout SECONDS", "publishing timeout (sec). default #{timeout} sec"){|v| timeout = v.to_i}
  opts.on("-r",  "--response", 'Show response code and body') do
    on_response = Proc.new do |code, body|
      puts code
      puts body
    end
  end
end
opt.banner="Usage: pub.rb [options] url"
opt.parse!

url = "http://#{server}#{ARGV.last}"

puts "Publishing to #{url}."


loopmsg=("\r"*20) + "sending message #"

pub = Publisher.new url, nostore: true, timeout: timeout, verbose: verbose
pub.accept=accept
pub.nofail=true
repeat=true
i=1
if loop then
  puts "Press enter to send message."
end
while repeat do
  if msg or msg_gen
    if loop
      sleep repeat_sec
      print "#{loopmsg} #{i}"
    elsif !runonce
      STDIN.gets
    end
    if msg
      pub.submit msg, method, content_type, &on_response
    elsif msg_gen
      this_msg = msg_gen.call(i).to_s
      puts this_msg
      pub.submit this_msg, method, content_type, &on_response
    end
  else
    if loop
      puts "Can't repeat with custom message. use -m option"
    end
    puts "Enter message, press enter twice."
    message = ""
    while((line = STDIN.gets) != "\n") do #doesn't work when there are parameters. wtf?
      message << line
    end
    message=message[0..-2] #remove trailing newline
    
    pub.submit message, method, content_type, &on_response
    puts ""
  end
  i+=1
  exit 0 if runonce
end
