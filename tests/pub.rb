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

opt=OptionParser.new do |opts|
  opts.on("-s", "--server SERVER (#{server})", "server and port."){|v| server=v}
  opts.on("-v", "--verbose", "Blabberhttp"){Typhoeus::Config.verbose=true}
  opts.on("-r", "--repeat [SECONDS]", "re-send message every N seconds (#{repeat_sec})") do |v| 
    loop=true
    repeat_sec=Float(v) unless v.nil?
  end 
  opts.on("-m", "--message MSG", "publish this message instead of prompting"){|v| msg=v}
  opts.on("-c", "--content-type TYPE", "set content-type for all messages"){|v| content_type=v}
  opts.on("-e",  "--eval RUBY_BLOCK", '{|n| "message #{n}" }'){|v| msg_gen = eval " Proc.new #{v} "}
end
opt.banner="Usage: pub.rb [options] url"
opt.parse!

url = "http://#{server}#{ARGV.last}"

puts "Publishing to #{url}."


loopmsg=("\r"*20) + "sending message #"

pub = Publisher.new url
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
    else
      STDIN.gets
    end
    if msg
      pub.post msg, content_type
    elsif msg_gen
      this_msg = msg_gen.call(i).to_s
      puts this_msg
      pub.post this_msg, content_type
    end
  else
    if loop
      puts "Can't repeat with custom message. use -m option"
    end
    puts "Enter one-line message, press enter."
    message=STDIN.gets #doesn't work when there are parameters. wtf?
    pub.post message, content_type
    puts ""
  end
  i+=1
end
