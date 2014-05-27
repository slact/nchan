#!/usr/bin/ruby
require 'securerandom'
require_relative 'pubsub.rb'
require "optparse"
server= "localhost:8082"
msg=false
loop=false

opt=OptionParser.new do |opts|
  opts.on("-s", "--server SERVER (#{server})", "server and port."){|v| server=v}
  opts.on("-v", "--verbose", "Blabberhttp"){Typhoeus::Config.verbose=true}
  opts.on("-l", "--loop", "don't wait to send message"){loop=true}
  opts.on("-m", "--message MSG", "publish this message instead of prompting"){|v| msg=v}
end
opt.banner="Usage: pub.rb [options] url"
opt.parse!

url = "http://#{server}#{ARGV.last}"

puts "Publishing to #{url}."


loopmsg=("\r"*20) + "sending message #"

pub = Publisher.new url
repeat=true
i=1
while repeat do
  if msg
    if loop
      print loopmsg
      print i
      i+=1
    else
      puts "Press enter to send message."
      STDIN.gets
    end
    pub.post msg
  else
    if loop
      puts "Can't repeat with custom message. use -m option"
    end
    puts "Enter one-line message, press enter."
    message=STDIN.gets #doesn't work when there are parameters. wtf?
    pub.post message
    puts ""
  end
end
