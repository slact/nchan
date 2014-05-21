#!/usr/bin/ruby
require 'securerandom'
require_relative 'pubsub.rb'
require "optparse"
server= "localhost:8082"
OptionParser.new do |opts|
  opts.on("-s", "--server SERVER", "server and port."){|v| server=v}
  opts.on("-v", "--verbose", "Blabberhttp"){Typhoeus::Config.verbose=true}
end.parse!

url = "http://#{server}#{ARGV.last}"

puts "Publishing to #{url}."
 
a=gets

pub = Publisher.new url
while true do
  puts "Enter one-line message, press enter."
  msg=gets #doesn't work when there are parameters. wtf?
  pub.post msg
  puts ""
end
