#!/usr/bin/ruby
require 'securerandom'
require_relative 'pubsub.rb'
require "optparse"

server= "localhost:8082"
par=1
quit_msg='FIN'
max_wait=60
OptionParser.new do |opts|
  opts.on("-s", "--server SERVER", "server and port."){|v| server=v}
  opts.on("-p", "--parallel NUM", "number of parallel clients"){|v| par = v.to_i}
  opts.on("-t", "--timeout SEC", "Long-poll timeout"){|v| max_wait = v}
  opts.on("-q", "--quit STRING", "Quit message"){|v| quit_msg = v}
  opts.on("-v", "--verbose", "Blabberhttp"){Typhoeus::Config.verbose=true}
end.parse!

url = "http://#{server}#{ARGV.last}"

puts "Subscribing #{par} client#{par!=1 ? "s":""} to #{url}."
puts "Timeout: #{max_wait}sec, quit msg: #{quit_msg}"


sub = Subscriber.new url, par, timeout: max_wait, quit_message: quit_msg

sub.on_message do |msg|
  puts msg
end

errors_shown=false
sub.on_failure do |x,y|
  puts sub.errors.join "\r\n" unless errors_shown
  errors_shown=true
  false
end


sub.run
sub.wait