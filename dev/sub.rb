#!/usr/bin/ruby
require 'securerandom'
require_relative 'pubsub.rb'
require "optparse"

server= "localhost:8082"
par=1
quit_msg='FIN'
no_message=false
max_wait=60
msg_count=0
print_content_type = false
show_id=false
client=:longpoll

opt=OptionParser.new do |opts|
  opts.on("-s", "--server SERVER (#{server})", "server and port."){|v| server=v}
  opts.on("-p", "--parallel NUM (#{par})", "number of parallel clients"){|v| par = v.to_i}
  opts.on("-t", "--timeout SEC (#{max_wait})", "Long-poll timeout"){|v| max_wait = v}
  opts.on("-q", "--quit STRING (#{quit_msg})", "Quit message"){|v| quit_msg = v}
  opts.on("-l", "--client STRING (#{client})", "sub client"){|v| client = v.to_sym}
  opts.on("-c", "--content-type", "show received content-type"){|v| print_content_type = true}
  opts.on("-i", "--id", "Print message id (last-modified and etag headers)."){|v| show_id = true}
  opts.on("-n", "--no-message", "Don't output retrieved message."){|v| no_message = true}
  opts.on("-v", "--verbose", "somewhat rather extraneously wordful output"){Typhoeus::Config.verbose=true}
end
opt.banner="Usage: sub.rb [options] url"
opt.parse!

url = "http://#{server}#{ARGV.last}"

puts "Subscribing #{par} #{client} client#{par!=1 ? "s":""} to #{url}."
puts "Timeout: #{max_wait}sec, quit msg: #{quit_msg}"


sub = Subscriber.new url, par, timeout: max_wait, quit_message: quit_msg, client: client

nomsgmessage="\r"*30 + "Received message %i, len:%i"

sub.on_message do |msg|
  if no_message
    msg_count+=1
    printf nomsgmessage, msg_count, msg.message.length
  else
    if print_content_type
      out = "(#{msg.content_type}) #{msg}"
    else
      out = msg.to_s
    end
    if show_id
      out = "<#{msg.id}> #{out}"
    end
    puts out
  end
end

errors_shown=false
sub.on_failure do |x,y|
  puts sub.errors.join "\r\n" unless errors_shown
  errors_shown=true
  false
end


sub.run
sub.wait
