#!/usr/bin/ruby
require 'securerandom'
require_relative 'pubsub.rb'
require "optparse"
server= "localhost:8082"
msg=false
loop=false
repeat_sec=0.5
content_type=nil
on_response=Proc.new {}
method=:POST
runonce=false

#Typhoeus::Config.verbose=true

pubs = []
ARGV.each do |v|
  url = "http://#{server}/pub/#{v}"
  puts "Publishing to #{url}."
  pub = [v, Publisher.new(url, :timeout => 10000)]
  pub[1].nofail = true
  pubs << pub
end

repeat=true
i=1

while repeat do
  puts "press enter to send message ##{i}"
  STDIN.gets
  n=0
  pubs.each do |p|
    n+=1
    this_msg = "#{p.first} (#{n}) -- #{i}"
    p.last.submit this_msg, method, content_type, &on_response
    puts "sent #{this_msg}"
  end
  i+=1
end
