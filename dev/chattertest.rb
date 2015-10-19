#!/usr/bin/ruby
require 'test/unit'
require 'securerandom'
require "./pubsub.rb"

#invocation: ./chattertest.rb [channel] [pub/sub] [concurrency/message]
role, channel, sub_concurrency, pub_msg = nil, nil, nil, nil
#parse args
raise "Not enough args. ./chattertest.rb [channel] [pub/sub] [concurrency/message]" if ARGV.length < 3
channel=ARGV[0]
role=ARGV[1]
sub_concurrency=ARGV[2].to_i if role=="sub"
pub_msg=ARGV[2] if role=="pub"


def url(part)
  "http://127.0.0.1:8082/#{part}"
end
def pubsub(concurrent_clients=1, opt={})
    urlpart=opt[:urlpart] || 'broadcast'
    timeout = opt[:timeout] || 120
    sub_url=opt[:sub] || "sub/#{urlpart}/"
    pub_url=opt[:pub] || "pub/"
    chan_id = opt[:channel] || SecureRandom.hex
    sub = Subscriber.new url("#{sub_url}#{chan_id}"), concurrent_clients, timeout: timeout, quit_message: 'FIN'
    pub = Publisher.new url("#{pub_url}#{chan_id}")
    return pub, sub
end


pub, sub = pubsub(sub_concurrency, channel: channel)
if role=="sub"
  puts "Subscribing #{sub_concurrency} client(s) to channel #{channel}."
  sub.on_message do |msg|
    puts "got message \"#{msg.to_s}\""
  end
  sub.run false
elsif role=="pub"
  puts "Publishing #{pub_msg} to channel #{channel}"
  pub.post  pub_msg
else
  raise "unknown role #{role}"
end