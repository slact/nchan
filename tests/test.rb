#!/usr/bin/ruby
require 'test/unit'
require 'securerandom'
require "./pubsub.rb"

def url(part)
  "http://127.0.0.1:8082/#{part}"
end

class PubSubTest < Test::Unit::TestCase
  def setup
    Celluloid.boot
  end
  def test_message_delivery
    rnd = SecureRandom.hex
    sub = Subscriber.new url("broadcast/sub/#{rnd}"), 1, quit_message: 'FIN'
    pub = Publisher.new url("broadcast/pub/#{rnd}")
    sub.run
    assert_equal sub.messages.messages.count, 0
    pub.post "hi there"
    sleep 0.2
    assert_equal sub.messages.messages.count, 1
    pub.post "FIN"
    sleep 0.2
    assert_equal sub.messages.messages.count, 2
    assert sub.messages.matches? pub.messages
  end
  
  def test_broadcast
    num_clients = 700
    sub = Subscriber.new url('broadcast/sub/broadcast'), num_clients, quit_message: 'FIN'
    sub.run #celluloid async FTW
    pub = Publisher.new url('broadcast/pub/broadcast')
    ["hello there", "what is this", "it's nothing", "FIN"].each{|m| pub.post m}
    sub.wait
    assert sub.messages.matches?(pub.messages)
    sub.messages.each do |msg|
      assert_equal msg.times_seen, num_clients
    end
  end
  
  def test_queueing
    rnd = SecureRandom.hex
    sub = Subscriber.new url("broadcast/sub/#{rnd}"), 5, quit_message: 'FIN'
    pub = Publisher.new url("broadcast/pub/#{rnd}")
    %w( what is this_thing andnow 555555555555555555555 eleven FIN ).each {|m| pub.post m}
    sub.run
    sub.wait
    assert sub.messages.matches?(pub.messages)
    sub.messages.each do |msg|
      assert_equal msg.times_seen, 5
    end
  end
end