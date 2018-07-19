#!/usr/bin/env ruby
require 'rubygems'
require 'bundler/setup'

require 'securerandom'
require_relative 'pubsub.rb'
require "optparse"
require 'timers'

$server_url="http://127.0.0.1:8082"
$default_client=:websocket
$omit_longmsg=false
$verbose=false
$parallel = 1
$sub_timeout = 10
$channels = 1000

$rand_seed = SecureRandom.rand(10000000)

class Flock
  class PubSub
    attr_accessor :pub, :sub, :channel_id, :published
    
    @msgnum = 0
    
    @@rand = Random.new($rand_seed)
    
    def self.short_id
      @@rand.bytes(5).unpack('H*')[0]
      #SecureRandom.hex.to_i(16).to_s(36)[0..5]
    end
    def self.url(part="")
      part=part[1..-1] if part[0]=="/"
      "#{$server_url}/#{part}"
    end
    
    def initialize(concurrent_clients=1, opt={})
      test_name = caller_locations(1,1)[0].label
      urlpart=opt[:urlpart] || 'broadcast'
      timeout = opt[:timeout]
      sub_url=opt[:sub] || "sub/broadcast/"
      pub_url=opt[:pub] || "pub/"
      @published = 0
      @channel_id = opt[:channel] || self.class.short_id
      @sub = Subscriber.new self.class.url("#{sub_url}#{@channel_id}#{opt[:sub_param] ? "&#{URI.encode_www_form(opt[:sub_param])}" : ""}"), concurrent_clients, nostore: true, timeout: timeout, use_message_id: opt[:use_message_id], quit_message: opt[:quit_message] || 'FIN', gzip: opt[:gzip], retry_delay: opt[:retry_delay], client: opt[:client] || $default_client, extra_headers: opt[:extra_headers], verbose: opt[:verbose] || $verbose, permessage_deflate: opt[:permessage_deflate], subprotocol: opt[:subprotocol], log: opt[:log]
      @lastmsg={}
      @lastnum={}
      @sub.on_message do |msg, bundle|
        matches = msg.message.match(/global message (?<num>\d+)/)
        if !matches || !matches[:num]
          puts "weird msg for channel #{@channel_id}!"
        else
          num = matches[:num].to_i
          if num == 1 && @lastmsg[bundle]
            #puts "first message wrong for channel #{@channel_id}!"
          elsif num >1 && @lastnum[bundle] != num - 1
            #puts "wrong message for channel #{@channel_id} (prev: #{@lastmsg[bundle] or "<none>"}, cur: #{num})!"
          end
          
          @lastnum[bundle] = num
          @lastmsg[bundle] = msg.message
          @lastmsg_anybundle = msg.message
        end
      end
      @sub.on_failure do |err, bundle|
        puts "failure for sub on channel #{@channel_id}:#{err}. published times: #{@published}. last received (this bundle): #{@lastmsg[bundle] or "<none>"}, last received(any bundle): #{@lastmsg_anybundle or "<none>"}"
        false
      end
      
      @pub = Publisher.new self.class.url("#{pub_url}#{@channel_id}#{opt[:pub_param] ? "&#{URI.encode_www_form(opt[:pub_param])}" : ""}"), timeout: timeout, websocket: opt[:websocket_publisher]
    end
  end
  def initialize()
    @pubsub = []
    @n={}
    $channels.times do |n|
      id= "#{n}ZZZ#{PubSub.short_id}"
      @pubsub << PubSub.new($parallel, timeout: $sub_timeout, channel: id)
    end
  end
  
  def run
    puts "subscribing to #{@pubsub.count} channels.."
    @pubsub.each do |ps|
      ps.sub.run
      ps.sub.wait :ready
    end
    puts "waiting a little..."
    
    puts "ok go!"
    while true do
      random_publish
      #sleep(rand 0.01..0.015)
    end
  end
  
  def random_publish
    ps = @pubsub.sample
    ps.published+=1
    @n[ps]||=0
    @n[ps]+=1
    begin
      resp = ps.pub.post "global message #{@n[ps]}"
    
      active_subs = resp.body.match(/active subscribers: (\d+)/)[1].to_i
      if active_subs != $parallel
        #puts "wrong active_subscriber count for #{ps.channel_id}: expected #{$parallel}, got #{active_subs}"
      end
    rescue Publisher::PublisherError => e
      puts "publishing error #{e}"
    end
  end
end

flock = Flock.new
flock.run
