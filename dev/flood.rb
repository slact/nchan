#!/usr/bin/ruby
require 'securerandom'
require_relative 'pubsub.rb'
require 'json'
require 'celluloid/current'

seed = 10
MAX_CHANS= 5

SUB_URL="http://127.0.0.1:8082/sub/broadcast/"
PUB_URL="http://127.0.0.1:8082/pub/"

class Chans
  
  
  class Chan
    
    class PubWrap
      include Celluloid::IO
      def initialize(url, opt={})
        @pub=Publisher.new(url, opt)
      end
      def post(stuff, chans)
        @pub.post stuff
        chans.published!
      end
    end
    
    attr_accessor :id, :sub, :pub
    def initialize(id, chan, subscriber_opt={})
      @id=id
      @sub=Subscriber.new("#{SUB_URL}#{id}", 1, subscriber_opt.merge(client: :websocket, quit_message: 'FIN'))
      @sub.run
      @pub=Publisher.new("#{PUB_URL}#{id}")
      @chan = chan
    end
        
    def pub(what)
      if PubWrap === @pub
        @pub.async.post(what, @chan)
      else
        @pub.post(what)
        @chan.published!
      end
    end
  end
  
  attr_accessor :published
  def published!
    @published += 1
  end
  
  def initialize(seed=0)
    @rng = Random.new(seed)
    @chans = []
    @msgs = ["hey", "what", "wuh", "hello!"*100, "longer"*150]
    @sent=0
    @published = 0
  end
  
  def chid
    id=""
    num=rand(2..45)
    num.times do
      id << (65 + rand(26)).chr
    end
    id
  end
  
  private :chid
  
  def chan
    rn = @rng.rand(MAX_CHANS)
    chan = @chans[rn]
    unless chan
      chan = Chan.new(chid, self)
      @chans << chan
    end
    chan
  end
  
  def pubrand
    mn = @rng.rand(@msgs.length)
    chan.pub(@msgs[mn])
  end
  
  def randmsg
    
  end
end


class OutputTimer
  include Celluloid
  attr_reader :fired, :timer

  def initialize(chans, interval = 1)
    @chans = chans
    @prev_count = 0
    @interval = interval
    @last_msg_length = 0 
    @timer = every(interval) do
      pubd = @chans.published
      puts "Publishing at #{(pubd - @prev_count) / interval} msgs/sec (total: #{pubd})"
      @prev_count  = pubd
    end
  end
end

chans = Chans.new
out = OutputTimer.new(chans)

loop do
  chans.pubrand
end
