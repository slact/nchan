class Rdsck
  require "async"
  require "async/redis"
  
  #doesn't support psubscribe by default. can you believe it?!
  class Async::Redis::Context::Psubscribe < Async::Redis::Context::Subscribe
    MESSAGE = 'pmessage'
  
    def listen
      while response = @connection.read_response
        return response if response.first == MESSAGE
      end
    end
    
    def subscribe(channels)
      @connection.write_request ['PSUBSCRIBE', *channels]
      @connection.flush
    end
    
    def unsubscribe(channels)
      @connection.write_request ['PUNSUBSCRIBE', *channels]
      @connection.flush
    end
  end
  
  class Async::Redis::Client
    def psubscribe(*channels)                                                                                                                                             
      context = Async::Redis::Context::Psubscribe.new(@pool, channels)                                                                                                            
      return context unless block_given?                                                                                                                           
      begin                                                                                                                                                        
        yield context                                                                                                                                        
      ensure                                                                                                                                                       
        context.close                                                                                                                                        
      end                                                                                                                                                          
    end 
  end
  
  attr_accessor :url, :verbose, :namespace
  attr_accessor :redis, :masters
  
  def dbg(*args)
    if @verbose
      print("# ")
      puts(*args)
    end
  end
  
  def initialize(opt)
    @url=opt[:url]
    @verbose=opt[:verbose]
    @namespace=opt[:namespace]
    @channel_id=opt[:channel_id]
  end
  
  def cluster?
    @cluster_mode
  end
  
  def connect
    begin
      @redis=Redis.new url: @url
      
      mode = redis.info["redis_mode"]
      
    rescue StandardError => e
      STDERR.puts e.message
      return false
    end

    if mode == "cluster"
      @redis.close
      begin
        @redis=Redis.new cluster: [@url]
        @redis.ping
      rescue StandardError => e
        STDERR.puts e.message
        return false
      end
      
      @cluster_mode = true
      @masters = []
      
      redis.connection.each do |c|
        node = Redis.new url: c[:id]
        @masters << node
      end
    else
      @masters = [@redis]
    end
    
    dbg "Connected to Redis #{mode == "cluster" ? "cluster" : "server"}"
    (Array === @redis.connection ? @redis.connection : [@redis.connection]) .each do |v|
      dbg "  #{v[:id]}"
    end
    self
  end
  
  def key(subkey=nil)
    k = "{channel:#{@namespace}/#{@channel_id}}"
    return subkey ? "#{k}:#{subkey}" : k
  end
  
  def info
    channel_hash=@redis.hgetall key
    hash_ttl=@redis.ttl key
    channel_subs=@redis.hgetall key("subscribers")
    #...
  end
  
  class Watch
    def initialize(rdsck, node, filters, set_notify_config = nil)
      @rdsck = rdsck
      @sync = node
      @filters = filters
      @set_notify_config = set_notify_config
      @host, @port, @location = @sync.connection[:host], @sync.connection[:port]
      @url = @sync.connection[:id]
      @async = Async::Redis::Client.new(Async::IO::Endpoint.tcp(@host, @port))
      if set_notify_config
        @rdsck.dbg "set #{@url} notify-keyspace-events to \"Kh\""
        @prev_notify_keyspace_event_config = @sync.config("get", "notify-keyspace-events")
        @prev_notify_keyspace_event_config = @prev_notify_keyspace_event_config[1] if @prev_notify_keyspace_event_config
        
        @sync.config :set, "notify-keyspace-events", "Kh"
      end
    end
    
    def watch(task)
      task.async do
        #puts "subscribeme"
        while true do
          @async.psubscribe "__keyspace*__:{channel:*}" do |ctx|
            type, pattern, name, msg = ctx.listen
            #puts "TYPE: #{type}, PAT:#{pattern}, NAME:#{name}, MSG:#{msg}"
            m=name.match(/^__.*__:(\{.*\})/)
            if m && m[1]
              key = m[1]
              
              filtered = false
              subs = nil
              
              if @filters[:min_subscribers]
                subs = @sync.hget key, "fake_subscribers"
                subs = subs.to_i
                filtered = true if subs < @filters[:min_subscribers]
              end
              
              if !filtered
                if subs
                  puts "#{key} subscribers: #{subs}"
                else
                  puts "#{key}"
                end
              end
              
            end
          end
        end
      end
    end
    
    def stop
      @async.close
      if @set_notify_config
        @rdsck.dbg "set #{@url} notify-keyspace-events back to #{@prev_notify_keyspace_event_config}"
        @sync.config(:set, "notify-keyspace-events", @prev_notify_keyspace_event_config)
      end
    end
  end
  
  def watch_channels(filters={}, set_notify_keyspace_events=nil)
    watchers = []
    @masters.each do |m|
      watchers << Watch.new(self, m, filters, set_notify_keyspace_events)
    end


    begin
      Async do |task|
        watchers.each do |watcher|
          watcher.watch(task)
        end
      end
    rescue Interrupt => e
      dbg "stopping watch"
    ensure
      watchers.each do |watcher|
        watcher.stop
      end
    end
    
  end
  
  def filter_channels(filters={})
    script = <<~EOF
      local prev_cursor = ARGV[1]
      local pattern = ARGV[2]
      local scan_batch_size = ARGV[3]
      
      local min_subscribers = ARGV[4] and #ARGV[4] > 0 and tonumber(ARGV[4])
      
      local cursor, iteration 
      if pattern and #pattern > 0 then
        cursor, iteration = unpack(redis.call("SCAN", prev_cursor, "MATCH", pattern, "COUNT", scan_batch_size))
      else
        cursor, iteration = unpack(redis.call("SCAN", prev_cursor, "COUNT", scan_batch_size))
      end
      
      local matched = {}
      for _, chankey in pairs(iteration) do
        local match = true
        if min_subscribers then
          match = match and (tonumber(redis.call('HGET', chankey, 'fake_subscribers') or 0) >= min_subscribers)
        end
        if match then
          table.insert(matched, chankey)
        end
      end
      
      return {cursor, matched}
    EOF
    
    results = []
    batch_size=500
    @masters.each do |m|
      hash = m.script "load", script
      cursor, pattern = "0", "{channel:*}"
      loop do
        cursor, batch_results = m.evalsha hash, keys: [], argv: [cursor, pattern, batch_size, filters[:min_subscribers]]
        results += batch_results
        pattern = ""
        break if cursor.to_i == 0
      end
    end
    results
    
    results.map! do |key|
      m = key.match(/^\{channel\:(.*)\}$/)
      m[1] || key
    end
  end
end
