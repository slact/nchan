class Rdsck
  attr_accessor :url, :verbose, :namespace
  attr_accessor :redis, :masters
  
  def dbg(*args)
    if $opt[:verbose]
      print("# ")
      puts(*args)
    end
  end
  
  def initialize(opt)
    @url=opt[:url]
    @verbose=opt[:verbose]
    @namespace=opt[:namespace]
  end
  
  def cluster?
    @cluster_mode
  end
  
  def connect
    begin
      @redis=Redis.new url: $opt[:url]
      mode = redis.info["redis_mode"]
    rescue StandardError => e
      STDERR.puts e.message
      return false
    end

    if mode == "cluster"
      @redis.close
      begin
        @redis=Redis.new cluster: [$opt[:url]]
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
    k = "{channel:#{$opt[:namespace]}/#{$opt[:channel_id]}}"
    return subkey ? "#{k}:#{subkey}" : k
  end
  
  def info
    channel_hash=@redis.hgetall key
    hash_ttl=@redis.ttl key
    channel_subs=@redis.hgetall key("subscribers")
    #...
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
    masters.each do |m|
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
