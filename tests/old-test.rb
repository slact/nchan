require 'rubygems'
require 'em-http'

def subscribe(num, opts)
  puts "Start listener " + num.to_s
  listener = EventMachine::HttpRequest.new('http://127.0.0.1:8082/broadcast/sub?channel='+ opts[:channel]).get :head => opts[:head]
  listener.callback { 
    # print recieved message, re-subscribe to channel with
    # the last-modified header to avoid duplicate messages 
    puts "Listener " + num.to_s + " received: " + listener.response + "\n"
    
    modified = listener.response_header['LAST_MODIFIED']
    subscribe(num, {:channel => opts[:channel], :head => {'If-Modified-Since' => modified}})
  }
end

EventMachine.run {
  channel = "pub"
  numofem = 100
  
  # Publish new message every 5 seconds
  EM.add_periodic_timer(5) do
    time = Time.now
    i = 0
    numofem.times do
       i += 1
       channel = i.to_s

    publisher = EventMachine::HttpRequest.new('http://127.0.0.1:8082/broadcast/pub?channel='+channel).post :body => "Hello @ #{time}"
    publisher.callback {
      #puts "Published message @ #{time}"
      #puts "Response code: " + publisher.response_header.status.to_s
      #puts "Headers: " + publisher.response_header.inspect
      #puts "Body: \n" + publisher.response
      #puts "\n"
    }
    end
  end

  i = 0
  numofem.times do
    i += 1
    subscribe(i, :channel => i.to_s)
  end
}

