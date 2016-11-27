import websocket
import thread
import time  




def on_message(ws, message):
    print "got: message %s" % message

def on_error(ws, error):
    print error

def on_close(ws):
    print "### closed ###"

def on_open(ws):
    def run(*args):
        for i in range(10):
            #time.sleep(0.1)
            for y in range(100):                                                                              
                print "send Hello %d %d" % (i, y)
                ws.send("Hello %d %d" % (i, y))
        print "thread terminating..."
    thread.start_new_thread(run, ()) 



if __name__ == "__main__":
    websocket.enableTrace(True)
    url = "ws://localhost:8082/pub/foo"
    #url = "ws://localhost:8082/upstream_pubsub/foo"
    #url = "ws://localhost:7000/"
    ws = websocket.WebSocketApp(url,
                              on_message = on_message,
                              on_error = on_error,
                              on_close = on_close)
    ws.on_open = on_open
    ws.run_forever()
