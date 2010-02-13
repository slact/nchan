#A dirty "sync" example.
#TODO: 1. Re-Write with twisted for async
#2. Improve on the code.

import urllib2
import simplejson
import random
import time
from threading import Thread
import threadpool
import os
PUBLISHER_URL = 'http://localhost:8080/publish/?channel=232'
SUBSCRIBER_URL = 'http://localhost:8080/activity/?channel=232'
import logging
logging.basicConfig(level = logging.DEBUG)
log = logging.getLogger('test.py')

def publish(i):
    print i
    res = urllib2.urlopen(PUBLISHER_URL, \
                              data = simplejson.dumps(\
            {'message': 'hello world %d' % i}))
    return 

def subscribe(num):
    et = None
    last = None
    while True:
        req = urllib2.Request(SUBSCRIBER_URL, 
                              headers={'If-None-Match':et, \
                                           'If-Modified-Since': last})
        resp = urllib2.urlopen(req)
        et = resp.headers['etag']
        last = resp.headers['last-modified']
        log.info('Subscriber:%d \r\nmsg:: %s\n' % 
                 (num, resp.read()))


if __name__ == '__main__':
    pool = threadpool.ThreadPool(10)
    requests = threadpool.makeRequests(subscribe, range(20))
    [pool.putRequest(request) for request in requests]
    for i in range(100):
        publish(i)
    time.sleep(random.randint(3, 4))       
        
