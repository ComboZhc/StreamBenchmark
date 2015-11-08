#!/usr/bin/env python
import requests
import time
import threading
import sys

url = sys.argv[1]

freq = 5.0
lastAcked = None
lastLat = None

def run():
    threading.Timer(freq, run).start()
    global lastAcked
    global lastLat
    try:
        print "====="
        print time.time()
        j = requests.get(url).json()
        acked = int(j['topologyStats'][-1]['acked'])
        lat = float(j['topologyStats'][-1]['completeLatency'])
        if lastAcked is not None:
            if acked - lastAcked > 0:
                th = (acked - lastAcked) / freq
                l = (acked * lat - lastAcked * lastLat) / (acked - lastAcked)
                print "acked", acked
                print "throughput", th
                print "avglat", lat
                print "lat", l
        lastAcked = acked
        lastLat = lat
        sys.stdout.flush()
    except:
        pass
run()