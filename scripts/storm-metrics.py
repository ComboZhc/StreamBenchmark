import requests
from sys import argv
import time
import threading

url = argv[1]

freq = 10.0
lastAcked = None
lastLat = None

def run():
    threading.Timer(freq, run).start()
    global lastAcked
    global lastLat
    print "====="
    print time.time()
    j = requests.get(url).json()
    acked = int(j['topologyStats'][-1]['acked'])
    lat = float(j['topologyStats'][-1]['completeLatency'])
    if lastAcked is not None:
        th = (acked - lastAcked) / freq
        l = (acked * lat - lastAcked * lastLat) / (acked - lastAcked)
        print "acked", acked
        print "throughput", th
        print "avglat", lat
        print "lat", l
    lastAcked = acked
    lastLat = lat

run()