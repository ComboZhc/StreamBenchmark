#!/usr/bin/env python
import os
import sys

s = int(sys.argv[1])
e = s + int(sys.argv[2])

def printKafka(filename):
    f = open(filename)
    for line in f:
        xs = line.split('\t')
        try:
            t = int(xs[0].split(' ')[-1])
            if t >= s and t <= e and xs[3].startswith('kafka'):
                print t, xs[3], xs[4]
        except:
            pass
for n in sorted(os.listdir('storm/logs/logs/'), reverse=True):
    printKafka('storm/logs/logs/' + n)
printKafka('storm/logs/metrics.log')