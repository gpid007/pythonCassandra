#!/usr/bin/python

# Import libraries
import pandas as pd
import os
from multiprocessing import Process, Queue
import urllib2

# Global variables
SEC = 2 # wait time
FILE_PATH = '/home/greg/dns-rec.csv'
PROC_NUM = 30

# Read file
os.system("echo 'dns,response' > result") # create result file
df = pd.read_csv(
    FILE_PATH,
    delimiter=';',
    error_bad_lines=False
)

# Atomize dataframe by primary key
qIN = Queue()
for item in df.dns.tolist():
    qIN.put(item)

# Process function
def procFun(qIN):
    while not qIN.empty(): # terminate when Q is empty
        print int(qIN.qsize())
        item = qIN.get()
        try:
            response = urllib2.urlopen("http://" + item, None, SEC)
            isHttps = response.url.startswith("https")
            os.system("echo {},{} >> result".format(item, isHttps)) # Append to file
        except Exception as err:
            os.system("echo {},{} >> result".format(item, err))     # Append to file

# Process list and output queue
procList = []
for x in xrange(PROC_NUM):
    procList.append(
        Process(
            name = 'Process {}'.format(x),
            target = procFun,
            args = (qIN, ) # <-- Important comma (x ,)
        ).start()
    )
