# -*- coding: utf-8 -*-
"""
Created on Mon Feb  8 16:21:40 2021

@author: bgregor
"""
from multiprocessing import Process, Pipe
import re
import gzip
from collections import Counter
import time


# This demonstrates the Pipe.  Flags are used to indicate to the 
# worker processes when they should quit.
# A Queue, or even better a JoinableQueue, is a little easier to use.
# Check the docs: https://docs.python.org/3/library/multiprocessing.html


def get_counts(recv_conn, send_conn):
    ''' Return a dictionary of digraphs and their counts from a line.'''
    # Run forever
    while True:
        counter, line = recv_conn.recv()
        if counter == -1:
            # Reader ran out of lines. Close the 
            # pipe.
            send_conn.send( (-1, None) )
            # and quit.
            return
        tmp = line.lower()
        counts = {}
        for m in re.finditer(r'[a-z](?=[a-z])', tmp):
            graph = tmp[m.start():m.end()+1]
            counts[graph] = counts.get(graph, 0) + 1
        # Write the counts to the pipe going back to the main
        # process.
        send_conn.send((0, counts))

def read_files(filename, conn):
        with gzip.open(filename,'r') as f:        
            for i,line in enumerate(f):        
                # Add the line to the pipe. The pipe will hold an OS
                # dependent number of things. If it's full this process
                # will block (i.e. wait) until there's room in the pipe to
                # keep going.
                # If you want control of the size of the stuff in the Pipe,
                # use a Queue instead.
                conn.send((i, line.decode('utf-8')))
        # Done, so send a line number of -1 to indicate
        # to the receiving end of the pipe that it's ok to quit.
        conn.send( (-1,None) )
  
if __name__=='__main__':
    filename = 'data/shakespeare.txt.gz'
    
    # Check the runtime...
    st = time.perf_counter()
    
    # Inter-sub-process pipe
    source_conn, sink_conn = Pipe()
    # pipe to go from get_counts back to the main Python proc
    source2_conn, sink2_conn = Pipe()

    producer = Process(target=read_files, args=(filename, source_conn,))
    consumer = Process(target=get_counts, args=(sink_conn, source2_conn))
    producer.start()
    consumer.start()
    
    # Read the results of get_counts until it's done.
    pipe_counts = Counter()

    while True:
        flag, counts =  sink2_conn.recv()
        if flag == -1:
            break
        pipe_counts.update(counts)
    
    # Close out the processes once they're done.
    producer.join()
    consumer.join()
    producer.close()
    consumer.close()   
    
    # end time
    et = time.perf_counter()
    print('Elapsed time (sec): %s' % (et-st))