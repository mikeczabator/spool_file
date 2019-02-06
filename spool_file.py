#!/usr/bin/python

import time
import sys
import os
import argparse
from subprocess import Popen, PIPE

#get arguments
parser = argparse.ArgumentParser()
parser.add_argument("-f","--file",default='/tmp/streams_sample.csv',type=str,help="filename and path to spool (ex: /tmp/streams_sample.csv)",required=False)
parser.add_argument("-b","--batch_count",default=1000,type=int,help="number of records to include in each batch (ex: 1000)",required=False)
parser.add_argument("-i","--interval",default=1,type=int,help="time interval between each batch (ex: 1)",required=False)
args = parser.parse_args()

def main():

    # delete_topic()

    output_string = ""
    count = 0
    
    #file_length = get_file_length()
    #print("seconds to fully spool file:  "+str((file_length / args.batch_count )*args.interval))

    for line in open(args.file):
        output_string += str(line)
        count+=1
        if count % args.batch_count == 0:
            sys.stdout.write(output_string)
            output_string = ""
            time.sleep(args.interval)
        

def get_file_length():
    return int(os.popen('wc -l /tmp/streams_sample.csv | awk \'{print $1}\' ' ).readline())

def delete_topic():
    os.system('/tmp/kafka_2.11-2.0.0/bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic media')
    
    


if __name__== "__main__":
  main()
