#!/usr/bin/python3

# mike czabator
# https://github.com/mikeczabator/memsql_procedure_backup
# last update: 2019-12-04 : added .gzip support

import time
import sys
import os
import argparse
from subprocess import Popen, PIPE
import gzip


#get arguments
parser = argparse.ArgumentParser()
parser.add_argument("-f","--file",type=str,help="filename and path to spool (ex: /tmp/streams_sample.csv)",required=True)
parser.add_argument("-b","--batch_count",default=1000,type=int,help="number of records to include in each batch (ex: 1000)",required=False)
parser.add_argument("-i","--interval",default=1,type=int,help="time interval between each batch (ex: 1)",required=False)
parser.add_argument('--print_file_info', dest='fileinfo', default=False, action='store_true',help="prints file size and time to spool before. Useful for debugging. Bad for spooling to kafka. This is ignored when using a gziped file",required=False)
args = parser.parse_args()

def main():
    compressed_file = check_if_compressed()

    output_string = ""
    count = 0
        
    if args.fileinfo is True and compressed_file is False: 
        file_length = get_file_length()
        print("records in file: "+str(file_length) +"\nseconds to fully spool file:  "+str((file_length / args.batch_count )*args.interval))
    
    if compressed_file is True: # path for gzipped files
        for line in gzip.open(args.file,'rt'):
            output_string += str(line)
            count+=1
            if count % args.batch_count == 0:
                print_out(output_string)
                output_string = ""
                time.sleep(args.interval)
        print_out(output_string) # print remainder of batch when it is less than a full batch size

    if compressed_file is False: # path for non-compressed files
        for line in open(args.file,'r'):
            output_string += str(line)
            count+=1
            if count % args.batch_count == 0:
                print_out(output_string)
                output_string = ""
                time.sleep(args.interval)
        print_out(output_string) # print remainder of batch when it is less than a full batch size
        
def print_out(output_string):
    sys.stdout.write(output_string)

def get_file_length():
    return int(os.popen('wc -l '+args.file+' | awk \'{print $1}\' ' ).readline())

def check_if_compressed():
    if args.file.endswith('.zip'):
        print(".zip files are not currently supported!  please unzip or convert to a gzip file!")
        print("also let the author know if this should support zip files as well")
        quit()

    if args.file.endswith('.gz'):
        return True
    else:
        return False
      

if __name__== "__main__":
  main()
