# Processing code for the V3 using a watchdog
# this code is called by the receiver code
# after the extraction of the amplitudes these vales are sent to the DB

import time
time0 = time.perf_counter() # HOW MUCH TIME DOES THIS WHOLE CODE TAKES?
import sys
import os
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import datetime
sys.path.append("/home/aldo//dataprocessing2/dataprocessingv2/code/")

import utils
import json
import fft_pavnet
import process_file
from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer




# Folders 
arrival_dir = sys.argv[1]
failed_dir = os.path.join(arrival_dir, failed)
processed_dir = os.path.join(arrival_dir, processed)

           
class FileHandler(FileSystemEventHandler):
    def __init__(self, arrival_dir):
        '''
        Arrival dir is the path where datafiles arrive from the RedPitaya.
        Inside this Arrival dir, the processed and failed dir are created.
        '''
        self.arrival_dir = arrival_dir
        self.failed_dir = os.path.join(arrival_dir, "failed")
        self.processed_dir = os.path.join(arrival_dir, "processed")
        self.processing_ok = None
        os.makedirs(self.failed_dir, exist_ok=True)
        os.makedirs(self.processed_dir, exist_ok=True)

        
    def process_and_move(self, path):
        '''
        process the file event/path
        processing params are taken from the config file inside the process_file.py and the process function.
        '''
        if not path.is_directory and path.dest_path.endswith(".bin"):
            try:
                ct, amp_data = process_file.process(path)
                # TODO send to DB
                # ...

                # show values
                for k in amp_data.keys():
                    print(f"{k}= {amp_data[k][0]:.4f} _dt={time1-time0:.4f}", end="\t")
                print("_dt={time1-time0:.4f}")
                # sys.exit(0) # flag to move the file to the processed folder
                self.processing_ok = True
            except Exception as e:
                print(f"Error: {e}")
                self.processing_ok = False
                # sys.exit(1) # flag to move file to the failed folder
                return None
            try:
                if self.processing_ok: 
                    shutil.move(processed_dir)
                    return ct, amp_data
                else: 
                    shutil.move(failed_dir)
            except Exception as e:
                print(f"Failed Moving: {e}")
                return None

    def on_moved(seld, event):
        self.process_and_move(event.src_path)
        
    def on_created(self, event):
        self.process_and_move(event.dest_path)

    def on_closed(self, event):
        if not event.is_directory and ps.path.exists(event.src_path):
            self.process_and_move(event.src_path)

if __name__ == "__main__":
    # check if arguments are completed
    if len(sys.argv) != 2:
        print("argument path (dir to watch) missing")
        sys.exit(1)

    path_to_watch = sys.argv[1]
    event_handler = FileHandler(path_to_watch)
    _obs = Observer()

    # observer recursive=False to avoid subdirectories
    observer.schedule(event_handler, path_to_watch, recursive=False)
    obsever.start()
    try:
        while True:
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("Ftopping watcher/receiver")
        observer.stop()
    observer.join()
