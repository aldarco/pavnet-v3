# Processing code for the V3 using a watchdog
# this code is called by the receiver code
# after the extraction of the amplitudes these vales are sent to the DB

import sys
import os
import time
import shutil
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import json
import datetime
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

#sys.path.append("/home/aldo/dataprocessing2/dataprocessingv2/code/")
from . import utils
from . import process_file
from .sendtodb import DBClient

class FileHandler(FileSystemEventHandler):
    '''
    Arrival dir is the path where datafiles arrive from the RedPitaya.
    Inside this Arrival dir, the processed and failed dir are created.
    '''
    def __init__(self, arrival_dir, key, localdbname="localdbname.db"):
        # arrival_dir: path where datafiles arrive from RP
        # subfolders processed and failed are created inside this path
        self.arrival_dir = arrival_dir
        self.key = key
        self.failed_dir = os.path.join(arrival_dir, "failed")
        self.processed_dir = os.path.join(arrival_dir, "processed")
        self.db_client = DBClient(key, localdbname) # integrates remote and local (for unloaded data)

        os.makedirs(self.failed_dir, exist_ok=True)
        os.makedirs(self.processed_dir, exist_ok=True)

    def process_and_move(self, filepath):
        '''
        Process the datafile arrived using the processing parameters from config file.
        returns: creation time (ct) and dict of amplitudes {"Tx1":amp1, ...}
        '''
        ct = None
        amp_data = {}
        if not filepath.endswith('.bin') and not filepath.startswith(self.key):
            return ct, amp_data

        print(f"Processing {os.path.basename(filepath)}")
        time0 = time.perf_counter()

        try:
            #ct, amp_data = process_file.process(filepath) # v3 format: .bin
            ct, amp_data = process_file.process_channels(filepath) # improved version
            time1 = time.perf_counter()
            # print(ct)
            # print(amp_data)
            for k, v in amp_data.items():
                print(f"{k}={v:.4f}", end="\t")
            print(f"_dt={time1-time0:.4f}")
            processing_ok = True
        except Exception as e:
            print(f"Error processing {filepath}: {e}")
            processing_ok = False

        # Move file based on outcome
        try:
            if processing_ok:
                dest = self.processed_dir
            else:
                dest = self.failed_dir
            shutil.move(filepath, os.path.join(dest, os.path.basename(filepath)))
        except Exception as e:
            print(f"Failed to move {filepath}: {e} - Destination: {dest}")
        
        # send to DB
        self.db_client.send_to_db(ct, amp_data)

        return ct, amp_data

    def on_moved(self, event):
        if not event.is_directory:
            self.process_and_move(event.dest_path)

    def on_created(self, event):
        if not event.is_directory:
            self.process_and_move(event.src_path)

    def on_closed(self, event):
        if not event.is_directory and os.path.exists(event.src_path):
            self.process_and_move(event.src_path)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python watcher.py <directory_to_watch> <KEY>")
        print("       - directory_to_watch: /arrival/path/")
        print("       - KEY:  PLO, PIU, TEST, etc...")
        sys.exit(1)

    path_to_watch = sys.argv[1]
    key           = sys.argv[2]
    
    localdbname = f"localdb_{key}.db"
    event_handler = FileHandler(path_to_watch, key, localdbname=localdbname)
    observer = Observer()
    observer.schedule(event_handler, path_to_watch, recursive=False)

    # Before the observer starts, lets grab the existing files in the arrival directory
    files = [os.path.join(path_to_watch, f) for f in os.listdir(path_to_watch) if (key in f and f.endswith(".bin"))]
    # start the observer, it runs as a thread (kinda paralel with the pending processing)
    observer.start()
    # process the existing, so we can avoid the race condition
    for f in files:
        event_handler.process_and_move(f )

    print(f"Watching {path_to_watch}...")
    # just run til keyboard interrupt
    # this keeps observer running alongside this main
    try:
        while True:
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("KeyboardInterrupt: Stopping watcher")
        observer.stop()
    observer.join()
