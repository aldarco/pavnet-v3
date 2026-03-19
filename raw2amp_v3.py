
import os
import sys
import time
import pandas as pd 
from pathlib import Path
from code.process_file import process_channels_v2

if __name__ == "__main__":
    source_dir = sys.argv[1]
    destination_dir = sys.argv[2] if len(sys.argv)>1 else source_dir
    key  = "ANTAR"
    loc = "PIU"
    fmt = ".tar.gz"
    data = []
    count = 0
    for root, dirs, files in os.walk(source_dir):
        for _file in files:
            if not _file.endswith(fmt): continue
            filepath = os.path.join(root, _file)

            ct, amps = process_channels_v2(filepath)
            print(ct, "::", amps)
            amps["DateTime"] = ct
            data.append(amps)
            count += 1

    df = pd.DataFrame(data, )
    df = df.set_index('DateTime')
    df = df.sort_index()
    df.to_csv(f"{key}_amplitude_{df.index[0]}-to-{df.index[0]}.csv")
