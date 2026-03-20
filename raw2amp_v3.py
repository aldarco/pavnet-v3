
import os
import sys
import time
import pandas as pd 
from pathlib import Path
import code.process_file as processfile
# from code.process_file import process_channels_v2, process_channels

if __name__ == "__main__":
    source_dir = sys.argv[1]
    destination_dir = sys.argv[2] if len(sys.argv)>1 else source_dir
    key  = "ANTAR"
    loc = "PIU"
    fmt = ".tar.gz"
    data = []
    count = 0
    downsample_factor = 10 # only each downsample files
    # define the processing function based on the version
    # _process_channels = processfile.process_channels_v2 if "v2" in sys.argv else processfile.process_channels
    print(f"Input: {source_dir}")
    print(f"Output: {destination_dir}")
    if "v2" in sys.argv:
        print(" -> V2")
        fmt = "tar.gz"
        processfile.bb.set_v2params()
        _process_channels = processfile.process_channels_v2
    else:
        print(" -> V3")
        fmt = ".bin"
        _process_channels = processfile.process_channels

    for root, dirs, files in os.walk(source_dir):
        t0 = time.perf_counter()
        print("-> Processing:", root, "...")
        files = list(map(lambda x: os.path.join(root, x), files))
        sorted_files = sorted(files, key=lambda x: os.path.getctime(x))
        for _file in files[::10]:
            if not _file.endswith(fmt): continue
            #filepath = os.path.join(root, _file)
            
            ct, amps = _process_channels(_file)
            #print(ct, "::", amps)
            amps["DateTime"] = ct
            data.append(amps)
            count += 1
        print(f"  Completed in {time.perf_counter()-t0:.4f} s")

    df = pd.DataFrame(data )
    #print()
    df = df.set_index('DateTime')
    df = df.sort_index()
    output_file = f"{destination_dir}/{key}_amplitude_{df.index[0].date()}-to-{df.index[-1].date()}.csv"
    df.to_csv(output_file)
    print(f"Data stored in {output_file}")
    print("All done.")
