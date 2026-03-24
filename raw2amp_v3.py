
import os
import sys
import time
import pandas as pd 
from pathlib import Path
import code.process_file as processfile
import code.utils as utils
import argparse
    
# from code.process_file import process_channels_v2, process_channels

def isfileok(filename, key, fmt):
    basename = os.path.basename(filename)
    return basename.endswith(fmt) and basename.startswith(key)


if __name__ == "__main__":
    # grab arguments
    parser = argparse.ArgumentParser(description='Process some inputs.')
    parser.add_argument('-i','--source', help='file to process')
    parser.add_argument('-o', '--output', help='output directory', default="./")
    parser.add_argument('-p', '--prefix', help='prefix in file name [ANTAR, PIU, PLO, ...]')
    parser.add_argument('-v', '--dataversion', help='version of datafiles: [v2, v3]')
    parser.add_argument('-f', '--format', help='format of data files')
    parser.add_argument('-s', '--sampling', default=1, help='donsampling factor')
    parser.add_argument('-loc', '--location', default='', help='toadd inthe output CSV file location "PIU", "PLO"')
    
    args = parser.parse_args()
    
    source_dir = args.source
    destination_dir = args.output if args.output else source_dir
    key  = args.prefix
    fmt = args.format
    SAMPLING_FACTOR = args.sampling
    loc = args.location
    data = []
    count = 0
    # define the processing function based on the version
    # _process_channels = processfile.process_channels_v2 if "v2" in sys.argv else processfile.process_channels
    print(f"Input: {source_dir}")
    print(f"Output: {destination_dir}")
    if "v2" == args.dataversion:
        # print(" -> V2")
        # fmt = "tar.gz"
        processfile.bb.set_v2params()
        _process_channels = processfile.process_channels_v2
    
    elif "v3" == args.dataversion:
        # print(" -> V3")
        fmt = ".bin"
        _process_channels = processfile.process_channels
    else:
        print("Version not foud")
        sys.exit()
    print(args)
    for root, dirs, files in os.walk(source_dir):
        t0 = time.perf_counter()
        print("-> Processing:", root, "...")
        files = list(map(lambda x: os.path.join(root, x), files))
        sorted_files = sorted(files, key=lambda x: os.path.getctime(x))
        count_sd = 0
        for _file in files[::SAMPLING_FACTOR]:
            if not isfileok(_file,key, fmt): continue
            #filepath = os.path.join(root, _file)
            try:
                ct, amps = _process_channels(_file)
                #print(ct, "::", amps)
                amps["DateTime"] = ct
                data.append(amps)
                count += 1
                count_sd += 1
            except Exception as e:
                print(f"    !{e} : file {_file} ")
        te= time.perf_counter()-t0
        print(f"   Completed in {te:.4f} s, {count_sd:.2f} files")

    df = pd.DataFrame(data )
    #print()
    df = df.set_index('DateTime')
    df = df.sort_index()
    output_file = f"{destination_dir}/{loc}_{key}_amplitude_{df.index[0].date()}-to-{df.index[-1].date()}.csv"
    df.to_csv(output_file)
    print(f"Data stored in {output_file}")
    print("All done.")
