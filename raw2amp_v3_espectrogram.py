
import os
import sys
import time
import pandas as pd 
from pathlib import Path
try:
    import vlfcode.process_file as processfile
    import vlfcode.utils as utils
    import vlfcode.fft_pavnet as fft_pavnet
except (ImportError, ModuleNotFound):
    from vlfcode import process_file as processfile
    from vlfcode import utils
    from vlfcode import fft_pavnet

import argparse
import numpy as np
import matplotlib.pyplot as plt

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
    parser.add_argument('-s', '--sampling', default=1, help='donsampling factor', type=int)
    parser.add_argument('-loc', '--location', default='', help='toadd inthe output CSV file location "PIU", "PLO"')
    parser.add_argument('-n', '--fftnpts', default=4096, help='number of point for fft')
    args = parser.parse_args()
    
    source_dir = args.source
    destination_dir = args.output if args.output else source_dir
    key  = args.prefix
    fmt = args.format
    SAMPLING_FACTOR = args.sampling
    loc = args.location
    fft_npts = int(args.fftnpts)
    data_spectrum = []
    data_time = []
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
                xi, xq, n, ct = utils.read_datafile(_file)
                # print(type(fft_npts), type(xi))
                spectrum = fft_pavnet.fft_overlap(xi+1j*xq, fft_npts=fft_npts, sampling_freq=100e3) # fs=100k for v3
                # ct, amps = _process_channels(_file)
                #print(ct, "::", amps)
                #_data["spectrum]
                #amps["DateTime"] = ct
                data_spectrum.append(spectrum)
                data_time.append(ct)
                #data.append({"spectrum":spectrum, "time":ct})
                count += 1
                count_sd += 1
            except Exception as e:
                print(f"    !{e} : file {_file} ")
        te= time.perf_counter()-t0
        print(f"   Completed in {te:.4f} s, {count_sd:.2f} files")

    df = pd.DataFrame(np.array(data_spectrum).T )
    
    
    #print()
    freq_arr = np.arange(fft_npts)*100e3/fft_npts
    # df = df.set_index('time')
    df.columns = data_time
    print(df.shape, len(freq_arr))
    df.index = freq_arr
    
    df = df.sort_index()
    
    output_file = f"{destination_dir}/{loc}_{key}_Sxx_Ds{SAMPLING_FACTOR}_{df.index[0]}-to-{df.index[-1]}.csv"
    df.to_csv(output_file)
    print(f"Data stored in {output_file}")
    print("All done.")


    fig = plt.figure()
    plt.pcolormesh(df.columns, df.index, 20*np.log10(df.values))
    plt.show()