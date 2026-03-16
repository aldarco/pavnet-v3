import shutil
import os 
import sys
import datetime as dt
from pathlib import Path
import utils
import time
# Script that sortfiles into folders according to their creation date





def sort_files(source_dir, destination_dir=None, 
                dt_from_fname=utils.grab_nominal_datetime, 
                fmt=".bin"
                ):
    '''
    takes all the files inside a source_dir path and sorts it by month day:
    YEAR:
        └ MM (Month):
           └ DD (Day):
    
    source_dir      : directory/forder that contains all the files
    destination_dir : where it is going to be placed
    dt_from_fname   : function to extract the acquisiton time from datafile name. 
                        Default utils.grab_nominal_datetime (v3 .bin files)
    fmt             : datafile format
    '''
    path_data = source_dir
    path_dest = destination_dir if destination_dir else source_dir
    fnames = [f for f in os.listdir(path_data) if f.endswith(fmt)]
    #files = [os.path.join(path_data, f) for f in os.listdir(path_data)]
    # files stored as dict, keys are the datetime in the name
    print()
    files = {tf : os.path.join(path_data, fname) for tf, fname in zip(list(map(dt_from_fname, fnames)), fnames) }
    # sort by time
    files = {key: files[key] for key in sorted(files.keys())}
    
    print("files sorted", len(files))
    #dates = [dt.datetime.fromtimestamp(ts).date() for ts in [os.path.getmtime(f) for f in files]]
    dates = [x.date() for x in files.keys()]
    print(dates[:3])
    unique_dates = sorted(set(dates))
    #unique_yymm = sorted(set([(ud.year, ud.month) for ud in unique_dates]))
    
    # index to define the ranges of files
    term_index = {} #str(ud): None for ud in unique_dates}
    for ud in unique_dates:
        _dir = "/".join([path_data,*[f"{ud.year}", f"{ud.month:02d}", f"{ud.day:02d}"]])
        Path(_dir).mkdir(parents=True, exist_ok=True)
        print(f"created {_dir}")
        term_index[str(ud)] = max( ii for ii,dd in enumerate(dates) if str(dd)==str(ud))

    #print("index ready")
    id0=0
    time0 = time.perf_counter()
    total_moved_files = 0
    for ii in range(len(term_index)):
        idf = term_index[str(unique_dates[ii])]
        idf = 1 if idf==0 else idf
        #print(f"[{id0}]", end=" ")

        files_ = [files[_t] for _t in list(files.keys())[id0 :idf]]
        #print(unique_dates[ii])
        
        destdir = Path(path_dest, 
                        "{:04d}/{:02d}/{:02d}".format(
                                                    unique_dates[ii].year, 
                                                    unique_dates[ii].month, 
                                                    unique_dates[ii].day
                                                    )
                                                    )
        print(destdir, files_[0])
        files_dest_ = [os.path.join(destdir, f.split("/")[-1]) for f in files_]
        print("moving to {}".format(unique_dates[ii]) )
        print("Ex: " + str(files_[0]) + " to " + str(files_dest_[0]))
        
        moved_files = 0
        for f, fout in zip(files_, files_dest_): 
            try: 
                shutil.move(f, fout)
                moved_files += 1
            except PermissionError as e:
                print(f" {e}--> file {f} not moved... skipped.")
        id0 = idf
        total_moved_files += moved_files

    timef = time.perf_counter()
    time_consumed = timef-time0
    print(f"{total_moved_files} files moved in {time_consumed:.4f} seconds")
    print(f"    > rate: {total_moved_files}/{time_consumed} files/second")


if __name__ == "__main__":
    path_data = sys.argv[1]
    path_dest = sys.argv[2]
    file_version = sys.argv[3] if len(sys.argv) >3 else "v3"

    #if file_version not in ["v2,v3"]: raise ("worng version. Version must be v2 or v3.")

    if file_version == "v3":
        dt_from_fname = utils.grab_nominal_datetime
    elif file_version == "v2":
        dt_from_fname = utils.grab_nominal_datetime_v2file
    else: 
        raise ("worng version. Version must be v2 or v3.")
        sys.exit(0)
    
    sort_files(path_data, path_dest, )
