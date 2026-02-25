# Processing code for the V3 
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

def amp_at_fx_integral(X, X_f, tx_index):
    amps = {}
    for name in tx_index.keys():
        inf, sup = tx_index[name]
        wX = X[inf : sup+1]
        wf = X_f[inf : sup+1]
        df = wf[1]-wf[0]
        amps[name] = sum(wX * df)/(sum(np.diff(wf)))
    return amps

def amp_at_fx_localmax(X, X_f, tx_index):
    amps = {}
    for name in tx_index.keys():
        inf, sup = tx_index[name]
        wX = X[inf : sup+1]
        # wf = X_f[inf : sup+1]
        # df = wf[1]-wf[0]
        amps[name] = max(wX)
    return amps


with open(f'/home/aldo//dataprocessing2/dataprocessingv2/code/config_params.json', 'r') as f:
    config = json.load(f)
    #print(json.dumps(config, indent=4))
dspconfig = config["DSP"]
fft_npts = dspconfig["fft_npts"]
Fs = dspconfig["sampling_frequency"]
deltaf = dspconfig["delta_f"]
Txs = dspconfig["vlf_transmitters"]
backup_path = None #"/data/pavnet/datafiles/"
ramdisk = "/run/user/1000/temprpdata/"
SLEEPT = 2 #seconds
farr = np.arange(fft_npts)*Fs/fft_npts
rxname = "SI_" # config["vlf_transmitters"]["PLO"]
txs_index = {}
for txname in Txs.keys():
    f_ = Txs[txname]
    idx_inf = np.argmin(abs(farr-(f_ - deltaf)))
    idx_sup = np.argmin(abs(farr-(f_ + deltaf)))
    txs_index[txname] = (idx_inf, idx_sup)


def process(FILEPATH):
    amp_data = {name:[] for name in Txs.keys()}#pd.DataFrame()
    timeindex = []
    texec = []
    # for k, file_ in enumerate(files):
    
    xi, xq, lenx = utils.read_datafile(FILEPATH)
    creationtime = os.path.getctime(FILEPATH)
    xt = xi + 1j*xq
    # print(lenx)
    X = fft_pavnet.fft_overlap(xt, fft_npts=fft_npts, sampling_freq=Fs)
    amps = amp_at_fx_localmax(X, farr, txs_index)
    #amp_data = pd.concat((amp_data, pd.Series(amps)))
    for txname in Txs.keys():
        amp_data[txname].append(amps[txname])
    
    ct = datetime.datetime.fromtimestamp(creationtime)
    
    return ct, amp_data



if __name__ == "__main__":
    try: 
        FILEPATH = sys.argv[1]
        ct, amp_data = process(FILEPATH)
        time1 = time.perf_counter()
        print(ct, end=" :: ")
        for k in amp_data.keys():
            # print(k)
            print(f"{k}= {amp_data[k][0]:.4f}", end="\t")
        
        print(f"_dt={time1-time0:.4f}")
        
        # texec.append(time1-time0)
        # let the system know it worked correctly
        sys.exit(0)
    
    except Exception as e:
        # some error occurred and the file must be moved to
        print(f"Error {e}")
        sys.exit(1)
    
    
    
            