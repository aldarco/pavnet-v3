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
import json
try:
    from . import utils
    from . import fft_pavnet
    from .import baseband_processor as baseband
except ImportError as e:
    import utils
    import fft_pavnet
    import baseband_processor as baseband

with open(f'{os.path.dirname(__file__)}/config_params.json', 'r') as f:
    config = json.load(f)
    #print(json.dumps(config, indent=4))

dspconfig = config["DSP"]
npts = dspconfig["fft_npts"]
nptsv2 = dspconfig["npts-v2"]
Fs = dspconfig["sampling_frequency"]
deltaf = dspconfig["delta_f"]
Txs = dspconfig["vlf_transmitters"]
backup_path = None #"/data/pavnet/datafiles/"
ramdisk = "/run/user/1000/temprpdata/"
SLEEPT = 2 #seconds
farr = np.arange(npts)*Fs / npts
rxname = "SI_" # config["vlf_transmitters"]["PLO"]
txs_index = {}

# IF demod
bb = baseband.baseband(Fs, deltaf, npts, targets=Txs)

for txname in Txs.keys():
    f_ = Txs[txname]
    idx_inf = np.argmin(abs(farr-(f_ - deltaf)))
    idx_sup = np.argmin(abs(farr-(f_ + deltaf)))
    txs_index[txname] = (idx_inf, idx_sup)

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

def _process_iq(xi, xq):
    xt = xi + 1j*xq
    # print(lenx)
    X = fft_pavnet.fft_overlap(xt, fft_npts=fft_npts, sampling_freq=Fs)
    amps = amp_at_fx_localmax(X, farr, txs_index)
    return amps



def process(FILEPATH):
    # process v3 / BIN FILES
    xi, xq, lenx, ct = utils.read_datafile(FILEPATH) 

    #creationtime = os.path.getctime(FILEPATH) # creationtime based on system 
    #creationtime = os.path.getmtime(FILEPATH) # modificationtime based on system
    # ct = datetime.datetime.fromtimestamp(creationtime) 
    amps = _process_iq(xi, xq)    
    return ct, amps

def process_channels(FILEPATH):
    # v3 datafiles, using RF oscillator to baseband
    xi, xq, lenx, ct = utils.read_datafile(FILEPATH) 

    amps = {}
    for name in Txs.keys():
        ftx = Txs[name]
        _amp = bb.ftx_to_baseband(xi,xq, ftx)
        amps[name] = _amp

    return ct, amps

def process_channels_v2(FILEPATH):
    # v3 datafiles, using RF oscillator to baseband
    # Fs fixed to 50e3
    xi, xq, ct = utils.read_datafilev2(FILEPATH) 
    #bb.npts = npts
    amps = {}
    # for name in Txs.keys():
    #     ftx = Txs[name]
    #     _amp = bb.ftx_to_baseband(xi,xq, f_target=ftx)
    #     amps[name] = _amp
    amps = bb.multi_ftx_to_baseband(xi,xq)
    
    return ct, amps

def processv2(FILEPATH):
    #amp_data = {name:0 for name in Txs.keys()}#pd.DataFrame()
    
    xi, xq, ct = utils.read_datafilev2(FILEPATH) 
    #creationtime = os.path.getctime(FILEPATH) # creationtime based on system 
    #creationtime = os.path.getmtime(FILEPATH) # modificationtime based on system
    # ct = datetime.datetime.fromtimestamp(creationtime) 
    amps = _process_iq(xi, xq) 
    return ct, amps

if __name__ == "__main__":
    try: 
        FILEPATH = sys.argv[1]
        #ct, amp_data = process(FILEPATH)
        ct, amp_data = process_channels(FILEPATH)
        time1 = time.perf_counter()
        print(ct)#, end=" :: ")
        #print(amp_data)
        for k in amp_data.keys():
            #print(k)
            print(f"{k}= {amp_data[k]:.4f}", end="\t")
        
        print(f"_dt={time1-time0:.4f}")
        
        # texec.append(time1-time0)
        # let the system know it worked correctly
        sys.exit(0)
    
    except Exception as e:
        # some error occurred and the file must be moved to
        print(f"Error {e}")
        sys.exit(1)
    
    
    
            
