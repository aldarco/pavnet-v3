import numpy as np
import scipy.signal as signal


import os
from pathlib import Path
import numpy as np
import matplotlib.pyplot as plt
from scipy.signal import butter, filtfilt, detrend
import glob
import os


# --- only the rf


def apply_lowpass(sig, cutoff=300, Fs=100000, order=4):
    nyq = 0.5 * Fs
    normal_cutoff = cutoff / nyq
    b, a = butter(order, normal_cutoff, btype='low')
    return filtfilt(b, a, sig)
    
def ftx_to_baseband(xi,xq, npts, Fs, f_target):
    N = npts 
    t = np.arange(N) / Fs
    
    z_bb = xi + 1j * xq
    
    # RECONSTRUCCIÓN RF: z_rf = z_bb * exp(j * 2 * pi * Fc * t)
    z_rf = z_bb * np.exp(1j * 2 * np.pi * Fc * t)
    # real signal
    s_re = np.real(z_rf)
    
    z_raw = 2 * s_re * np.exp(-1j * 2 * np.pi * f_target * t)
        
    z_filtered = apply_lowpass(z_raw, cutoff=400, Fs=Fs)
    amp_val = np.mean(np.abs(z_filtered))    
  
    return amp_val


    
