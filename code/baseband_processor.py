# module for baseband operator


import os
from pathlib import Path
import numpy as np
import matplotlib.pyplot as plt
from scipy.signal import butter, filtfilt, detrend
import glob
import os


class baseband:
    def __init__(self, Fs, cutoff, npts=int(2**16), order=4):
        self.Fs = Fs
        self.npts = npts
        #self.N = 
        self.order = order
        self.cutoff = cutoff
        self.coefs =  self.coefs_filter(cutoff, Fs, order)
        self._t = np.arange(self.npts) / self.Fs
        self.Fc = Fs/2

    def coefs_filter(self, cutoff=400, Fs=1e5, order=4):
        nyq = 0.5 * self.Fs
        normal_cutoff = self.cutoff / nyq
        return butter(order, normal_cutoff, btype='low')
    
    def lowpass_filter(self, sig):
        return filtfilt(*self.coefs, sig)
    
    def ftx_to_baseband(self, xi, xq, f_target):
        t = self._t
        z_bb = xi[:self.npts] + 1j * xq[self.npts]
        # RECONSTRUCCIÓN RF: z_rf = z_bb * exp(j * 2 * pi * Fc * t)
        z_rf = z_bb * np.exp(1j * 2 * np.pi * self.Fc * t)
        # real signal
        s_re = np.real(z_rf)
        z_raw = 2 * s_re * np.exp(-1j * 2 * np.pi * f_target * t)
            
        z_filtered = self.lowpass_filter(z_raw)
        amp_val = np.mean(np.abs(z_filtered))    
    
        return amp_val
    
    # def __del__(self):


    
