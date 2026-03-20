# module for baseband operator
# class for processor ojects 
# Diace 2026

import os
from pathlib import Path
import numpy as np
import matplotlib.pyplot as plt
from scipy.signal import butter, filtfilt, detrend
import glob
import os


class baseband:
    # this basenband is initialized by default for v3 processing
    # but it can work with v2 using the  method set_2params
    def __init__(self, Fs, cutoff, npts=int(2**16), order=4, targets={"NAA":24e3}):
        self.Fs = Fs
        self.npts = npts
        #self.N = 
        self.order = order
        self.cutoff = cutoff
        self.coefs =  self.coefs_filter(cutoff, Fs, order)
        self._t = np.arange(self.npts) / self.Fs
        self.Fc = Fs/2
        self.targets = targets
        # to avoid redundant calculations:
        self.expFc = self.calculate_expFc() # sets self.expFc
        self.exp_fTx = self.calculate_expTxs() 

    def set_v2params(self, npts=int(2**13), Fs=int(50e3)):
        self.npts = npts
        self.Fs = Fs
        print("Npts:", npts, ", Fs", self.Fs)
        self.coefs =  self.coefs_filter(self.cutoff, self.Fs, self.order)
        self._t = np.arange(self.npts) / self.Fs
        self.Fc = Fs/2
        # to avoid redundant calculations:
        self.expFc = self.calculate_expFc() # sets self.expFc
        self.exp_fTx = self.calculate_expTxs() 
        
    def calculate_expTxs(self):
        expTx = {}
        for txname in self.targets:
            _f = self.targets[txname]
            expTx[txname] = np.exp(-1j * 2 * np.pi * _f * self._t)
        return expTx
        
    def calculate_expFc(self):
        return np.exp(1j * 2 * np.pi * self.Fc * self._t)
    
    def coefs_filter(self, cutoff=400, Fs=1e5, order=4):
        nyq = 0.5 * self.Fs
        normal_cutoff = self.cutoff / nyq
        return butter(order, normal_cutoff, btype='low')
    
    def lowpass_filter(self, sig):
        return filtfilt(*self.coefs, sig)
    
    def ftx_to_baseband(self, xi, xq, f_target):
        '''
        xi, xq : IQ components
        
        '''
        
        z_bb = xi[:self.npts] + 1j * xq[:self.npts]
        # print(len(z_bb), type(z_bb), self.npts)
        # RECONSTRUCCIÓN RF: z_rf = z_bb * exp(j * 2 * pi * Fc * t)
        z_rf = z_bb * self.expFc
        # real signal
        s_re = np.real(z_rf)
        z_raw = 2 * s_re * np.exp(-1j * 2 * np.pi * f_target * self._t)
            
        z_filtered = self.lowpass_filter(z_raw)
        amp_val = np.mean(np.abs(z_filtered))    
        return amp_val
        
    def multi_ftx_to_bsaeband(self, xi, xq):
        '''
        baseband for multiple targets.
        xi, xq : IQ components
        targets: dict "Tx":freqcuency Hz

        returns: dict of amplitudes
        '''
        amps = {}
        for name in self.targets:
            ftx = targets["name"]
            a = self.ftx_to_baseband(xi, xq, ftx)
            amps[name] = a
        return amps


    
