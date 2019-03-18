import zarr
import numpy as np
import os

class ZarrTable:
    
    def __init__(self, store, data = None, store_type='sqlite'):
        
