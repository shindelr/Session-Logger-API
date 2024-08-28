"""
Work with wave spectrum data from NDBC.
"""

from wavespectra.input.ndbc import read_ndbc
from xarray import open_dataset

if __name__ == '__main__':
    path = 'Session-Logger/session-logger-backend/data/RAW_spectral_data_46050.csv'
    URL = 'https://www.ndbc.noaa.gov/data/realtime2/46050.data_spec'
    dset = read_ndbc(url=path)
    # dset = open_dataset(filename_or_obj=URL, engine='netcdf4')