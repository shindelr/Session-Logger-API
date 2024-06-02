"""
Work with wave spectrum data from NDBC.
"""

from wavespectra.input.ndbc import read_ndbc


if __name__ == '__main__':
    path = 'data/RAW_spectral_data_46050.spec'
    dset = read_ndbc(url=path, directional=True)
