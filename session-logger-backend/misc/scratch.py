"""Scratch Paper"""

from json import dump
from pandas import read_csv

STATION = '46050'
url = f'https://www.ndbc.noaa.gov/data/realtime2/{STATION}.data_spec'

data = read_csv(url, sep=r'\s+')
PATH = 'Session-Logger/session-logger-backend/data/'
file_name = f'{PATH}RAW_spectral_data_{STATION}.json'
# data = download(url, out=file_name)
with open(file_name, 'w', encoding='utf-8-sig') as f:
    dump(data.iloc[1:, :].to_dict(orient='records'), f)
    print('Success: Raw spectral data dump')
