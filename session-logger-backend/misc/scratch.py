"""Scratch Paper"""
from requests import get, exceptions
import pandas as pd

def get_tides_noaa(station_id: str, time_in: str, time_out: str, sesh_date: str) -> pd.DataFrame:
    """
    Retrieve a set of tide values for the session.
    :params:
        station_id -- str representing the station ID.
        time_in -- str representing the start time of the session.
        time_out -- str representing the end time of the session.
        sesh_date -- str representing the date of the session.
    :return:
        A pandas dataframe. Where the headers are t: time, v: value.
    """
    payload = {
        "station": station_id,
        "begin_date": f"{sesh_date} {time_in}",
        "end_date": f"{sesh_date} {time_out}",
        "product": "water_level",
        "datum": "MLLW",
        "units": "english",
        "time_zone": "lst",
        "interval": "30",
        "format": "json"
    }
    try:
        response = get("https://api.tidesandcurrents.noaa.gov/api/prod/datagetter",
                    params=payload, timeout=5)

        data = response.json()
        df = pd.DataFrame(data['data'])[['t','v']]
        df['t'] = pd.to_datetime(df['t']) # Convert to datetime
        df['v'] = df['v'].astype(float) # Convert to float
        return df

    except exceptions.RequestException as e:
        print(f'Error: {e}')
        return None


def get_tide_sesh_data(sesh_data: dict[str, str | int | float],
                       station_id: str) -> dict[str, float | bool]:
    """
    Retrieve a set of tide values for the session.
    :params:
        sesh_data -- dict containing the session data.
        station_id -- str representing the NOAA station ID.
    :return:
        A dictionary containing the tide values for the session:
            {Incoming:bool, MaxHeight:float, MinHeigth:float, MedianHeight:float}
    """
    date = sesh_data['date'][:10]  # Slice to only include the date, no timestamp
    date = date.replace('-', '')  # NOAA API requires date in the format YYYYMMDD
    time_in = sesh_data['timeIn']
    time_out = sesh_data['timeOut']

    data = get_tides_noaa(station_id, time_in, time_out, date)
    if not data.empty:
        max_height = data['v'].max()
        max_height_time = data['t'].loc[data['v'].idxmax()]
        min_height = data['v'].min()
        min_height_time = data['t'].loc[data['v'].idxmin()]
        median_height = data['v'].median()

        # Nifty way to assign a boolean value depending on the condition
        incoming = max_height_time > min_height_time

        res_dict = {
            "incoming": incoming,
            "max_h": max_height,
            "min_h": min_height,
            "median_h": median_height
        }
        return res_dict
    
    print('Error: Unable to retrieve tide data.')
    return None


d = {
    'spot': 'Otter Rock',
    'timeIn': '12:03',
    'timeOut': '13:31',
    'rating': 3,
    'date': '2024-09-03T21:02:44.064Z'
        }

print(get_tide_sesh_data(d, '9435380'))