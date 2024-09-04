"""Scratch Paper"""
from requests import get
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
        A pandas dataframe.
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
    response = get("https://api.tidesandcurrents.noaa.gov/api/prod/datagetter",
                   params=payload, timeout=5)
    data = response.json()
    return pd.DataFrame(data['data'])[['t','v']]


print(get_tides_noaa("9435380", "13:30", "15:00", "20240903"))