"""
Deprecated functions from the server.py file.
"""

def get_sesh_meteor_averages(start: str, end: str) -> dict[str, (float | str)]:
    """
    Retrieve a set of average meteorlogical values for the session. Includes
    wind direction, speed, gust speed, sig wave height, period, and direction,
    water and air temperature.
    #### Parameters:
    ----------------
    - start: `str`
        Start time for the session.
    - end: `str`
        End time for the session.
    #### Returns:
    -------------
    A dictionary in the following format:
        {"WDIR": 128.08, ...}
    """
    bd = BuoyData()
    return bd.get_mean_meteor_vals(bd.curr_df, start, end)


def initialize_buoy_ping_thread(station_list: list[str]) -> None:
    """
    Initialize the daemonized threads which will periodically ping the NDBC buoy
    stations for their data.
    :params:
        station_list -- List[str], list of strings representing buoy numbers.
    """

    # !!!!! THIS COULD BE A PROBLEM AREA !!!!
    # Will need to figure out how to handle the data dump from multiple stations.
    # Maybe a new file for each available station?

    # !!!!!!!! HANDLE CONCURRENT DATA ACCESS IN THE CSV FILES !!!!!!!!!!!

    for station in station_list:
        # Create the thread targeting the ping_station function
        station_thread = Thread(target=ping_station, args=(station, ))  # args expects tuple
        # Designate it a daemon
        station_thread.daemon = True
        # Initialize the daemonized thread.
        station_thread.start()


def dump_full_meteor_data(station: str) -> None:
    """
    Retrieve the most recent meteorlogical data from NDBC station. Full
    on meteorlogical data is retrieved from the station by using the .txt 
    extension. Currently only supports 25 hours worth of data dumped at a time.
    Uses wget via the OS to download the file and deposit it in CSV format into
    the specified file.
    :params:
        station -- str representing buoy station number.
    """

    try:
        url = f'https://www.ndbc.noaa.gov/data/realtime2/{station}.txt'
        path = r'~/Users/robinshindelman/repos/The\ Surf\ App/Session-Logger/session-logger-backend/data/'
        file_name = f'{path}RAW_meteor_data_{station}.csv'
        cmd = f'wget -O {file_name} {url}'
        system(cmd)
        print('Success: Full Meteorlogical data dump')

    except Exception as e:
        raise CustFlaskException('Unable to locate data.', status_code=404) from e


def ping_station(station: str) -> None:
    """
    Ping the desired station once an hour to stay updated.
    :params:
        station -- str representing buoy station number.
    """
    while True:
        print('Fetching meteorlogical data now.')
        dump_full_meteor_data(station)
        print('Fetching raw spectral data now.')
        dump_raw_spec_data(station)
        sleep(1200)  # Every twenty minutes


