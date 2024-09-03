"""
Main logic to run the Session Logger's backend. Routes/handles things like 
pinging NDBC for data, form submission from the frontend, communication to and
from the database, and data processing.
"""

# Third party imports
import os
from typing import Union
from threading import Thread
from time import sleep
from os import system
from flask_cors import CORS
from flask import Flask, request, jsonify
import pyodbc
from dotenv import load_dotenv

# Robin made imports
from errors import CustFlaskException
from data_proc import BuoyData

# APP SETUP
app = Flask(__name__)
CORS(app)
PORT_NUM = 5001

# ROUTES

@app.route('/session_form_submission', methods=['POST'])
def session_form_submission():
    """
    Receives the HTTP POST request from the client's session logger form
    submission. 
    :return:
        A success message for successful connectino.
    """
    data = request.json
    print(f'Received the following data:\n{data}')

    # Connect to the database
    cursor, conn = connect_to_db()

    station_query = "select BuoyNum from Location where SpotName = ?"
    try:
        cursor.execute(station_query, data['spot'])
        row = cursor.fetchone()
        station = row.BuoyNum
    except pyodbc.Error as e:
        print(f'Error: {e}')
        return jsonify({'message': 'Error: Unable to connect to the database.'})

    # means = get_sesh_meteor_averages(data['timeIn'], data['timeOut'])
    means = get_sesh_meteor_averages_2(data['date'], data['timeIn'], data['timeOut'], station)

    data.update(means)
    print(f'Full data: {data}')

    # Missing date, username, tideIncoming, and tideMax/Min
    submssion_query_str = """
                        exec session_data @SpotName = ?, @Date = ?, @TimeIn = ?, 
                        @TimeOut = ?, @Rating = ?, @ATemp = ?, @WTemp = ?,
                        @MeanWaveDir = ?, @MeanWaveDirCard = ?, 
                        @MeanWaveHeight = ?, @DomPeriod = ?, @MeanWindDir = ?,
                        @MeanWindDirCard = ? , @MeanWindSpeed = ?, @GustSpeed = ?
                    """

    try:
        # Missing date, username, tideIncoming, and tideMax/Min
        cursor.execute(submssion_query_str,
                    data['spot'], data['date'][:10], data['timeIn'], data['timeOut'],
                    data['rating'], data['ATMP'], data['WTMP'],
                    data['MWD'], data['MWD_CARD'], data['WVHT'],
                    data['DPD'], data['WDIR'], data['WDIR_CARD'],
                    data['WSPD'], data['GST']
                    )
        conn.commit()
    except pyodbc.Error as e:
        print(f'Error: {e}')
        return jsonify({'message': 'Error: Unable to connect to the database.'})

    cursor.close()
    conn.close()

    return jsonify({'message': f'Success: {means}'})

# DB CONNECTION
def connect_to_db():
    """
    Establish a connection to the Azure SQL database.
    :return:
        A tuple containing the cursor and connection objects.
    """
    load_dotenv()
    conn = pyodbc.connect(os.environ['AZURE_CONN_STR'])
    cursor = conn.cursor()
    return cursor, conn

# UTILITIES
def get_sesh_meteor_averages(start: str, end: str) -> dict[str, Union[float, str]]:
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

def get_sesh_meteor_averages_2(sesh_date: str, time_in: str, time_out: str, station: str) -> dict[str, float]:
    """
    Retrieve a set of average meteorlogical values for the session. Includes
    wind direction, speed, gust speed, sig wave height, period, and direction,
    water and air temperature.
    :params:
        time_in -- str representing the start time of the session.
        time_out -- str representing the end time of the session.
        station -- str representing the buoy station number.
    :return:
        A dictionary in the following format:
            {"WDIR": 128.08, ...}
    """
    bd = BuoyData()
    url = f'https://www.ndbc.noaa.gov/data/realtime2/{station}.txt'
    # Slice sesh_date to only include the date
    return bd.get_mean_meteor_vals_2(sesh_date[:10], time_in, time_out, url)


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
        path = r'/Users/robinshindelman/repos/The\ Surf\ App/Session-Logger/session-logger-backend/data/'
        file_name = f'{path}RAW_meteor_data_{station}.csv'
        cmd = f'wget -O {file_name} {url}'
        system(cmd)
        print('Success: Full Meteorlogical data dump')

    except Exception as e:
        raise CustFlaskException('Unable to locate data.', status_code=404) from e


def dump_raw_spec_data(station: str) -> None:
    """
    Retrieve the most recent raw spectral data from NDBC station. This particular
    data set is retrieved from the station by using the .data_spec extension.
    Currently only supports 25 hours worth of data dumped at a time. Uses wget 
    via the OS to download the file and deposit it in CSV format into the 
    specified file.
    :params:
        station -- str representing buoy station number.
    """
    try:
        url = f'https://www.ndbc.noaa.gov/data/realtime2/{station}.data_spec'
        path = r'/Users/robinshindelman/repos/The\ Surf\ App/Session-Logger/session-logger-backend/data/'
        file_name = f'{path}RAW_spectral_data_{station}.spec'
        cmd = f'wget -O {file_name} {url}'
        system(cmd)
        print('Success: Raw spectral data dump')

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


# ERROR REGISTERS
@app.errorhandler(CustFlaskException)
def handle_bad_file(error):
    """A custom Flask error handler used to log an error opening a file."""
    response = jsonify(error.to_dict())  # Might be a problem here
    response.status_code = error.status_code
    return response


if __name__ == '__main__':
    available_stations = ['46050']
    # initialize_buoy_ping_thread(available_stations)

    print(f'Running on port {PORT_NUM}')
    app.run(debug=True, port=PORT_NUM, host="0.0.0.0")
