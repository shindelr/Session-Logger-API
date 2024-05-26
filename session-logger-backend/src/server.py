"""
Main logic to run the Session Logger's backend. Routes/handles things like 
pinging NDBC for data, form submission from the frontend, communication to and
from the database, and data processing.
"""

# Third party imports
from threading import Thread
from time import sleep
from os import system
from flask_cors import CORS
from flask import Flask, request, jsonify

# Robin made imports
from errors import CustFlaskException


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
    sleep(1)
    data = request.json
    print(data)
    return jsonify({'message': 'Form received successfully'})


# UTILITIES

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
        path = 'Session-Logger/session-logger-backend/data/'
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
        path = 'Session-Logger/session-logger-backend/data/'
        file_name = f'{path}RAW_spectral_data_{station}.csv'
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
        sleep(3600)  # Once an hour


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
    initialize_buoy_ping_thread(available_stations)

    app.run(debug=False, port=PORT_NUM)
    print(f'Running on port {PORT_NUM}')
