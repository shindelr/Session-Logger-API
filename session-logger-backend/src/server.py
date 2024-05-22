"""
TODO: Module docstring
"""

from threading import Thread
from json import dump
from time import sleep
from flask_cors import CORS
from flask import Flask, request, jsonify
from pandas import read_csv

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

def dump_buoy_data(station):
    """
    Retrieve the most recent meteorlogical data from NDBC station 46050.
    :params:
        station -- str representing buoy station number.
    """
    url = f'https://www.ndbc.noaa.gov/data/realtime2/{station}.txt'

    try:
        data = read_csv(url, sep=r'\s+')
        file_name = f'./RAW_meteor_data_{station}.json'
        with open(file_name, 'w', encoding='utf-8-sig') as f:
            dump(data.iloc[1:, :].to_dict(orient='records'), f)
        print('Success: Meteorlogical data dump')

    except Exception as e:
        raise CustFlaskException('Unable to locate data.', status_code=404) from e


def ping_station(station):
    """
    Ping the desired station once an hour to stay updated.
    :params:
        station -- str representing buoy station number.
    """
    while True:
        print('Fetching meteorlogical data now.')
        dump_buoy_data(station)
        sleep(3600)


def initialize_meteorlogical_thread(station_list):
    """
    Initialize the daemonized threads which will periodically ping the NDBC buoy
    stations for their data.
    :params:
        station_list -- List[str], list of strings representing buoy numbers.
    """

    # !!!!! THIS COULD BE A PROBLEM AREA !!!! #
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
    """TODO: Docstring"""
    response = jsonify(error.to_dict())  # Might be a problem here
    response.status_code = error.status_code
    return response


if __name__ == '__main__':
    available_stations = ['46050']
    initialize_meteorlogical_thread(available_stations)

    app.run(debug=True, port=PORT_NUM)
    print(f'Running on port {PORT_NUM}')
