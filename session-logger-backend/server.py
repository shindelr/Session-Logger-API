from flask import Flask, request, jsonify
from flask_cors import CORS
from time import sleep
from pandas import read_csv
from json import dump
from threading import Thread


# APP SETUP
app = Flask(__name__)
CORS(app)
port_num = 5001


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

def dump_meteorlogical_NBDC_data(station):
    """
    Retrieve the most recent meteorlogical data from NDBC station 46050.
    :params:
        station -- str representing buoy station number.
    """
    url = 'https://www.ndbc.noaa.gov/data/realtime2/{}.txt'.format(station)

    try:
        data = read_csv(url, sep='\s+')
        file_name = './RAW_meteor_data_{}.json'.format(station)
        with open(file_name, 'w') as f:
            dump(data.iloc[1:, :].to_dict(orient='records'), f)
        print('Success: Meteorlogical data dump')

    except Exception as e:
        print('Failure: Meteorlogical data, ' + str(e))


def ping_station(station):
    """
    Ping the desired station once an hour to stay updated.
    :params:
        station -- str representing buoy station number.
    """
    while True:
        print('Fetching meteorlogical data now.')
        dump_meteorlogical_NBDC_data(station)
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


if __name__ == '__main__': 
    available_stations = ['46050']
    initialize_meteorlogical_thread(available_stations)

    app.run(debug=True, port=port_num)
    print(f'Running on port {port_num}')
