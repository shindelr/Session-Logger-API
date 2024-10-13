"""
Main logic to run the Session Logger's backend. Routes/handles things like 
pinging NDBC for data, form submission from the frontend, communication to and
from the database, and data processing.
"""

# Third party imports
from os import system
from flask_cors import CORS
from flask import Flask, request, jsonify
import pyodbc
# from dotenv import load_dotenv
import pandas as pd

# Robin made imports
from errors import CustFlaskException
from data_proc import BuoyData
from logger_db import LoggerDB

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
    # Spin up a database object
    db = LoggerDB()

    data = request.json
    print(f'Received the following data:\n{data}')
    data['date'] = str(pd.Timestamp(data['date']).tz_convert('US/Pacific'))

    # Connect to the database
    try:
        cursor, conn = db.connect_to_db()
    except pyodbc.OperationalError:
        return jsonify({'message': 'Error:\nConnection timeout, try again in 30 seconds'}), 500

    try:
        meteor_station_id = db.get_meteor_station(data['spot'], cursor)
        tide_station_id = db.get_tide_station(data['spot'], cursor)
    except pyodbc.Error as e:
        return jsonify({'message': f'Error: {e}'}), 500

    # Get met & tide date from NOAA/NDBC
    meteor_data = get_sesh_meteor_averages_2(data['date'], data['timeIn'],
                                    data['timeOut'], meteor_station_id)
    tide_data = get_tide_data(data, tide_station_id)
    data.update(meteor_data)
    data.update(tide_data)

    # Insert to db
    try:
        print(f'Full data: {data}')
        db.insert_session_info(data, cursor, conn)
    except pyodbc.Error as e:
        return jsonify({'message': f'Error: {e}'}), 500

    cursor.close()
    conn.close()

    return jsonify({'message': f'Successfully posted: {data}'})


# UTILITIES
def get_sesh_meteor_averages_2(sesh_date: str, time_in: str,
                               time_out: str, station: str) -> dict[str, float]:
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


def get_tide_data(sesh_data: dict[str, str | int ],
                  tide_station_id: str) -> dict[str, bool | float]:
    """
    TODO: Write the docstring
    """
    bd = BuoyData()
    return bd.get_tide_sesh_data(sesh_data, tide_station_id)


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


# ERROR REGISTERS
@app.errorhandler(CustFlaskException)
def handle_bad_file(error):
    """A custom Flask error handler used to log an error opening a file."""
    response = jsonify(error.to_dict())  # Might be a problem here
    response.status_code = error.status_code
    return response


if __name__ == '__main__':
    print(f'Running on port {PORT_NUM}')
    app.run(debug=True, port=PORT_NUM, host="0.0.0.0")
