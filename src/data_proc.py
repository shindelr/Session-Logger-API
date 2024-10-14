"""
This module handles data cleaning and processing before being handed off to the
database. It handles swell discovery from raw spectra data, conversion of units
from metric or degree to standard or cardinal, etc..
"""
from subprocess import run
from datetime import date, datetime
# from pint import UnitRegistry, Quantity
import pint
from pandas import read_csv, errors, DataFrame, Timestamp, to_datetime, to_numeric
from requests import get, exceptions

class UnitConverter:
    """
    Unit conversion utility. Use to convert various heights, speeds, 
    directions.
    """

    def __init__(self) -> None:
        # Pint library unit converter
        self.ureg = pint.UnitRegistry()
        # Separate Temperature Unit Registry
        self.ureg_temp = pint.UnitRegistry()
        # self.ureg_temp.default_format = '.3f'
        self.ureg_temp.formatter.default_format = '.3f'


    def find_cardinal_direction(self, value: int) -> str:
        """
        Convert direction in degrees to its corresponding cardinal direction.
        Works by dividing the given value by 45 (since this func uses 8 
        cardinal directions) and then finding the corresponding index in an
        array containing all the cardinal directions.
        :param:
            - value: int currently representing direction in degrees. \n
        :return:
            A string value representing the direction in cardinal form.
        """
        cardinals = ['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW']
        return cardinals[((value % 360) // 45) % 8]


    def meters_to_feet(self, value: float) -> float:
        """
        Conver meters to feet.
        :param:
            - value: float currenty in meters \n
        :return:
            - Float representing the value in feet.
        """
        meters = value * self.ureg.meter
        feet = meters.to(self.ureg.foot)
        return round(feet.magnitude, 1)


    def feet_to_meters(self, value: float) -> float:
        """
        Convert feet to meters.
        :param:
            - value: float currenty in feet \n
        :return:
            - Float representing the value in meters.
        """
        feet = value * self.ureg.foot
        meters = feet.to(self.ureg.meter)
        return round(meters.magnitude, 1)


    def meters_per_sec_to_mph(self, value: float) -> float:
        """
        Convert m/s to mph.
        :param:
            - value: float currently in m/s \n
        :return:
            - Float representing the value converted to mph.
        """
        # value * m/s
        m_per_sec = value * (self.ureg.meter / self.ureg.second)
        mph = m_per_sec.to(self.ureg.mile / self.ureg.hour)
        return round(mph.magnitude, 1)


    # Quantity class isn't letting me use type hints
    def celsius_to_fahrenheit(self, value):
        """
        Convert Celsius values to Fahrenheit.
        :param:
            - value: a float currently in degrees Celsius \n
        :return:
            - Quantity object representing the value converted to F.
        """
        # Convert value to a Pint Quantity object first
        celsius = pint.Quantity(value, self.ureg_temp.degC)
        fahrenheit = celsius.to(self.ureg_temp.degF)
        return round(fahrenheit.magnitude, 1)


    def fahrenheit_to_celsius(self, value):
        """
        Convert Fahrenheit values to Celsius.
        :param:
            - value: a float currently in degrees Fahrenheit \n
        :return:
            - Quantity object representing the value converted to Celsius.
        """
        # Convert value to a Pint Quantity object first
        fahrenheit = pint.Quantity(value, self.ureg_temp.degF)
        celsius = fahrenheit.to(self.ureg_temp.degC)
        return round(celsius.magnitude, 1)


class InvalidTimeframeException(Exception):
    """Invalid time frame entered."""


class BuoyData:
    """
    Buoy data utility. Do things like excise useless data columns, convert
    units, etc.
    """
    def __init__(self) -> None:
        self.unit_conv = UnitConverter()
        # self.stonewall_bank_csv = 'Session-Logger/session-logger-backend/data/RAW_meteor_data_46050.csv'
        # self.curr_df = self.build_da_frame(self.stonewall_bank_csv)  # Deprecated

    def build_da_frame(self, file_path: str) -> DataFrame:
        """
        Read a CSV file into a Pandas dataframe. Intended to use as the initial
        reading in of raw data from the buoys.
        :params:
        - file_path: A string denoting the file path containing the csv.
        :returns:
        - A pandas dataframe object.
        """
        try:
            data = read_csv(file_path, sep=r'\s+')
        except errors.EmptyDataError as ede:
            print(f'Exception ocurred: {ede}')
        except errors.ParserError as pe:
            print(f'Exception ocurred: {pe}')
        return data


    def build_da_frame_2(self, sesh_date: str, time_in: str, time_out: str, url: str) -> DataFrame:
        """
        Read NDBC text file into a Pandas dataframe. Intended to use as the initial
        reading in of raw data from the buoys.
        :params:
        - sesh_date: A string denoting the date of the session.
        - time_in: A string denoting the start time of the session.
        - time_out: A string denoting the end time of the session.
        - url: A string denoting the URL from which to fetch the data.
        :returns:
        - A pandas dataframe object.
        """
        cmd = self.build_command(sesh_date, time_in, time_out, url)
        print(cmd)
        # Run the command and capture the output, decode it to a string
        res = run(cmd, shell=True, capture_output=True, check=True)
        out = res.stdout.decode('utf-8')
        err = res.stderr.decode('utf-8')
        print(f"out: {out}")
        if err:
            print(f"err: {err}")

        # Split the output into lines on the newline
        raw_lines = out.split('\n')

        # Remove empty lines, clean off any whitespace, and split into columns
        clean_lines = [line.split() for line in raw_lines if line.strip()]

        cols = ['#YY', 'MM', 'DD', 'hh', 'mm', 'WDIR', 'WSPD', 'GST', 'WVHT', 'DPD',
                'APD', 'MWD', 'PRES', 'ATMP', 'WTMP', 'DEWP', 'VIS', 'PTDY', 'TIDE']

        return DataFrame(clean_lines, columns=cols)


    def parse_time_and_date(self, sesh_date: str, time_in: str, time_out: str) -> tuple[str]:
        """
        Parse the date and time strings into the appropriate format for the command
        string. Hours are shifted by 7 from PST to UTC, which is the universal time
        zone used by the NDBC.

        Parameters:
        -----------
        date: str
            The date in the format 'YYYY-MM-DD'.
        time_in: str
            The start time in the format 'HH:MM'.
        time_out: str
            The end time in the format 'HH:MM'.

        Returns:
        --------
        tuple[str]: The parsed date and time strings.
        """

        # Build timestamps
        stamp_in = Timestamp(f'{sesh_date} {time_in}')
        stamp_out = Timestamp(f'{sesh_date} {time_out}')
        print(f'stamp_in: {stamp_in}')
        print(f'stamp_out: {stamp_out}')

        # Localize in PST then convert to UTC
        pst_in = to_datetime(stamp_in).tz_localize('US/Pacific')
        pst_out = to_datetime(stamp_out).tz_localize('US/Pacific')
        utc_in = pst_in.tz_convert('UTC')
        utc_out = pst_out.tz_convert('UTC')

        # Format strings to be able to filter NDBC data
        hr_in = f'{utc_in.hour:02d}'
        min_in = f'{utc_in.minute:02d}'
        hr_out = f'{utc_out.hour:02d}'
        min_out = f'{utc_out.minute:02d}'

        # Check if the day is different in UTC than in PST for the midnight case
        if utc_in.day != pst_in.day:
            day = f'{utc_in.day:02d}'
            month = f'{utc_in.month:02d}'
        else:
            day = f'{pst_in.day:02d}' 
            month = f'{pst_in.month:02d}'

        return hr_in, min_in, hr_out, min_out, month, day


    def build_command(self, sesh_date: str, time_in: str, time_out: str, url: str) -> str:
        """
        Build a command string for fetching data from a specific URL and filtering 
        it based on time and date.

        Parameters:
        -----------
        date: str
            The date in the format 'YYYY-MM-DD'.
        time_in: str
            The start time in the format 'HH:MM'.
        time_out: str
            The end time in the format 'HH:MM'.
        url: str
            The URL from which to fetch the data.

        Returns:
        --------
        str: The constructed command string.
        """
        hr_in, min_in, hr_out, min_out, month, day = self.parse_time_and_date(sesh_date,
                                                                        time_in,
                                                                        time_out)
        # Command construction
        time = (
            f"($4 == {hr_in} && $5 >= {min_in}) || "
            f"($4 > {hr_in} && $4 < {hr_out}) || "
            f"($4 == {hr_out} && $5 <= {min_out})"
        )
        utc_date = f"($2 == {month} && $3 == {day})"
        awk = f"awk '{utc_date} && ({time})' "
        wget = f'wget -qO- {url}'

        return f'{wget} | {awk}'


    def trunc_meteor_df_24_hrs(self, df: DataFrame) -> DataFrame:
        """
        Truncate standard NDBC meteorlogical data down to 24hrs worth of 
        information, or 145 rows including metadata. This function will also 
        trim off columns relating to tide, dewpoint, visibility, and pressure
        tendency, etc..
        :params:
        - df: A pandas dataframe. Must be the standard meteorlogical buoy report.
        :returns:
        - Truncated pandas dataframe.
        """
        df = df[['#YY', 'MM', 'DD', 'hh', 'mm', 'WDIR', 'WSPD',
                'GST', 'WVHT', 'DPD', 'MWD', 'ATMP', 'WTMP']]
        return df.iloc[:145, :]  # 24 hrs worth of rows


    def get_df_in_timeframe(self, df: DataFrame,
                            start: str, end: str) -> DataFrame:
        """
        Retrieve a subset of the dataframe holding information only from the
        hours given. For use only with dataframes containing 'DD' and 'hh' cols.
        For some reason, the start and end times have to be padded by 7 hours
        to get the correct hourly metric in the downloaded NDBC data.
        #### Parameters:
        ----------------
        - df: A pandas dataframe.
        - start: string representing the time to begin with.
        - end: string representing the time to end with.\n
        #### Returns:
        ------------
        - A much smaller dataframe where all values are converted to floats.
        """
        start_hh, end_hh = int(start[:2]) + 7, int(end[:2]) + 7
        hours = [str(_) for _ in range(start_hh, end_hh + 1)]  # Range of hours

        today = date.today().day
        today = f'0{today}' if today < 10 else today
        df = df[df['DD'] == str(today)]  # Get rows only with today's date
        df = df[(df['hh'].isin(hours))]
        self.drop_mm(df)
        return df.astype(float)  # Converts all data to floats


    def drop_mm(self, df: DataFrame):
        """
        Clean any null values out of the given dataframe. Null values must be
        represented by the string "MM", as built in to the NOAA meteorlogical
        data. Replaces "MM" with None.
        #### Parameters:
        ---------------
        - df: A pandas dataframe.
        #### Returns:
        ------------
        A pandas dataframe where "MM" is replaced with None. 
        """
        df.replace(to_replace='MM', value=None, inplace=True)


    def get_mean_meteor_vals(
                self, df: DataFrame, start: str, end: str) -> dict[str, float]:
        """
        Get the mean values of the following data: 'WDIR', 'WSPD', 'GST', 'WVHT',
        'DPD', 'MWD', 'ATMP', 'WTMP'. Values are calculated from the argued data 
        frame, which is truncated to just the hours passed in by the `start` and 
        `end` parameters. This time truncation is imprecise as minutes are 
        disregarded. So for example, 2:25pm to 4:05pm will simply create a data
        frame including all data samples from 2:00pm to 4:50pm.
        #### Parameters:
        ----------------
        - df: A pandas data frame.
        - start: The time at which data evaluation should begin. 
        - end: The time at which data evaluation should end.\n
        Note: start and end values which have not happened yet in PST will not be
        successful and an exception will be raised.
        #### Returns:
        -------------
        A dictionary containing the mean values of each column plus a cardinal
        wind direct in the timeframe, minus the data-time values themselves. 
        Format: {"WDIR": 128.08, ...}\n
        All the values in the resulting dict are in their original units.
        """

        # Handle incorrect times
        current_hour = datetime.time(datetime.now()).hour
        if current_hour < int(start[:2]) or\
              int(start[:2]) > int(end[:2]) or\
              current_hour < int(end[:2]):
            raise InvalidTimeframeException(f"Invalid timeframe, {start} -> {end}")

        df = self.trunc_meteor_df_24_hrs(df)
        df = self.get_df_in_timeframe(df, start, end)
        cols = ['WDIR', 'WSPD', 'GST', 'WVHT', 'DPD', 'MWD', 'ATMP', 'WTMP']
        mean_series = df[cols].mean()
        mean_series = mean_series.round(decimals=2)
        mean_series = mean_series.to_dict()
        self.convert_means_dict_units_to_std(mean_series)
        return mean_series


    def get_mean_meteor_vals_2(self, sesh_date: str, time_in: str, time_out: str,
                                url: str) -> dict[str, float]:
        """
        A more precise and accurate version of the `get_mean_meteor_vals()` method.
        The build_da_frame_2() method uses wget and awk to filter the data down
        to the exact timeframe argued, including minutes. This method will also
        covert all timestamps from PST to UTC, which is the timezone used by the
        NDBC.
        """
        # Handle incorrect times
        if int(time_in[:2]) > int(time_out[:2]):
            raise InvalidTimeframeException(f"Invalid timeframe, {time_in} -> {time_out}")

        df = self.build_da_frame_2(sesh_date, time_in, time_out, url)
        cols = ['WDIR', 'WSPD', 'GST', 'WVHT', 'DPD', 'MWD', 'ATMP', 'WTMP']
        self.drop_mm(df)
        df[cols] = df[cols].apply(to_numeric, errors='coerce')
        mean_series = df[cols].mean()
        mean_series = mean_series.round(decimals=2)
        mean_series = mean_series.to_dict()
        self.convert_means_dict_units_to_std(mean_series)
        return mean_series


    def convert_means_dict_units_to_std(self, means: dict[str, float]) -> None:
        """
        Convert all values in the mean dictionary created from `get_mean_meteor_vals()`
        into their standard units.
        #### Parameters:
        ----------------
        - means: A dictionary generated via the `get_mean_meteor_vals()`
            method.
        #### Returns:
        -------------
            None, the dict is mutated.
        """
        if means['WDIR']:
            means['WDIR_CARD'] = self.unit_conv.find_cardinal_direction(int(means['WDIR']))
        if means['WSPD']:
            means['WSPD'] = self.unit_conv.meters_per_sec_to_mph(means['WSPD'])
        if means['GST']:
            means['GST'] = self.unit_conv.meters_per_sec_to_mph(means['GST'])
        if means['WVHT']:
            means['WVHT'] = self.unit_conv.meters_to_feet(means['WVHT'])
        if means['MWD']:
            means['MWD_CARD'] = self.unit_conv.find_cardinal_direction(int(means['MWD']))
        if means['ATMP']:
            means['ATMP'] = self.unit_conv.celsius_to_fahrenheit(means['ATMP'])
        if means['WTMP']:
            means['WTMP'] = self.unit_conv.celsius_to_fahrenheit(means['WTMP'])


    def get_most_recent_wdir_deg(self, df: DataFrame) -> float:
        """
        Retrieve the most recent windirection measurement from a standard
        meteorlogical dataframe.
        :params:
        - df: A pandas dataframe containing meteorlogical data. Must have a col
            named 'WDIR' (str). \n
        :returns:
        - Float representing the direction of the wind in degrees.
        """
        wdir_no_mm = df[df['WDIR'] != 'MM']  # Retrieve valid data only
        wdir_col = wdir_no_mm[['WDIR']]  # Isolate column
        # Retrieve val at first row
        return float(wdir_col.at[wdir_col.index[0], 'WDIR'])


    def get_most_recent_wdir_cardinal(self, df: DataFrame) -> str:
        """
        Retrieve the most recent windirection measurement from a standard
        meteorlogical dataframe.
        :params:
        - df: A pandas dataframe containing meteorlogical data. Must have a col
            named 'WDIR' (str). \n
        :returns:
        - String representing the direction of the wind in cardinal direction.
        """
        wdir = self.get_most_recent_wdir_deg(df)
        return self.unit_conv.find_cardinal_direction(int(wdir))

    def get_tides_noaa(self, station_id: str, time_in: str, time_out: str, sesh_date: str) -> DataFrame:
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
            df = DataFrame(data['data'])[['t','v']]
            df['t'] = to_datetime(df['t']) # Convert to datetime
            df['v'] = df['v'].astype(float) # Convert to float
            return df

        except exceptions.RequestException as e:
            print(f'Error: {e}')
            return None


    def get_tide_sesh_data(self, sesh_data: dict[str, str | int | float],
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
        sesh_date = sesh_data['date'][:10]  # Slice to only include the date, no timestamp
        sesh_date = sesh_date.replace('-', '')  # NOAA API requires date in the format YYYYMMDD
        time_in = sesh_data['timeIn']
        time_out = sesh_data['timeOut']

        data = self.get_tides_noaa(station_id, time_in, time_out, sesh_date)
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

def main():
    """Main function. Mostly just testing stuff."""
    bdc = BuoyData()
    # Parameters
    d = {'spot': 'Otter Rock',
          'timeIn': '10:30',
          'timeOut': '12:16',
          'rating': 2,
          'date': '2024-09-08T04:39:21.532Z'
          }

    res = bdc.parse_time_and_date('2024-09-08', '10:30', '12:16')
    print(res)

if __name__ == '__main__':
    main()
