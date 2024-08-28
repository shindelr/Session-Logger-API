"""
This module handles data cleaning and processing before being handed off to the
database. It handles swell discovery from raw spectra data, conversion of units
from metric or degree to standard or cardinal, etc..
"""
from datetime import date, datetime
from pint import UnitRegistry, Quantity
from pandas import read_csv, errors, DataFrame

class UnitConverter:
    """
    Unit conversion utility. Use to convert various heights, speeds, 
    directions.
    """

    def __init__(self) -> None:
        # Pint library unit converter
        self.ureg = UnitRegistry()
        # Separate Temperature Unit Registry
        self.ureg_temp = UnitRegistry()
        self.ureg_temp.default_format = '.3f'


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
        celsius = Quantity(value, self.ureg_temp.degC)
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
        fahrenheit = Quantity(value, self.ureg_temp.degF)
        celsius = fahrenheit.to(self.ureg_temp.degC)
        return round(celsius.magnitude, 1)


class BuoyDataException(Exception):
    """Handle exceptions related to the BuoyData class"""


class BuoyData:
    """
    Buoy data utility. Do things like excise useless data columns, convert
    units, etc.
    """
    def __init__(self) -> None:
        self.unit_conv = UnitConverter()
        self.stonewall_bank_csv = 'Session-Logger/session-logger-backend/data/RAW_meteor_data_46050.csv'
        self.curr_df = self.build_da_frame(self.stonewall_bank_csv)


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
            raise BuoyDataException(f"Invalid timeframe, {start} -> {end}")

        df = self.trunc_meteor_df_24_hrs(df)
        df = self.get_df_in_timeframe(df, start, end)
        cols = ['WDIR', 'WSPD', 'GST', 'WVHT', 'DPD', 'MWD', 'ATMP', 'WTMP']
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


def main():
    """Main function. Mostly just testing stuff."""
    bdc = BuoyData()
    print(bdc.get_mean_meteor_vals(bdc.curr_df, "12:30", "13:30"))

if __name__ == '__main__':
    main()
