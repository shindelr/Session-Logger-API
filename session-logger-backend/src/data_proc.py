"""
This module handles data cleaning and processing before being handed off to the
database. It handles swell discovery from raw spectra data, conversion of units
from metric or degree to standard or cardinal, etc..
"""
from datetime import date
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


class BuoyData:
    """
    Buoy data utility. Do things like excise useless data columns, convert
    units, etc.
    """
    def __init__(self) -> None:
        self.unit_conv = UnitConverter()


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
        - df: A pandas dataframe.
        - start: string representing the time to begin with.
        - end: string representing the time to end with.\n
        :returns:
        - A much smaller dataframe.
        """
        start_hh, end_hh = start[:2], end[:2]
        # start_mm, end_mm = start[2:], end[2:]
        today = date.today().day  # Day of the month (int)
        df = df[df['DD'] == str(today)]
        return df[(df['hh'].isin([start_hh, end_hh]))]


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


    # def get_most_recent_wspd(self, df: DataFrame) -> ?:


    # def get_most_recent_gst(self, df: DataFrame) -> ?:


    # def get_most_recent_wvht(self, df: DataFrame) -> ?:


    # def get_most_recent_dpd(self, df: DataFrame) -> ?:


    # def get_most_recent_mwd(self, df: DataFrame) -> ?:


    # def get_most_recent_atmp(self, df: DataFrame) -> ?:


    # def get_most_recent_wtmp(self, df: DataFrame) -> ?:


def main():
    """Main function. Mostly just testing stuff."""
    fp = '../data/RAW_meteor_data_46050.csv'
    bdc = BuoyData()
    df = bdc.build_da_frame(fp)
    df = bdc.trunc_meteor_df_24_hrs(df)
    df = bdc.get_df_in_timeframe(df, '12:30', '14:30')
    print(df)
    print(bdc.get_most_recent_wdir_cardinal(df))

if __name__ == '__main__':
    main()
