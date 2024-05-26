"""
This module handles data cleaning and processing before being handed off to the
database. It handles swell discovery from raw spectra data, conversion of units
from metric or degree to standard or cardinal, etc..
"""
from pint import UnitRegistry, Quantity

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


    def find_cardinal_direction(self, value: float) -> str:
        """
        Convert direction in degrees to its corresponding cardinal direction.
        Works by dividing the given value by 45 (since this func uses 8 
        cardinal directions) and then finding the corresponding index in an
        array containing all the cardinal directions.
        :param:
            - value: float currently representing direction in degrees. \n
        :return:
            A string value representing the direction in cardinal form.
        """
        cardinals = ['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW']
        res = cardinals[((int(value + 45)) % 360) // 45]
        return res


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


class BuoyDataCleaner:
    """
    Data cleaning utility. Do things like excise useless data columns, convert
    units, etc.
    """
    def __init__(self) -> None:
        self.unit_conv = UnitConverter()
