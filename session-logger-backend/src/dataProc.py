"""
This module handles data cleaning and processing before being handed off to the
database. It handles swell discovery from raw spectra data, conversion of units
from metric or degree to standard or cardinal, etc..
"""
from pint import UnitRegistry, Quantity

class UnitConverter:
    """Handle unit conversion."""

    def __init__(self) -> None:
        # Pint library unit converter
        self.ureg = UnitRegistry()
        # Separate Temperature Unit Registry
        self.ureg_temp = UnitRegistry()
        self.ureg_temp.default_format = '.3f'

    def find_cardinal_direction(self, value):
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
    
    def meters_to_feet(self, value):
        """
        Conver meters to feet.
        :param:
            - value: float currenty in meters \n
        :return:
            - String representing the value in feet.
        """
        meters = value * self.ureg.meter
        feet = meters.to(self.ureg.foot)
        return str(round(feet.magnitude, 1))

    def meters_per_sec_to_mph(self, value):
        """
        Convert m/s to mph.
        :param:
            - value: float currently in m/s \n
        :return:
            - String representing the value converted to mph.
        """
        # value * m/s
        m_per_sec = value * (self.ureg.meter / self.ureg.second)
        mph = m_per_sec.to(self.ureg.mile / self.ureg.hour)
        return str(round(mph.magnitude, 1))

    def celsius_to_fahrenheit(self, value):
        """
        Convert Celsius values to Fahrenheit.
        :param:
            - value: a float currently in degrees Celsius \n
        :return:
            - String representing the value converted to F.
        """
        # Convert value to a Pint Quantity object first
        celsius = Quantity(value, self.ureg_temp.degC)
        F = celsius.to(self.ureg_temp.degF)
        # Make the float manageable and convert to a string for displaying later
        return str(round(F.magnitude, 1))