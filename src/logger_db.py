"""
Primary logic for inteerations involving the Session-Logger-DB hosted on 
Azure SQL. 
"""
from os import environ
from dotenv import load_dotenv
import pyodbc

class LoggerDB:
    """Session Logger SQL database interactions."""
    # DB CONNECTION
    def connect_to_db(self):
        """
        Establish a connection to the Azure SQL database.
        :return:
            A tuple containing the cursor and connection objects.
        """
        load_dotenv()
        try:
            conn = pyodbc.connect(environ['AZURE_CONN_STR'], timeout=5)
            cursor = conn.cursor()
            return cursor, conn
        except pyodbc.OperationalError as e:
            print("Error: Connection timeout, try again in 30 seconds")
            raise e

    # DATABASE QUERIES
    def get_meteor_station(self, spot_name: str, db_cursor) -> str:
        """
        Retrieves the meteorlogical station ID for a given spot name.
        Args:
            spot_name (str): The name of the spot.
            db_cursor: The database cursor.
        Returns:
            str: The meteorlogical station ID.
        Raises:
            pyodbc.Error: If there is an error executing the database query.

        """
        query = """
                select s.StationID
                from Station s
                join Location l
                    on l.MeteorlogicalDataSource = s.StationTableID
                where s.Buoy = 1
                and l.SpotName = ?
                """
        try:
            db_cursor.execute(query, spot_name)
            row = db_cursor.fetchone()
            return row.StationID
        except pyodbc.Error as e:
            print(f'Error: {e}')
            raise e


    def get_tide_station(self, spot_name: str, db_cursor) -> str:
        """
        Retrieves the tide station ID for a given spot name.
        Args:
            spot_name (str): The name of the spot.
            db_cursor: The database cursor.
        Returns:
            str: The tide station ID.
        Raises:
            pyodbc.Error: If there is an error executing the database query.

        """
        query = """
                select s.StationID
                from Station s
                join Location l
                    on l.TideDataSource = s.StationTableID
                where s.WeatherStation = 1
                and l.SpotName = ?
                """
        try:
            db_cursor.execute(query, spot_name)
            row = db_cursor.fetchone()
            return row.StationID
        except pyodbc.Error as e:
            print(f'Error: {e}')
            raise e

    def insert_session_info(self, sesh_data: dict[str, str | float], db_cursor, db_conn) -> None:
        """
        Insert session information into the database.
        Args:
            sesh_data (dict[str, str | float]): A dictionary containing session data.
            db_cursor: The database cursor object.
            db_conn: The database connection object.
        Returns:
            None
        Raises:
            pyodbc.Error: If there is an error connecting to the database.
        
        """

        # TODO: Validation for any possibly NaN values

        # Missing username
        submssion_query_str = """
                            exec session_data @SpotName = ?, @Date = ?, @TimeIn = ?, 
                            @TimeOut = ?, @Rating = ?, @ATemp = ?, @WTemp = ?,
                            @MeanWaveDir = ?, @MeanWaveDirCard = ?, 
                            @MeanWaveHeight = ?, @DomPeriod = ?, @MeanWindDir = ?,
                            @MeanWindDirCard = ? , @MeanWindSpeed = ?, @GustSpeed = ?,
                            @TideIncoming = ?, @TideMaxHeight = ?, @TideMinHeight = ?,
                            @TideMedianHeight = ?
                        """
        try:
            # Missing date, username, tideIncoming, and tideMax/Min
            db_cursor.execute(submssion_query_str,
                        sesh_data['spot'], sesh_data['date'][:10], sesh_data['timeIn'],
                        sesh_data['timeOut'], sesh_data['rating'], sesh_data['ATMP'],
                        sesh_data['WTMP'], sesh_data['MWD'], sesh_data['MWD_CARD'],
                        sesh_data['WVHT'], sesh_data['DPD'], sesh_data['WDIR'],
                        sesh_data['WDIR_CARD'], sesh_data['WSPD'], sesh_data['GST'],
                        sesh_data['incoming'], sesh_data['max_h'],
                        sesh_data['min_h'], sesh_data['median_h']
                        )
            db_conn.commit()
        except pyodbc.Error as e:
            print(f'Error: {e}')
            raise e
