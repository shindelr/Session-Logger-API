"""Scratch Paper"""
from subprocess import run
import pandas as pd

# Parameters
ymd = "2024-09-01"
t_in = "14:00"
t_out = "16:20"
STATION = '46050'
url = f'https://www.ndbc.noaa.gov/data/realtime2/{STATION}.txt'


def parse_time_and_date(date: str, time_in: str, time_out: str) -> tuple[str]:
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
    stamp_in = pd.Timestamp(f'{date} {time_in}')
    stamp_out = pd.Timestamp(f'{date} {time_out}')

    # Localize in PST then convert to UTC
    pst_in = pd.to_datetime(stamp_in).tz_localize('US/Pacific')
    pst_out = pd.to_datetime(stamp_out).tz_localize('US/Pacific')
    utc_in = pst_in.tz_convert('UTC')
    utc_out = pst_out.tz_convert('UTC')

    # Format strings to be able to filter NDBC data
    hr_in = f'{utc_in.hour:02d}'
    min_in = f'{utc_in.minute:02d}'
    hr_out = f'{utc_out.hour:02d}'
    min_out = f'{utc_out.minute:02d}'
    month = f'{utc_in.month:02d}'
    day = f'{utc_in.day:02d}'

    return hr_in, min_in, hr_out, min_out, month, day

def build_command(date: str, time_in: str, time_out: str, url: str) -> str:
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
    hr_in, min_in, hr_out, min_out, month, day = parse_time_and_date(date,
                                                                     time_in,
                                                                     time_out)
    # Command construction
    time = (
        f"($4 == {hr_in} && $5 >= {min_in}) || "
        f"($4 > {hr_in} && $4 < {hr_out}) || "
        f"($4 == {hr_out} && $5 <= {min_out})"
    )
    date = f"($2 == {month} && $3 == {day})"
    awk = f"awk '{date} && ({time})' "
    wget = f'wget -qO- {url}'

    return f'{wget} | {awk}'

cmd = build_command(ymd, t_in, t_out, url)

# Run the command and capture the output, decode it to a string
out = run(cmd, shell=True, capture_output=True).stdout.decode('utf- 8')

# Split the output into lines on the newline
raw_lines = out.split('\n')

# Remove empty lines, clean off any whitespace, and split into columns
clean_lines = [line.split() for line in raw_lines if line.strip()]

cols = ['#YY', 'MM', 'DD', 'hh', 'mm', 'WDIR', 'WSPD', 'GST', 'WVHT', 'DPD',
        'APD', 'MWD', 'PRES', 'ATMP', 'WTMP', 'DEWP', 'VIS', 'PTDY', 'TIDE']
df = pd.DataFrame(clean_lines, columns=cols)

print(df)