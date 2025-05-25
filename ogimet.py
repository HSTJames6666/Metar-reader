import requests
from datetime import datetime, timedelta
from dateutil import tz
import pytz


import re
from datetime import datetime


class Metar:
    def __init__(self, raw: str):
        self.raw = raw
        self.station = None
        self.time = None
        self.wind_speed = None
        self.wind_direction = None
        self.wind_gust = None
        self.visibility = None
        self.clouds = []
        self.ceiling = None
        self.weather = None
        self.qnh = None
        self.temperature = None
        self.dewpoint = None

        self.parse()

    def parse(self):
        parts = self.raw.split()

        if parts[2] == 'COR':
            # If the METAR is a correction, we pop the 'COR' part
            parts.pop(2)

        # Station and time
        self.station = parts[2]
        # match = re.search(r'(\d{6})Z', self.raw)
        self.time = datetime.strptime(parts[0], '%Y%m%d%H%M')

        # Wind
        wind_match = re.findall(
            r'(\d{3}|VRB)(\d{2,3})G?(\d{2,3})?KT', self.raw)
        if wind_match:
            if wind_match[0][0] != 'VRB':
                self.wind_direction = int(wind_match[0][0])
            else:
                self.wind_direction = 'VRB'
            self.wind_speed = int(wind_match[0][1])
            if wind_match[0][2]:
                self.wind_gust = int(wind_match[0][2])

        # Visibility
        vis_match = re.search(r'\s(\d{4})\s', self.raw)
        if vis_match:
            self.visibility = int(vis_match.group(1))  # meters

        # Clouds
        self.clouds = re.findall(r'(FEW|SCT|BKN|OVC)(\d{3})', self.raw)
        self.ceiling = self.get_ceiling()

        self.qnh = re.findall(r'Q(\d{4})', self.raw)

        self.temperature = self.get_temperature(
            re.findall(r'(\d{2}|M\d{2})\/(\d{2}|M\d{2})', self.raw)[0])
        self.dewpoint = self.get_dewpoint(re.findall(
            r'(\d{2}|M\d{2})\/(\d{2}|M\d{2})', self.raw)[0])

        # Weather
        wx_match = re.search(
            r'\s(-|\+)?(RA|SN|BR|FG|HZ|TS|DZ|VC\S+)\s', self.raw)
        if wx_match:
            self.weather = wx_match.group(0).strip()

    def get_temperature(self, temp_dew) -> float:
        """Returns temperature in Celsius."""
        if temp_dew:
            temp = temp_dew[0]
            if temp.startswith('M'):
                return -int(temp[1:])
            return int(temp)
        return None

    def get_dewpoint(self, temp_dew) -> float:
        """Returns dewpoint in Celsius."""
        if temp_dew:
            temp = temp_dew[1]
            if temp.startswith('M'):
                return -int(temp[1:])
            return int(temp)
        return None

    def parse_time(self, timestr: str) -> datetime:
        pass

    def get_ceiling(self):
        """Returns the base of the lowest BKN or OVC layer in feet AGL."""
        for cover, height in self.clouds:
            if cover in ("BKN", "OVC"):
                return int(height) * 100  # height in feet
        return None

    def is_vfr(self):
        """Returns True if visibility ≥ 5000m and ceiling ≥ 1500ft or no ceiling."""
        vis_ok = self.visibility is None or self.visibility >= 5000
        ceiling_ok = self.ceiling is None or self.ceiling >= 1500
        return vis_ok and ceiling_ok


def get_metar(station, start_date, end_date):
    url = (
        f"https://www.ogimet.com/display_metars2.php?lang=en&lugar={station}"
        f"&tipo=ALL&ord=REV&nil=SI&fmt=txt&ano={start_date.year}&mes={start_date.month}"
        f"&day={start_date.day}&hora={start_date.hour}&anof={end_date.year}&mesf={end_date.month}"
        f"&dayf={end_date.day}&horaf={end_date.hour}&minf=59&send=send")
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(url, headers=headers)

    if "No METARs found" in response.text:
        raise Exception("No data found for this date range.")

    lines = response.text.splitlines()
    metars = []
    for line in lines:
        if (str(f'{start_date.year}{start_date.month:02d}{start_date.day:02d}')) in line:
            if 'METAR' in line:
                metars.append(line.strip())
    return metars


def fetch_metars_ogimet(station, start_date, end_date, local_start_hour, local_end_hour, local_start_hour_summer, local_end_hour_summer):
    tz_local = pytz.timezone("Europe/London")
    tz_utc = pytz.UTC

    start = datetime.strptime(start_date, "%d-%m-%Y")
    end = datetime.strptime(end_date, "%d-%m-%Y")
    list_of_metars = []
    for day in range((end - start).days + 1):
        current_date = start + timedelta(days=day)

        # Local times
        local_start = tz_local.localize(datetime(
            current_date.year, current_date.month, current_date.day, local_start_hour))
        local_end = tz_local.localize(datetime(
            current_date.year, current_date.month, current_date.day, local_end_hour))
        if local_start.dst() != timedelta(0):
            local_start = local_start.replace(hour=local_start_hour_summer)
            local_end = local_end.replace(hour=local_end_hour_summer)
        # Convert to UTC
        utc_start = local_start.astimezone(tz_utc)
        utc_end = local_end.astimezone(tz_utc)

        print(
            f"{current_date.strftime('%Y-%m-%d')}: Local {local_start_hour:02d}–{local_end_hour:02d} "
            f"→ UTC {utc_start.hour:02d}–{utc_end.hour:02d} "
            f"(DST: {'Yes' if local_start.dst() != timedelta(0) else 'No'})"
        )
        list_of_metars.append(get_metar(station, utc_start, utc_end))
    return list_of_metars


def clean_metar(raw_line: str) -> str:
    parts = raw_line.split()
    # Assuming format like: "202308011850 METAR EGKA ..."
    if len(parts) >= 3:
        return ' '.join(parts[2:])  # Keep from ICAO onward
    return raw_line  # fallback if unexpected format


# Example usage
daily_metar_list = fetch_metars_ogimet(
    "EGKA", "20-05-2025", "24-05-2025", 8, 18, 8, 20)
for day in daily_metar_list:
    vfr_metar = 0
    non_vfr_metar = 0
    for metar in day:
        if "NIL=" not in metar:
            metar_obj = Metar(metar)
            # print(f"Station: {metar_obj.station}, Time: {metar_obj.time}, Wind Speed: {metar_obj.wind_speed}, "
            #       f"Visibility: {metar_obj.visibility}, Ceiling: {metar_obj.ceiling}, Weather: {metar_obj.weather}")
            # print(f"VFR: {'Yes' if metar_obj.is_vfr() else 'No'}")
            if metar_obj.is_vfr():
                vfr_metar += 1
            else:
                non_vfr_metar += 1
    print(f"Date: {metar_obj.time.day}-{metar_obj.time.month}-{metar_obj.time.year}, VFR METARs: {vfr_metar}, Non-VFR METARs: {non_vfr_metar}, Percentage VFR: {vfr_metar / (vfr_metar + non_vfr_metar) * 100:.2f}%")
