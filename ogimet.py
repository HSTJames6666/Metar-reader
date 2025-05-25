import requests
import sqlite3
import re
from datetime import datetime, timedelta
import pytz
import time
import json

# -- METAR Class --


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
        self.base = None
        self.ceiling = None
        self.weather = None
        self.qnh = None
        self.temperature = None
        self.dewpoint = None

        self.parse()

    def parse(self):
        parts = self.raw.split()
        if parts[2] == 'COR':
            parts.pop(2)

        self.station = parts[2]
        self.time = datetime.strptime(parts[0], '%Y%m%d%H%M')

        wind_match = re.findall(
            r'(\d{3}|VRB)(\d{2,3})G?(\d{2,3})?KT', self.raw)
        if wind_match:
            self.wind_direction = wind_match[0][0] if wind_match[0][0] != 'VRB' else 'VRB'
            self.wind_speed = int(wind_match[0][1])
            if wind_match[0][2]:
                self.wind_gust = int(wind_match[0][2])

        vis_match = re.search(r'\s(\d{4})\s', self.raw)
        if vis_match:
            self.visibility = int(vis_match.group(1))

        self.clouds = re.findall(r'(FEW|SCT|BKN|OVC)(\d{3})', self.raw)
        self.base = self.get_base()
        self.ceiling = self.get_ceiling()

        self.qnh = re.findall(r'Q(\d{4})', self.raw)

        temp_dew = re.findall(r'(\d{2}|M\d{2})\/(\d{2}|M\d{2})', self.raw)
        if temp_dew:
            self.temperature = self.get_temperature(temp_dew[0])
            self.dewpoint = self.get_dewpoint(temp_dew[0])

        wx_match = re.search(
            r'\s(-|\+)?(RA|SN|BR|FG|HZ|TS|DZ|VC\S+)\s', self.raw)
        if wx_match:
            self.weather = wx_match.group(0).strip()

    def get_temperature(self, temp_dew):
        t = temp_dew[0]
        return -int(t[1:]) if t.startswith('M') else int(t)

    def get_dewpoint(self, temp_dew):
        d = temp_dew[1]
        return -int(d[1:]) if d.startswith('M') else int(d)

    def get_ceiling(self):
        for cover, height in self.clouds:
            if cover in ("BKN", "OVC"):
                return int(height) * 100
        return None

    def get_base(self):
        for cover, height in self.clouds:
            return int(height) * 100
        return None

    def is_vfr(self):
        vis_ok = self.visibility is None or self.visibility >= 5000
        ceiling_ok = self.ceiling is None or self.ceiling >= 1500
        return vis_ok and ceiling_ok

    def is_flyable(self):
        vis_ok = self.visibility is None or self.visibility >= 5000
        ceiling_ok = self.ceiling is None or self.ceiling >= 1400
        base_ok = self.base is None or self.base >= 1000
        wind_ok = self.wind_speed is None or self.wind_speed <= 14
        gust_ok = self.wind_gust is None or self.wind_gust <= 25
        return vis_ok and ceiling_ok and wind_ok and gust_ok and base_ok


# -- SQLite Functions --
def create_db():
    conn = sqlite3.connect("metars.db")
    c = conn.cursor()
    c.execute(f"""
        CREATE TABLE IF NOT EXISTS metars (
            station TEXT,
            obs_time TEXT,
            raw TEXT,
            wind_dir TEXT,
            wind_speed INTEGER,
            wind_gust INTEGER,
            visibility INTEGER,
            cloud TEXT,
            base INTEGER,
            ceiling INTEGER,
            weather TEXT,
            qnh INTEGER,
            temperature INTEGER,
            dewpoint INTEGER,
            vfr INTEGER,
            flyable INTEGER,
            PRIMARY KEY (station, obs_time)
        )
    """)
    conn.commit()
    return conn


def save_metars_from_objects(conn, metar_objs):
    c = conn.cursor()
    for m in metar_objs:
        try:
            c.execute("""
                INSERT OR IGNORE INTO metars (
                    station, obs_time, raw, wind_dir, wind_speed, wind_gust,
                    visibility, cloud, ceiling, base, weather, qnh, temperature, dewpoint, vfr, flyable
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                m.station,
                m.time.isoformat(),
                m.raw,
                m.wind_direction,
                m.wind_speed,
                m.wind_gust,
                m.visibility,
                json.dumps(m.clouds),
                m.ceiling,
                m.base,
                m.weather,
                int(m.qnh[0]) if m.qnh else None,
                m.temperature,
                m.dewpoint,
                int(m.is_vfr()),
                int(m.is_flyable())
            ))
        except Exception as e:
            print(f"Error saving METAR: {m.raw} -> {e}")
    conn.commit()


def get_cached_metars(conn, station, start_dt, end_dt):
    c = conn.cursor()
    c.execute("""
        SELECT raw FROM metars
        WHERE station = ? AND obs_time BETWEEN ? AND ?
    """, (station, start_dt.isoformat(), end_dt.isoformat()))
    return [row[0] for row in c.fetchall()]


# -- METAR Fetching --
def get_metar_from_ogimet(station, start_date, end_date):
    print(
        f"Fetching METARs for {station} from {start_date} to {end_date} from Ogimet")
    time.sleep(1)  # To avoid hitting the server too fast
    url = (f"https://www.ogimet.com/display_metars2.php?lang=en&lugar={station}"
           f"&tipo=ALL&ord=REV&nil=SI&fmt=txt&ano={start_date.year}&mes={start_date.month}"
           f"&day={start_date.day}&hora={start_date.hour}&anof={end_date.year}&mesf={end_date.month}"
           f"&dayf={end_date.day}&horaf={end_date.hour}&minf=59&send=send")
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(url, headers=headers)

    if "No METARs found" in response.text:
        return []

    lines = response.text.splitlines()
    while 'quota limit' in lines[-1]:
        print("Quota limit reached, waiting for reset...")
        time.sleep(10)  # Wait for quota reset
        response = requests.get(url, headers=headers)
        if "No METARs found" in response.text:
            return []
        lines = response.text.splitlines()
    metars = []
    for line in lines:
        if (str(f'{start_date.year}{start_date.month:02d}{start_date.day:02d}')) in line:
            if 'METAR' in line:
                metars.append(line.strip())
    return metars


# -- Full Flow --
def fetch_metars(station, start_date, end_date, local_start_hour, local_end_hour, local_start_hour_summer, local_end_hour_summer):
    conn = create_db()
    tz_local = pytz.timezone("Europe/London")
    tz_utc = pytz.UTC

    start = datetime.strptime(start_date, "%d-%m-%Y")
    end = datetime.strptime(end_date, "%d-%m-%Y")
    for day in range((end - start).days + 1):
        current_date = start + timedelta(days=day)
        local_start = tz_local.localize(datetime(
            current_date.year, current_date.month, current_date.day, local_start_hour))
        local_end = tz_local.localize(datetime(
            current_date.year, current_date.month, current_date.day, local_end_hour))

        if local_start.dst() != timedelta(0):
            local_start = local_start.replace(hour=local_start_hour_summer)
            local_end = local_end.replace(hour=local_end_hour_summer)

        utc_start = local_start.astimezone(tz_utc)
        utc_end = local_end.astimezone(tz_utc)

        cached_raws = get_cached_metars(conn, station, utc_start, utc_end)
        metars = cached_raws[:]
        if not cached_raws:
            fetched_raws = get_metar_from_ogimet(station, utc_start, utc_end)
            metars.extend(fetched_raws)
            metar_objs = [Metar(raw) for raw in fetched_raws]
            save_metars_from_objects(conn, metar_objs)
            cached_raws = get_cached_metars(conn, station, utc_start, utc_end)
            metars = cached_raws[:]
        vfr = 0
        non_vfr = 0
        for raw in metars:
            m = Metar(raw)
            if m.is_vfr():
                vfr += 1
            else:
                non_vfr += 1

        total = vfr + non_vfr
        pct = vfr / total * 100 if total else 0
        print(
            f"{current_date.strftime('%Y-%m-%d')}: VFR={vfr}, Non-VFR={non_vfr}, VFR%={pct:.2f}%")


# -- Run example --
fetch_metars("EGKA", "02-01-2025", "02-01-2025", 8, 18, 8, 20)
