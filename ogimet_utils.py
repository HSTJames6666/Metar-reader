import sqlite3
import json
import time
import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

def create_db():
    conn = sqlite3.connect("metars.db")
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS metars (
            station TEXT,
            time TEXT,
            raw TEXT,
            wind_direction TEXT,
            wind_speed INTEGER,
            wind_gust INTEGER,
            visibility INTEGER,
            clouds TEXT,
            base INTEGER,
            ceiling INTEGER,
            weather TEXT,
            qnh INTEGER,
            temperature INTEGER,
            dewpoint INTEGER,
            PRIMARY KEY (station, time)
        )
    """)
    c.execute("""CREATE TABLE IF NOT EXISTS metar_analysis (
        station TEXT,
        time TEXT,
        vfr INTEGER,
        flyable INTEGER,
        ruleset TEXT DEFAULT 'default',
        PRIMARY KEY (station, time, ruleset),
        FOREIGN KEY (station, time) REFERENCES metars(station, time)
    )""")
    conn.commit()
    return conn

def save_metars_from_objects(conn, metar_objs):
    c = conn.cursor()
    for m in metar_objs:
        try:
            c.execute("""
                INSERT OR IGNORE INTO metars (
                    station, time, raw, wind_direction, wind_speed, wind_gust,
                    visibility, clouds, ceiling, base, weather, qnh, temperature, dewpoint
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                json.dumps(m.weather),
                int(m.qnh[0]) if m.qnh else None,
                m.temperature,
                m.dewpoint
            ))
        except Exception as e:
            print(f"Error saving METAR: {m.raw} -> {e}")
    conn.commit()

def get_cached_metars(conn, station, start_dt, end_dt):
    c = conn.cursor()
    c.execute("""
        SELECT raw FROM metars
        WHERE station = ? AND time BETWEEN ? AND ?
    """, (station, start_dt.isoformat(), end_dt.isoformat()))
    return [row[0] for row in c.fetchall()]

def fetch_metars_with_selenium(url):
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(options=options)
    driver.get(url)
    time.sleep(1)
    html = driver.page_source
    driver.quit()
    return html

def fetch_metars_with_requests(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Referer": "https://www.ogimet.com/",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }
    session = requests.Session()
    session.headers.update(headers)
    response = session.get(url, timeout=15)
    return response.text

def get_metar_from_ogimet(station, start_date, end_date, use_requests=False):
    print(
        f"Fetching METARs for {station} from {start_date} to {end_date} from Ogimet")
    time.sleep(1)  # To avoid hitting the server too fast
    url = (f"https://www.ogimet.com/display_metars2.php?lang=en&lugar={station}"
           f"&tipo=ALL&ord=REV&nil=SI&fmt=txt&ano={start_date.year}&mes={start_date.month}"
           f"&day={start_date.day}&hora={start_date.hour}&anof={end_date.year}&mesf={end_date.month}"
           f"&dayf={end_date.day}&horaf={end_date.hour}&minf=59&send=send")
    if use_requests == True:
        response = fetch_metars_with_requests(url)
    else:
        response = fetch_metars_with_selenium(url)

    if "No METARs found" in response:
        return []

    lines = response.splitlines()
    while 'quota limit' in lines[-1]:
        print("Quota limit reached, waiting for reset...")
        time.sleep(10)  # Wait for quota reset
        response = fetch_metars_with_selenium(url)
        if "No METARs found" in response:
            return []
        lines = response.splitlines()
    metars = []
    for line in lines:
        if (str(f'{start_date.year}{start_date.month:02d}{start_date.day:02d}')) in line:
            if 'METAR' in line:
                metars.append(line.strip())
    return metars