import sqlite3
import json
import time
import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import os

class MetarScraper:
    def __init__(self, headless=True, wait_timeout=40):
        self.wait_timeout = wait_timeout
        options = Options()
        if headless:
            options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                     "AppleWebKit/537.36 (KHTML, like Gecko) "
                     "Chrome/124.0.0.0 Safari/537.36")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("useAutomationExtension", False)
        service = Service(log_path=os.devnull)
        self.driver = webdriver.Chrome(options=options, service=service)

    def fetch_html(self, url, wait_for_selector='pre'):
        # self.driver.get('https://www.ogimet.com/')
        # time.sleep(.5)
        # self.driver.get('https://www.ogimet.com/metars.phtml.en') 
        # time.sleep(.5)
        self.driver.get(url)
        WebDriverWait(self.driver, self.wait_timeout).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, wait_for_selector))
        )
        return self.driver.page_source

    def close(self):
        self.driver.quit()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

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

def get_metar_from_ogimet(station, start_date, end_date, scraper, use_requests=False):
    print(
        f"Fetching METARs for {station} from {start_date} to {end_date} from Ogimet")
    # time.sleep(1)  # To avoid hitting the server too fast
    url = (f"https://www.ogimet.com/display_metars2.php?lang=en&lugar={station}"
           f"&tipo=SA&ord=REV&nil=NO&fmt=txt&ano={start_date.year}&mes={start_date.month}"
           f"&day={start_date.day}&hora={start_date.hour}&anof={end_date.year}&mesf={end_date.month}"
           f"&dayf={end_date.day}&horaf={end_date.hour}&minf=59&send=send")
    if use_requests == True:
        time.sleep(1)
        response = fetch_metars_with_requests(url)
    else:
        # time.sleep(1)
        response = scraper.fetch_html(url, wait_for_selector='pre')

    if "No METARs found" in response:
        return []

    lines = response.splitlines()
    while 'quota limit' in lines[-1]:
        print("Quota limit reached, waiting for reset...")
        time.sleep(18)  # Wait for quota reset
        response = scraper.fetch_html(url, wait_for_selector='pre')
        if "No METARs found" in response:
            return []
        lines = response.splitlines()
    metars = []
    for line in lines:
        if (str(f'{start_date.year}{start_date.month:02d}{start_date.day:02d}')) in line:
            if 'METAR' in line:
                metars.append(line.strip())
    return metars