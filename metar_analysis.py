import sqlite3
import json
from ogimet_model import Metar
from ogimet_utils import create_db
from metar_fetcher import fetch_metars

def analyze_metars(ruleset="default"):
    fetched_metars = fetch_metars(
        station="EGKA",
        start_date="02-01-2023",
        end_date="08-01-2023",
        local_start_hour=8,
        local_end_hour=18,
        local_start_hour_summer=8,
        local_end_hour_summer=20
    )
    
    
    for metar in fetched_metars:
        print(metar.is_flyable(min_visibility=1500, min_ceiling=1500, max_wind_speed=14, min_base=1000, max_wind_gust=25))
    

def save_metar_analysis(conn, metar_objs, ruleset="default"):
    c = conn.cursor()
    for m in metar_objs:
        try:
            c.execute("""
                INSERT OR REPLACE INTO metar_analysis (
                    station, obs_time, vfr, flyable, ruleset
                ) VALUES (?, ?, ?, ?, ?)
            """, (
                m.station,
                m.time.isoformat(),
                int(m.is_vfr()),
                int(m.is_flyable()),
                ruleset
            ))
        except Exception as e:
            print(f"Error saving analysis: {m.raw} -> {e}")
    conn.commit()

if __name__ == "__main__":
    analyze_metars()