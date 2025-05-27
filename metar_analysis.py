import time
from ogimet_model import Metar
from ogimet_utils import create_db
from metar_fetcher import fetch_metars

def analyze_metars(ruleset="default", station='EGKA', start_date="01-01-2024", end_date="31-12-2024"):
    start_time = time.time()

    fetched_metars = fetch_metars(
        station=station,
        start_date=start_date,
        end_date=end_date,
        local_start_hour=8,
        local_end_hour=18,
        local_start_hour_summer=8,
        local_end_hour_summer=20
    )
    
    flyable_count = 0
    nonflyable_count = 0
    for metar in fetched_metars:
        
        if metar.is_flyable(
            min_visibility=8000, 
            min_ceiling=1500, 
            max_wind_speed=14, 
            min_base=1000, 
            max_wind_gust=25,
            min_temperature=1,
            max_temperature=30,
            bad_weather=["FZFG", "+TSRA", "TS", "TSRA", "BR", "HZ", "FG", "GR", "GS"]
        ) == True:
            flyable_count += 1
        else:   
            nonflyable_count += 1
    end_time = time.time()
    print(f"For the period {start_date} to {end_date} at {station}, the following analysis was made:")
    print(f"Total METARs analyzed in {end_time-start_time:.2f} seconds: {flyable_count + nonflyable_count}")
    print(f"Flyable METARs: {flyable_count}, Non-flyable METARs: {nonflyable_count}")
    print(f'This means that {flyable_count / (flyable_count + nonflyable_count) * 100:.2f}% of the METARs were flyable.')
    

def save_metar_analysis(conn, metar_objs, ruleset="default"):
    conn = create_db()
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
    analyze_metars(start_date="01-01-2024", end_date="31-12-2024", station="EGKA")