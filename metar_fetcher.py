import sqlite3
import pytz
import json
import time
from datetime import datetime, timedelta
from ogimet_model import Metar  # We'll move the Metar class to ogimet_model.py
from ogimet_utils import (
    create_db,
    save_metars_from_objects,
    get_cached_metars,
    get_metar_from_ogimet
)

def fetch_metars(
    station, start_date, end_date,
    local_start_hour, local_end_hour,
    local_start_hour_summer, local_end_hour_summer
    ):
    list_of_metars = []
    conn = create_db()
    c = conn.cursor()
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
        print(f"Fetched {len(metars)} METARs for {current_date.strftime('%Y-%m-%d')}")
        
        c.execute("SELECT * FROM metars WHERE station = ? AND time BETWEEN ? AND ?""", 
                  (station, utc_start.isoformat(), utc_end.isoformat()))
        rows = c.fetchall()
        for row in rows:
            metar = Metar.from_db_row(row)
            list_of_metars.append(metar)
    return list_of_metars

# Example usage:
if __name__ == "__main__":
    print(fetch_metars("EGKA", "02-01-2023", "09-01-2023", 8, 18, 8, 20))