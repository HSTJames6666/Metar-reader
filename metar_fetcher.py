import pytz
from datetime import datetime, timedelta
from ogimet_model import Metar
from ogimet_utils import (
    create_db,
    save_metars_from_objects,
    get_cached_metars,
)
import ogimet_cgi


def merge_consecutive_metar_requests(requests):
    # Ensure requests are sorted by begin time
    requests.sort(key=lambda r: r["begin"])

    merged = []
    current = None

    for req in requests:
        begin_dt = datetime.strptime(req["begin"], "%Y%m%d%H%M")
        end_dt = datetime.strptime(req["end"], "%Y%m%d%H%M")

        if current is None:
            current = {
                "icao": req["icao"],
                "begin": begin_dt,
                "end": end_dt
            }
        else:
            # Check if the current request is consecutive with the last one
            if (req["icao"] == current["icao"] and
                    end_dt <= current["end"] + timedelta(days=1)):
                # Extend the current group
                if end_dt > current["end"]:
                    current["end"] = end_dt
                if current["begin"] + timedelta(weeks=4) < current["end"]:
                    # If the gap is too large, push the current group
                    merged.append({
                        "icao": current["icao"],
                        "begin": current["begin"].strftime("%Y%m%d%H%M"),
                        "end": current["end"].strftime("%Y%m%d%H%M"),
                        "header": "no"
                    })
                    current = {
                        "icao": req["icao"],
                        "begin": begin_dt,
                        "end": end_dt
                    }
            else:
                # Push the current group and start a new one
                merged.append({
                    "icao": current["icao"],
                    "begin": current["begin"].strftime("%Y%m%d%H%M"),
                    "end": current["end"].strftime("%Y%m%d%H%M"),
                    "header": "no"
                })
                current = {
                    "icao": req["icao"],
                    "begin": begin_dt,
                    "end": end_dt
                }

    # Add the last group
    if current:
        merged.append({
            "icao": current["icao"],
            "begin": current["begin"].strftime("%Y%m%d%H%M"),
            "end": current["end"].strftime("%Y%m%d%H%M"),
            "header": "no"
        })

    return merged


def fetch_metars(station, start_date, end_date, local_start_hour, local_end_hour, local_start_hour_summer, local_end_hour_summer):
    list_of_metars = []
    conn = create_db()
    c = conn.cursor()
    tz_local = pytz.timezone("Europe/London")
    tz_utc = pytz.UTC

    start = datetime.strptime(start_date, "%d-%m-%Y")
    end = datetime.strptime(end_date, "%d-%m-%Y")

    # Build requests for each day
    requests = []
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

        # Only fetch if not cached
        cached_raws = get_cached_metars(conn, station, utc_start, utc_end)
        if not cached_raws:
            requests.append({
                "icao": station,
                "begin": utc_start.strftime("%Y%m%d%H%M"),
                "end": utc_end.strftime("%Y%m%d%H%M"),
                "header": "no"
            })
    # Merge consecutive requests and fetch METARs from Ogimet
    if requests:
        merged_requests = merge_consecutive_metar_requests(requests)
        for req in merged_requests:
            print(
                f"Fetching METARs for {req['icao']} from {req['begin'][:4]}-{req['begin'][4:6]}-{req['begin'][6:8]} to {req['end'][:4]}-{req['end'][4:6]}-{req['end'][6:8]}")
            metars = ogimet_cgi.fetch_metar(
                icao=req["icao"],
                begin=req["begin"],
                end=req["end"],
                header=req["header"]
            )

            # Parse and save each METAR
            for raw in metars:
                metar = Metar(raw=raw, parse=True)
                list_of_metars.append(metar)
                save_metars_from_objects(conn, [metar])

    # Now collect all metars for the requested period from the DB
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

        c.execute("SELECT * FROM metars WHERE station = ? AND time BETWEEN ? AND ?",
                  (station, utc_start.isoformat(), utc_end.isoformat()))
        rows = c.fetchall()
        for row in rows:
            metar = Metar.from_db_row(row)
            list_of_metars.append(metar)
        # print(f"Fetched {len(rows)} METARs for {current_date.strftime('%Y-%m-%d')}")

    return list_of_metars

# Example usage:
# if __name__ == "__main__":
#     print(fetch_metars("EGKA", "30-12-2022", "03-01-2023", 8, 18, 8, 20))
