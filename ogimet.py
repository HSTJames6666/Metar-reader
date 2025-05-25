import requests
from datetime import datetime, timedelta


def fetch_metars_ogimet(station="EGKA", start_date="2023-01-01", end_date="2023-01-31"):
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    url = (
        f"http://www.ogimet.com/display_metars2.php?lang=en&lugar={station}"
        f"&tipo=ALL&ord=ASC&nil=SI&fmt=txt&ano={start.year}&mes={start.month}&dia={start.day}"
        f"&hora=00&anof={end.year}&mesf={end.month}&diaf={end.day}&horaf=23&min=0"
        f"&ndays={(end - start).days + 1}"
    )
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(url, headers=headers)

    if "No METARs found" in response.text:
        raise Exception("No data found for this date range.")

    lines = response.text.splitlines()
    metars = [line.strip() for line in lines if line.startswith(station)]
    return metars


# Example usage
metars = fetch_metars_ogimet("EGKA", "2023-06-01", "2023-06-30")
print(f"Downloaded {len(metars)} METARs.")
for m in metars[:5]:
    print(m)
