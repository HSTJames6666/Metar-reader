import requests
import csv
import aiohttp
import asyncio

def fetch_metar(
    icao=None,
    state=None,
    begin="202405010000",
    end="202405012359",
    lang="eng",
    header="no"
):
    base_url = "http://www.ogimet.com/cgi-bin/getmetar"
    params = {
        "begin": begin,
        "end": end,
        "lang": lang,
        "header": header,
    }

    if icao:
        params["icao"] = icao
    if state:
        params["state"] = state

    print(f"Fetching METAR data with params: {params}")
    response = requests.get(base_url, params=params)

    lines = response.text.strip().split("\n")

    # Skip header if present
    if header == "yes":
        lines = lines[1:]

    formatted_lines = []
    for line in lines:
        parts = line.split(",")
        if len(parts) == 7:
            icao_code, year, month, day, hour, minute, metar = parts
            timestamp = f"{year}{month}{day}{hour}{minute}"
            formatted_line = f"{timestamp} {metar}"
            formatted_lines.append(formatted_line)
    return formatted_lines



# Example usage
if __name__ == "__main__":
    fetch_metar(
        icao="EGKA",
        begin="202405010000",
        end="202405012359",
        output_file="egll_metar_may1.csv"
    )
