import re
from datetime import datetime
from ast import literal_eval

class Metar:
    def __init__(self, raw: str, parse: bool = True):
        self.raw = raw
        self.station = None
        self.time = None
        self.wind_speed = None
        self.wind_direction = None
        self.wind_gust = None
        self.visibility = None
        self.clouds = None
        self.base = None
        self.ceiling = None
        self.weather = None
        self.qnh = None
        self.temperature = None
        self.dewpoint = None

        if parse and raw is not None:
            self.parse()

    @classmethod
    def from_db_row(cls, row):
        obj = cls(raw=row['raw'], parse=False)
        obj.station = row['station']
        obj.time = datetime.fromisoformat(row['time']) if isinstance(row['time'], str) else row['time']
        obj.wind_speed = row['wind_speed']
        obj.wind_direction = row['wind_direction']
        obj.wind_gust = row['wind_gust']
        obj.visibility = row['visibility']
        obj.clouds = literal_eval(row['clouds']) if row['clouds'] != "null" else None
        obj.base = row['base']
        obj.ceiling = row['ceiling']
        obj.weather = literal_eval(row['weather']) if row['weather'] != "null" else None
        obj.qnh = [row['qnh']] if row['qnh'] else None
        obj.temperature = row['temperature']
        obj.dewpoint = row['dewpoint']
        return obj

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

        clouds = re.findall(r'(FEW|SCT|BKN|OVC)(\d{3})', self.raw)
        if clouds != []:
            self.clouds = clouds
        self.base = self.get_base()
        self.ceiling = self.get_ceiling()

        self.qnh = re.findall(r'Q(\d{4})', self.raw)

        temp_dew = re.findall(r'(\d{2}|M\d{2})\/(\d{2}|M\d{2})', self.raw)
        if temp_dew and temp_dew[0][0] != '//' and temp_dew[0][1] != '//':
            self.temperature = self.get_temperature(temp_dew[0])
            self.dewpoint = self.get_dewpoint(temp_dew[0])

        wx_match = re.findall(r'\s(-|\+)?(VC)?(RA|SN|BR|FG|HZ|TS|DZ|SH|PL|GR|GS|UP)', self.raw)
        if wx_match:
            merged_wx = []
            for wx in wx_match:
                merged_wx.append(f'{wx[0]}{wx[1]}{wx[2]}')
            self.weather = merged_wx

    def get_temperature(self, temp_dew):
        t = temp_dew[0]
        return -int(t[1:]) if t.startswith('M') else int(t)

    def get_dewpoint(self, temp_dew):
        d = temp_dew[1]
        return -int(d[1:]) if d.startswith('M') else int(d)

    def get_ceiling(self):
        if self.clouds is not None:
            for cover, height in self.clouds:
                if cover in ("BKN", "OVC"):
                    return int(height) * 100
        return None

    def get_base(self):
        if self.clouds is not None:
            for cover, height in self.clouds:
                return int(height) * 100
        return None

    def is_vfr(self, min_visibility=1500, min_ceiling=1500):
        vis_ok = self.visibility is None or self.visibility >= min_visibility
        ceiling_ok = self.ceiling is None or self.ceiling >= min_ceiling
        return vis_ok and ceiling_ok

    def is_flyable(self, **kwargs): 
        defaults = {
            "min_visibility": None, 
            "min_ceiling": None, 
            "min_base": None, 
            "max_wind_speed": None, 
            "max_wind_gust": None,
            "bad_weather": None,
            "min_temperature": None,
            "max_temperature": None
        }
        config = {**defaults, **kwargs}
        '''
        Determines if the METAR conditions are suitable for flying.
        :param min_visibility: Minimum visibility in meters.
        :param min_ceiling: Minimum ceiling in meters.
        :param min_base: Minimum cloud base in meters.
        :param max_wind_speed: Maximum wind speed in knots.
        :param max_wind_gust: Maximum wind gust in knots.
        :param bad_weather: List of weather conditions that are considered unsuitable for flying.
        :return: True if conditions are suitable for flying, False otherwise.
        
        If any of the parameters are None, they are ignored in the check.'''
        if config["min_visibility"] is not None:
            vis_ok = self.visibility is None or self.visibility >= config["min_visibility"]
        else:
            vis_ok = True
        if config["min_ceiling"] is not None:
            ceiling_ok = self.ceiling is None or self.ceiling >= config["min_ceiling"]
        else:
            ceiling_ok = True
        if config["min_base"] is not None:
            base_ok = self.base is None or self.base >= config["min_base"]
        else:
            base_ok = True
        if config["max_wind_speed"] is not None:
            wind_ok = self.wind_speed is None or self.wind_speed <= config["max_wind_speed"]
        else:
            wind_ok = True
        if config["max_wind_gust"] is not None:
            gust_ok = self.wind_gust is None or self.wind_gust <= config["max_wind_gust"]
        else:
            gust_ok = True
        if config["bad_weather"] is not None:
            weather_ok = self.weather is None or not any(
                wx in self.weather for wx in config["bad_weather"]
            )
        else:
            weather_ok = True
        if config["min_temperature"] is not None:
            min_temp_ok = self.temperature is None or self.temperature >= config["min_temperature"]
        else:
            min_temp_ok = True
        if config["max_temperature"] is not None:
            max_temp_ok = self.temperature is None or self.temperature <= config["max_temperature"]
        else:
            max_temp_ok = True  

        return vis_ok and ceiling_ok and wind_ok and gust_ok and base_ok and weather_ok and min_temp_ok and max_temp_ok