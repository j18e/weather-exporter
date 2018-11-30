#!/usr/bin/env python

"""Weather Exporter

Usage:
    weather-exporter -h | --help
    weather-exporter --api-key <api_key> --coordinates <coordinates> [options]
    weather-exporter test <filename>

Options:
    -h, --help                      shows the help
    --api-key <api_key>              Dark Sky API key
    --coordinates <coordinates>     Coordinates for forecast (49.38383,11.38383)
    --refresh-interval <interval>   Seconds to wait between fetching weather data
"""

from datetime import datetime, timedelta
from docopt import docopt
from json import loads
from requests import get
from time import sleep

from prometheus_client import start_http_server
from prometheus_client.core import REGISTRY, GaugeMetricFamily

def log_message(message):
    time_format = "%Y-%m-%dT%H:%M:%S"
    timestamp = datetime.now()
    print(timestamp.strftime(time_format), message)

def get_forecast():
    if args['test']:
        with open(args['<filename>'], 'r') as stream:
            data = loads(stream.read())
        data = populate_timestamps(data)
    else:
        url = 'https://api.darksky.net/forecast/{}/{}?units=si'
        url = url.format(args['--api-key'], args['--coordinates'])
        resp = get(url)
        if resp.status_code != 200:
            log_message('error: got {} fetching forecast'.format(resp.status_code))
            return
        data = resp.json()
    log_message('successfully fetched forecast')
    return data

def populate_timestamps(data):
    now = datetime.now()
    daily = now.replace(hour=0, minute=0, second=0, microsecond=0)
    hourly = now.replace(minute=0, second=0, microsecond=0)
    data['currently']['time'] = int(now.strftime("%s"))
    for day in data['daily']['data']:
        day['time'] = int(daily.strftime("%s"))
        daily += timedelta(days=1)
    for hour in data['hourly']['data']:
        hour['time'] = int(hourly.strftime("%s"))
        hourly += timedelta(hours=1)
    return data

def render_time(timestamp):
    timestamp = datetime.fromtimestamp(timestamp)
    time = timestamp.strftime("%H:%M")
    now = datetime.now()
    if timestamp.day == now.day:
        day = 'today'
    elif timestamp.day == (now.day + 1):
        day = 'tomorrow'
    else:
        day = timestamp.strftime("%A")
    return day, time

def build_hourly(entries):
    results = []
    for entry in entries:
        day, time = render_time(entry['time'])
        results.append({
            'day': day,
            'time': time,
            'temp': entry['apparentTemperature'],
        })
    return results

def build_daily(entries):
    results = []
    for entry in entries:
        day, time = render_time(entry['time'])
        results.append({
            'day': day,
            'temp_high': entry['apparentTemperatureHigh'],
            'temp_low': entry['apparentTemperatureLow'],
            'summary': entry['summary']
        })
    return results

class DarkSkyCollector(object):
    def collect(self):
        data = get_forecast(args)

        TEMP = GaugeMetricFamily('forecast_temperature',
            'forecasted temperature', labels=['day', 'time'])
        TEMP.add_metric(['today', 'now'],
            data['currently']['apparentTemperature'])

        for hour in build_hourly(data['hourly']['data']):
            TEMP.add_metric([hour['day'], hour['time']], hour['temp'])

        yield TEMP


if __name__ == '__main__':
    args = docopt(__doc__, options_first=True)
    REGISTRY.register(DarkSkyCollector())
    start_http_server(8080)
    while True:
        sleep(1)

