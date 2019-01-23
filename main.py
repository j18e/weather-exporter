#!/usr/bin/env python3

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
    --port <port>                   TCP port on which to expose Prometheus metrics
"""

from prometheus_client import start_http_server, Gauge
from datetime import datetime, timedelta
from docopt import docopt
from json import loads
from requests import get
from time import sleep
from datetime import datetime

def log_message(message):
    time_format = "%Y-%m-%dT%H:%M:%S"
    timestamp = datetime.now()
    print(timestamp.strftime(time_format), message)

def get_forecast(args):
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

def purge_labels(metric, old, new):
    new_labels = [e['day'] + e['time'] for e in new]
    for entry in old:
        if entry['day'] + entry['time'] not in new_labels:
            metric.remove(entry['day'], entry['time'])

def main(args):
    if args['--refresh-interval']:
        refresh_interval = int(args['--refresh-interval'])
    else:
        refresh_interval = 120
    if args['--port']:
        metrics_port = int(args['--port'])
    else:
        metrics_port = 8080
    start_http_server(metrics_port)
    TEMP = Gauge('forecast_temperature', 'forecasted temperature in degrees celcius', ['day', 'time'])
    old_hourly = []
    while True:
        forecast_data = get_forecast(args)

#        hourly = build_hourly(forecast_data['hourly']['data'])
#        purge_labels(TEMP, old_hourly, hourly)
#        TEMP.labels('today', 'now').set(forecast_data['currently']['apparentTemperature'])
#        for entry in hourly:
#            TEMP.labels(entry['day'], entry['time']).set(entry['temp'])
#        old_hourly = hourly

        daily = build_daily(forecast_data['daily']['data'])
        for e in daily:
            print(e)

        sleep(refresh_interval)

if __name__ == '__main__':
    args = docopt(__doc__, options_first=True)
    main(args)

