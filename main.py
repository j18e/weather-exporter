#!/usr/bin/env python3

from datetime import datetime
from datetime import datetime, timedelta
from json import loads
from os import environ
from requests import get, post
from sys import argv
from time import sleep

desired_metrics = [
    "temperature",
    "apparentTemperature",
    "precipProbability",
    "precipIntensity",
    "humidity",
    "temperatureHigh",
    "temperatureLow",
    "temperatureHighTime",
    "temperatureLowTime",
    "apparentTemperatureHigh",
    "apparentTemperatureLow",
    "apparentTemperatureHighTime",
    "apparentTemperatureLowTime",
]


def log_message(message):
    time_format = "%Y-%m-%dT%H:%M:%S"
    timestamp = datetime.now()
    print(timestamp.strftime(time_format), message)

def get_forecast():
    if len(argv) > 1 and argv[1] == "test":
        with open(argv[2], 'r') as stream:
            data = loads(stream.read())
        data = populate_timestamps(data)
    else:
        url = 'https://api.darksky.net/forecast/{}/{}?units=si'
        url = url.format(environ["API_KEY"], environ["COORDINATES"])
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

def build_measurement(prefix, entry):
    timestamp = entry["time"] * 1000000000
    template = "{} value={} {}\n"
    result = ""
    for key, value in entry.items():
        if key in desired_metrics:
            result += template.format(prefix + key, value, timestamp)
    result += template.format(prefix + "summary",
        '"{}"'.format(entry["summary"]), timestamp)
    return result

def write_data(database, payload):
    path = "/write?db=" + database
    resp = post(environ["INFLUXDB_URI"] + path, data=payload)
    print(resp.text)
    resp.raise_for_status()

def main():
    if "REFRESH_INTERVAL" in environ:
        refresh_interval = int(environ["REFRESH_INTERVAL"])
    else:
        log_message("REFRESH_INTERVAL not set. using 120 as refresh interval")
        refresh_interval = 120
    database = "weather_forecast"
    resp = post(environ["INFLUXDB_URI"] + "/query", params={"q": "CREATE DATABASE {}".format(database)})
    resp.raise_for_status()
    forecast_data = get_forecast()
    results = ""
    for entry in forecast_data["hourly"]["data"]:
        results += build_measurement("hourly_", entry)
    for entry in forecast_data["daily"]["data"]:
        results += build_measurement("daily_", entry)
    results += build_measurement("current_", forecast_data["currently"])
    print(results)
    write_data(database, results)
    exit()

if __name__ == '__main__':
    main()
