#!/usr/bin/env python

from os import environ
from prometheus_client import start_http_server, Gauge
from requests import get
from sys import stdin
from time import strftime, localtime, sleep
import json

def log_message(message):
    ts = strftime('%Y-%m-%dT%H:%M:%S')
    print(ts, message)

def get_forecast(api_key, coordinates):
    url = 'https://api.darksky.net/forecast/{}/{}?units=si'
    url = url.format(api_key, coordinates)
    resp = get(url)
    if resp.status_code != 200:
        log_message('error: got {} fetching forecast'.format(resp.status_code))
        return
    log_message('successfully fetched forecast')
    return resp.json()

def set_current(entry, location):
    current_temp.labels(location=location).set(entry['apparentTemperature'])
    current_pop.labels(location=location).set(entry['precipProbability'])

def render_time(timestamp):
    result = strftime('%H%M', localtime(timestamp))
    return result

def render_day(timestamp):
    today = int(strftime('%d'))
    ts_day = int(strftime('%d', localtime(timestamp)))
    if ts_day == today:
        return 'today'
    elif ts_day == (today + 1):
        return 'tomorrow'
    else:
        return strftime('%A', localtime(timestamp))

def set_hourly(entries, location, today):
    for entry in entries:
        hourly_temp.labels(location=location, day=render_day(entry['time']), time=render_time(entry['time'])).set(entry['apparentTemperature'])

def set_daily(entries, location, today):
    for entry in entries:
        daily_temp_high.labels(location=location, day=render_day(entry['time']), time=render_time(entry['time'])).set(entry['apparentTemperatureHigh'])

def main():
    while True:
        data = get_forecast(api_key, coordinates)
        set_current(data['currently'], location)
        set_hourly(data['hourly']['data'], location, int(strftime('%d')))
        set_daily(data['daily']['data'], location, int(strftime('%d')))
        data = None
        sleep(request_interval)

if __name__ == '__main__':
    api_key = environ['DARK_SKY_API_KEY']
    coordinates = environ['DARK_SKY_COORDINATES']
    location = environ['LOCATION_NAME']
    metrics_port = int(environ['METRICS_PORT'])
    request_interval = int(environ['REQUEST_INTERVAL'])

    start_http_server(metrics_port)
    current_temp = Gauge('weather_current_temp', 'Current temperature (feels like)', ['location'])
    current_pop = Gauge('weather_current_pop', 'Current probability of precipitation', ['location'])
    hourly_temp = Gauge('weather_hourly_temp', 'Hourly temperature (feels like)', ['location', 'day', 'time'])
    daily_temp_high = Gauge('weather_daily_temp_high', 'Daily temperature high (feels like)', ['location', 'day', 'time'])
    daily_temp_low = Gauge('weather_daily_temp_low', 'Daily temperature low (feels like)', ['location', 'day', 'time'])

    main()
