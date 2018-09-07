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

def render_time(timestamp):
    time = strftime('%H:%M', localtime(timestamp))
    today = int(strftime('%d'))
    ts_day = int(strftime('%d', localtime(timestamp)))
    if ts_day == today:
        day = 'today'
    elif ts_day == (today + 1):
        day = 'tomorrow'
    else:
        day = strftime('%A', localtime(timestamp))
    return (day, time)

def set_current(entry, location):
    current.labels(
        location=location,
        summary=entry['summary'],
        humidity=entry['humidity'],
        pop=entry['precipProbability'],
        precip_mmph=entry['precipIntensity'],
        wind_mps=entry['windSpeed']
    ).set(entry['temperature'])

def set_hourly(entries, location):
    counter = 0
    for entry in entries:
        if counter < 10:
            order = '0' + str(counter)
        else:
            order = str(counter)
        hourly.labels(
            order=order,
            location=location,
            summary=entry['summary'],
            day=render_time(entry['time'])[0],
            time=render_time(entry['time'])[1],
            humidity=entry['humidity'],
            pop=entry['precipProbability'],
            precip_mmph=entry['precipIntensity'],
            wind_mps=entry['windSpeed']).set(
                entry['apparentTemperature'])
        counter+=1

def set_daily(entries, location):
    counter = 0
    for entry in entries:
        if counter < 10:
            order = '0' + str(counter)
        else:
            order = str(counter)
    for entry in entries:
        daily.labels(
            order=order,
            location=location,
            day=render_time(entry['time'])[0],
            summary=entry['summary'],
            humidity=entry['humidity'],
            pop=entry['precipProbability'],
            precip_mmph=entry['precipIntensity'],
            wind_mps=entry['windSpeed'],
            temp_high=entry['apparentTemperatureHigh'],
            temp_high_time=entry['apparentTemperatureHighTime'],
            temp_low=entry['apparentTemperatureLow'],
            temp_low_time=entry['apparentTemperatureLowTime']
        ).set(entry['apparentTemperatureLow'])
        counter+=1

def main():
    while True:
        data = get_forecast(api_key, coordinates)
        set_current(data['currently'], location)
        set_hourly(data['hourly']['data'], location)
        set_daily(data['daily']['data'], location)
        data = None
        sleep(request_interval)

if __name__ == '__main__':
    api_key = environ['DARK_SKY_API_KEY']
    coordinates = environ['DARK_SKY_COORDINATES']
    location = environ['LOCATION_NAME']
    metrics_port = int(environ['METRICS_PORT'])
    request_interval = int(environ['REQUEST_INTERVAL'])

    start_http_server(metrics_port)
    current = Gauge(
        'weather_current',
        'Current weather conditions. Value is the apparent temperature.',
        ['location', 'summary', 'humidity', 'pop', 'precip_mmph', 'wind_mps']
    )
    hourly = Gauge(
        'weather_hourly', 'Hourly weather forecast',
        ['order', 'location', 'summary', 'day', 'time', 'humidity', 'pop',
         'precip_mmph', 'wind_mps']
    )
    daily = Gauge(
        'weather_daily',
        'Daily weather forecast. Value is the apparent low temperature',
        ['order', 'location', 'day', 'summary', 'humidity', 'pop',
         'precip_mmph', 'wind_mps', 'temp_high', 'temp_high_time',
         'temp_low', 'temp_low_time']
    )

    main()
