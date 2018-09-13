#!/usr/bin/env python

"""Weather Exporter

Usage:
    weather-exporter -h | --help
    weather-exporter [options]

Options:
    -h, --help                      shows the help
    --db-host <hostname>            Hostname of Postgres forecast database
    --db-user <username>            Database user
    -api-key <api_key>              Dark Sky API key
    --coordinates <coordinates>     Coordinates for forecast (49.38383,11.38383)
    --refresh-interval <interval>   Seconds to wait between fetching weather data
    -f <filename>                   use a json file containing weather data instead of querying the Dark Sky API
"""

from datetime import datetime, timedelta
from docopt import docopt
from json import loads
from os import environ
from requests import get
from time import sleep
import psycopg2

def log_message(message):
    time_format = "%Y-%m-%dT%H:%M:%S"
    timestamp = datetime.now()
    print(timestamp.strftime(time_format), message)

def get_forecast(args):
    if args['-f']:
        with open(args['-f'], 'r') as stream:
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
        day = 'Today'
    elif timestamp.day == (now.day + 1):
        day = 'Tomorrow'
    else:
        day = timestamp.strftime("%A")
    return (day, time)

def insert_hourly(cur, columns):
    command = """INSERT INTO HOURLY ("""
    for c in columns:
        command+='{},'.format(c[0])
    command = '{}) VALUES ('.format(command[:-1])
    for c in columns:
        if c[2] == 'TEXT':
            command+="'{}',".format(c[1])
        else:
            command+='{},'.format(c[1])
    command = '{});'.format(command[:-1])
    cur.execute(command)

def init_table(cur, schema):
    schema_string = ''
    for c in schema:
        schema_string+='{} {},'.format(c[0], c[1])
    schema_string = schema_string[:-1]
    command =  """CREATE TABLE IF NOT EXISTS hourly ({});""".format(schema_string)
    cur.execute(command)

def clean_tables(cur, tables):
    for table in tables:
        cur.execute("""DELETE FROM {};""".format(table))

def main(args):
    conn = psycopg2.connect(dbname=args['--db-user'], user=args['--db-user'], host=args['--db-host'])
    cur = conn.cursor()
    while True:
        data = get_forecast(args)
        clean_tables(cur, ['hourly'])
        update_hourly(cur, data['hourly']['data'])
        conn.commit()
        sleep(int(args['--refresh-interval']))
    cur.close()
    conn.close()

def update_hourly(cur, data):
    schema = [
        ('row', 'SMALLINT'),
        ('day', 'TEXT'),
        ('hour', 'TEXT'),
        ('summary', 'TEXT'),
        ('precipProbability', 'float(2)'),
        ('precipIntensity', 'float(2)'),
        ('windSpeed', 'float(2)'),
        ('humidity', 'float(2)'),
        ('temperature', 'float(2)'),
    ]
    init_table(cur, schema)
    count = 0
    for entry in data['hourly']['data']:
        fields = []
        for c in hourly_schema:
            if c[0] in entry:
                fields.append((c[0], entry[c[0]], c[1]))
            elif c[0] == 'day':
                fields.append((c[0], render_time(entry['time'])[0], c[1]))
            elif c[0] == 'hour':
                fields.append((c[0], render_time(entry['time'])[1], c[1]))
            elif c[0] == 'row':
                fields.append((c[0], count, c[1]))
        insert_hourly(cur, fields)
        count+=1


if __name__ == '__main__':
    args = docopt(__doc__, options_first=True)
    main(args)

