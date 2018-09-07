#!/usr/bin/env python

from os import environ
from requests import get
from time import strftime, localtime, sleep
import psycopg2

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
        day = 'Today'
    elif ts_day == (today + 1):
        day = 'Tomorrow'
    else:
        day = strftime('%A', localtime(timestamp))
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

def main(api_key, coordinates, refresh_interval, db_user, db_host):
    conn = psycopg2.connect(dbname=db_user, user=db_user, host=db_host)
    cur = conn.cursor()
    hourly_schema = [
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
    init_table(cur, hourly_schema)
    while True:
        data = get_forecast(api_key, coordinates)
        clean_tables(cur, ['hourly'])
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
        conn.commit()
        sleep(refresh_interval)
    cur.close()
    conn.close()

if __name__ == '__main__':
    main(
        environ['DARK_SKY_API_KEY'],
        environ['COORDINATES'],
        int(environ['REFRESH_INTERVAL']),
        environ['DB_USER'],
        environ['DB_HOST']
    )
