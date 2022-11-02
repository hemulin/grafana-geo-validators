from datetime import datetime
import json
import re
import time
from uuid import uuid4

from tqdm import tqdm
import sqlite3
import requests


def fetch_validators_ips_and_account():
    r = requests.get('https://0lexplorer.io/api/webmonitor/vitals')
    data = r.json()
    data_to_be_inserted = []
    for entry in data['chain_view']['validator_view']:
        entry_organized_data = {
            'account_address': entry['account_address'],
            'validator_ip': entry['validator_ip'],
            'vfn_ip': entry['vfn_ip']
        }
        data_to_be_inserted.append(entry_organized_data)
    return data_to_be_inserted

def get_ip_location(ip):
    url = 'https://freeipapi.com/api/json/' + ip
    response = requests.get(url)
    if response.status_code == 429:
        time.sleep(60) # rate limit reached, sleep for 1 minute and retry
        response = requests.get(url)
    data = response.json()
    return (data['latitude'], data['longitude'])

def find_and_add_ip_geolocation(data_to_insert):
    data_with_geolocation = []
    # db = open('db.sql', 'w')
    for entry in tqdm(data_to_insert):
        location = get_ip_location(entry['validator_ip'])
        if location:
            (lat, lon) = location
            entry['validator_lat'] = lat
            entry['validator_lon'] = lon
        location = get_ip_location(entry['vfn_ip'])
        if location:
            (lat, lon) = location
            entry['vfn_lat'] = lat
            entry['vfn_lon'] = lon
        data_with_geolocation.append(entry)

    return data_with_geolocation

def connect_to_sqlite():
    try:
        conn = sqlite3.connect('validators_geo.db')
        conn.row_factory = sqlite3.Row
    except Exception as e:
        print("Error connecting to db: {}".format(e))
        raise(e)
    curr = conn.cursor()
    curr.execute('''CREATE TABLE IF NOT EXISTS validators_geo_data(
        id text PRIMARY KEY,
        account text NOT NULL,
        validator_ip text NOT NULL,
        vfn_ip text NOT NULL,
        validator_lat text NOT NULL,
        validator_lon text NOT NULL,
        vfn_lat text NOT NULL,
        vfn_lon text NOT NULL,
        timestamp text NOT NULL);
        ''')
    conn.commit()
    return conn, curr

def insert_validators_geo_data(conn, curr, validators_geo_data, now_timestamp):
    curr.execute('INSERT INTO validators_geo_data VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)', (str(uuid4()), validators_geo_data['account_address'], validators_geo_data['validator_ip'], validators_geo_data['vfn_ip'], validators_geo_data['validator_lat'], validators_geo_data['validator_lon'], validators_geo_data['vfn_lat'], validators_geo_data['vfn_lon'], now_timestamp))
    conn.commit()

def collect_geo_data():
    initial_data = fetch_validators_ips_and_account()
    data_with_geolocation = find_and_add_ip_geolocation(initial_data)
    conn, curr = connect_to_sqlite()
    now_timestamp = datetime.now().timestamp()
    for entry in data_with_geolocation:
        insert_validators_geo_data(conn, curr, entry, now_timestamp)
    conn.close()
    print("done")


def main():
    collect_geo_data()

if __name__ == "__main__":
    main()