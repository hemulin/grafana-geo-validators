from datetime import datetime
import json
import re
import time
from uuid import uuid4

from tqdm import tqdm
import sqlite3
import requests

def get_stream(url):
    output = ''
    s = requests.Session()

    try:
        with s.get(url, headers=None, stream=True) as resp:
            for line in resp.iter_lines():
                if line:
                    output += line.decode('utf-8')
                    break # Need only one stream to figure validators data
    except Exception:
        return None
    # write_lines_to_file(output)
    return output

def write_lines_to_file(lines):
    with open('/tmp/vitals_response.txt', 'w') as fin:
        for line in lines:
            fin.write(line)


def extract_validators_data(output):
    p = re.compile('"account_address":"[a-zA-Z0-9\.]+"|"validator_ip":"[0-9\.]+"|"vfn_ip":"[0-9\.]+"')
    res = p.findall(output)
    return res

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

def organize_validators_data(validators_data):
    ret = []
    for i in range(0, len(validators_data), 3):
        entry = {}
        validator_account = validators_data[i].split(':')[1][1:-1]
        validator_ip = validators_data[i+1].split(':')[1][1:-1]
        vfn_ip = validators_data[i+2].split(':')[1][1:-1]
        entry['account'] = validator_account
        entry['validator_ip'] = validator_ip
        entry['vfn_ip'] = vfn_ip
        ret.append(entry)
    return ret

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
    curr.execute('INSERT INTO validators_geo_data VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)', (str(uuid4()), validators_geo_data['account'], validators_geo_data['validator_ip'], validators_geo_data['vfn_ip'], validators_geo_data['validator_lat'], validators_geo_data['validator_lon'], validators_geo_data['vfn_lat'], validators_geo_data['vfn_lon'], now_timestamp))
    conn.commit()

######################################################

def write_json_to_file(output):
    with open('/tmp/vitals_response.json', 'w') as fin:
        json.dump(output, fin, indent=4, sort_keys=True)

def fetch_validators_ips(conn, curr):
    curr.execute('SELECT DISTINCT validator_ip FROM validators_geo_data')
    validator_ips = curr.fetchall()
    return [x[0] for x in validator_ips]

def get_validator_state(ip):
    url = 'http://'+ ip +':3030/vitals'
    output = get_stream(url)
    if output is None:
        return None
    jsons = output[5:]
    output = json.loads(jsons)
    write_json_to_file(output)
    return state

######################################################

def collect_geo_data():
    url = 'https://0lexplorer.io/api/webmonitor/vitals'
    output = get_stream(url)
    validators_data = extract_validators_data(output)
    initial_data = organize_validators_data(validators_data)
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