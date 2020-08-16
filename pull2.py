import json
import logging
import sys
from datetime import datetime
from datetime import timedelta

from tinydb import TinyDB, Query
import requests
from TwitterAPI import TwitterAPI
from requests.models import Response

twitter_key_file = 'twitter.json'
user_file = 'users.json'
default_timezone = 'America/Toronto'

url = 'https://c4s.torontopolice.on.ca/arcgis/rest/services/CADPublic/C4S/MapServer/0/query'

time_format1 = '%I:%M %p'
time_format2 = '%b-%d% I:%M %p'

hour_start = 7
hour_end = 23


def event(user_id, timestamp, message):
    return {"event": {
        "type": "message_create",
        "message_create": {
            "target": {
                "recipient_id": user_id
            },
            "message_data": {
                "text": f"{timestamp}: {message}",
            }
        }
    }}


def twitter_api():
    with open(file=twitter_key_file) as f:
        data = json.load(f)
    return TwitterAPI(data['api_key'], data['api_secret_key'], data['access_token'], data['access_token_secret'])


def users():
    division_users = {}
    with open(file=user_file) as f:
        data = json.load(f)

        for u in data['users']:
            for d in u['divisions']:
                division_users.setdefault(d, []).append((u['id'], u['streets']))

    return division_users

def load():
    db: TinyDB = TinyDB('/gtaupdate/history.json')
    Alert = Query()
    return db.all()

def insert(object_id, user_id):
    db: TinyDB = TinyDB('/gtaupdate/history.json')
    db.insert({'object_id': object_id, 'user_id': user_id})


def get_time(now, time_string):
    try:
        t = datetime.strptime(time_string, time_format1).replace(tzinfo=zone)
        return now.replace(hour=t.hour, minute=t.minute)
    except ValueError:
        try:
            t = datetime.strptime(time_string, time_format2).replace(tzinfo=zone)
            return (now - timedelta(days=1)).replace(hour=t.hour, minute=t.minute)
        except ValueError:
            pass


def query(divisions):
    where_query = ' or '.join(map(lambda a: f'DGROUP=\'{a}\'', divisions))

    response: Response = requests.get(url, params={'f': 'json', 'returnGeometry': False,
                                                   'outFields': 'ATSCENE_TS,DGROUP,TYP_ENG,XSTREETS,OBJECTID',
                                                   'where': where_query})
    if response.ok:
        return response.json()
    else:
        logging.error(response.text)
    return None


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s.%(msecs)03d %(levelname)s %(message)s',
                        datefmt='%Y-%m-%d,%H:%M:%S', level=logging.DEBUG, stream=sys.stdout)

    alerts = []
    logging.info("Starting!")
    insert("hello", "abc")
    users = users()
    features = query(users.keys())['features']
    for feature in features:
        attr = feature['attributes']
        nearby = attr['XSTREETS']
        if any(s in nearby for s in []):
            alerts.append((attr['ATSCENE_TS'], attr['TYPE_ENG'], attr['XSTREETS']))

    api = twitter_api()
    # for alert in alerts:
    #     logging.info("Sending %r", alert)
    #     r = api.request('direct_messages/events/new', json.dumps(event(*alert)))
