import json
import logging
import sys
from datetime import datetime

import pytz
import requests
from TwitterAPI import TwitterAPI
from requests.models import Response
from tinydb import TinyDB, Query

twitter_key_file = 'twitter.json'
user_file = 'users.json'
alert_db = '/gtaupdate/db.json'

default_timezone = 'America/Toronto'

threshold = 600

url = 'https://c4s.torontopolice.on.ca/arcgis/rest/services/CADPublic/C4S/MapServer/0/query'

time_format = '%Y.%m.%d %H:%M:%S'


def event(user_id, timestamp, typ_eng, nearby_roads):
    return {"event": {
        "type": "message_create",
        "message_create": {
            "target": {
                "recipient_id": user_id
            },
            "message_data": {
                "text": f"{timestamp}: {typ_eng} at {nearby_roads}",
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


def load(object_id, user_id):
    db: TinyDB = TinyDB(alert_db)
    q: Query = Query()
    return db.search(q.object_id == object_id and q.user_id == user_id)


def insert(object_id, user_id):
    db: TinyDB = TinyDB(alert_db)
    db.insert({'object_id': object_id, 'user_id': user_id})


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

    tz = pytz.timezone(default_timezone)
    alerts = []
    logging.info("Starting!")
    users = users()
    features = query(users.keys())['features']
    cur_time = datetime.now(tz=tz)
    for feature in features:
        attr = feature['attributes']
        dgroup = attr['DGROUP']

        interested_users = users[dgroup]
        if interested_users:
            at_scene = datetime.strptime(attr['ATSCENE_TS'], time_format).astimezone(tz=tz)
            for u in interested_users:
                user_id, roads = u[0], u[1]
                prev = load(attr['OBJECTID'], user_id)
                if not prev and (cur_time - at_scene).total_seconds() < threshold and any(
                        s.lower() in attr['XSTREETS'].lower() for s in roads):
                    alerts.append((user_id, attr['ATSCENE_TS'], attr['TYP_ENG'], attr['XSTREETS'], attr['OBJECTID']))

    api = twitter_api()
    logging.info(f"Sending {len(alerts)} alerts")
    for alert in alerts:
        logging.info("Sending %r", alert)
        r = api.request('direct_messages/events/new', json.dumps(event(*alert[:-1])))
        insert(alert[4], alert[0])
