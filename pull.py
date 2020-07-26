import json
import logging
import sys
from datetime import datetime
from datetime import timedelta

import pytz
import requests
from TwitterAPI import TwitterAPI
from bs4 import BeautifulSoup

twitter_key_file = 'twitter.json'
user_file = 'users.json'
default_timezone = 'America/Toronto'
second_threshold = 120
url = 'https://gtaupdate.com/gta/'
time_format1 = '%I:%M %p'
time_format2 = '%b-%d% I:%M %p'

hour_start = 8
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
                division_users.setdefault(d, set()).add(u['id'])

    return division_users


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


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s.%(msecs)03d %(levelname)s %(message)s',
                        datefmt='%Y-%m-%d,%H:%M:%S', level=logging.DEBUG, stream=sys.stdout)

    logging.info("Starting!")

    page = requests.get(url)
    soup = BeautifulSoup(page.text, 'html.parser')
    rows = soup.find_all('tr')

    alerts = []
    zone = pytz.timezone(default_timezone)
    cur_time = datetime.now(tz=zone)

    divisions = users()
    for row in rows:
        children = list(row.children)
        division = children[1]
        if division.next and division.next.name and division.next.text in divisions:
            time, note = children[0].next, children[2].next
            time_of_event = get_time(cur_time, time)
            if time_of_event.hour < hour_start or time_of_event.hour > hour_end:
                break
            delta = cur_time - time_of_event
            if delta.seconds > second_threshold:
                break
            alerts.extend((u, time, note) for u in divisions[division.next.text])

    api = twitter_api()
    for alert in alerts:
        logging.info("Sending %r", alert)
        r = api.request('direct_messages/events/new', json.dumps(event(*alert)))
