FROM python:3.8.5-alpine

COPY pull.py requirements.txt users.json twitter.json ./
COPY gtaupdate-cron /etc/crontabs/root
RUN pip install -r requirements.txt
CMD ["crond", "-f", "-d", "8"]