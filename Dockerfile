FROM python:3.6-alpine3.6

RUN apk add --no-cache --update \
    build-base \
    postgresql-dev

RUN pip install \
    requests \
    psycopg2

ADD main.py main.py

CMD ["python", "-u", "main.py"]
