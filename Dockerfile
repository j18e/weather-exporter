FROM python:3.6-alpine3.6

RUN apk add --no-cache --update \
    build-base \
    postgresql-dev

RUN pip install \
    requests \
    psycopg2 \
    docopt

ADD main.py main.py

ENTRYPOINT ["python", "-u"]
CMD ["main.py"]
