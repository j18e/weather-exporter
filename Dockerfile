FROM python:3.6-alpine3.6

RUN pip install \
    prometheus_client \
    requests \
    docopt

ADD main.py main.py

ENTRYPOINT ["python", "-u", "main.py"]
