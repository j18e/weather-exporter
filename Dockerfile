FROM python:3.6-alpine3.6

RUN pip install \
    prometheus_client \
    requests

ADD main.py main.py

CMD ["python", "-u", "main.py"]
