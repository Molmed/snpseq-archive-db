FROM python:3.9-slim

COPY . /archive-db
WORKDIR /archive-db

RUN \
  apt-get update && \
  apt-get install -y git && \
  python3 -m venv --upgrade-deps .venv && \
  .venv/bin/pip install --upgrade pip && \
  .venv/bin/pip install -e .[test] && \
  mkdir -p /tmp/arteria/archive-db

RUN \
  .venv/bin/nosetests tests/

CMD [ ".venv/bin/archive-db-ws", "--config=/archive-db/config/", "--debug" ]
