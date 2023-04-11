FROM python:3.9-slim

COPY . /archive-db
WORKDIR /archive-db

RUN \
  apt-get update && \
  apt-get install -y git && \
  sed -i -re 's/(port:).*/\1 8787/' config/app.config && \
  python3 -m venv --upgrade-deps .venv && \
  .venv/bin/pip install --upgrade pip && \
  .venv/bin/pip install -e .[test] && \
  mkdir -p /tmp/arteria/archive-db

RUN \
  .venv/bin/nosetests tests/

EXPOSE 8787

CMD [ "/archive-db/.venv/bin/archive-db-ws", "--config=/archive-db/config/", "--debug" ]
