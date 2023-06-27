SNPSEQ Archive DB
==================

A self contained (Tornado) REST service that serves as a frontend for a simple SQL db that contains the state of our
uploads, verifications and removals done by other SNPSEQ archive services.

Trying it out
-------------

    python3 -m venv --upgrade-deps .venv
    source .venv/bin/activate
    pip install .

Try running it:

     archive-db-ws --config=config/ --debug

And then you can find a simple API documentation by going to:

    http://localhost:8888/api

Running tests
-------------

    source .venv/bin/activate
    pip install -e .[test]
    nosetests tests/


REST endpoints
--------------

Creating a new Upload (and associated Archive if none exists): 

    curl -i -X "POST" -d '{"path": "/path/to/directory/", "host": "my-host", "description": "my-descr"}' http://localhost:8888/api/1.0/upload

Creating a new Verification (and associated Archive if none exists):
    
    curl -i -X "POST" -d '{"path": "/path/to/directory/", "host": "my-host", "description": "my-descr"}' http://localhost:8888/api/1.0/verification

Getting a randomly picked Archive that has been uploaded within a certain timespan, but never verified before: 

    curl -i -X "GET" -d '{"age": "7", "safety_margin": "3"}' http://localhost:8888/api/1.0/randomarchive

Print the records from the database:

    curl -i -X "GET" http://localhost:8888/api/1.0/view

Print the N (positive integer) latest uploads from the database:

    curl -i -X "GET" http://localhost:8888/api/1.0/view/N

Query the database for uploads matching specific criteria:
    
    curl -i -X "POST" -d '{"host": "biotank", "uploaded_before": "2023-03-01", "verified": "False"}' http://localhost:8888/api/1.0/query

Docker container
----------------

For testing purposes, you can also build a Docker container using the resources in the docker/ folder:

    # build and start Docker container
    docker/up

This will build and start a Docker container that the archive-db service and listens on port 8787:

    # interact with archive-verify service on port 8787
    curl 127.0.0.1:8787/api/1.0/version
        #   {"version": "1.2.0"}

The container log output can be followed:

    # follow the container log output (Ctrl+C to stop)
    docker/log

The docker container can be stopped and removed:

    # stop and remove the running docker container
    docker/down
