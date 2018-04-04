SNPSEQ Archive DB
==================

A self contained (Tornado) REST service that serves as a frontend for a simple SQL db that contains the state of our uploads, verifications and removals done by other SNPSEQ archive services.  

Trying it out
-------------

    python3 -m pip install pipenv
    pipenv install --deploy

Try running it:

     pipenv run ./archive-db-ws --config=config/ --port=8888 --debug

And then you can find a simple API documentation by going to:

    http://localhost:8888/api/1.0

Running tests
-------------

    pipenv install --dev
    pipenv run nosetests tests/


REST endpoints
--------------

Creating a new Upload (and associated Archive if none exists): 

    curl -i -X "POST" -d '{"path": "/path/to/directory/", "host": "my-host", "description": "my-descr"}' http://localhost:8888/api/1.0/upload

Creating a new Verification (and associated Archive if none exists):
    
    curl -i -X "POST" -d '{"path": "/path/to/directory/", "host": "my-host", "description": "my-descr"}' http://localhost:8888/api/1.0/verification

Getting a randomly picked Archive that has been uploaded within a certain timespan, but never verified before: 

    curl -i -X "GET" -d '{"age": "7", "safety_margin": "3"}' http://localhost:8888/api/1.0/randomarchive
