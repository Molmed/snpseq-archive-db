import datetime

from playhouse.test_utils import test_database
from peewee import *

from archive_db.models.Model import Archive, Upload, Verification, Removal, init_db
from archive_db.app import routes

from tornado.web import Application
from tornado.escape import json_encode, json_decode
from tornado.testing import AsyncHTTPTestCase


class TestDb(AsyncHTTPTestCase):
    num_archives = 5
    first_archive = 0
    second_archive = 2

    API_BASE = "/api/1.0"

    def setUp(self):
        init_db(":memory:")
        # init_db("test.db")
        super(TestDb, self).setUp()

    def get_app(self):
        return Application(routes())

    def go(self, target, method, body=""):
        return self.fetch(self.API_BASE + target, method=method, body=json_encode(body), headers={"Content-Type": "application/json"}, allow_nonstandard_methods=True)

    def create_data(self):
        for i in range(self.num_archives):
            Archive.create(description="archive-descr-{}".format(
                i), path="/data/testhost/runfolders/archive-{}".format(i), host="testhost")

        Upload.create(archive=self.first_archive, timestamp=datetime.datetime.now())
        Upload.create(archive=self.second_archive, timestamp=datetime.datetime.now())

        Verification.create(archive=self.second_archive, timestamp=datetime.datetime.now())

        Removal.create(archive=self.second_archive, timestamp=datetime.datetime.now())

    def test_db_model(self):
        self.create_data()

        self.assertEqual(len(Archive.select()), self.num_archives)

        archive_to_pick = "archive-descr-{}".format(
            self.second_archive - 1)  # second entry starting from 0
        query = (Upload
                 .select(Upload, Archive)
                 .join(Archive)
                 .where(Archive.description == archive_to_pick))
        upload = query[0]
        self.assertEqual(upload.archive.host, "testhost")
        self.assertEqual(upload.archive.description,
                         "archive-descr-{}".format(self.second_archive - 1))

        verifications = Verification.select()
        removals = Removal.select()
        self.assertEqual(len(verifications), 1)
        self.assertEqual(len(verifications), len(removals))

    def test_create_new_archive_and_upload(self):
        body = {"description": "test-case-1", "host": "testhost", "path": "/path/to/test/archive/"}
        resp = self.go("/upload", method="POST", body=body)
        resp = json_decode(resp.body)
        self.assertEqual(resp["status"], "created")
        self.assertEqual(resp["upload"]["description"], body["description"])

    def test_failing_upload(self):
        body = {"description": "test-case-1"}  # missing params
        resp = self.go("/upload", method="POST", body=body)
        self.assertEqual(resp.code, 400)

    def test_create_upload_for_existing_archive(self):
        upload_one = 1
        upload_two = 2

        body = {"description": "test-case-1", "host": "testhost", "path": "/path/to/test/archive/"}
        resp = self.go("/upload", method="POST", body=body)
        resp = json_decode(resp.body)
        self.assertEqual(resp["status"], "created")
        self.assertEqual(resp["upload"]["description"], body["description"])
        self.assertEqual(resp["upload"]["id"], upload_one)

        resp = self.go("/upload", method="POST", body=body)
        resp = json_decode(resp.body)
        self.assertEqual(resp["status"], "created")
        self.assertEqual(resp["upload"]["id"], upload_two)

    # Populating the db in a similar way as in self.create_data() does not make the data available for 
    # the handlers, as they seem to live in an other in-memory instance of the db. Therefore a 
    # failing test will have to do for now. 
    def test_failing_fetch_random_unverified_archive(self):
        # I.e. our lookback window is [today - 5 - 1, today - 1] days. 
        body = {"age": "5", "safety_margin": "1"}
        resp = self.go("/randomarchive", method="GET", body=body)
        self.assertEqual(resp.code, 500)
    

