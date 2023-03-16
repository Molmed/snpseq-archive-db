import datetime

from playhouse.test_utils import test_database
from peewee import *

import archive_db.handlers.DbHandlers
from archive_db.models.Model import Archive, Upload, Verification, Removal, init_db
from archive_db.app import routes

from tornado.web import Application
from tornado.escape import json_encode, json_decode
from tornado.testing import AsyncHTTPTestCase


class TestDb(AsyncHTTPTestCase):
    num_archives = 5
    first_archive = 1
    second_archive = 3

    API_BASE = "/api/1.0"

    def setUp(self):
        init_db(":memory:")
        # init_db("test.db")
        super(TestDb, self).setUp()

    def get_app(self):
        return Application(routes())

    def go(self, target, method, body=None):
        return self.fetch(
            self.API_BASE + target,
            method=method,
            body=json_encode(body),
            headers={"Content-Type": "application/json"},
            allow_nonstandard_methods=True)

    def create_data(self):
        now = datetime.datetime.now()
        archives = [
            {
                "description": f"archive-descr-{i}",
                "path": f"/data/testhost/runfolders/archive-{i}",
                "host": "testhost",
                "uploaded": now.isoformat() if i in [
                    self.first_archive, self.second_archive] else None,
                "verified": now.isoformat() if i == self.second_archive else None,
                "removed": now.isoformat() if i == self.second_archive else None
            }
            for i in range(self.num_archives)
        ]
        for i, archive in enumerate(archives):
            Archive.create(
                description=archive["description"],
                path=archive["path"],
                host=archive["host"]
            )
            for (tbl, key) in zip(
                    [Upload, Verification, Removal],
                    ["uploaded", "verified", "removed"]):
                if archive[key]:
                    tbl.create(
                        archive=int(i+1),
                        timestamp=now
                    )

        return archives

    def test_db_model(self):
        self.create_data()

        self.assertEqual(len(Archive.select()), self.num_archives)

        archive_to_pick = "archive-descr-{}".format(
            self.second_archive)  # second entry starting from 0
        query = (Upload
                 .select(Upload, Archive)
                 .join(Archive)
                 .where(Archive.description == archive_to_pick))
        upload = query[0]
        self.assertEqual(upload.archive.host, "testhost")
        self.assertEqual(upload.archive.description,
                         "archive-descr-{}".format(self.second_archive))

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
        self.assertEqual(resp.code, 204)
    
    def test_version(self):
        resp = self.go("/version", method="GET")
        self.assertEqual(resp.code, 200)
        resp = json_decode(resp.body)
        self.assertEqual(resp["version"], archive_db.handlers.DbHandlers.version)

    def test_view(self):
        resp = self.go("/view", method="GET")
        self.assertEqual(resp.code, 204)

        expected_archives = self.create_data()
        resp = self.go("/view", method="GET")
        self.assertEqual(resp.code, 200)
        resp = json_decode(resp.body)
        observed_archives = resp["archives"]
        self.assertEqual(
            len(observed_archives),
            len(expected_archives))
        for observed_archive in observed_archives:
            self.assertIn(observed_archive, expected_archives)

        resp = self.go("/view/3", method="GET")
        self.assertEqual(resp.code, 200)
        resp = json_decode(resp.body)
        observed_archives = resp["archives"]
        self.assertEqual(
            len(observed_archives),
            3)
        for observed_archive in observed_archives:
            self.assertIn(observed_archive, expected_archives)

    def test_query(self):
        def _assert_response(resp, expected_code, expected_archives):
            self.assertEqual(resp.code, expected_code)
            observed_archives = json_decode(resp.body)["archives"]
            self.assertEqual(
                len(observed_archives),
                len(expected_archives)
            )
            for observed_archive, expected_archive in zip(observed_archives, expected_archives):
                self.assertIn(observed_archive, expected_archives)
                self.assertIn(expected_archive, observed_archives)

        archives = self.create_data()
        resp = self.go(
            "/query",
            method="POST",
            body={"verified": "True"})
        expected_archives = list(filter(lambda x: x["verified"] is not None, archives))
        _assert_response(resp, 200, expected_archives)

        resp = self.go(
            "/query",
            method="POST",
            body={"removed": "False"})
        expected_archives = list(filter(lambda x: x["removed"] is None, archives))
        _assert_response(resp, 200, expected_archives)

        resp = self.go(
            "/query",
            method="POST",
            body={
                "before_date": datetime.datetime.now().strftime("%Y-%m-%d"),
                "after_date": datetime.datetime.now().strftime("%Y-%m-%d")
            })
        expected_archives = list(filter(lambda x: x["uploaded"] is not None, archives))
        _assert_response(resp, 200, expected_archives)

        resp = self.go(
            "/query",
            method="POST",
            body={
                "host": "testhost"
            })
        expected_archives = list(filter(lambda x: x["host"] == "testhost", archives))
        _assert_response(resp, 200, expected_archives)

        resp = self.go(
            "/query",
            method="POST",
            body={
                "description": "descr-2"
            })
        expected_archives = list(filter(lambda x: x["description"] == "archive-descr-2", archives))
        _assert_response(resp, 200, expected_archives)

        resp = self.go(
            "/query",
            method="POST",
            body={
                "path": "archive-"
            })
        _assert_response(resp, 200, archives)

        resp = self.go(
            "/query",
            method="POST",
            body={
                "path": "this-will-not-match-anything"
            })
        self.assertEqual(resp.code, 204)
