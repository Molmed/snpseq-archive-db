import datetime
from importlib.metadata import version

from archive_db.models.Model import Archive, Upload, Verification, Removal, init_db
from archive_db.app import routes

from tornado.web import Application
from tornado.escape import json_encode, json_decode
from tornado.testing import AsyncHTTPTestCase


class TestDb(AsyncHTTPTestCase):

    now = datetime.datetime(
        year=2023,
        month=6,
        day=15,
        hour=14,
        minute=50,
        second=23,
        microsecond=123456)
    num_archives = 5
    first_archive = 1
    second_archive = 3
    third_archive = 4

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

    def example_data(self):
        for i in range(self.num_archives):
            yield {
                "description": f"archive-descr-{i}",
                "path": f"/data/testhost/runfolders/archive-{i}",
                "host": "testhost",
                "uploaded": str(self.now - datetime.timedelta(days=i)) if i in [
                    self.first_archive, self.second_archive, self.third_archive] else None,
                "verified": str(self.now) if i in [
                    self.second_archive] else None,
                "removed": str(self.now) if i == self.second_archive else None
            }

    def create_data(self, data=None):
        archives = data or list(self.example_data())
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
                        timestamp=datetime.datetime.fromisoformat(archive[key])
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
        test_data = next(self.example_data())
        body = {
            "description": test_data["description"],
            "host": test_data["host"],
            "path": test_data["path"]
        }
        resp = self.go("/upload", method="POST", body=body)
        resp = json_decode(resp.body)
        self.assertEqual(resp["status"], "created")
        self.assertEqual(resp["upload"]["description"], body["description"])

    def test_failing_upload(self):
        test_data = next(self.example_data())
        body = {
            "description": test_data["description"]
        }
        resp = self.go("/upload", method="POST", body=body) # missing params
        self.assertEqual(resp.code, 400)

    def test_create_upload_for_existing_archive(self):

        test_data = next(self.example_data())
        body = {
            "description": test_data["description"],
            "host": test_data["host"],
            "path": test_data["path"]
        }

        for upload_id in range(1, 3):
            resp = self.go("/upload", method="POST", body=body)
            resp = json_decode(resp.body)
            self.assertEqual(resp["status"], "created")
            self.assertEqual(resp["upload"]["description"], body["description"])
            self.assertEqual(resp["upload"]["id"], upload_id)

    def _verification_of_archive_helper(self, archive):

        body = {
            "description": archive["description"],
            "host": archive["host"],
            "path": archive["path"],
            "timestamp": self.now.isoformat()
        }

        # recording a verification on a non-existing archive should create the archive entry
        # on-the-fly
        resp = self.go("/verification", method="POST", body=body)
        self.assertEqual(resp.code, 200)
        resp = json_decode(resp.body)
        self.assertEqual(resp["status"], "created")
        obs_verification = resp["verification"]

        # query the database and ensure that one and only one archive entry exists as expected
        resp = self.go(
            "/query",
            method="POST",
            body={})
        self.assertEqual(resp.code, 200)
        obs_archives = json_decode(resp.body).get("archives")
        self.assertEqual(len(obs_archives), 1)

        # ensure that the db field match the input data
        for db_entity in (obs_verification, obs_archives[0]):
            for key in ("description", "host", "path"):
                self.assertEqual(db_entity[key], body[key])
        self.assertEqual(obs_archives[0]["verified"], obs_verification["timestamp"])

    def test_verification_of_existing_archive(self):
        archive = next(self.example_data())
        self.create_data(data=[archive])
        self._verification_of_archive_helper(archive)

    def test_verification_of_non_existing_archive(self):
        archive = next(self.example_data())
        self._verification_of_archive_helper(archive)

    def test_failing_fetch_random_unverified_archive(self):
        self.create_data()
        # I.e. our lookback window is [today - age - safety_margin, today - safety_margin] days.
        body = {
            "age": "1",
            "safety_margin": "2",
            "today": self.now.date().isoformat()
        }
        resp = self.go("/randomarchive", method="GET", body=body)
        self.assertEqual(resp.code, 204)

    def test_fetch_random_unverified_archive(self):
        archives = self.create_data()
        body = {
            "age": "2",
            "safety_margin": "1",
            "today": self.now.date().isoformat()
        }
        resp = self.go("/randomarchive", method="GET", body=body)
        self.assertEqual(resp.code, 200)
        obs_archive = json_decode(resp.body).get("archive")
        exp_archive = archives[self.first_archive]
        for key in ("description", "host", "path"):
            self.assertEqual(obs_archive[key], exp_archive[key])

    def test_fetch_random_archive_with_criteria(self):
        archives = self.create_data()
        body = {
            "age": "5",
            "safety_margin": "2",
            "description": f"-{self.third_archive}",
            "today": self.now.date().isoformat()
        }
        resp = self.go("/randomarchive", method="POST", body=body)
        self.assertEqual(resp.code, 200)
        obs_archive = json_decode(resp.body).get("archive")
        exp_archive = archives[self.third_archive]
        for key in ("description", "host", "path"):
            self.assertEqual(obs_archive[key], exp_archive[key])

    def test_version(self):
        resp = self.go("/version", method="GET")
        self.assertEqual(resp.code, 200)
        resp = json_decode(resp.body)
        self.assertEqual(resp["version"], version("archive_db"))

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
            body={})
        _assert_response(resp, 200, archives)

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
                "uploaded_before": self.now.strftime("%Y-%m-%d"),
                "uploaded_after": (
                        self.now - datetime.timedelta(days=self.num_archives)
                ).strftime("%Y-%m-%d")
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
                "description": f"descr-{self.second_archive}",
                "removed": True
            })
        expected_archives = list(
            filter(
                lambda x: x["description"] == f"archive-descr-{self.second_archive}" and
                          x["removed"] is not None,
                archives))
        _assert_response(resp, 200, expected_archives)

        resp = self.go(
            "/query",
            method="POST",
            body={
                "path": "this-will-not-match-anything"
            })
        self.assertEqual(resp.code, 204)
