import datetime as dt
import os

from arteria.web.handlers import BaseRestHandler

from archive_db.models.Model import Archive, Upload, Verification, Removal
from archive_db import __version__ as version

from peewee import *
from tornado import gen
from tornado.web import HTTPError
from tornado.escape import json_decode


class BaseHandler(BaseRestHandler):
    # BaseRestHandler.body_as_object() does not work well
    # in Python 3 due to string vs byte strings.

    def decode(self, required_members=None):
        obj = json_decode(self.request.body)

        if required_members:
            for member in required_members:
                if member not in obj:
                    raise HTTPError(400, "Expecting '{0}' in the JSON body".format(member))
        return obj



class UploadHandler(BaseHandler):

    @gen.coroutine
    def post(self):
        """
        Creates a new Upload object in the db, and the associated Archive if it doesn't already exist. 

        :param path: Path to archive uploaded
        :param description: The unique TSM description of the archive
        :param host: From which host the archive was uploaded
        :return Information about the created object
        """

        body = self.decode(required_members=["path", "description", "host"])
        archive, created = Archive.get_or_create(
            description=body["description"], path=body["path"], host=body["host"])

        upload = Upload.create(archive=archive, timestamp=dt.datetime.utcnow())

        self.write_json({"status": "created", "upload":
                         {"id": upload.id,
                          "timestamp": str(upload.timestamp),
                          "description": upload.archive.description,
                          "path": upload.archive.path,
                          "host": upload.archive.host}})


class VerificationHandler(BaseHandler):

    @gen.coroutine
    def post(self):
        """
        Creates a new Verification object in the db, associated to a certain Archive object. 
        If no Archive object matching the input parameters is found one will be created. 
        This way we can take care of verifications done for archives uploaded to PDC before
        this web service and db existed. 

        :param description: The unique TSM description of the archive we've verified. 
        :param path: The path to the archive that was uploaded 
        :param host: The host from which the archive was uploaded
        :return Information about the created object
        """
        body = self.decode(required_members=["description", "path", "host"])

        archive, created = Archive.get_or_create(description=body["description"], host=body["host"], path=body["path"])

        verification = Verification.create(archive=archive, timestamp=dt.datetime.utcnow())

        self.write_json({"status": "created", "verification":
                        {"id": verification.id,
                         "timestamp": str(verification.timestamp),
                         "description": verification.archive.description,
                         "path": verification.archive.path,
                         "host": verification.archive.host}})

class RandomUnverifiedArchiveHandler(BaseHandler):

    @gen.coroutine
    def get(self):
        """
        Returns an unverified Archive object that has an associated was Upload object 
        within the interval [today - age - margin, today - margin]. The margin value is 
        used as a safety buffer, to make sure that the archived data has been properly 
        flushed to tape upstreams at PDC.

        :param age: Number of days we should look back when picking an unverified archive
        :param safety_margin: Number of days we should use as safety buffer
        :return A randomly pickedunverified archive within the specified date interval
        """
        body = self.decode(required_members=["age", "safety_margin"])
        age = int(body["age"])
        margin = int(body["safety_margin"])

        from_timestamp = dt.datetime.utcnow() - dt.timedelta(days=age+margin)
        to_timestamp = dt.datetime.utcnow() - dt.timedelta(days=margin)

        # "Give me a randomly chosen archive that was uploaded between from_timestamp and 
        # to_timestamp, and has no previous verifications"
        query = (Upload
                .select()
                .join(Verification, JOIN.LEFT_OUTER, on=(
                    Verification.archive_id == Upload.archive_id))
                .where(Upload.timestamp.between(from_timestamp, to_timestamp))
                .group_by(Upload.archive_id)
                .having(fn.Count(Verification.id) < 1)
                .order_by(fn.Random())
                .limit(1))

        result_len = query.count()

        if result_len > 0:
            upload = next(query.execute())
            archive_name = os.path.basename(os.path.normpath(upload.archive.path))
            self.write_json({"status": "unverified", "archive":
                            {"timestamp": str(upload.timestamp),
                             "path": upload.archive.path,
                             "description": upload.archive.description,
                             "host": upload.archive.host,
                             "archive": archive_name}})
        else:
            msg = "No unverified archives uploaded between {} and {} was found!".format(
                    from_timestamp.strftime("%Y-%m-%d %H:%M:%S"), to_timestamp.strftime("%Y-%m-%d %H:%M:%S"))
            self.set_status(204, reason=msg)


# TODO: We might have to add logic in some of the services
# that adds a file with the description inside the archive,
# so we can verify that we're operating on the correct
# archive before (verifying/)removing.

class RemovalHandler(BaseHandler):

    @gen.coroutine
    def post(self):
        """
        Archive `foo` was either staged for removal or actually just physically removed from local disk, as well 
        as all its associated files (e.g. runfolder etc). 
        """
        pass
    

        """
        # This is an example for how one could start implementing the handler that first schedules archives for 
        # removal. 

        body = self.decode(required_members=["description", "action"])

        try:
            archive = Archive.get(description=body["description"])
        except Archive.DoesNotExist:
            msg = "No archive with the unique description {} exists in the database!".format(body["description"])
            self.set_status(500, msg)
            self.write_json({"status": msg})

        if body["action"] == "set_removable":
            removal = Removal.create(archive=archive, timestamp_scheduled=dt.datetime.utcnow())

            self.write_json({"status": "scheduled", "removal":
                            {"id": removal.id,
                             "timestamp_scheduled": str(removal.timestamp_scheduled),
                             "description": removal.archive.description,
                             "path": removal.archive.path,
                             "host": removal.archive.host,
                             "done": removal.done}})
        elif body["action"] == "set_removed":
            pass
        else:
            msg = "Expecting parameter 'action' to be 'set_removable' or set_removed'."
            raise HTTPError(400, msg)
        """

    @gen.coroutine
    def get(self):
        """
        HTTP GET /removal is in this imagined implementation supposed to return those Archive objects
        that are removable and are verified. One could probably do this by e.g. 

            - fetch latest date from Verify, which has done == False, and call this X
            - fetch all Uploads that have has a timestamp older or equal to X
            - the set of Archives belonging to those Uploads should be OK to remove 
        """
        pass


class ViewHandler(BaseHandler):

    @staticmethod
    def _db_query():

        query = Archive.select(
            Archive.host,
            Archive.path,
            Archive.description,
            Upload.timestamp.alias("uploaded"),
            Verification.timestamp.alias("verified")
        ).join(
            Upload
        ).join(
            Verification, JOIN.LEFT_OUTER, on=(Verification.archive_id == Archive.id)
        ).order_by(
            Upload.timestamp.desc(),
            Archive.path.asc())
        return query

    def _do_query(self, query):
        if query:
            self.write_json({
                "archives": [{
                    "host": row["host"],
                    "path": row["path"],
                    "description": row["description"],
                    "uploaded": row["uploaded"].isoformat(),
                    "verified": row["verified"].isoformat() if row["verified"] else None}
                    for row in query
                ]})
        else:
            msg = "no entries matching criteria found in database"
            self.set_status(204, reason=msg)

    @gen.coroutine
    def get(self, limit=None):
        """
        GET archives recorded in the database, sorted by upload timestamp (descending) and
        archive path (ascending)

        /view returns all archives recorded in the database
        /view/[LIMIT] returns the LIMIT (positive integer) most recent records from database

        :param limit: positive integer specifying the number of rows to limit the results to
        :return archives recorded in the database as a json object under the key "archives"
        """
        try:
            limit = int(limit)
        except ValueError:
            limit = None

        query = self._db_query()
        query = (
            query.limit(
                limit
            ).dicts()
        )
        self._do_query(query)

    @gen.coroutine
    def post(self):
        """
        Retrieve archives recorded in the database, conditioned by the parameters supplied in the
        request body and sorted by upload timestamp (descending) and archive path (ascending).

        :param path: (optional) fetch archives whose path fully or partially match this string
        :param description: (optional) fetch archives whose unique TSM description fully or
        partially match this string
        :param host: (optional) fetch archives that were uploaded from a host whose hostname fully
        or partially match this string
        :param before_date: (optional) fetch archives that were uploaded on or before this date,
        formatted as YYYY-MM-DD
        :param after_date: (optional) fetch archives that were uploaded on or after this date,
        formatted as YYYY-MM-DD
        :param verified: (optional) if True, fetch only archives that have been successfully
        verified. If False, fetch only archives that have not been verified. If omitted, fetch
        archives regardless of verification status
        :return archives in the database matching the criteria in the request body as a json object
        under the key "archives"
        """
        body = self.decode()
        query = self._db_query()

        for parameter, condition in body.items():
            if parameter == "path" and body[parameter]:
                query = query.where(Archive.path.contains(body[parameter]))
            elif parameter == "description" and body[parameter]:
                query = query.where(Archive.description.contains(body[parameter]))
            elif parameter == "host" and body[parameter]:
                query = query.where(Archive.host.contains(body[parameter]))
            elif parameter == "before_date" and body[parameter]:
                query = query.where(
                    Upload.timestamp <= dt.datetime.strptime(
                        f"{body[parameter]} 23:59:59",
                        "%Y-%m-%d %H:%M:%S"))
            elif parameter == "after_date" and body[parameter]:
                query = query.where(
                    Upload.timestamp >= dt.datetime.strptime(body[parameter], "%Y-%m-%d"))
            elif parameter == "verified" and body[parameter] in ["True", "False"]:
                query = query.where(Verification.timestamp.is_null(body[parameter] == "False"))

        query = (query.dicts())
        self._do_query(query)


class VersionHandler(BaseHandler):

    """
    Get the version of the service
    """

    def get(self):
        """
        Returns the version of the checksum-service
        """
        self.write_object({"version": version})
