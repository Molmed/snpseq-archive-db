from peewee import *

# For schema migrations, see http://docs.peewee-orm.com/en/latest/peewee/database.html#schema-migrations
# and http://docs.peewee-orm.com/en/latest/peewee/playhouse.html#migrate
#
# Make sure that we *always*, as extra security, take a backup of the previous
# db before doing a migration. We should also take continous backups

db_proxy = Proxy()


def init_db(mydb="archives.db"):
    db = SqliteDatabase(mydb)
    db_proxy.initialize(db)
    db.create_tables([Archive, Upload, Verification, Removal], safe=True)


class BaseModel(Model):

    class Meta:
        database = db_proxy


class ChildModel(BaseModel):

    def __repr__(self):
        return "ID: {}, Archive ID: {}, Timestamp: {}".format(self.id, self.archive, self.timestamp)


class Archive(BaseModel):

    def __repr__(self):
        return "ID: {}, Description: {}, Path: {}, Host: {}".format(self.id, self.description, self.path, self.host)

    description = CharField(index=True, unique=True)
    path = CharField(index=True)
    host = CharField()


class Upload(ChildModel):
    archive = ForeignKeyField(Archive, related_name="uploads")
    timestamp = DateTimeField()


class Verification(ChildModel):
    archive = ForeignKeyField(Archive, related_name="verifications")
    timestamp = DateTimeField()


class Removal(ChildModel):
    archive = ForeignKeyField(Archive, related_name="removals")
    timestamp = DateTimeField()

    """
    To let archive-remove better support archives staged/marked/scheduled for removal I envision that
    one could modify this to something like the following instead: 

        archive = ForeignKeyField(Archive, related_name="removals")
        done = BooleanField(default=False)      # False = archive has been scheduled for removal; True = archive has been removed.
        timestamp_scheduled = DateTimeField()   # Or one can just let the queries look to see which timestamp has been filled with a value.
        timestamp_done = DateTimeField()
    """

