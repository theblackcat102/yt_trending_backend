import asyncio
import logging
from peewee import *
from playhouse.postgres_ext import PostgresqlExtDatabase, JSONField, ArrayField, IntervalField, TSVectorField
from settings import POSTGRESQL_SETTINGS



postgres_database = PostgresqlExtDatabase(POSTGRESQL_SETTINGS['DATABASE'],
    user=POSTGRESQL_SETTINGS['USER'],
    host=POSTGRESQL_SETTINGS['HOST'],
    port=POSTGRESQL_SETTINGS['PORT'],
    password=POSTGRESQL_SETTINGS['PASSWORD'],
    register_hstore=False,
    # stale_timeout=300,
    )


class BaseModel(Model):
    class Meta:
        database = postgres_database

class Region(BaseModel):

    name = CharField(max_length=32)
    region_id = CharField(max_length=2, unique=True, index=True)
    lat = FloatField(default=0)
    lon = FloatField(default=0)

class Channel(BaseModel):

    '''
    https://www.googleapis.com/youtube/v3/channels?part=snippet,contentDetails&id={}&key=AIzaSyAtbO0oNahwQ0Sikyg9vEcn5xqeUywb64s
    '''
    channel_id = CharField(max_length=32, unique=True, primary_key=True)
    title = CharField(max_length=64, unique=True)
    country = ForeignKeyField(Region)
    description = CharField(max_length=2000)
    thumbnails = JSONField(default={})
    content_details = JSONField(default={})
    meta = JSONField(default={})


class Video(BaseModel):
    id = CharField(max_length=32, unique=True, primary_key=True)
    etag = CharField(max_length=64)

    published = DateTimeField()
    # channel_id = CharField(max_length=32)
    title = CharField(max_length=128)

    thumbnails = JSONField(default={})
    topic_details = JSONField(default={})
    meta = JSONField(default={})

    description = CharField(max_length=6000)
    '''
        search_description=fn.to_tsvector(description))
    '''
    search_description = TSVectorField()
    tags = ArrayField(CharField)
    category_id = IntegerField(default=0)
    duration = IntervalField() # timedelta
    caption = BooleanField(default=False)
    license_content = BooleanField(default=False)
    defintion = CharField(max_length=8)
    projection = CharField(max_length=15)
    dimension = CharField(max_length=3)

    localization = JSONField(default={})

    channel = ForeignKeyField(Channel, backref='videos')

class Statistic(BaseModel):

    date = DateTimeField()
    view = BigIntegerField(default=0)
    like = BigIntegerField(default=0)
    dislike = BigIntegerField(default=0)
    comment = BigIntegerField(default=0)
    favorite = BigIntegerField(default=0)
    rank = IntegerField(default=0) 
    trending_region = ForeignKeyField(Region, null=True)
    video = ForeignKeyField(Video, backref='stats')


class Activity(BaseModel):
    action = CharField(max_length=32)
    video = ForeignKeyField(Video, null=True)
    channel = ForeignKeyField(Channel, backref='activities')
    details = JSONField(default={})

class DataPoint(BaseModel):
    '''
        Data point pairs for serving frontend
    '''
    key = CharField(max_length=128)
    value = CharField(max_length=128)
    metrics = JSONField(default={})
    time = DateTimeField()
    region = ForeignKeyField(Region)
    video = ForeignKeyField(Video)
    
    class Meta:
        indexes = (
            (("key", "value"), False),
            (("key", "value", "time", "region"), True),
        )



def create_table():
    postgres_database.drop_tables([DataPoint])
    postgres_database.create_tables([DataPoint])

if __name__ == '__main__':
    create_table()