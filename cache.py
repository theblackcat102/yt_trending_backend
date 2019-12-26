from peewee import *
from playhouse.sqlite_ext import SqliteExtDatabase, JSONField
from models import Video, DailyTrend,Activity, Region, Channel, Stats, postgres_database
from utils import get_today_trend
from tqdm import tqdm
from main import all_region

sqlite_db = SqliteExtDatabase('cache.db', pragmas={
    'journal_mode': 'wal',
    'cache_size': -1024 * 128})


class BaseModel(Model):
    class Meta:
        database = sqlite_db

class LatestTrend(BaseModel):

    region_id = CharField(max_length=2, unique=True)
    metrics = JSONField(default={})


def cache_today_stats():
    for region_id in tqdm(all_region):
        region = Region.get(Region.region_id == region_id)
        today_stats = get_today_trend(region)
        if LatestTrend.select().where(LatestTrend.region_id == region_id).exists():
            trend = LatestTrend.get(LatestTrend.region_id == region_id)
            trend.metrics = today_stats
            with postgres_database.atomic():
                trend.save()
        else:
            LatestTrend.create(region_id=region_id,metrics=today_stats).save()


def get_main_cache(region_id, unit, start, end):
    try:
        query = MainCache.get(
            (MainCache.region_id == region_id) & 
            (MainCache.unit == unit)
            # (MainCache.start == start) &
            # (MainCache.end == end)
        )
        return query.data
    except:
        None
    

def create_table():
    sqlite_db.create_tables([LatestTrend])

if __name__ == '__main__':
    cache_today_stats()