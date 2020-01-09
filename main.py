from fastapi import FastAPI
from datetime import datetime
from starlette.middleware.cors import CORSMiddleware
from utils import topic_filter, topic_interest, unit_value, validate_daterange, trending_topic
from datetime import datetime
import multiprocessing as mp
import pandas as pd
from models import DailyTrend, Region, Video, Channel, Stats, DataPoint
from peewee import NodeList, SQL
from custom_pool import CustomPool, NoDaemonProcess
import dateparser
from dateutil.relativedelta import relativedelta 
from playhouse.postgres_ext import Match
from playhouse.shortcuts import model_to_dict, dict_to_model

app = FastAPI(debug=False)
app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

all_region = [ r.strip() for r in open('valid_region.txt', 'r').readlines() ]

def pool_wrapper(function, params, queue, pool_size=1):
    '''Wrap a pool inside a pool of size 1
    '''
    pool = CustomPool(pool_size)
    p_results = pool.starmap(function, params)
    pool.close()
    results = []
    for r in p_results:
        if len(r['topic']) > 0:
            results.append(r)
    queue.put(results)

@app.get("/main")
def primary_view(search: str=None, unit: str="day",
    region: str="all", start:str=None, end:str=None,
    lw: float=0, vw: float=0, cw: float=0, rw: float=1, dw: float=0,
    top: int=5):

    if unit not in ['week', 'day', 'month', 'year']:
        return {
            'status': 'error',
            'msg': "unit should be :week, day, month, year"
        }

    target_regions = all_region
    if region != "all":
        target_regions = []
        for r in region.split(','):
            if len(r) > 1:
                target_regions.append(r)

    params = []
    if start is not None:
        start = dateparser.parse(str(start))

    end = datetime.now()
    if end is not None:
        end = dateparser.parse(str(end))
    if start is None:
        start = end-relativedelta(days=unit_value[unit]+2)
    if end is not None and start is not None:
        if not validate_daterange(start, end ):
            return {
                'status': 'error',
                'msg': "Invalid daterange, start date must be earlier than end date"
            }
    results = []
    for r in target_regions:
        param = (r, unit, search, start, end, False, top, lw, vw, cw, rw, dw)
        results.append(trending_topic(*param))
        # params.append(param)

    return {
        'status': 'ok',
        'date': {
            'start': start.strftime('%Y-%m-%d'), 
            'end': end.strftime('%Y-%m-%d')
        },
        'results':  results
    }


@app.get("/main/{region_id}")
def read_item(region_id:str, search: str="", unit: str="day",
    start:str=None, end:str=None,
    lw: float=0, vw: float=0, cw: float=0, rw: float=1, dw: float=0,
    top: int=5):

    if unit not in ['week', 'day', 'month', 'year']:
        return {
            'status': 'error',
            'msg': "unit should be :week, day, month, year"
        }

    if start is not None:
        start = dateparser.parse(str(start))

    end = datetime.now()
    if end is not None:
        end = dateparser.parse(str(end))
    if start is None:
        start = end-relativedelta(days=unit_value[unit]+2)
    if end is not None and start is not None:
        if not validate_daterange(start, end ):
            return {
                'status': 'error',
                'msg': "Invalid daterange, start date must be earlier than end date"
            }

    result = topic_filter(region_id, unit=unit, search=search,
        start=start, end=end, topic_limit=top, lw=lw, vw=vw, cw=cw, rw=rw, dw=dw)
    result['date'] = {
        'start': start.strftime('%Y-%m-%d'), 
        'end': end.strftime('%Y-%m-%d'),
    }
    return result


@app.get("/tag/{tag}")
def get_tags(tag:str,start:str=None, end:str=None,unit: str="day",):
    if unit not in ['week', 'day', 'month', 'year']:
        return {
            'status': 'error',
            'msg': "unit should be :week, day, month, year"
        }
    if start is not None:
        start = dateparser.parse(str(start))

    end = datetime.now()
    if end is not None:
        end = dateparser.parse(str(end))
    if start is None:
        start = end-relativedelta(days=1000)
    if end is not None and start is not None:
        if not validate_daterange(start, end ):
            return {
                'status': 'error',
                'msg': "Invalid daterange, start date must be earlier than end date"
            }

    daily_metrics = []
    datapoints = DataPoint.select().where( (DataPoint.key == 'tag') & (DataPoint.value == tag))
    if datapoints.exists():
        for datapoint in datapoints:
            for point in datapoint.metrics:
                m = point
                m.pop('tag')
                m['region'] = datapoint.region.region_id
                time = datetime.strptime(m['time'].split(' ')[0], "%Y-%m-%d")
                if time >= start and time <= end:
                    daily_metrics.append(m)
    return {
        'status': 'ok',
        'date': {
            'start': start.strftime('%Y-%m-%d'), 
            'end': end.strftime('%Y-%m-%d')
        },
        'results':  daily_metrics
    }


@app.get("/tag/{tag}/similar")
def get_tags(tag:str,start:str=None, end:str=None,unit: str="day", ratio:float=1, top:int=5):
    if unit not in ['week', 'day', 'month', 'year']:
        return {
            'status': 'error',
            'msg': "unit should be :week, day, month, year"
        }
    if start is not None:
        start = dateparser.parse(str(start))

    end = datetime.now()
    if end is not None:
        end = dateparser.parse(str(end))
    if start is None:
        start = end-relativedelta(days=1000)
    if end is not None and start is not None:
        if not validate_daterange(start, end ):
            return {
                'status': 'error',
                'msg': "Invalid daterange, start date must be earlier than end date"
            }

    daily_metrics = []

    edit = int(len(tag)*ratio)

    exp = NodeList([
            SQL("levenshtein("),
            DataPoint.value,
            SQL(", '{}') <= {}".format(tag, edit)),
            SQL(" order by levenshtein("),
            DataPoint.value,
            SQL(", '{}')".format(tag))
            ], glue='')
    datapoints = DataPoint.select().where(exp)

    if datapoints.exists():
        for datapoint in datapoints[:top]:
            datapoint_metrics = []
            for point in datapoint.metrics:
                m = point
                m.pop('tag')
                m['region'] = datapoint.region.region_id
                time = datetime.strptime(m['time'].split(' ')[0], "%Y-%m-%d")
                if time >= start and time <= end:
                    datapoint_metrics.append(m)
            daily_metrics.append({
                'tag': datapoint.value,
                'data': datapoint_metrics
            })
    return {
        'status': 'ok',
        'date': {
            'start': start.strftime('%Y-%m-%d'), 
            'end': end.strftime('%Y-%m-%d')
        },
        'results':  daily_metrics
    }


@app.get("/video")
def list_video(search: str="", start:str=None, end:str=None, offset=0):
    videos_query = Video.select(Video.title, Video.published, Video.id, Video.tags, Video.category_id, Video.duration, Video.channel)
    if start is not None:
        start = dateparser.parse(str(start))
        videos_query = videos_query.where(Video.published >= start)
    if end is not None:
        end = dateparser.parse(str(end))
        videos_query = videos_query.where(Video.published <= end)
    if search is not None:
        search = str(search)
        videos_query = videos_query.where(Match(Video.title, search) | Match(Video.description, search))
    

    videos = []
    for idx, v in enumerate(videos_query[offset:]):
        json_data = model_to_dict(v)
        videos.append(json_data)
        if idx > 10:
            break

    return {
        'count': len(videos),
        'videos': videos
    }

@app.get("/video/{video_id}")
def get_video(video_id: str):
    try:
        print(video_id)
        video = Video.get(Video.id==video_id)
        video_stats = Stats.get(Stats.video==video) 
        video_dict = model_to_dict(video)
        video_stats_ = model_to_dict(video_stats)

        video_dict['statistic'] = video_stats_
        video_dict['status'] = 'success'
        return video_dict
    except BaseException as e:
        # print(e)
        return {
            'status': 'not found',
        }


@app.get("/channel/{channel_id}")
def get_channel(channel_id: str):

    try:
        channel = Channel.get(Channel.channel_id==channel_id)
        channel_ = model_to_dict(channel)
        videos = Video.select().where(Video.channel==channel) 
        videos_ = []
        for v in videos:
            videos_.append(model_to_dict(v))
        channel_['videos'] = videos_
        channel_['status'] = 'success'
        return channel_
    except BaseException as e:
        print(e)
        return {
            'status': 'not found',
        }

@app.get("/channel")
def list_channel(search: str=None, country: str=None ,offset=0):

    channel_query = Channel.select(Channel.channel_id, Channel.title, Channel.description, Channel.country)

    if search is not None:
        print(search)
        search = str(search)
        channel_query = channel_query.where(Match(Channel.title, search) | Match(Channel.description, search))

    if country is not None:
        print('search country')
        region = Region.get(Region.region_id==country)
        channel_query = channel_query.where(Channel.country == region)

    channels = []
    for idx, c in enumerate(channel_query[offset:]):
        json_data = model_to_dict(c)
        channels.append(json_data)
        if idx > 10:
            break

    return {
        'count': len(channels),
        'channels': channels
    }


@app.get("/suggestion/{search}")
def suggestion(search:str, ratio=0.5, top=20):

    edit = int(len(search)*ratio)

    exp = NodeList([
            SQL("levenshtein("),
            DataPoint.value,
            SQL(", '{}') <= {}".format(search, edit)),
            SQL(" order by levenshtein("),
            DataPoint.value,
            SQL(", '{}')".format(search))
            ], glue='')
    datapoints = DataPoint.select().where(exp)
    tags = []
    if datapoints.exists():
        for datapoint in datapoints[:top]:
            tags.append(datapoint.value)
    return {
        'tags': tags
    }
