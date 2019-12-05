from models import Video, Activity, Region, Channel, Stats, postgres_database
from cachetools import LRUCache, cached
from datetime import datetime, timedelta
from collections import defaultdict
from dateutil.relativedelta import relativedelta 
from playhouse.shortcuts import model_to_dict, dict_to_model
import pandas as pd
import numpy as np
from fuzzywuzzy import process, fuzz
from tqdm import tqdm
import math
import re
import logging
import multiprocessing as mp
from custom_pool import CustomPool
from cache import MainCache, SecondaryCache, get_main_cache
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s : %(message)s')

black_list_tags = list(set([ tag.strip() for tag in open('blacklist.txt', 'r').readlines() ]))

unit_value = {
    'day': 1,
    'week': 7,
    'month': 30,
}


def get_unit_value(date, unit):
    if unit == 'week':
        return int(date.strftime('%V'))
    elif unit == 'daily':
        start_of_year = datetime(date.year, 1, 1, 0, 0, 0)
        delta = date - start_of_year
        return delta.days
    elif unit == 'month':
        return date.month
    elif unit == 'yearly':
        return date.year

def extract_video_unique_keyword(video):
    tags = video.tags
    result = []
    cleaned_tags = []
    for tag in tags:
        cleaned_tags += re.split(r',|、|，|】', tag)

    for tag in cleaned_tags:

        if len(tag) <= 1:
            continue
        if tag[:3] == 'sp:':
            continue
        if tag in black_list_tags:
            continue

        if 'channel' in video.meta and 'title' in video.meta['channel']:
            channel_title = video.meta['channel']['title']
            if len(channel_title) != 0:
                title_similarity = fuzz.ratio(tag, channel_title)
                if title_similarity > 30:
                    continue

        match = process.extractBests(tag, cleaned_tags)
        result.append(match[0][0])
    f_tags = list(set(result))

    with postgres_database.atomic():
        video.tags = f_tags
        video.save()

    return f_tags

def cluster_tags(tag_pair):
    final_tag = []
    added_tag = []
    for tag, value in tag_pair:
        similar_tag = []

        if tag in added_tag:
            continue
        if len(tag) <= 1:
            continue

        for tag2, value in tag_pair:
            if len(tag2) <= 1:
                continue
            match_score = fuzz.ratio(tag, tag2)
            if match_score > 30:
                similar_tag.append((match_score, tag2, value))

        if len(similar_tag) == 0:
            final_tag.append((tag, value))
            added_tag.append(tag)

        similar_tag.sort(key=lambda x: len(x[1]), reverse=True)
        total_value = 0
        for _, _tag, value in similar_tag:
            total_value += value
            added_tag.append(_tag)
        final_tag.append((similar_tag[0][1], total_value/len(similar_tag) ))
    return final_tag


def _extract_tag(df):
    tag_data = defaultdict(list)
    for _, s in df.iterrows():
        prime_key = get_unit_value(s['date'],s['unit'])
        secondary_key = s['date'].year
        key = '{}-{}'.format(prime_key, secondary_key)
        s_dict = s
        s_dict['key'] = key
        tags = s['video'].tags
        for tag in tags:
            tag_data[tag].append(s_dict)        
    return tag_data

def cluster_stats_date(stats, unit):
    tag_data = defaultdict(list)
    # exist_video = {}
    # stats = stats.to_dict()
    tag_data = defaultdict(list)
    stats['unit'] = unit

    if False:
        dfs = np.split(stats, [len(stats)//3, len(stats)//2, len(stats)*2//3], axis=0)

        pool = CustomPool(3)

        results = pool.map(_extract_tag, dfs)
        pool.close()
        for r in results:
            for key, value in r.items():
                tag_data[key] += value
    else:
        results =  _extract_tag(stats)
        for key, value in results.items():
            tag_data[key] += value

    return tag_data

@cached(cache=LRUCache(maxsize=512))
def topic_interest(region_id, unit: str, search:str=None, start: datetime=None, end: datetime=None, 
    sum:bool=False, topic_limit=100, 
    lw: float=0, vw: float=0, cw: float=0, rw: float=1, dw: float=0):
    if unit not in ['week', 'day', 'month', 'year']:
        raise ValueError("Invalid unit value")


    region = Region.get(Region.region_id == region_id)
    result = {
        'id': region.region_id,
        'name': region.name,
        'topic': [],
        'geo': {
            'lat': region.lat,
            'lon': region.lon
        }
    }

    if end is None:
        end = datetime.now()
        # end = datetime(year=end.year, month=end.month, day=end.day, hour=end.hour)
    if start is None:
        start = end-relativedelta(days=unit_value[unit])

    videos = Video.select().where((Video.published >= start) & (Video.published <= end))

    # for v in videos:
    statistic = Stats.select().where((Stats.trending_region == region) & Stats.video.in_(videos))
    stats = []
    for s in statistic:
        v = s.video
        if 'data' not in s.stats:
            continue
        sub_stats = s.stats['data']
        t = pd.DataFrame(sub_stats)
        v.tags = extract_video_unique_keyword(v)
        t['video'] = v
        stats.append(t)

    if len(stats) == 0:
        return result

    df = pd.concat(stats, axis=0)
    df['date'] = pd.to_datetime(df['date'])
    df = df[(df['date'] > start) & (df['date'] < end)]
    # print(len(df))
    tag_data = cluster_stats_date(df, unit)

    total_weight = 0
    for key, data in tag_data.items():
        if len(key) > 3 and len(key) < 30:
            df = pd.DataFrame(data)
            df['norm_view'] = df['view']/df['view'].sum()
            # df['weight'] =  (df['like'] + df['dislike'])/df['view'] + ((101-df['rank'])*1000)*df['norm_view']
            df['weight'] = (101-df['rank'])*rw + ((df['comment']*cw) + (df['view']*vw) + (df['like']*lw) - (df['dislike']*dw))/df['view']
            interest_weight = df['weight'].mean()
            total_weight += interest_weight
            result['topic'].append((key.lower(), interest_weight))

    result['topic'] = result['topic'][:topic_limit]
    result['topic'].sort(key=lambda x: x[1], reverse=True)
    return result

@cached(cache=LRUCache(maxsize=512))
def topic_filter(region_id:str, unit: str, search:str=None, start: datetime=None, end: datetime=None, 
    topic_limit=100, sum:bool=False, 
    lw: float=0, vw: float=0, cw: float=0, rw: float=1, dw: float=0):
    if unit not in ['week', 'day', 'month', 'year']:
        raise ValueError("Invalid unit value")
    target_region = Region.get(Region.region_id == region_id)
    result = {
        'id': target_region.region_id,
        'name': target_region.name,
        'topic': [],
        'geo': {
            'lat': target_region.lat,
            'lon': target_region.lon
        }
    }
    if end is None:
        end = datetime.now()
        end = datetime(year=end.year, month=end.month, day=end.day, hour=end.hour)
    if start is None:
        start = end-relativedelta(days=unit_value[unit]+2)

    videos = Video.select().where((Video.published >= start) & (Video.published <= end))
    stats = []
    # for v in videos:
    statistic = Stats.select().where((Stats.trending_region == target_region) & Stats.video.in_(videos))

    for s in statistic:
        v = s.video
        if 'data' not in s.stats:
            continue
        sub_stats = s.stats['data']
        t = pd.DataFrame(sub_stats)
        t['video'] = v
        stats.append(t)
    if len(stats) == 0:
        return result
    df = pd.concat(stats, axis=0)
    df['date'] = pd.to_datetime(df['date'])
    df = df[(df['date'] > start) & (df['date'] < end)]
    # print(len(df))

    tag_data = cluster_stats_date(df, unit)

    total_weight = 0

    for key, data in tag_data.items():
        if len(key) > 3 and len(key) < 30:
            df = pd.DataFrame(data)
            df['norm_view'] = df['view']/df['view'].sum()
            # df['weight'] =  (df['like'] + df['dislike'])/df['view'] + ((101-df['rank'])*1000)*df['norm_view']
            df['weight'] = (101-df['rank'])*rw + ((df['comment']*cw) + (df['view']*vw) + (df['like']*lw) - (df['dislike']*dw))/df['view']
            stats = df[['weight', 'like', 'dislike', 'view', 'rank', 'norm_view', 'date']].to_dict(orient='records')
            result['topic'].append({
                'tag': key,
                'stats': stats
            })
    return result

if __name__ == '__main__':
    data = topic_filter('TW', 'day', topic_limit=10)
    print(len(data['topic']))
