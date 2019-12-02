from models import Video, Statistic, Activity, DataPoint, Region, Channel
from cachetools import LRUCache, cached
from datetime import datetime, timedelta
from collections import defaultdict
from dateutil.relativedelta import relativedelta 
from playhouse.shortcuts import model_to_dict, dict_to_model
import pandas as pd
from fuzzywuzzy import process, fuzz
from tqdm import tqdm
import math
import re
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s : %(message)s')

black_list_tags = list(set([ tag.strip() for tag in open('blacklist.txt', 'r').readlines() ]))

unit_value = {
    'day': 1,
    'week': 7,
    'month': 30,
    'year': 365
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

def extract_video_unique_keyword(video_id):
    video = Video.select(Video.tags, Video.meta).where(Video.id == video_id).get()

    tags = video.tags #['tags']
    result = []
    cleaned_tags = []

    for tag in tags:
        cleaned_tags += re.split(r',|、|，|】', tag)

    for tag in cleaned_tags:
        if tag[:3] == 'sp:':
            continue

        if tag in black_list_tags:
            continue

        title_similarity = fuzz.ratio(tag, video.meta['channel']['title'])
        if title_similarity > 40:
            continue

        match = process.extractBests(tag, cleaned_tags)
        result.append(match[0][0])

    return list(set(result))

def cluster_tags(tag_pair):
    final_tag = []
    added_tag = []
    for tag, value in tag_pair:
        similar_tag = []

        if tag in added_tag:
            continue

        for tag2, value in tag_pair:
            match_score = fuzz.ratio(tag, tag2)
            if match_score > 50:
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


def cluster_stats_date(stats, unit):
    tag_data = defaultdict(list)

    # exist_video = {}
    stats = list(stats.dicts())
    for s in tqdm(stats):
        if s is None:
            continue
        prime_key = get_unit_value(s['date'], unit)
        secondary_key = s['date'].year
        key = '{}-{}'.format(prime_key, secondary_key)
        s_dict = s
        s_dict['key'] = key
        tags = extract_video_unique_keyword(s['video'])
        for tag in tags:
            tag_data[tag].append(s_dict)
    return tag_data

@cached(cache=LRUCache(maxsize=128))
def topic_interest(region, unit: str, search:str=None, start: datetime=None, end: datetime=None, 
    sum:bool=False, topic_limit=100):
    if unit not in ['week', 'day', 'month', 'year']:
        raise ValueError("Invalid unit value")

    if end is None:
        end = datetime.now()
    if start is None:
        start = end-relativedelta(days=unit_value[unit])

    target_region = Region.get(Region.region_id == region)

    stats = Statistic.select(
        Statistic.video,
        Statistic.date,
        Statistic.comment, Statistic.like, Statistic.dislike,
        Statistic.rank, Statistic.view, Statistic.trending_region,
    )

    stats = stats.where((Statistic.date >= start ) & (Statistic.date <= end) 
        & (Statistic.trending_region == target_region)
        ).order_by(-Statistic.date)

    logging.info('start cluster')
    tag_data = cluster_stats_date(stats, unit)
    logging.info('stop cluster')

    result = {
        'id': target_region.region_id,
        'name': target_region.name,
        'topic': [],
        'geo': {
            'lat': target_region.lat,
            'lon': target_region.lon
        }
    }
    total_weight = 0
    for key, data in tag_data.items():
        if len(key) > 3 and len(key) < 30:
            df = pd.DataFrame(data)
            df['norm_view'] = df['view']/df['view'].sum()
            # df['weight'] =  (df['like'] + df['dislike'])/df['view'] + ((101-df['rank'])*1000)*df['norm_view']
            df['weight'] = 101-df['rank']
            interest_weight = df['weight'].mean()
            total_weight += interest_weight
            result['topic'].append((key.lower(), interest_weight))

    result['topic'] = result['topic'][:topic_limit]
    result['topic'].sort(key=lambda x: x[1], reverse=True)

    return result


@cached(cache=LRUCache(maxsize=128))
def topic_filter(region_id, unit: str, search:str=None, start: datetime=None, end: datetime=None, 
    sum:bool=False, topic_limit=100):
    if unit not in ['week', 'day', 'month', 'year']:
        raise ValueError("Invalid unit value")

    if end is None:
        end = datetime.now()
    if start is None:
        start = end-relativedelta(days=unit_value[unit])

    target_region = Region.get(Region.region_id == region_id)

    stats = Statistic.select(
        Statistic.video,
        Statistic.date,
        Statistic.comment, Statistic.like, Statistic.dislike,
        Statistic.rank, Statistic.view, Statistic.trending_region,
    )

    stats = stats.where((Statistic.date >= start ) & (Statistic.date <= end) 
        & (Statistic.trending_region == target_region)
    ).order_by(-Statistic.date)

    tag_data = cluster_stats_date(stats, unit)

    result = {
        'id': target_region.region_id,
        'name': target_region.name,
        'topic': [],
        'geo': {
            'lat': target_region.lat,
            'lon': target_region.lon
        }
    }
    total_weight = 0
    for key, data in tag_data.items():
        if len(key) > 3 and len(key) < 30:
            df = pd.DataFrame(data)
            df['norm_view'] = df['view']/df['view'].sum()
            df['weight'] =  (df['like'] + df['dislike'])/df['view'] + ((101-df['rank'])*1000)*df['norm_view']
            df['weight'] = 101-df['rank']
            stats = df[['weight', 'like', 'dislike', 'view', 'rank', 'norm_view', 'date']].to_dict(orient='records')
            result['topic'].append({
                'tag': key,
                'stats': stats
            })
    return result

if __name__ == '__main__':
    data = topic_interest('TW', 'week', topic_limit=200)
    print(data)

