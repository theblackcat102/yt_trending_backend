from peewee import NodeList, SQL
from models import Video, DailyTrend,Activity, Region, Channel, Stats, postgres_database
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
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s : %(message)s')

black_list_tags = list(set([ tag.strip() for tag in open('blacklist.txt', 'r').readlines() ]))

unit_value = {
    'day': 1,
    'week': 7,
    'month': 30,
}


def validate_daterange(start:datetime, end: datetime):
    if end <= start:
        return False
    return True

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
        s_dict['category'] = s['video'].category_id
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
        start = end-relativedelta(days=unit_value[unit]+2)

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
    df.set_index('date')
    df = df[(df['date'] > start) & (df['date'] < end)]
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
            result['topic'].append({
                'tag': key,
                'stats': stats,
                'category': list(set(df['category'].tolist())),
            })

    result['topic'] = result['topic'][:topic_limit]
    result['topic'].sort(key=lambda x: x[1], reverse=True)
    return result

@cached(cache=LRUCache(maxsize=512))
def topic_filter(region_id:str, unit: str, search:str=None, start: datetime=None, end: datetime=None, 
    topic_limit=100, sum:bool=False, 
    lw: float=0, vw: float=0, cw: float=0, rw: float=1, dw: float=0):
    if unit not in ['week', 'day', 'month', 'year']:
        raise ValueError("Invalid unit value")
    today = datetime.now()
    today = datetime(year=today.year, month=today.month, day=today.day)
    if end is None:
        end = today
    else:
        end = datetime(year=end.year, month=end.month, day=end.day)
    if start is None:
        start = end-relativedelta(days=unit_value[unit]+2)

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
    daily_trends = DailyTrend.select().where(
            (DailyTrend.time >= start) & (DailyTrend.time <= end) & (DailyTrend.region == region))

    if search is not None and len(search) > 0:
        exp = NodeList([
            SQL("jsonb_message_to_tsvector("),
            DailyTrend.metrics,
            SQL(") @@ '{}'".format(search))
            ], glue='')
        daily_trends = daily_trends.where(exp)

    daily_metrics = []
    for trend in daily_trends:
        stats = []
        for metric in trend.metrics:
            m_ = metric['stats']
            m_['tag'] = metric['tag'].replace('#', '')
            m_['date'] = trend.time
            m_['category'] = metric['category']
            stats.append(m_)
        df = pd.DataFrame(stats)
        # df['date'] = pd.to_datetime(df['date'])
        daily_metrics.append(df)

    if end >= today:
        from cache import LatestTrend
        try:
            trend = LatestTrend.get(LatestTrend.region_id == region_id)
            today_stats = trend.metrics
        except:
            today_stats = []
        stats = []
        for metric in today_stats:
            m_ = metric['stats']
            m_['date'] = today
            m_['tag'] = metric['tag'].replace('#', '')
            if 'category' not in metric:
                m_['category'] = [-1]
            else:
                m_['category'] = metric['category']
            stats.append(m_)
        if len(stats):
            df = pd.DataFrame(stats)
            daily_metrics.append(df)

    if len(daily_metrics) > 0:
        df = pd.concat(daily_metrics, axis=0)
        if search is not None and len(search) > 0:
            df = df.loc[df['tag'].str.contains(search, regex=False)]

        df.set_index('tag')
        has_col = False

        if 'category' in df.columns:
            df['category'] = [','.join(map(str, l)) for l in df['category']]
            has_col = True
            df = df.groupby(['tag', 'date', 'category']).mean()
        else:
            df = df.groupby(['tag', 'date']).mean()
        df['weight'] = (101-df['rank'])*rw + ((df['view'])*vw + (df['comment'])*cw  + (df['like'])*lw - (df['dislike']*dw))/df['view']
        df['tag'] = list([ r[0] for r in df.index] )
        df['date'] = list([ r[1].strftime("%Y-%m-%dT%HH:%MM:%SS") for r in df.index] )

        if has_col:
            df['category'] = list( [ [ int(float(l)) for l in r[2].split(',')] for r in df.index] )
        topics = df.to_dict(orient='records')

        result['topic'] = topics
    return result

@cached(cache=LRUCache(maxsize=512))
def get_today_trend(region):
    day_ = datetime.now()
    day = datetime(year=day_.year, month=day_.month, day=day_.day)
    date = day.strftime("%Y-%m-%d")
    end_of_day = day + timedelta(hours=23, minutes=23, seconds=23)
    region_id = region.id
    cursor = postgres_database.execute_sql("select m.id, m.video_id, m.trending_region_id, m.stats from stats as m, jsonb_array_elements(stats->'data') point where point->>'date' like '{}%%' and m.trending_region_id = {};".format(date, int(region_id)))
    stats, count = [], 0
    result = []

    for row in tqdm(cursor.fetchall()):
        stats_id, video_id, _, stat_ = row
        v = Video.get(Video.id == video_id)
        if 'data' not in stat_:
            continue
        sub_stats = stat_['data']

        t = pd.DataFrame(sub_stats)
        v.tags = extract_video_unique_keyword(v)
        t['video'] = v

        stats.append(t)
        count += 1

    if len(stats) == 0:
        print('return early')
        return result
    df = pd.concat(stats, axis=0)
    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date')
    df = df[(df['date'] >= day) & (df['date'] <= end_of_day)]
    numeric_columns = ["like", "rank", "view", "comment", "dislike", "favorite"]
    df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric)

    tag_data = cluster_stats_date(df, 'day')

    total_weight = 0

    for key, data in tag_data.items():
        if len(key) >= 3 and len(key) < 30:
            df = pd.DataFrame(data)

            df = df.groupby(['date']).mean()
            df['norm_view'] = df['view']/df['view'].sum()
            # df['weight'] =  (df['like'] + df['dislike'])/df['view'] + ((101-df['rank'])*1000)*df['norm_view']
            df['weight'] = (101-df['rank']) + ((df['comment']) + (df['view']) + (df['like']) - (df['dislike']))/df['view']

            stats = df[['weight', 'like', 'dislike', 'comment', 'view', 'rank', 'norm_view']].mean(axis=0).to_dict()
            result.append({
                'tag': key,
                'stats': stats
            })
    return result

@cached(cache=LRUCache(maxsize=512))
def trending_topic(region_id, unit: str, search:str=None, start: datetime=None, end: datetime=None, 
    sum:bool=False, topic_limit=100, 
    lw: float=1, vw: float=1, cw: float=1, rw: float=1, dw: float=1):

    today = datetime.now()
    today = datetime(year=today.year, month=today.month, day=today.day)
    if end is None:
        end = today
    if start is None:
        start = end-relativedelta(days=unit_value[unit]+2)
    print(start, end)
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
    daily_trends = DailyTrend.select().where(
            (DailyTrend.time >= start) & (DailyTrend.time <= end) & (DailyTrend.region == region))

    if search is not None and len(search) > 0:
        exp = NodeList([
            SQL("jsonb_message_to_tsvector("),
            DailyTrend.metrics,
            SQL(") @@ '{}'".format(search))
            ], glue='')
        daily_trends = daily_trends.where(exp)
    print('size', len(daily_trends))
    daily_metrics = []
    for trend in daily_trends:
        stats = []
        for metric in trend.metrics:
            m_ = metric['stats']
            m_['tag'] = metric['tag'].replace('#', '')
            m_['date'] = trend.time
            if 'category' not in metric:
                m_['category'] = [-1]
            else:
                m_['category'] = metric['category']

            stats.append(m_)

        df = pd.DataFrame(stats)
        if len(df)> 0:
            daily_metrics.append(df)

    if end >= today:
        from cache import LatestTrend
        try:
            trend = LatestTrend.get(LatestTrend.region_id == region_id)
            today_stats = trend.metrics
        except:
            today_stats = []
        stats = []
        for metric in today_stats:
            m_ = metric['stats']
            m_['tag'] = metric['tag'].replace('#', '')
            m_['date'] = today
            if 'category' not in metric:
                m_['category'] = [-1]
            else:
                m_['category'] = metric['category']
            stats.append(m_)
        if len(stats):
            df = pd.DataFrame(stats)
            if len(df)> 0:
                daily_metrics.append(df)
    print('m size', len(daily_metrics))
    if len(daily_metrics) > 0:
        df = pd.concat(daily_metrics, axis=0)
        if search is not None and len(search) > 0:
            df = df.loc[df['tag'].str.contains(search, regex=False)]

        df.set_index('tag')
        df = df.drop(columns=["date"])
        if 'category' in df.columns:
            # df['category'] = [','.join(map(str, l)) for l in df['category']]
            # df = df.groupby(['tag', 'category'],as_index=False).mean()
            f2 = lambda x: [z for y in x for z in y]
            f1 = lambda x: ', '.join(x.dropna())
            d = dict.fromkeys(df[['tag','category']].columns.difference(['tag','category']), f1)
            d['category'] = f2
            df1 = df.groupby('tag', as_index=False).agg(d)
            df2 = df[['tag', 'rank', 'view', 'comment', 'like', 'dislike']].groupby(['tag'], as_index=False ).mean()
            df = pd.concat([df1.set_index('tag'), df2.set_index('tag')], axis=1, join='inner').reset_index()

        else:
            df = df.groupby(['tag'], as_index=False ).mean()
        df['weight'] = (101-df['rank'])*rw + ((df['view'])*vw + (df['comment'])*cw  + (df['like'])*lw - (df['dislike']*dw))/df['view']
        # df['tag'] = [ r[0] for r in df.index]
        # df['category'] = [ r[1] for r in df.index]

        topics = df.to_dict(orient='records')
        topics.sort(key=lambda x: x['weight'], reverse=True)
        result['topic'] = []
        for t in topics[:topic_limit]:
            e = {
                'tag':t['tag'], 
                'weight': t['weight'], 
                'rank': t['rank'], 
                'view': t['view'], 
                'like': t['like'], 
                'dislike': t['like'], 
                'comment': t['comment']  
            }
            if 'category' in t:
                e['category'] = list(set(t['category']))
            result['topic'].append(e)
    return result



def test_query():
    exp = NodeList([
            DailyTrend.metrics,
            SQL("->>'tag' in ('{}') ".format('阿努納奇'))
            ], glue='')
    daily_trends = DailyTrend.select().where( exp)
    print(daily_trends.sql())
    print(len(daily_trends))


if __name__ == '__main__':
    # from peewee import NodeList, SQL
    # exp = NodeList([
    #     SQL("jsonb_message_to_tsvector("),
    #     DailyTrend.metrics,
    #     SQL(") @@ '韓國瑜'")
    #     ], glue='')
    # query = DailyTrend.select().where(exp)
    # print(len(query))
    # end = datetime.now()
    # start = datetime.now() - timedelta(days=10)

    # data = trending_topic('TW', 'day', topic_limit=100, end=end, start=start)
    # print(len(data['topic']))
    # print(data['topic'][:20])
    test_query()