from models import Video, Statistic, Activity, DataPoint, Region, Channel, postgres_database
import json
import re
from collections import Counter, defaultdict
from tqdm import tqdm
from utils import extract_video_unique_keyword

def isfloat(value):
  try:
    float(value)
    return True
  except ValueError:
    return False

def clean_tag(t):
    if isfloat(t):
        return None
    return re.sub(r"[#&'()]", '', t)
    
def extract_frequency(region):
    stats = Statistic.select(Statistic.video).where(
        Statistic.trending_region == region
    ).group_by(Statistic.video) #.limit(10000)

    unique_video = defaultdict()

    for s in tqdm(stats):
        if s.video.id not in unique_video:
            unique_video[s.video.id] = s.video

    valid_tag = []

    print('videos {}'.format(len(unique_video)))
    
    for _, video in unique_video.items():
        tags = extract_video_unique_keyword(video)
        for tag in tags:
            _tag = clean_tag(tag)
            if _tag and len(_tag) > 1:
                valid_tag.append(_tag)
    print('total tag {}, videos {}'.format(len(valid_tag), len(unique_video)))

    return Counter(valid_tag)


def extract_blacklist():
    # region = Region.get(Region.region_id == 'TW')
    # channels = Channel.select().where(Channel.country == region)
    channels = Channel.select()
    blacklist_tags = []
    for c in tqdm(channels):
        videos = Video.select().where(Video.channel == c)
        tags = []
        count = 0
        for v in videos:
            tags += v.tags
            count += 1
        counter = Counter(tags)
        for key, value in counter.items():
            if value >= count*0.7 and count > 3:
                blacklist_tags.append(key)
    blacklist_tags = list(set(blacklist_tags))
    with open('video_blacklist_tag_0.7.txt', 'w') as f:
        for key in blacklist_tags:
            f.write('{}\n'.format(key))


def push_video():
    videos = Video.select()
    for v in tqdm(videos):
        if 'channel' in v.meta:
            continue
        channel_title = v.channel.title
        v.meta['channel'] = {
            'title': channel_title
        }
        with postgres_database.atomic():
            v.save()


if __name__ == '__main__':
    # region = Region.get(Region.region_id == 'TW')
    # frequency_result = dict(extract_frequency(region))
    # update_frequency = frequency_result

    # for key, value in frequency_result.items():
    #     update_frequency.append((key, value))
    # update_frequency.sort(key=lambda x: x[1], reverse=True)


    # with open('tag_frequency_TW.txt', 'w') as f:
    #     for pair in update_frequency:
    #         f.write(str(pair)+'\n')
    push_video()



