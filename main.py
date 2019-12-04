from fastapi import FastAPI
from datetime import datetime
from starlette.middleware.cors import CORSMiddleware
from utils import topic_filter, topic_interest
from models import Region
import multiprocessing as mp
from custom_pool import CustomPool
import dateparser

app = FastAPI(debug=False)
app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

all_region = [ r.strip() for r in open('valid_region.txt', 'r').readlines() ]

@app.get("/main")
def primary_view(search: str=None, unit: str="day",
    region: str="all", start:str=None, end:str=None,
    lw: float=1, vw: float=1, cw: float=1, rw: float=1, dw: float=1,
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

    if end is not None:
        end = dateparser.parse(str(end))

    for r in target_regions:
        param = (r, unit, search, start, end, False, top, lw, vw, cw, rw, dw)
        params.append(param)

    pool = CustomPool(1)

    p_results = pool.starmap(topic_interest, params)
    pool.close()

    results = []
    for r in p_results:
        if len(r['topic']) > 0:
            results.append(r)

    return {
        'status': 'ok',
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

    if end is not None:
        end = dateparser.parse(str(end))

    result = topic_filter(region_id, unit=unit, search=search,
        start=start, end=end, topic_limit=top, lw=lw, vw=vw, cw=cw, rw=rw, dw=dw)

    return result