from fastapi import FastAPI
from datetime import datetime
from starlette.middleware.cors import CORSMiddleware
from utils import topic_filter, topic_interest
from models import Region
from datetime import datetime
import multiprocessing as mp
from custom_pool import CustomPool, NoDaemonProcess
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

    for r in target_regions:
        param = (r, unit, search, start, end, False, top, lw, vw, cw, rw, dw)
        params.append(param)

    pool_size = min(len(params), 3)

    q = mp.Queue()
    process = NoDaemonProcess(target=pool_wrapper, args=(topic_interest, params, q, pool_size))
    process.start()
    results = q.get()
    process.join()

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

    end = datetime.now()
    if end is not None:
        end = dateparser.parse(str(end))

    result = topic_filter(region_id, unit=unit, search=search,
        start=start, end=end, topic_limit=top, lw=lw, vw=vw, cw=cw, rw=rw, dw=dw)

    return result