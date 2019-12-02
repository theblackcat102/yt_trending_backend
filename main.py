from fastapi import FastAPI
from datetime import datetime
from starlette.middleware.cors import CORSMiddleware
from utils import topic_filter
from models import Region

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

all_region = [r.region_id for r in Region.select()]

@app.get("/main")
def primary_view(search: str=None, unit: str="hourly", 
    region: str="all", start:str=None, end:str=None,
    lw: float=1, vw: float=1, cw: float=1, rw: float=1, dw: float=1,
    top: int=5):
    results = []
    target_regions = all_region
    if region != "all":
        target_regions = []
        for r in region.split(','):
            if len(r) > 1:
                target_regions.append(r)
    for r in target_regions:
        results.append( topic_filter(r, unit, search, start, end, topic_limit=top) )

    return {
        'status': 'ok',
        'results':  results
    }


@app.get("/main/{region_id}")
def read_item(search: str="", unit: str="hourly", 
    start:str="", end:str="",
    lw: float=1, vw: float=1, cw: float=1, rw: float=1, dw: float=1,
    top: int=5):

    result = topic_filter(region_id, unit=unit, search=search, 
        start=start, end=end, topic_limit=top)

    return result