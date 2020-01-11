# [Youtube Trending Backend](https://notify.institute/)

Visualize the trending tags from youtube trending list across 30+ countries


This is the backend service serving the final visualization. You can check out the frontend [youtube-viz-demo](https://github.com/vxncetxn/youtube-viz-demo)

## Setup

This project is setup using fastapi and served using uvicorn. Data uses Peewee ORM with Postgresql 10. 

you need to create a config file .env in the root path with the following fields:

```
DATABASE="DB_NAME"
DB_USER="DB_USER"
HOST="DB_IP_ADDRESS"
PORT="DB_PORT"
PASSWORD="DB_PASSWORD"
```


Then setup the environment and you 

```
virtualenv -p python3 env
source env/bin/activate
pip install -r requirements.txt
```

Initialize the database table

```
python models.py
```

Once you have populate the table with data you can run the server using this command

```
uvicorn main:app
```


Here's some showcase of the final visualization results

![Landing page](https://github.com/theblackcat102/yt_trending_backend/blob/master/example/landing.png)

![Race chart](https://github.com/theblackcat102/yt_trending_backend/blob/master/example/race.png)


![Tag Header page](https://github.com/theblackcat102/yt_trending_backend/blob/master/example/tag_header.png)

![Tag Stats](https://github.com/theblackcat102/yt_trending_backend/blob/master/example/tag_stats.png)

