import os
from dotenv import load_dotenv
load_dotenv()


DEBUG = True

POSTGRESQL_SETTINGS = {
    'DATABASE': os.getenv('DATABASE'),
    'USER': os.getenv('USER'),
    'HOST': os.getenv('HOST'),   
    'PORT': os.getenv('PORT'),
    'PASSWORD': os.getenv('PASSWORD'),
}

