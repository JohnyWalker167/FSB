import os
import logging
from os import environ
from logging.handlers import RotatingFileHandler
from dotenv import load_dotenv
from requests import get as rget

# Configure logging
LOG_FILE_NAME = "log.txt"

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s - %(levelname)s] - %(name)s - %(message)s",
    datefmt='%d-%b-%y %H:%M:%S',
    handlers=[
        RotatingFileHandler(
            LOG_FILE_NAME,
            maxBytes=50000000,
            backupCount=10
        ),
        logging.StreamHandler()
    ]
)
logging.getLogger("pyrogram").setLevel(logging.ERROR)

logger = logging.getLogger(__name__)

CONFIG_FILE_URL = environ.get('CONFIG_FILE_URL')
try:
    if len(CONFIG_FILE_URL) == 0:
        raise TypeError
    try:
        res = rget(CONFIG_FILE_URL)
        if res.status_code == 200:
            with open('config.env', 'wb+') as f:
                f.write(res.content)
        else:
            logger.error(f"Failed to download config.env {res.status_code}")
    except Exception as e:
        logger.info(f"CONFIG_FILE_URL: {e}")
except:
    pass

load_dotenv('config.env', override=True)

#TMDB API 
TMDB_API_KEY = os.getenv('TMDB_API_KEY')

#IMGBB API 
IMGBB_API_KEY = os.getenv('IMGBB_API_KEY')

#TELEGRAM API
API_ID = int(os.getenv('API_ID'))
API_HASH = os.getenv('API_HASH')
BOT_TOKEN = os.getenv('BOT_TOKEN')
OWNER_ID = int(os.getenv('OWNER_ID'))
STRING_SESSION = os.getenv('STRING_SESSION')


DB_CHANNEL_ID = int(os.getenv('DB_CHANNEL_ID'))
UPDATE_CHANNEL_ID = int(os.getenv('UPDATE_CHANNEL_ID'))


#MONGO URI
MONGO_URI = os.getenv('MONGO_URI')
MONGO_DB_NAME = os.getenv('MONGO_DB_NAME')
COLLECTION_NAME = 'details'

