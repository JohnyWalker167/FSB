
import os
import logging
from os import environ
from dotenv import load_dotenv
from requests import get as rget

LOG_FILE_NAME = "log.txt"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s - %(levelname)s] - %(name)s - %(message)s",
    datefmt='%d-%b-%y %H:%M:%S',
    handlers=[
        logging.StreamHandler()  # Only log to console
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

#TELEGRAM API
API_ID = int(os.getenv('API_ID'))
API_HASH = os.getenv('API_HASH')
BOT_TOKEN = os.getenv('BOT_TOKEN')
DB_CHANNEL_ID = int(os.getenv('DB_CHANNEL_ID'))
OWNER_USERNAME = os.getenv('OWNER_USERNAME')
STRING_SESSION = os.getenv('STRING_SESSION')

IMGBB_API_KEY = os.getenv('IMGBB_API_KEY')

#MONGO URI
MONGO_URI = os.getenv('MONGO_URI')
MONGO_DB_NAME = "tgfiles"
COLLECTION_NAME = 'details'
