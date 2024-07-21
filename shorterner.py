import asyncio
import aiohttp
from config import *
from pyshorteners import Shortener
from utils import auto_delete_message, get_readable_time

async def tiny(long_url):
    # Shorten URL using pyshorteners library
    try:
        s = Shortener()
        return await asyncio.to_thread(s.tinyurl.short, long_url)
    except Exception as e:
        logger.error(f'Failed to shorten URL: {long_url}, Error: {e}')
        return long_url

async def shorten_url(url):
    try:
        api_url = f"https://{SHORTERNER_URL}/api"
        params = {
            "api": URLSHORTX_API_TOKEN,
            "url": url,
            "format": "text"
        }

        async with aiohttp.ClientSession() as session:
            async with session.get(api_url, params=params) as response:
                if response.status == 200:
                    return (await response.text()).strip()
                else:
                    logger.error(
                        f"URL shortening failed. Status code: {response.status}, Response: {await response.text()}"
                    )
                    return url
    except Exception as e:
        logger.error(f"URL shortening failed: {e}")
        return url
    
