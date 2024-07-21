import io
import re
import asyncio
import aiohttp
import ffmpeg as render
from config import *
from PIL import Image
from pyrogram.types import User
from mutagen.mp3 import MP3
from mutagen.flac import FLAC
from mutagen.mp4 import MP4
from mutagen.id3 import ID3, APIC
from mutagen import File as MutagenFile


async def auto_delete_message(user_message, bot_message):
    try:
        await user_message.delete()
        await asyncio.sleep(60)
        await bot_message.delete()
    except Exception as e:
        logger.error(f"{e}")
        
def get_readable_time(seconds: int) -> str:
    result = ""
    (days, remainder) = divmod(seconds, 86400)
    days = int(days)
    if days != 0:
        result += f"{days}d"
    (hours, remainder) = divmod(remainder, 3600)
    hours = int(hours)
    if hours != 0:
        result += f"{hours}h"
    (minutes, seconds) = divmod(remainder, 60)
    minutes = int(minutes)
    if minutes != 0:
        result += f" {minutes}m"
    seconds = int(seconds)
    result += f" {seconds}s"
    return result
    
async def remove_unwanted(caption):
    try:
        # Remove .mkv and .mp4 extensions if present
        cleaned_caption = re.sub(r'\.mkv|\.mp4|\.webm', '', caption)
        return cleaned_caption
    except Exception as e:
        logger.error(e)
        return None

def humanbytes(size):
    # Function to format file size in a human-readable format
    if not size:
        return "0 B"
    # Define byte sizes
    suffixes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
    i = 0
    while size >= 1024 and i < len(suffixes) - 1:
        size /= 1024
        i += 1
    f = ('%.2f' % size).rstrip('0').rstrip('.')
    return f"{f} {suffixes[i]}"

async def extract_movie_info(caption):
    try:
        regex = re.compile(r'(.+?)(\d{4})')
        match = regex.search(caption)

        if match:
             movie_name = match.group(1).replace('.', ' ').strip()
             release_year = match.group(2)
             return movie_name, release_year
    except Exception as e:
        print(e)
    return None, None

async def get_movie_poster(movie_name, release_year):
    tmdb_search_url = f'https://api.themoviedb.org/3/search/multi?api_key={TMDB_API_KEY}&query={movie_name}'
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(tmdb_search_url) as search_response:
                search_data = await search_response.json()

                if search_data['results']:
                    # Filter results based on release year and first air date
                    matching_results = [
                        result for result in search_data['results']
                        if ('release_date' in result and result['release_date'][:4] == str(release_year)) or
                        ('first_air_date' in result and result['first_air_date'][:4] == str(
                            release_year))
                    ]

                    if matching_results:
                        result = matching_results[0]

                        # Fetch additional details using movie ID
                        movie_id = result['id']
                        media_type = result['media_type']
 
                        # Use the backdrop_path or poster_path
                        poster_path = None
                        if 'poster_path' in result and result['poster_path']:
                            poster_path = result['poster_path']
                            
                            poster_url = f"https://image.tmdb.org/t/p/original{poster_path}"
                            return poster_url

    except Exception as e:
        print(f"Error fetching TMDB data: {e}")

    return None
  
async def extract_tg_link(telegram_link):
    try:
        pattern = re.compile(r'https://t\.me/c/(-?\d+)/(\d+)')
        match = pattern.match(telegram_link)
        if match:
            message_id = match.group(2)
            return message_id
        else:
            return None, None
    except Exception as e:
        logger.error(e)

async def extract_channel_id(telegram_link):
    try:
        pattern = re.compile(r'https://t\.me/c/(-?\d+)/(\d+)')
        match = pattern.match(telegram_link)
        if match:
            channel_id = match.group(1)
            formatted_channel_id = f'-100{channel_id}'
            return formatted_channel_id
        else:
            return None
    except Exception as e:
        logger.error(e)
        
'''
async def download_initial_part(client, media, file_path, chunk_size):
    # Open the file for writing in binary mode
    with open(file_path, 'wb') as f:
        async for chunk in client.stream_media(media):
            f.write(chunk)
            if f.tell() >= chunk_size:
                break
'''

async def generate_combined_thumbnail(file_path: str, intervals: list, grid_columns: int) -> str:
    try:
        # List to store individual thumbnails
        thumbnails = []

        for i, interval in enumerate(intervals):
            thumbnail_path = f"{file_path}_thumb_{i}.jpg"
            # Use ffmpeg to generate thumbnails at specified intervals
            (
                render
                .input(file_path, ss=interval)  # Seek to the interval
                .output(thumbnail_path, vframes=1)
                .run(capture_stdout=True, capture_stderr=True)
            )
            thumbnails.append(thumbnail_path)

        # Open all thumbnails and combine them into a grid
        images = [Image.open(thumb) for thumb in thumbnails]
        widths, heights = zip(*(img.size for img in images))

        max_width = max(widths)
        max_height = max(heights)

        # Calculate grid dimensions
        grid_rows = (len(images) + grid_columns - 1) // grid_columns
        grid_width = grid_columns * max_width
        grid_height = grid_rows * max_height

        combined_image = Image.new('RGB', (grid_width, grid_height))

        for index, img in enumerate(images):
            x = (index % grid_columns) * max_width
            y = (index // grid_columns) * max_height
            combined_image.paste(img, (x, y))

        combined_thumbnail_path = f"{file_path}_combined.jpg"
        combined_image.save(combined_thumbnail_path)

        # Clean up individual thumbnails
        for thumb in thumbnails:
            os.remove(thumb)

        return combined_thumbnail_path
    except Exception as e:
        print(f"Error generating combined thumbnail: {e}")
        return None

async def get_audio_thumbnail(audio_path):
    audio = MutagenFile(audio_path)
    if isinstance(audio, MP3):
        if audio.tags and isinstance(audio.tags, ID3):
            for tag in audio.tags.values():
                if isinstance(tag, APIC):
                    return io.BytesIO(tag.data)
    elif isinstance(audio, FLAC):
        if audio.pictures:
            return io.BytesIO(audio.pictures[0].data)
    elif isinstance(audio, MP4):
        if audio.tags and 'covr' in audio.tags:
            cover = audio.tags['covr'][0]
            return io.BytesIO(cover)
        
    return None

'''
def generate_thumbnail(file_path: str) -> str:
    try:
        # Output thumbnail path
        thumbnail_path = f"{file_path}.jpg"

        # Use ffmpeg to generate a thumbnail
        (
            zender
            .input(file_path, ss='00:00:01')  # Seek to 1 second
            .output(thumbnail_path, vframes=1)
            .run(capture_stdout=True, capture_stderr=True)
        )
        return thumbnail_path
    except Exception as e:
        print(f"Error generating thumbnail: {e}")
        return None
'''

async def get_user_link(user: User) -> str:
    user_id = user.id
    first_name = user.first_name
    return f'<a href=tg://user?id={user_id}>{first_name}</a>'
