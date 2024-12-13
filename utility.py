import re
import subprocess
import asyncio
from config import *
from PIL import Image


async def remove_unwanted(input_string):
    # Use regex to match .mkv or .mp4 and everything that follows
    result = re.split(r'(\.mkv|\.mp4)', input_string)
    # Join the first two parts to get the string up to the extension
    return ''.join(result[:2])

async def extract_tg_link(telegram_link):
    try:
        pattern = re.compile(r'https://t\.me/c/(-?\d+)/(\d+)')
        match = pattern.match(telegram_link)
        if match:
            message_id = match.group(2)
            return message_id
        else:
            return None
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

async def auto_delete_message(user_message, bot_message):
    try:
        await user_message.delete()
        await asyncio.sleep(60)
        await bot_message.delete()
    except Exception as e:
        logger.error(f"{e}")

async def remove_extension(caption):
    try:
        # Remove .mkv and .mp4 extensions if present
        cleaned_caption = re.sub(r'\.mkv|\.mp4|\.webm', '', caption)
        return cleaned_caption
    except Exception as e:
        logger.error(e)
        return None

async def generate_combined_thumbnail(file_path: str, num_thumbnails: int, grid_columns: int) -> tuple:
    try:
        # List to store individual thumbnails
        thumbnails = []

        # Use ffprobe to get video duration
        duration_cmd = [
            'ffprobe', '-v', 'error', '-show_entries', 'format=duration', 
            '-of', 'default=noprint_wrappers=1:nokey=1', file_path
        ]
        duration = float(subprocess.check_output(duration_cmd).strip())

        # Generate evenly spaced intervals (excluding the very end)
        intervals = [duration * i / (num_thumbnails + 1) for i in range(1, num_thumbnails + 1)]

        # Create thumbnails at specified intervals
        for i, interval in enumerate(intervals):
            thumbnail_path = f"{file_path}_thumb_{i}.jpg"
            thumbnail_cmd = [
                'ffmpeg', '-ss', str(interval), '-i', file_path, 
                '-vf', 'thumbnail', '-frames:v', '1', thumbnail_path, '-y'
            ]
            subprocess.run(thumbnail_cmd, capture_output=True, check=True)
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

        # Clean up individual thumbnails, except the single thumbnail
        for thumb in thumbnails:
            os.remove(thumb)

        # Return combined thumbnail path, a single thumbnail path from first 60 seconds, and duration
        return combined_thumbnail_path
    except Exception as e:
        print(f"Error generating combined thumbnail: {e}")
        return None



