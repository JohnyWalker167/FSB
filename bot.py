import os
import asyncio
import imgbbpy
import time
from tzlocal import get_localzone
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from pyrogram import Client, enums, filters, utils as pyroutils
from config import *
from utility import *
from inspect import signature
from motor.motor_asyncio import AsyncIOMotorClient 
from html import escape
from asyncio import Queue

pyroutils.MIN_CHAT_ID = -999999999999
pyroutils.MIN_CHANNEL_ID = -100999999999999

last_update = {"current": 0, "time": time.time()}

THUMBNAIL_COUNT = 9
GRID_COLUMNS = 3 # Number of columns in the grid

# Initialize the client with your API key
imgclient = imgbbpy.SyncClient(IMGBB_API_KEY)

# Define an async queue to handle messages sequentially
message_queue = Queue()

user_data = {}

# Initialize MongoDB client
MONGO_COLLECTION = "users"
PHOTO_COLLECTION = "info"

mongo_client = AsyncIOMotorClient(MONGO_URI)  # Use AsyncIOMotorClient
db = mongo_client[MONGO_DB_NAME]
collection = db[COLLECTION_NAME]
mongo_collection = db[MONGO_COLLECTION]
info_collection = db[PHOTO_COLLECTION]

def wztgClient(*args, **kwargs):
    if 'max_concurrent_transmissions' in signature(Client.__init__).parameters:
        kwargs['max_concurrent_transmissions'] = 1000
    return Client(*args, **kwargs)

bot = Client(
    "bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    workers=1000,
    parse_mode=enums.ParseMode.HTML
).start()

'''
user = Client(
                "user",
                api_id=int(API_ID),
                api_hash=API_HASH,
                session_string=STRING_SESSION,
                no_updates = True
).start()
'''

bot_loop = bot.loop
bot_username = bot.me.username
AsyncIOScheduler(timezone = str(get_localzone()), event_loop = bot_loop)

@bot.on_message(filters.private & filters.command("start"))
async def start_command(client, message):
        reply = await message.reply_text(f"<b>💐Welcome</b>")
        await auto_delete_message(message, reply)

async def progress(current, total):
    global last_update
    
    now = time.time()
    elapsed_time = now - last_update["time"]
    if elapsed_time > 0:
        speed = (current - last_update["current"]) / elapsed_time  # Bytes per second
        speed_str = f"{speed / 1024:.2f} KB/s" if speed < 1024 ** 2 else f"{speed / (1024 ** 2):.2f} MB/s"
    else:
        speed_str = "Calculating..."
    
    percentage = current * 100 / total
    print(f"\r{percentage:.1f}% | Speed: {speed_str}", end="")
    
    # Update last progress details
    last_update["current"] = current
    last_update["time"] = now

def reset_progress_tracker():
    """Reset the progress tracker to its initial state."""
    global last_update
    last_update = {"current": 0, "time": time.time()}

async def process_message(client, message):
    media = message.document or message.video or message.audio

    if media:
        caption = await remove_unwanted(message.caption if message.caption else media.file_name)
        file_name = await remove_extension(caption)

        # Set title and artist only if the media is audio
        title = media.title if message.audio else None
        artist = media.performer if message.audio else None

        # Check if the file_name already exists in the database
        existing_document = await info_collection.find_one({"file_name": file_name})

        if existing_document:
            await message.reply_text(f"Duplicate file detected. The file '<code>{file_name}</code>' already exists in the database.")
            return
        else:
            if message.video:
                # Download media with progress updates
                file_path = await bot.download_media(
                                    message, 
                                    file_name=f"{message.id}", 
                                    progress=progress 
                                )
                
                # Generate thumbnails after downloading
                screenshots, thumbnail, duration = await generate_combined_thumbnail(file_path, THUMBNAIL_COUNT, GRID_COLUMNS)

                if thumbnail:
                    reset_progress_tracker()
                    print(f"\n Now Uploading")
                    await bot.send_video(DB_CHANNEL_ID, 
                                            video=file_path, 
                                            caption=f"<b>{escape(caption)}</b>", 
                                            duration=duration, 
                                            width=480, 
                                            height=320, 
                                            thumb=f"{thumbnail}",
                                            progress=progress
                                            )          

                if screenshots :
                    logger.info(f"Thumbnail generated: {screenshots}")
                    try:
                        ss = imgclient.upload(file=f"{screenshots}")
                        thumb = imgclient.upload(file=f"{thumbnail}")
                        
                        document = {
                            "file_name": file_name,
                            "thumbnail_url": ss.url,
                            "screenshot_url": thumb.url
                        }
                        if thumb:
                            # Insert into MongoDB
                            info_collection.insert_one(document)
                            os.remove(file_path)
                            os.remove(screenshots)
                            os.remove(thumbnail)  
                    except Exception as e:
                        await message.reply_text(f'{e}')    
            if message.audio:
                # Download media with progress updates
                file_path = await bot.download_media(
                                    message, 
                                    file_name=f"{message.id}", 
                                    progress=progress 
                                )
                thumb_path = await get_audio_thumbnail(file_path)
                if thumb_path :
                    logger.info(f"Thumbnail generated: {thumb_path}")
                    try:
                        thumb = imgclient.upload(file=thumb_path)

                        document = {
                                        "file_name": file_name,
                                        "title": title,
                                        "artist": artist,
                                        "thumbnail_url": thumb.url,
                                        "screenshot_url": thumb.url
                                    }                       
                        if thumb:
                            # Insert into MongoDB
                            info_collection.insert_one(document)
                            os.remove(file_path)
                            os.remove(thumb_path)
                    except Exception as e:
                        await message.reply_text(f'{e}')  
  
@bot.on_message(filters.private & (filters.document | filters.video | filters.audio) & filters.user(OWNER_ID))
async def handle_new_message(client, message):
    # Add the message to the queue for sequential processing
    await message_queue.put(message)
    
'''
# Modify the on_message handler to enqueue the messages
@bot.on_message(filters.chat(DB_CHANNEL_ID) & (filters.document | filters.video | filters.audio))
async def handle_new_message(client, message):
    # Add the message to the queue for sequential processing
    await message_queue.put(message)

'''

# Function to process the queue in sequence
async def process_queue():
    while True:
        message = await message_queue.get()  # Get the next message from the queue
        if message is None:  # Exit condition
            break
        await process_message(bot, message)  # Process the message
        message_queue.task_done()

@bot.on_message(filters.private & filters.command("send") & filters.user(OWNER_ID))
async def handle_file(client, message):
    try:
        
        # Helper function to get user input
        async def get_user_input(prompt):
            bot_message = await message.reply_text(prompt)
            user_message = await bot.listen(message.chat.id)
            asyncio.create_task(auto_delete_message(bot_message, user_message))
            return await extract_tg_link(user_message.text.strip())
        
        # Get the start and end message IDs
        start_msg_id = int(await get_user_input("Send first post link"))
        end_msg_id = int(await get_user_input("Send end post link"))
        
        batch_size = 199
        for start in range(int(start_msg_id), int(end_msg_id) + 1, batch_size):
            end = min(start + batch_size - 1, int(end_msg_id))
            file_messages = await bot.get_messages(DB_CHANNEL_ID, range(start, end + 1))

            for file_message in file_messages:
                media = file_message.document or file_message.video or file_message.audio
                if media:
                    caption = await remove_unwanted(file_message.caption if file_message.caption else media.file_name)
                    file_name = await remove_extension(caption)

                    # Set title and artist only if the media is audio
                    title = media.title if message.audio else None
                    artist = media.performer if message.audio else None

                    # Check if the file_name already exists in the database
                    existing_document = await info_collection.find_one({"file_name": file_name})

                    if existing_document:
                        await message.reply_text(f"Duplicate file detected. The file '<code>{file_name}</code>' already exists in the database.")
                        continue
                    else:
                        if file_message.video:           
                            # Download media with progress updates
                            file_path = await bot.download_media(
                                                file_message, 
                                                file_name=f"{file_message.id}", 
                                                progress=progress 
                                            )
                    
                            # Generate thumbnails after downloading
                            screenshots, thumbnail, duration = await generate_combined_thumbnail(file_path, THUMBNAIL_COUNT, GRID_COLUMNS)

                            if screenshots :
                                logger.info(f"Thumbnail generated: {screenshots}")
                                try:
                                    ss = imgclient.upload(file=f"{screenshots}")
                                    thumb = imgclient.upload(file=f"{thumbnail}")
                                    
                                    document = {
                                        "file_name": file_name,
                                        "thumbnail_url": ss.url,
                                        "screenshot_url": thumb.url
                                    }
                                    if thumb:
                                        # Insert into MongoDB
                                        info_collection.insert_one(document)
                                        os.remove(file_path)
                                        os.remove(screenshots)
                                        os.remove(thumbnail)  
                                except Exception as e:
                                    await message.reply_text(f"Error in data update {e}")
                                    await asyncio.sleep(3)
                        if file_message.audio:
                            # Download media with progress updates
                            file_path = await bot.download_media(
                                                file_message, 
                                                file_name=f"{file_message.id}", 
                                                progress=progress 
                                            )
                            thumb_path = await get_audio_thumbnail(file_path)
                            if thumb_path :
                                logger.info(f"Thumbnail generated: {thumb_path}")
                                try:
                                    thumb = imgclient.upload(file=thumb_path)

                                    document = {
                                                    "file_name": file_name,
                                                    "title": title,
                                                    "artist": artist,
                                                    "thumbnail_url": thumb.url,
                                                    "screenshot_url": thumb.url
                                                }                       
                                    if thumb:
                                        # Insert into MongoDB
                                        info_collection.insert_one(document)
                                        os.remove(file_path)
                                        os.remove(thumb_path)
                                except Exception as e:
                                    await message.reply_text(f'{e}')  

        await message.reply_text(f"✅ Update Completed ")
    except Exception as e:
        bot_message = await message.reply_text(f"An error occurred: {e}")
        await auto_delete_message(message, bot_message)

'''
@bot.on_message(filters.private & filters.command("delete") & filters.user(OWNER_ID))
async def handle_file(client, message):
    try:
        
        # Helper function to get user input
        async def get_user_input(prompt):
            bot_message = await message.reply_text(prompt)
            user_message = await bot.listen(message.chat.id)
            asyncio.create_task(auto_delete_message(bot_message, user_message))
            return await extract_tg_link(user_message.text.strip())
        
        # Get the start and end message IDs
        start_msg_id = int(await get_user_input("Send first post link"))
        end_msg_id = int(await get_user_input("Send end post link"))
        
        batch_size = 199
        
        for start in range(int(start_msg_id), int(end_msg_id) + 1, batch_size):
            end = min(start + batch_size - 1, int(end_msg_id))
            file_messages = await user.get_messages(UPDATE_CHANNEL_ID, range(start, end + 1))

            for file_message in file_messages:
                media = file_message.photo
                if media:
                    await file_message.delete()
                    await asyncio.sleep(3)
    except Exception as e:
        bot_message = await message.reply_text(f"An error occurred: {e}")
        await auto_delete_message(message, bot_message)
 '''

# Get Log Command
@bot.on_message(filters.command("log") & filters.user(OWNER_ID))
async def log_command(client, message):
    user_id = message.from_user.id

    # Send the log file
    try:
        reply = await bot.send_document(user_id, document=LOG_FILE_NAME, caption="Bot Log File")
        await auto_delete_message(message, reply)
    except Exception as e:
        await bot.send_message(user_id, f"Failed to send log file. Error: {str(e)}")

async def main():
    await asyncio.create_task(process_queue())

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logger.info("Bot is starting...")
    
    try:
        bot.loop.run_until_complete(main())
        bot.loop.run_forever()
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received. Shutting down gracefully...")
    finally:
        logger.info("Bot has stopped.")
