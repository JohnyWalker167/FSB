import os
import asyncio
import time
import imgbbpy
from tzlocal import get_localzone
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from pyrogram import Client, enums, filters
from config import *
from utility import *
from motor.motor_asyncio import AsyncIOMotorClient 
from asyncio import Queue

THUMBNAIL_COUNT = 9
GRID_COLUMNS = 3 # Number of columns in the grid

last_time = time.time()

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
    global last_time
    current_time = time.time()
    
    # Calculate time difference
    diff = current_time - last_time
    if diff > 0:  # Avoid division by zero
        # Calculate percentage
        percentage = current * 100 / total
        
        # Calculate speed in bytes per second and convert to Mbps
        speed = current / diff
        speed_mbps = (speed / (1024 * 1024)) * 8  # Convert bytes to MB and MB to Mbps
        
        # Calculate elapsed time in milliseconds
        elapsed_time = round(diff * 1000)
        
        # Calculate time to completion in milliseconds
        time_to_completion = round((total - current) / speed) * 1000
        
        # Calculate estimated total time in milliseconds
        estimated_total_time = elapsed_time + time_to_completion
    else:
        percentage = 0
        speed_mbps = 0
        elapsed_time = 0
        time_to_completion = 0
        estimated_total_time = 0
    
    # Update last_time for the next call
    last_time = current_time
    
    # Display progress details
    print(
        f"\rProgress: {percentage:.1f}% | Speed: {speed_mbps:.2f} Mbps | "
        f"Elapsed Time: {elapsed_time} ms | Time to Completion: {time_to_completion} ms | "
        f"Estimated Total Time: {estimated_total_time} ms",
        end=""
    )

async def process_message(client, message):
    media = message.document or message.video or message.audio

    if media:
        caption = await remove_unwanted(message.caption if message.caption else media.file_name)
        file_name = await remove_extension(caption)

        # Download media with progress updates
        file_path = await bot.download_media(
                            message, 
                            file_name=f"{message.id}", 
                            progress=progress
                        )
        
        # Generate thumbnails after downloading
        screenshots = await generate_combined_thumbnail(file_path, THUMBNAIL_COUNT, GRID_COLUMNS)

        if screenshots :
            logger.info(f"Thumbnail generated: {screenshots}")
            try:
                thumb = imgclient.upload(file=f"{screenshots}")

                document = {
                    "caption": file_name,
                    "thumbnail_url": thumb.url,
                }
                if thumb:
                    # Insert into MongoDB
                    info_collection.insert_one(document)
                    # Send the photo to the update channel
                    await bot.send_photo(
                        chat_id=UPDATE_CHANNEL_ID,
                        photo=f"{screenshots}",
                        caption=f"<b>{file_name}</b>\n\n✅ Now Available."
                    )    
                    os.remove(file_path)
                    os.remove(screenshots)
            except Exception as e:
                await message.reply_text(f'{e}')

# Modify the on_message handler to enqueue the messages
@bot.on_message(filters.chat(DB_CHANNEL_ID) & (filters.document | filters.video | filters.audio))
async def handle_new_message(client, message):
    # Add the message to the queue for sequential processing
    await message_queue.put(message)

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
        
                    # Download media with progress updates
                    file_path = await bot.download_media(
                                        file_message, 
                                        file_name=f"{file_message.id}", 
                                        progress=progress
                                    )
                    
                    # Generate thumbnails after downloading
                    screenshots = await generate_combined_thumbnail(file_path, THUMBNAIL_COUNT, GRID_COLUMNS)

                    if screenshots :
                        logger.info(f"Thumbnail generated: {screenshots}")
                        try:
                            thumb = imgclient.upload(file=f"{screenshots}")

                            document = {
                                "caption": file_name,
                                "thumbnail_url": thumb.url,
                            }
                            if thumb:
                                # Insert into MongoDB
                                info_collection.insert_one(document)
                                os.remove(file_path)
                                # Send the photo to the update channel
                                await bot.send_photo(
                                    chat_id=UPDATE_CHANNEL_ID,
                                    photo=f"{screenshots}",
                                    caption=f"<b>{file_name}</b>\n\n✅ Now Available."
                                )    
                                os.remove(screenshots)
                        except Exception as e:
                            await message.reply_text(f"Error in data update {e}")
                            await asyncio.sleep(3)
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

@bot.on_message(filters.private & filters.command("del") & filters.user(OWNER_ID))
async def delete_command(client, message):
    try:               
        bot_message = await message.reply_text("Send file link")
        user_message = await bot.listen(message.chat.id)
        user_file_link = user_message.text.strip()
        id = await extract_tg_link(user_file_link)
        asyncio.create_task(auto_delete_message(bot_message, user_message))

        file_id = int(id)  # Assuming file_id is a string; adjust if needed.
        file_msg = await bot.get_message(DB_CHANNEL_ID, file_id)
        caption = await remove_extension(file_msg.caption)
        result = await info_collection.delete_one({"caption": caption})
        
        if result.deleted_count > 0:
                bot_message = await message.reply_text(f"{caption} deleted successfully.")
                await auto_delete_message(message, bot_message)
    except Exception as e:
        bot_message = await message.reply_text(f"Error: {e}")
        await auto_delete_message(message, bot_message)

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
