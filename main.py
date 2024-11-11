import asyncio
import imgbbpy
import requests
from inspect import signature
from datetime import datetime, timezone
from config import *
from pymongo import MongoClient
from utils import *
from pyrogram import idle
from pyromod import listen
from pyrogram import Client, filters, enums, utils as pyroutils
from pyrogram.errors import FloodWait
from status import *
from asyncio import get_event_loop

loop = get_event_loop()

# Initialize the client with your API key
imgclient = imgbbpy.SyncClient(IMGBB_API_KEY)

# Initialize MongoDB client
MONGO_COLLECTION = "users"
mongo_client = MongoClient(MONGO_URI)  
db = mongo_client[MONGO_DB_NAME]
collection = db[COLLECTION_NAME]
mongo_collection = db[MONGO_COLLECTION]

pyroutils.MIN_CHAT_ID = -999999999999
pyroutils.MIN_CHANNEL_ID = -100999999999999


THUMBNAIL_COUNT = 9
GRID_COLUMNS = 3 # Number of columns in the grid

app = Client(
    "my_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    workers=1000,
    parse_mode=enums.ParseMode.HTML
)

user = Client(
                "userbot",
                api_id=int(API_ID),
                api_hash=API_HASH,
                session_string=STRING_SESSION,
                no_updates = True
)

async def main():
    async with app, user:
        await idle()

with app:
    bot_username = (app.get_me()).username

def wztgClient(*args, **kwargs):
    if 'max_concurrent_transmissions' in signature(Client.__init__).parameters:
        kwargs['max_concurrent_transmissions'] = 1000
    return Client(*args, **kwargs)

@app.on_message(filters.command("send") & filters.user(OWNER_USERNAME))
async def handle_media_message(client, message):
    user_id = message.from_user.id
    try:
        async def get_user_input(prompt):
            rply = await message.reply_text(prompt)
            link_msg = await app.listen(message.chat.id)
            return link_msg.text

        # Collect input from the user
        start_msg_id = int(await extract_tg_link(await get_user_input("Send first post link")))
        end_msg_id = int(await extract_tg_link(await get_user_input("Send end post link")))

        batch_size = 199
        for start in range(start_msg_id, end_msg_id + 1, batch_size):
            end = min(start + batch_size - 1, end_msg_id)
            # Get and copy messages
            file_messages = await app.get_messages(DB_CHANNEL_ID, range(start, end + 1))

            for file_message in file_messages:
                media = file_message.document or file_message.video
                
                if media:
                    caption = file_message.caption if file_message.caption else None

                    if caption:
                        file_name = await remove_unwanted(caption)

                        logger.info(f"Starting download of {file_message.id}...")

                        # Download media with progress updates
                        file_path = await app.download_media(
                            file_message, 
                            file_name=f"{file_message.id}", 
                            progress=progress 
                        )

                        # Generate thumbnails after downloading
                        screenshots, thumbnail, duration = await generate_combined_thumbnail(file_path, THUMBNAIL_COUNT, GRID_COLUMNS)

                        if screenshots and thumbnail:
                            logger.info(f"Thumbnail generated: {screenshots}")

                            file_info = {
                                "file_id": file_message.id, 
                                "file_name": file_name, 
                                "file_size": humanbytes(media.file_size)
                            }

                            try:
                                # Upload thumbnail and screenshots to ImgBB
                                thumb = imgclient.upload(file=f"{thumbnail}")
                                os.remove(thumbnail)

                                await asyncio.sleep(5)

                                ss = imgclient.upload(file=f"{screenshots}")
                                os.remove(screenshots)
                                                        
                                # Create the document to store in MongoDB
                                document = {
                                    "file_info": file_info,
                                    "thumbnail_url": thumb.url, 
                                    "screenshot_url": ss.url, 
                                    "timestamp": datetime.now(timezone.utc).isoformat()
                                }

                                if thumb:
                                    # Insert into MongoDB
                                    try:
                                        collection.insert_one(document)
                                        logger.info(f"File {file_name} uploaded and data saved successfully.")
                                        os.remove(file_path)
                                    except Exception as e:
                                        logger.error(f"Error in handle_media_message: {e}")
                                        os.remove(file_path)
                                        await app.send_message(user_id, text=f"An error occurred while adding the file information {file_name}")

                            except Exception as e:
                                await app.send_message(user_id, text=f"Failed to upload the video thumbnail for {file_name}. Please try again.")
                                logger.error(f"Error uploading video thumbnail: {e}")

                    await asyncio.sleep(3)  # To prevent rate limiting
            await message.reply_text("Data Update Successfull")

    except Exception as e:
        logger.error(f"Error in handle_media_message: {e}")
        await message.reply_text("An unexpected error occurred.") 
    finally:
        if os.path.exists(file_path):
            os.remove(file_path)

@app.on_message(filters.command("sendm") & filters.user(OWNER_USERNAME))
async def handle_media_message(client, message):
    user_id = message.from_user.id
    try:
        async def get_user_input(prompt):
            rply = await message.reply_text(prompt)
            link_msg = await app.listen(message.chat.id)
            return link_msg.text

        # Collect input from the user
        start_msg_id = int(await extract_tg_link(await get_user_input("Send first post link")))
        end_msg_id = int(await extract_tg_link(await get_user_input("Send end post link")))

        batch_size = 199
        for start in range(start_msg_id, end_msg_id + 1, batch_size):
            end = min(start + batch_size - 1, end_msg_id)
            # Get and copy messages
            file_messages = await app.get_messages(DB_CHANNEL_ID, range(start, end + 1))

            for file_message in file_messages:
                media = file_message.audio
                
                if media:

                        logger.info(f"Starting download of {file_message.id}...")

                        # Download media with progress updates
                        audio_path = await app.download_media(
                            file_message,
                            file_name=f"{file_message.id}",
                            progress=progress 
                        )

                        # Generate thumbnails after downloading
                        thumb_path = await get_audio_thumbnail(audio_path)

                        if thumb_path:
                            logger.info(f"Thumbnail generated: {thumb_path}")

                            file_info = {
                                "file_id": file_message.id, 
                                "title": media.title, 
                                "artist": media.performer,
                                "file_size": humanbytes(media.file_size)
                            }

                            try:
                                # Upload thumbnail and screenshots to ImgBB
                                thumb = imgclient.upload(file=f"{thumb_path}")
                                os.remove(thumb_path)

                                await asyncio.sleep(5)
                                                        
                                # Create the document to store in MongoDB
                                document = {
                                    "file_info": file_info,
                                    "thumbnail_url": thumb.url, 
                                    "timestamp": datetime.now(timezone.utc).isoformat()
                                }

                                if thumb:
                                    # Insert into MongoDB
                                    try:
                                        collection.insert_one(document)
                                        logger.info(f"File {media.title} uploaded and data saved successfully.")
                                        os.remove(audio_path)
                                    except Exception as e:
                                        logger.error(f"Error in handle_media_message: {e}")
                                        os.remove(audio_path)
                                        await app.send_message(user_id, text=f"An error occurred while adding the file information {media.title}")

                            except Exception as e:
                                await app.send_message(user_id, text=f"Failed to upload the video thumbnail for {media.title}. Please try again.")
                                logger.error(f"Error uploading video thumbnail: {e}")

                        await asyncio.sleep(3)  # To prevent rate limiting
            await message.reply_text("Data Update Successfull")

    except Exception as e:
        logger.error(f"Error in handle_media_message: {e}")
        await message.reply_text("An unexpected error occurred.") 
    finally:
        if os.path.exists(audio_path):
            os.remove(audio_path)



@app.on_message(filters.private & filters.command("start"))
async def start_command(client, message):
    reply = await message.reply_text(f"<b>💐Welcome to FileShare Bot")
    await auto_delete_message(message, reply)


# Get Log Command
@app.on_message(filters.command("log") & filters.user(OWNER_USERNAME))
async def log_command(client, message):
    user_id = message.from_user.id

    # Send the log file
    try:
        reply = await app.send_document(user_id, document=LOG_FILE_NAME, caption="Bot Log File")
        await auto_delete_message(message, reply)
    except Exception as e:
        await app.send_message(user_id, f"Failed to send log file. Error: {str(e)}")
                                    
@app.on_message(filters.command("copy") & filters.user(OWNER_USERNAME))
async def copy_msg(client, message):    
    try:
        await message.delete()
        async def get_user_input(prompt):
            rply = await message.reply_text(prompt)
            link_msg = await app.listen(message.chat.id)
            await rply.delete()
            return link_msg.text
        
        # Collect input from the user
        start_msg_id = int(await extract_tg_link(await get_user_input("Send first post link")))
        end_msg_id = int(await extract_tg_link(await get_user_input("Send end post link")))
        db_channel_id = int(await extract_channel_id(await get_user_input("Send db_channel link")))
        destination_id = int(await extract_channel_id(await get_user_input("Send destination channel link")))

        batch_size = 199
        for start in range(start_msg_id, end_msg_id + 1, batch_size):
            end = min(start + batch_size - 1, end_msg_id)  # Ensure we don't go beyond end_msg_id
            # Get and copy messages
            file_messages = await app.get_messages(db_channel_id, range(start, end + 1))

            for file_message in file_messages:
                if file_message and (file_message.document or file_message.video or file_message.audio or file_message.photo):
                    caption = file_message.caption
                    await file_message.copy(destination_id, caption=f"<b>{caption}</b>")
                    await asyncio.sleep(3)
                    
        await message.reply_text("Messages copied successfully!✅")
        
    except FloodWait as e:
        await asyncio.sleep(e.value)
    except Exception as e:
        logger.error(f'{e}')

# Delete Commmand
@app.on_message(filters.command("delete"))
async def delete_command(client, message):
    try:
        await message.reply_text("Enter channel_id")
        channel_id = int((await app.listen(message.chat.id)).text)

        await message.reply_text("Enter count")
        limit = int((await app.listen(message.chat.id)).text)

        await app.send_message(channel_id, "Hi")

        try:
            async for message in user.get_chat_history(channel_id, limit):
                await message.delete()
        except Exception as e:
            logger.error(f"Error deleting messages: {e}")
        await user.send_message(channel_id, "done")
    except Exception as e:
        logger.error(f"Error : {e}")

if __name__ == "__main__":
    logger.info("Bot is starting...")
    loop.run_until_complete(main())
    logger.info("Bot has stopped.")        
