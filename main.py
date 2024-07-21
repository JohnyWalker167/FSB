import uuid
from utils import *
from config import *
from time import time as tm
from pyrogram import idle
from pyromod import listen
from pyrogram.errors import FloodWait
from pyrogram import Client, filters, enums
from shorterner import *
from asyncio import get_event_loop
from pymongo import MongoClient
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton

DOWNLOAD_PATH = "downloads/"
CHUNK_SIZE = 1024 * 1024 * 200
loop = get_event_loop()
THUMBNAIL_INTERVALS = ['00:01:10', '00:2:00', '00:2:30', '00:03:00', '00:3:30', '00:4:30']  # Intervals to take screenshots
GRID_COLUMNS = 2  # Number of columns in the grid

os.makedirs(DOWNLOAD_PATH, exist_ok=True)

MONGO_COLLECTION = "users"
mongo_client = MongoClient(MONGO_URL)
mongo_db = mongo_client[MONGO_DB_NAME]
mongo_collection = mongo_db[MONGO_COLLECTION]

user_data = {}
TOKEN_TIMEOUT = 7200

app = Client(
    "my_bot",
      api_id=API_ID,
      api_hash=API_HASH, 
      bot_token=BOT_TOKEN, 
      workers=1000, 
      parse_mode=enums.ParseMode.HTML,
      in_memory=True)

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

@app.on_message(filters.chat(DB_CHANNEL_ID) & (filters.document | filters.video))
async def forward_message_to_new_channel(client, message):
    try:
        media = message.document or message.video or message.audio
        file_id = message.id

        if media:
            caption = message.caption if message.caption else None
            
            if caption:
                new_caption = await remove_unwanted(caption)
                movie_name, release_year = await extract_movie_info(new_caption)
                tmdb_poster_url = await get_movie_poster(movie_name, release_year)
                # Getting poster
                logger.info(f"Getting Poster for {movie_name}...")
                if movie_name and release_year and not tmdb_poster_url:
                    await app.send_message(LOG_CHANNEL_ID, text=f"no poster found for <code>{movie_name}</code> {release_year}")
                    await asyncio.sleep(3)

                if tmdb_poster_url:
                    file_info = f"🎥 <b>{new_caption}</b>\n\n🆔 <code>{file_id}</code>"
                    await app.send_photo(CAPTION_CHANNEL_ID, tmdb_poster_url, caption=file_info)
                    await asyncio.sleep(3)
            else:
                audio_path = await app.download_media(media.file_id)
                audio_thumb = await get_audio_thumbnail(audio_path)
                
                file_info = f"🎧 <b>{media.title}</b>\n🧑‍🎤 <b>{media.performer}</b>\n\n<code>🆔 {file_id}</code>"

                await app.send_photo(CAPTION_CHANNEL_ID, audio_thumb, caption=file_info)

                os.remove(audio_path)

                await asyncio.sleep(3)

    except Exception as e:
        logger.error(f'{e}')    


@app.on_message(filters.command("start"))
async def get_command(client, message): 
     input_token = message.command[1] if len(message.command) > 1 else None
     user_id = message.from_user.id
     user_link = await get_user_link(message.from_user)

     if input_token:
          token_msg = await verify_token(user_id, input_token)
          reply = await message.reply_text(token_msg)
          await app.send_message(LOG_CHANNEL_ID, f"User🕵️‍♂️{user_link} with 🆔 {user_id} {token_msg}", parse_mode=enums.ParseMode.HTML)
          await auto_delete_message(message, reply)
     else:
        mongo_collection.update_one(
                {'user_id': user_id},
                {'$set': {'user_id': user_id}}, 
                upsert=True
            )
        reply = await message.reply_text(f"<b>💐Welcome this is TG⚡️Flix Bot")
        await auto_delete_message(message, reply)


# Get Command      
@app.on_message(filters.command("get"))
async def handle_get_command(client, message):
    user_id = message.from_user.id

    if not await check_access(message, user_id):
         return    
    
    file_id = message.command[1] if len(message.command) > 1 else None

    if file_id:
        try:
            file_message = await app.get_messages(DB_CHANNEL_ID, int(file_id))
            media = file_message.video or file_message.audio or file_message.document
            if media:
                caption = file_message.caption if file_message.caption else None
                if caption:
                    new_caption = await remove_unwanted(caption.html)
                    copy_message = await file_message.copy(chat_id=message.chat.id, caption=f"<code>{new_caption}</code>", parse_mode=enums.ParseMode.HTML)
                else:
                    copy_message = await file_message.copy(chat_id=message.chat.id)

                await auto_delete_message(message, copy_message)
                await asyncio.sleep(3)

            else:
                 reply = await message.reply_text("File not found or inaccessible.")
                 await auto_delete_message(message, reply)

        except ValueError:
                reply = await message.reply_text("Invalid File ID") 
                await auto_delete_message(message, reply)  

        except FloodWait as f:
            await asyncio.sleep(f.value)
            if caption:
                copy_message = await file_message.copy(chat_id=message.chat.id, caption=f"<code>{new_caption}</code>", parse_mode=enums.ParseMode.HTML)
            else:
                copy_message = await file_message.copy(chat_id=message.chat.id)

            await auto_delete_message(message, copy_message)
            await asyncio.sleep(3)
    else:
        reply = await message.reply_text("Provide a File Id")
        await auto_delete_message(message, reply)  

# Send Single Command 
@app.on_message(filters.command("send") & filters.user(OWNER_USERNAME))
async def send_msg(client, message):
    custom_thumb = f"downloads/user_photo.jpg"
    try:
        await message.reply_text("send post link")
        tg_link = (await app.listen(message.chat.id)).text

        msg_id = await extract_tg_link(tg_link)

        file_message = await app.get_messages(DB_CHANNEL_ID, int(msg_id))

        media = file_message.document or file_message.video or file_message.audio
        file_id = file_message.id

        if media:
            caption = file_message.caption if file_message.caption else None

            if caption:
                new_caption = await remove_unwanted(caption)
                file_info = f"🎥 <b>{new_caption}</b>\n\n🆔 <code>{file_id}</code>"
                await app.send_photo(CAPTION_CHANNEL_ID, custom_thumb, caption=file_info)
                await asyncio.sleep(3)

            else:
                audio_path = await app.download_media(media.file_id)
                audio_thumb = await get_audio_thumbnail(audio_path)
                
                file_info = f"🎧 <b>{media.title}</b>\n🧑‍🎤 <b>{media.performer}</b>\n\n<code>🆔 {file_id}</code>"

                await app.send_photo(CAPTION_CHANNEL_ID, audio_thumb, caption=file_info)

                os.remove(audio_path)

                await asyncio.sleep(3)
            
        await message.reply_text("Messages send successfully!")
    except Exception as e:
        logger.error(f"{e}")

# Send Multiple Command
@app.on_message(filters.command("sendm") & filters.user(OWNER_USERNAME))
async def send_msg(client, message):
    try:
        await message.reply_text("send post start link")
        start_msg = (await app.listen(message.chat.id)).text

        await message.reply_text("send post end link")
        end_msg = (await app.listen(message.chat.id)).text

        start_msg_id = int(await extract_tg_link(start_msg))
        end_msg_id = int(await extract_tg_link(end_msg))

        batch_size = 199
        for start in range(start_msg_id, end_msg_id + 1, batch_size):
            end = min(start + batch_size - 1, end_msg_id)  # Ensure we don't go beyond end_msg_id
            file_messages = await app.get_messages(DB_CHANNEL_ID, range(start, end + 1))

            for file_message in file_messages:

                media = file_message.document or file_message.video or file_message.audio
                file_id = file_message.id

                if media:
                    caption = file_message.caption if file_message.caption else None

                    if caption:
                        new_caption = await remove_unwanted(caption)

                        movie_name, release_year = await extract_movie_info(new_caption)
                        tmdb_poster_url = await get_movie_poster(movie_name, release_year)
                        # Getting poster
                        logger.info(f"Getting Poster for {movie_name}...")
                        if movie_name and release_year and not tmdb_poster_url:
                            await app.send_message(LOG_CHANNEL_ID, text=f"no poster found for <code>{movie_name}</code> {release_year}")
                            await asyncio.sleep(3)

                        if tmdb_poster_url:
                            file_info = f"🎥 <b>{new_caption}</b>\n\n🆔 <code>{file_id}</code>"
                            await app.send_photo(CAPTION_CHANNEL_ID, tmdb_poster_url, caption=file_info)
                            await asyncio.sleep(3)

                    else:
                        audio_path = await app.download_media(media.file_id)
                        audio_thumb = await get_audio_thumbnail(audio_path)
                        
                        file_info = f"🎧 <b>{media.title}</b>\n🧑‍🎤 <b>{media.performer}</b>\n\n✅ <code>{file_id}</code>"

                        await app.send_photo(CAPTION_CHANNEL_ID, audio_thumb, caption=file_info)

                        os.remove(audio_path)

                        await asyncio.sleep(3)

        await message.reply_text("Messages send successfully!")

    except FloodWait as e:
        await asyncio.sleep(e.value)

    except Exception as e:
        logger.error(f'{e}')

# Delete Commmand
@app.on_message(filters.command("delete") & filters.user(OWNER_USERNAME))
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

# Get Total User Command
@app.on_message(filters.command("users") & filters.user(OWNER_USERNAME))
async def total_users_command(client, message):
    user_id = message.from_user.id

    total_users = await mongo_collection.count_documents({})
    response_text = f"Total number of users in the database: {total_users}"
    reply = await app.send_message(user_id, response_text)
    await auto_delete_message(message, reply)
    
# Help Command
@app.on_message(filters.command("help"))
async def handle_help_command(client, message):
    try:
        file_id = 932
        get_msg = await app.get_messages(DB_CHANNEL_ID, int(file_id))
        send_msg = await get_msg.copy(chat_id=message.chat.id)
        await message.delete()
        await asyncio.sleep(300)
        await send_msg.delete()
    except Exception as e:
        logger.error(f"{e}")
        
# Set Thumb command
@app.on_message(filters.photo & filters.user(OWNER_USERNAME))
async def setthumb_command(client, message):
    await app.download_media(message.photo.file_id, file_name='user_photo.jpg')
    await message.delete()

async def verify_token(user_id, input_token):
    current_time = tm()

    # Check if the user_id exists in user_data
    if user_id not in user_data:
        return 'Token Mismatched ❌' 
    
    stored_token = user_data[user_id]['token']
    if input_token == stored_token:
        token = str(uuid.uuid4())
        user_data[user_id] = {"token": token, "time": current_time, "status": "verified"}
        return f'Token Verified ✅'
    else:
        return f'Token Mismatched ❌'
    
async def check_access(message, user_id):

    if user_id in user_data:
        time = user_data[user_id]['time']
        status = user_data[user_id]['status']
        expiry = time + TOKEN_TIMEOUT
        current_time = tm()
        if current_time < expiry and status == "verified":
            return True
        else:
            button = await update_token(user_id)
            send_message = await app.send_message(user_id,f'<b>You need to collect your token first 🎟\n(Valid: {get_readable_time(TOKEN_TIMEOUT)})</b>', reply_markup=button)
            await auto_delete_message(message, send_message)
    else:
        button = await genrate_token(user_id)
        send_message = await app.send_message(user_id,f'<b>You need to collect your token first 🎟\n(Valid: {get_readable_time(TOKEN_TIMEOUT)})</b>', reply_markup=button)
        await auto_delete_message(message, send_message)

async def update_token(user_id):
    try:
        time = user_data[user_id]['time']
        expiry = time + TOKEN_TIMEOUT
        if time < expiry:
            token = user_data[user_id]['token']
        else:
            token = str(uuid.uuid4())
        current_time = tm()
        user_data[user_id] = {"token": token, "time": current_time, "status": "unverified"}
        urlshortx = await shorten_url(f'https://telegram.me/{bot_username}?start={token}')
        tinyurl = await tiny(urlshortx)
        button = InlineKeyboardMarkup([[InlineKeyboardButton("Collect Token", url=tinyurl)]])
        return button
    except Exception as e:
        logger.error(f"error in update_token: {e}")

async def genrate_token(user_id):
    try:
        token = str(uuid.uuid4())
        current_time = tm()
        user_data[user_id] = {"token": token, "time": current_time, "status": "unverified"}
        urlshortx = await shorten_url(f'https://telegram.me/{bot_username}?start={token}')
        tinyurl = await tiny(urlshortx)
        button = InlineKeyboardMarkup([[InlineKeyboardButton("Collect Token", url=tinyurl)]])
        return button
    except Exception as e:
        logger.error(f"error in genrate_token: {e}")

async def get_user_link(user: User) -> str:
    user_id = user.id
    first_name = user.first_name
    return f'<a href=tg://user?id={user_id}>{first_name}</a>'
      
if __name__ == "__main__":
    loop.run_until_complete(main())
