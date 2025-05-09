import base64
import hashlib
import hmac
import os
import time
import requests
import subprocess
import threading
import telebot
from telebot import types
from flask import Flask
access_key = "9302030bb186516563dbe39498e3588b"
access_secret = "1VvJZT0JIWngpvZjp0SIzMyeq9K3ul52wHXfDbyd"
requrl = "https://identify-ap-southeast-1.acrcloud.com/v1/identify"

BOT_TOKEN = "7619712697:AAH2sgrslxujkQc0XWRzm33CI5KP5R1Pdbo"

CHANNEL_ID = "@IG_ACC_CHATS"  # <<< Replace
GROUP_ID = "@IG_ACC_TBOT"      # <<< Replace

bot = telebot.TeleBot(BOT_TOKEN)

app = Flask(__name__)

@app.route('/')
def home():
    return "I am alive"

def run_flask():
    try:
        app.run(host='0.0.0.0', port=8085)
    except Exception as e:
        logging.error(f"Error in Flask server: {e}")

def create_signature():
    timestamp = str(time.time())
    string_to_sign = f"POST\n/v1/identify\n{access_key}\naudio\n1\n{timestamp}"
    sign = base64.b64encode(hmac.new(
        access_secret.encode('ascii'),
        string_to_sign.encode('ascii'),
        digestmod=hashlib.sha1
    ).digest()).decode('ascii')
    return sign, timestamp

def recognize_audio(file_path):
    sample_bytes = os.path.getsize(file_path)
    signature, timestamp = create_signature()

    files = [
        ('sample', ('audio.mp3', open(file_path, 'rb'), 'audio/mpeg'))
    ]
    data = {
        'access_key': access_key,
        'sample_bytes': sample_bytes,
        'timestamp': timestamp,
        'signature': signature,
        'data_type': "audio",
        "signature_version": "1"
    }

    response = requests.post(requrl, files=files, data=data)
    return response.json()

def extract_audio_from_video(video_path, output_audio_path):
    subprocess.run(['ffmpeg', '-i', video_path, '-q:a', '0', '-map', 'a', output_audio_path], check=True)

def is_user_joined(user_id):
    try:
        channel_status = bot.get_chat_member(CHANNEL_ID, user_id)
        group_status = bot.get_chat_member(GROUP_ID, user_id)
        return (channel_status.status in ['member', 'administrator', 'creator'] and
                group_status.status in ['member', 'administrator', 'creator'])
    except Exception as e:
        print(f"Error checking membership: {e}")
        return False

@bot.message_handler(commands=['start'])
def send_welcome(message):
    markup = types.InlineKeyboardMarkup()
    btn1 = types.InlineKeyboardButton("✨ Join Channel 1", url=f"https://t.me/{CHANNEL_ID.strip('@')}")
    btn2 = types.InlineKeyboardButton("✨ Join Group 2", url=f"https://t.me/{GROUP_ID.strip('@')}")
    markup.row(btn1)
    markup.row(btn2)
    bot.send_message(
        message.chat.id,
        "🎶 *Welcome to Music Recognizer Bot!* 🎶\n\n"
    "🔍 Find any song in seconds! Just send me an *audio*, *voice note*, or even a *video clip* — "
    "I'll quickly recognize the *song title* and *artist name* for you. 🎵\n\n"
    "✨ *Easy to Use:*\n"
    "➔ Join our *Channel* and *Group* first (buttons below).\n"
    "➔ Send your audio, voice, or video.\n"
    "➔ Get instant music results! 🚀\n\n"
    "⚡ *Fast, simple, and 100% free!* ⚡\n\n"
    "📢 *Note:* You must join both to use the bot.",
        parse_mode="Markdown",
        reply_markup=markup
    )
                
@bot.message_handler(content_types=['audio', 'voice', 'video'])
def handle_media(message):
    user_id = message.from_user.id
    if not is_user_joined(user_id):
        bot.reply_to(message, "⚠️ You have not joined both channels yet!\nPlease click /start and join both to use the bot.")
        return

    media = message.audio or message.voice or message.video
    if media.file_size > 20 * 1024 * 1024:
        bot.reply_to(message, "⚠️ Audio or video above 20 MB are not accepted.")
        return

    loading_msg = bot.reply_to(message, "⏳ Recognizing ▢▢▢▢", parse_mode="Markdown")

    try:
        loading_stages = ["⏳ Recognizing ▣▢▢▢", "⏳ Recognizing ▣▣▢▢", "⏳ Recognizing ▣▣▣▢", "⏳ Recognizing ▣▣▣▣"]
        for stage in loading_stages:
            time.sleep(0.5)
            bot.edit_message_text(stage, chat_id=loading_msg.chat.id, message_id=loading_msg.message_id, parse_mode="Markdown")

        file_id = media.file_id
        file_info = bot.get_file(file_id)
        downloaded_file = bot.download_file(file_info.file_path)

        file_path = f"temp_{message.message_id}"
        ext = ".mp3" if (message.audio or message.voice) else ".mp4"
        full_path = file_path + ext

        with open(full_path, 'wb') as new_file:
            new_file.write(downloaded_file)

        if ext == ".mp4":
            audio_path = file_path + ".mp3"
            extract_audio_from_video(full_path, audio_path)
            target_path = audio_path
        else:
            target_path = full_path

        result = recognize_audio(target_path)

        if "metadata" in result and "music" in result["metadata"]:
            music = result["metadata"]["music"][0]
            title = music.get("title", "Unknown")
            artists = ", ".join(artist['name'] for artist in music.get("artists", []))
            bot.edit_message_text(f"🎵 *Title:* {title}\n👤 *Artists:* {artists}", chat_id=loading_msg.chat.id, message_id=loading_msg.message_id, parse_mode="Markdown")
        else:
            bot.edit_message_text("😔 Sorry, I couldn't recognize the music.", chat_id=loading_msg.chat.id, message_id=loading_msg.message_id, parse_mode="Markdown")

    except Exception as e:
        bot.edit_message_text("❌ An error occurred while processing.", chat_id=loading_msg.chat.id, message_id=loading_msg.message_id, parse_mode="Markdown")
        print(e)

    finally:
        try:
            os.remove(full_path)
            if ext == ".mp4":
                os.remove(audio_path)
        except Exception as e:
            print("Cleanup error:", e)

def keep_alive():
    t = threading.Thread(target=run_flask, daemon=True)
    t.start()

def main():
    try:
        keep_alive()
        bot.polling(none_stop=True, timeout=60)
    except Exception as e:
        logging.error(f"Error in main bot polling loop: {e}")
        time.sleep(5)
        main()

if __name__ == "__main__":
    main()
