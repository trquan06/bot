import os
import time
import mimetypes
import uuid
import hashlib
import logging
import subprocess
import asyncio
import aiohttp
from asyncio import Lock, Semaphore
from pyrogram import Client, filters
from bs4 import BeautifulSoup
import psutil  # For system monitoring

# ---------------------------
# Configuration and Settings
# ---------------------------
API_ID = "21164074"
API_HASH = "9aebf8ac7742705ce930b06a706754fd"
BOT_TOKEN = "7878223314:AAGdrEWvu86sVWXCHIDFqqZw6m68mK6q5pY"

# Allowed user IDs (Fill this with actual user IDs allowed to use the bot)
ALLOWED_USERS = [123456789]  # Update with actual Telegram user IDs

# Base folder to store downloaded files
BASE_DOWNLOAD_FOLDER = os.path.join(os.path.expanduser("~"), "Downloads")
if not os.path.exists(BASE_DOWNLOAD_FOLDER):
    os.makedirs(BASE_DOWNLOAD_FOLDER)

# Global variables for management and statistics
download_lock = Lock()
# Increase concurrent download limit to 10 as per improvements
download_semaphore = Semaphore(10)
downloading = False
uploading = False
failed_files = []      # For failed downloads. Each entry: {"message": message, "media_type": type, "file_path": path}
failed_uploads = []    # For failed uploads (if any)
download_stats = {
    'files_downloaded': 0,
    'bytes_downloaded': 0
}
# To detect duplicate files using MD5 hashing
downloaded_files = {}  # key: md5 hash, value: file path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("bot.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

# Initialize Telegram Bot Client
app = Client("telegram_downloader", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)


# ---------------------------
# Utility Functions
# ---------------------------
def compute_md5(file_path, chunk_size=8192):
    """Compute MD5 hash for a given file."""
    hash_md5 = hashlib.md5()
    try:
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(chunk_size), b""):
                hash_md5.update(chunk)
    except Exception as e:
        logging.error(f"Error computing md5 for {file_path}: {e}")
        return None
    return hash_md5.hexdigest()


def is_authorized(user_id):
    """Check if the user is authorized."""
    return user_id in ALLOWED_USERS


async def delete_all_files():
    """Delete all files under BASE_DOWNLOAD_FOLDER."""
    count = 0
    for root, dirs, files in os.walk(BASE_DOWNLOAD_FOLDER):
        for file in files:
            try:
                file_path = os.path.join(root, file)
                os.remove(file_path)
                count += 1
            except Exception as e:
                logging.error(f"Failed to delete file {file_path}: {e}")
    return count


def get_system_status():
    """Return a dictionary with CPU, Memory, and Disk usage."""
    cpu_usage = psutil.cpu_percent(interval=1)
    mem = psutil.virtual_memory()
    disk = psutil.disk_usage(BASE_DOWNLOAD_FOLDER)
    return {
        "cpu": f"{cpu_usage}%",
        "memory": f"{mem.percent}%",
        "disk": f"{disk.percent}%"
    }


# ---------------------------
# Bot Command Handlers
# ---------------------------
@app.on_message(filters.command("start"))
async def start_command(client, message):
    if not is_authorized(message.from_user.id):
        return await message.reply("Unauthorized user.")
    welcome_text = (
        "Chào mừng!\n"
        "Các lệnh khả dụng:\n"
        "/download - Bắt đầu chế độ tải về hoặc tải file từ URL\n"
        "/stop - Ngừng chế độ tải về\n"
        "/upload - Đồng bộ hóa file lên Google Photos\n"
        "/retry_download - Tải lại file bị lỗi\n"
        "/retry_upload - Tải lại file upload bị lỗi\n"
        "/status - Hiển thị trạng thái hệ thống\n"
        "/delete - Xóa tất cả các file trong thư mục tải về\n"
        "/stats - Thống kê tải về\n"
        "/cleanup - Dọn dẹp file tạm thời"
    )
    await message.reply(welcome_text)


@app.on_message(filters.command("download"))
async def download_command(client, message):
    if not is_authorized(message.from_user.id):
        return await message.reply("Unauthorized user.")
    
    args = message.text.split(maxsplit=1)
    # URL-based download: if a URL is provided along with /download command.
    if len(args) > 1:
        url = args[1].strip()
        if url.startswith("http"):
            await download_from_url(message, url)
        else:
            await message.reply("URL không hợp lệ. Hãy đảm bảo bạn nhập đúng định dạng URL.")
        return

    # Activate download mode for forwarded media messages.
    global downloading
    async with download_lock:
        if downloading:
            await message.reply("Đã có tác vụ tải về đang chạy.")
            return
        downloading = True
    await message.reply("Chế độ tải về đã được kích hoạt. Forward các tin nhắn chứa ảnh, video, tài liệu để tải về.")


@app.on_message(filters.command("stop"))
async def stop_command(client, message):
    if not is_authorized(message.from_user.id):
        return await message.reply("Unauthorized user.")
    
    global downloading
    async with download_lock:
        if not downloading:
            await message.reply("Không có tác vụ tải về nào đang chạy.")
            return
        downloading = False
    await message.reply("Đã ngừng chế độ tải về.")


@app.on_message(filters.command("upload"))
async def upload_command(client, message):
    if not is_authorized(message.from_user.id):
        return await message.reply("Unauthorized user.")
    
    global uploading, failed_uploads
    if uploading:
        await message.reply("Đã có tác vụ đồng bộ hóa đang chạy.")
        return
    uploading = True
    await message.reply("Bắt đầu đồng bộ hóa file ảnh/video lên album ONLYFAN trên Google Photos...")
    album_name = "ONLYFAN"  # Album name for uploads

    try:
        with open("error_log.txt", "w", encoding="utf-8") as log_file:
            result = subprocess.run(
                [
                    "rclone", "copy", BASE_DOWNLOAD_FOLDER, f"GG PHOTO:album/{album_name}",
                    "--transfers=32", "--drive-chunk-size=128M", "--tpslimit=20", "-P"
                ],
                stdout=log_file, stderr=log_file, text=True, encoding="utf-8"
            )

        if result.returncode == 0:
            await message.reply("Đồng bộ hóa thành công tất cả các file lên album ONLYFAN trên Google Photos.")
        else:
            await message.reply("Có lỗi khi đồng bộ hóa, vui lòng kiểm tra error_log.txt để biết chi tiết.")
            # Record failed upload attempt for retry
            failed_uploads.extend(os.path.join(root, file) 
                                  for root, _, files in os.walk(BASE_DOWNLOAD_FOLDER) for file in files)

        # Delete all files after upload
        deleted = await delete_all_files()
        await message.reply(f"Tất cả ({deleted}) các file đã được xóa sau khi upload.")

    except Exception as e:
        logging.error(f"Upload error: {e}")
        await message.reply(f"Có lỗi xảy ra khi đồng bộ hóa: {e}")

    uploading = False


@app.on_message(filters.command("retry_download"))
async def retry_download_command(client, message):
    if not is_authorized(message.from_user.id):
        return await message.reply("Unauthorized user.")
    
    global failed_files
    if not failed_files:
        await message.reply("Không có file bị lỗi nào để tải lại.")
        return

    await message.reply("Bắt đầu tải lại các file bị lỗi...")
    for file_info in failed_files.copy():
        file_path = file_info["file_path"]
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
            await download_with_progress(file_info["message"], file_info["media_type"], retry=True)
            failed_files.remove(file_info)
        except Exception as e:
            await message.reply(f"Có lỗi khi tải lại file: {file_path}\nChi tiết: {e}")
    await message.reply("Hoàn thành tải lại các file bị lỗi.")


@app.on_message(filters.command("retry_upload"))
async def retry_upload_command(client, message):
    if not is_authorized(message.from_user.id):
        return await message.reply("Unauthorized user.")
    
    global failed_uploads, uploading
    if not failed_uploads:
        await message.reply("Không có file upload nào bị lỗi để tải lại.")
        return

    if uploading:
        await message.reply("Đã có tác vụ đồng bộ hóa đang chạy.")
        return

    uploading = True
    await message.reply("Bắt đầu tải lại quá trình upload cho các file bị lỗi...")
    album_name = "ONLYFAN"

    try:
        with open("error_log_retry.txt", "w", encoding="utf-8") as log_file:
            # Retry uploading only the failed files
            for file_path in failed_uploads.copy():
                # Using rclone copy for each file separately
                result = subprocess.run(
                    [
                        "rclone", "copy", file_path, f"GG PHOTO:album/{album_name}",
                        "--transfers=1", "--drive-chunk-size=128M", "--tpslimit=20", "-P"
                    ],
                    stdout=log_file, stderr=log_file, text=True, encoding="utf-8"
                )
                if result.returncode == 0:
                    failed_uploads.remove(file_path)
                else:
                    logging.error(f"Retry upload failed for {file_path}")
        if not failed_uploads:
            await message.reply("Tất cả các file upload bị lỗi đã được tải lại thành công.")
        else:
            await message.reply("Một số file vẫn chưa upload được. Vui lòng kiểm tra error_log_retry.txt để biết chi tiết.")
    except Exception as e:
        await message.reply(f"Có lỗi khi retry upload: {e}")
    uploading = False


@app.on_message(filters.command("status"))
async def status_command(client, message):
    if not is_authorized(message.from_user.id):
        return await message.reply("Unauthorized user.")
    
    status = get_system_status()
    status_message = f"CPU: {status['cpu']}\nMemory: {status['memory']}\nDisk: {status['disk']}\n"
    await message.reply(f"Trạng thái hệ thống:\n{status_message}")


@app.on_message(filters.command("delete"))
async def delete_command(client, message):
    if not is_authorized(message.from_user.id):
        return await message.reply("Unauthorized user.")
    
    deleted = await delete_all_files()
    await message.reply(f"Đã xóa {deleted} file trong thư mục tải về.")


@app.on_message(filters.command("stats"))
async def stats_command(client, message):
    if not is_authorized(message.from_user.id):
        return await message.reply("Unauthorized user.")
    
    stats_message = (
        f"Số file đã tải: {download_stats['files_downloaded']}\n"
        f"Tổng dung lượng đã tải: {download_stats['bytes_downloaded'] / (1024*1024):.2f} MB"
    )
    await message.reply(f"Thống kê tải về:\n{stats_message}")


@app.on_message(filters.command("cleanup"))
async def cleanup_command(client, message):
    if not is_authorized(message.from_user.id):
        return await message.reply("Unauthorized user.")
    
    # For cleanup, we can remove temporary files; in this example, we treat it same as delete.
    deleted = await delete_all_files()
    await message.reply(f"Đã dọn dẹp {deleted} file tạm thời trong thư mục tải về.")


@app.on_message()
async def handle_message(client, message):
    # Only process messages from authorized users
    if not is_authorized(message.from_user.id):
        return

    # Handle URL messages outside of download mode
    if message.text and message.text.startswith("http"):
        await download_from_url(message, message.text.strip())
        return

    # Only process if in download mode
    global downloading
    async with download_lock:
        if not downloading:
            return

    # Handle forwarded media messages (photo, video, document)
    if message.photo or message.video or message.document:
        try:
            tasks = []
            if message.photo:
                tasks.append(download_with_progress(message, "ảnh"))
            elif message.video:
                tasks.append(download_with_progress(message, "video"))
            elif message.document:
                tasks.append(download_with_progress(message, "tài liệu"))
            await asyncio.gather(*tasks)
        except Exception as e:
            logging.error(f"Error processing message: {e}")
            await message.reply(f"Có lỗi khi xử lý tin nhắn: {e}")
    else:
        await message.reply("Tin nhắn không chứa nội dung hợp lệ để tải.")

        
# ---------------------------
# Download Functions
# ---------------------------
async def download_with_progress(message, media_type, retry=False):
    global failed_files, download_stats, downloaded_files

    ext = "jpg" if media_type == "ảnh" else "mp4" if media_type == "video" else "dat"
    file_name = f"{uuid.uuid4().hex}.{ext}"
    file_path = os.path.join(BASE_DOWNLOAD_FOLDER, file_name)
    start_time = time.time()

    async def progress_callback(current, total):
        elapsed_time = time.time() - start_time
        speed = current / elapsed_time if elapsed_time > 0 else 0
        if current == total:
            await message.reply(
                f"Tải xong {media_type}: 100% ({total / (1024 * 1024):.2f} MB)\nTốc độ: {speed / 1024:.2f} KB/s",
                quote=True
            )

    try:
        async with download_semaphore:
            await app.download_media(
                message.photo or message.video or message.document,
                file_name=file_path,
                progress=progress_callback
            )
        # Check file size limit (10GB max)
        if os.path.getsize(file_path) > 10 * 1024**3:
            os.remove(file_path)
            await message.reply("File vượt quá giới hạn kích thước 10GB và đã bị xóa.")
            return

        # Compute MD5 to check for duplicates
        file_md5 = compute_md5(file_path)
        if file_md5 in downloaded_files:
            os.remove(file_path)
            await message.reply("File đã tồn tại (trùng lặp), không tải lại.")
            return
        else:
            downloaded_files[file_md5] = file_path

        # Update download statistics
        download_stats['files_downloaded'] += 1
        download_stats['bytes_downloaded'] += os.path.getsize(file_path)

    except Exception as e:
        logging.error(f"Download error: {e}")
        failed_files.append({"message": message, "media_type": media_type, "file_path": file_path})
        await message.reply(f"Tải file bị lỗi: {e}\nFile đã được thêm vào danh sách retry.", quote=True)


async def download_from_url(message, url):
    connector = aiohttp.TCPConnector(limit=50)
    async with aiohttp.ClientSession(connector=connector) as session:
        try:
            async with session.get(url) as response:
                content_type = response.headers.get("Content-Type", "")
                if response.status == 200 and "html" in content_type:
                    html_content = await response.text()
                    soup = BeautifulSoup(html_content, "html.parser")
                    
                    # Special handling for telegra.ph URLs
                    if "telegra.ph" in url:
                        media_links = [tag["src"] for tag in soup.find_all("img", src=True)]
                        if not media_links:
                            await message.reply("Không tìm thấy ảnh trong URL Telegra.ph.")
                            return
                        for media_url in media_links:
                            if media_url.startswith("/"):
                                media_url = f"https://telegra.ph{media_url}"
                            await download_from_url(message, media_url)
                        return
                    
                    # Find media tags in HTML
                    media_links = [tag["src"] for tag in soup.find_all(["img", "video"], src=True)]
                    if media_links:
                        for media_url in media_links:
                            if not media_url.startswith("http"):
                                media_url = os.path.join(os.path.dirname(url), media_url)
                            await download_from_url(message, media_url)
                        return
                    else:
                        await message.reply("Không tìm thấy ảnh hoặc video trong URL được cung cấp.")
                        return

                if response.status == 500:
                    await message.reply(f"Lỗi server (500) từ URL: {url}. Vui lòng thử lại sau.")
                    return

                if response.status != 200:
                    await message.reply(f"Không thể tải file từ URL: {url}\nMã lỗi: {response.status}")
                    return

                # Process non-HTML content (like media files)
                ext = mimetypes.guess_extension(content_type.split(";")[0]) or ""
                file_name = f"{uuid.uuid4().hex}{ext}"
                file_path = os.path.join(BASE_DOWNLOAD_FOLDER, file_name)
                with open(file_path, "wb") as f:
                    while chunk := await response.content.read(65536):
                        f.write(chunk)
                await message.reply(f"Tải thành công file từ URL: {url}\nĐã lưu tại: {file_path}")
                
                # Update download statistics and check duplicate
                file_md5 = compute_md5(file_path)
                if file_md5 in downloaded_files:
                    os.remove(file_path)
                    await message.reply("File đã tồn tại (trùng lặp), không tải lại.")
                    return
                else:
                    downloaded_files[file_md5] = file_path
                    download_stats['files_downloaded'] += 1
                    download_stats['bytes_downloaded'] += os.path.getsize(file_path)

        except Exception as e:
            logging.error(f"Error downloading from URL {url}: {e}")
            await message.reply(f"Có lỗi xảy ra khi tải file từ URL: {e}")


# ---------------------------
# Main Execution
# ---------------------------
if __name__ == "__main__":
    logging.info("Bot đang chạy...")
    print("Bot đang chạy...")
    app.run()