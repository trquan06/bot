import os
import time
import mimetypes
import uuid
import hashlib
import logging
import subprocess
import asyncio
import aiohttp
import zipfile
import tarfile
import shutil
import tempfile
import psutil  # For system monitoring
from asyncio import Lock, Semaphore
from pyrogram import Client, filters
from bs4 import BeautifulSoup

# ---------------------------
# Configuration and Settings
# ---------------------------
API_ID = "21164074"
API_HASH = "9aebf8ac7742705ce930b06a706754fd"
BOT_TOKEN = "7878223314:AAGdrEWvu86sVWXCHIDFqqZw6m68mK6q5pY"

# Base folder to store downloaded files (Windows Downloads folder)
BASE_DOWNLOAD_FOLDER = os.path.join(os.path.expanduser("~"), "Downloads")
if not os.path.exists(BASE_DOWNLOAD_FOLDER):
    os.makedirs(BASE_DOWNLOAD_FOLDER)

# Global variables for management and statistics
download_lock = Lock()
download_semaphore = Semaphore(10)  # Increase concurrent download limit to 10
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

# Supported archive and media extensions
ARCHIVE_EXTENSIONS = [".zip", ".tar", ".gz", ".tgz", ".rar", ".7z", ".bz2"]
IMAGE_EXTENSIONS = [".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp", ".tiff"]
VIDEO_EXTENSIONS = [".mp4", ".avi", ".mkv", ".mov", ".wmv", ".flv", ".webm", ".m4v"]

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
# Error Handling Decorator
# ---------------------------
def error_handler(func):
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            logging.exception(f"Unhandled exception in {func.__name__}: {e}")
            # Try to reply back using the message object (assumed to be the second argument)
            if len(args) >= 2 and hasattr(args[1], "reply"):
                await args[1].reply(f"Có lỗi xảy ra trong {func.__name__}: {e}")
            else:
                print(f"Error in {func.__name__}: {e}")
    return wrapper


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
        logging.error(f"Error computing MD5 for {file_path}: {e}")
        return None
    return hash_md5.hexdigest()


async def delete_all_files():
    """Delete all files under BASE_DOWNLOAD_FOLDER."""
    count = 0
    for root, dirs, files in os.walk(BASE_DOWNLOAD_FOLDER):
        for file in files:
            file_path = os.path.join(root, file)
            try:
                os.remove(file_path)
                count += 1
            except Exception as e:
                logging.error(f"Failed to delete {file_path}: {e}")
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


def extract_archive(file_path):
    """
    Extract archive file and return the extraction folder path.
    Supports common archive types.
    """
    ext = os.path.splitext(file_path)[1].lower()
    temp_extract_dir = tempfile.mkdtemp(dir=BASE_DOWNLOAD_FOLDER)
    try:
        if ext == ".zip":
            with zipfile.ZipFile(file_path, 'r') as zip_ref:
                zip_ref.extractall(temp_extract_dir)
        elif ext in [".tar", ".gz", ".tgz", ".bz2"]:
            with tarfile.open(file_path, 'r:*') as tar_ref:
                tar_ref.extractall(temp_extract_dir)
        elif ext in [".rar", ".7z"]:
            result = subprocess.run(["7z", "x", file_path, f"-o{temp_extract_dir}", "-y"],
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            if result.returncode != 0:
                logging.error(f"7z extraction error for {file_path}: {result.stderr}")
                shutil.rmtree(temp_extract_dir)
                return None
        else:
            logging.error(f"Unsupported archive type: {file_path}")
            shutil.rmtree(temp_extract_dir)
            return None
        logging.info(f"Extracted {file_path} to {temp_extract_dir}")
        return temp_extract_dir
    except Exception as e:
        logging.error(f"Error extracting {file_path}: {e}")
        shutil.rmtree(temp_extract_dir)
        return None


def move_media_files(source_dir, destination):
    """
    Recursively move image and video files from source_dir to destination.
    Returns a list of moved file paths.
    """
    moved_files = []
    for root, dirs, files in os.walk(source_dir):
        for file in files:
            if any(file.lower().endswith(ext) for ext in IMAGE_EXTENSIONS + VIDEO_EXTENSIONS):
                src_path = os.path.join(root, file)
                dest_path = os.path.join(destination, file)
                if os.path.exists(dest_path):
                    base, ext = os.path.splitext(file)
                    dest_path = os.path.join(destination, f"{base}_{uuid.uuid4().hex}{ext}")
                try:
                    shutil.move(src_path, dest_path)
                    moved_files.append(dest_path)
                    logging.info(f"Moved {src_path} to {dest_path}")
                except Exception as e:
                    logging.error(f"Failed to move {src_path}: {e}")
    return moved_files


# ---------------------------
# Bot Command Handlers
# ---------------------------
@app.on_message(filters.command("start"))
@error_handler
async def start_command(client, message):
    welcome_text = (
        "Chào mừng!\n"
        "Các lệnh khả dụng:\n"
        "/download - Bắt đầu chế độ tải về hoặc tải file từ URL\n"
        "/stop - Ngừng chế độ tải về\n"
        "/upload - Đồng bộ hóa file lên Google Photos\n"
        "/retry_download - Tải lại file bị lỗi\n"
        "/retry_upload - Tải lại file upload bị lỗi\n"
        "/status - Hiển thị trạng thái hệ thống\n"
        "/delete - Xóa tất cả các file trong thư mục tải về (xác nhận bằng cách trả lời 'yes')\n"
        "/stats - Thống kê tải về\n"
        "/cleanup - Dọn dẹp file tạm thời (xác nhận bằng cách trả lời 'yes')"
    )
    await message.reply(welcome_text)


@app.on_message(filters.command("download"))
@error_handler
async def download_command(client, message):
    args = message.text.split(maxsplit=1)
    if len(args) > 1:
        url = args[1].strip()
        if url.startswith("http"):
            await download_from_url(message, url)
        else:
            await message.reply("URL không hợp lệ. Hãy đảm bảo bạn nhập đúng định dạng URL.")
        return

    global downloading
    async with download_lock:
        if downloading:
            await message.reply("Đã có tác vụ tải về đang chạy.")
            return
        downloading = True
    await message.reply("Chế độ tải về đã được kích hoạt. Forward các tin nhắn chứa ảnh, video hay tài liệu để tải về.")


@app.on_message(filters.command("stop"))
@error_handler
async def stop_command(client, message):
    global downloading
    async with download_lock:
        if not downloading:
            await message.reply("Không có tác vụ tải về nào đang chạy.")
            return
        downloading = False
    await message.reply("Đã ngừng chế độ tải về.")


@app.on_message(filters.command("upload"))
@error_handler
async def upload_command(client, message):
    global uploading, failed_uploads
    if uploading:
        await message.reply("Đã có tác vụ đồng bộ hóa đang chạy.")
        return

    rclone_path = r"C:\rclone\rclone.exe"
    if not os.path.exists(rclone_path):
        await message.reply("Rclone không được tìm thấy tại C:\\rclone\\rclone.exe. Vui lòng kiểm tra lại đường dẫn.")
        return

    uploading = True
    await message.reply("Bắt đầu đồng bộ hóa file lên album ONLYFAN trên Google Photos...")
    album_name = "ONLYFAN"

    try:
        with open("error_log.txt", "w", encoding="utf-8") as log_file:
            result = subprocess.run(
                [
                    rclone_path, "copy", BASE_DOWNLOAD_FOLDER, f"GG PHOTO:album/{album_name}",
                    "--transfers=32", "--drive-chunk-size=128M", "--tpslimit=20", "-P"
                ],
                stdout=log_file, stderr=log_file, text=True, encoding="utf-8"
            )
        if result.returncode == 0:
            await message.reply("Đồng bộ hóa thành công!")
        else:
            await message.reply("Có lỗi khi đồng bộ hóa, vui lòng kiểm tra error_log.txt.")
            failed_uploads.extend(
                os.path.join(root, file)
                for root, _, files in os.walk(BASE_DOWNLOAD_FOLDER)
                for file in files
            )
        deleted = await delete_all_files()
        await message.reply(f"Đã xóa {deleted} file sau upload.")
    except Exception as e:
        logging.error(f"Upload error: {e}")
        await message.reply(f"Lỗi khi upload: {e}")
    uploading = False


@app.on_message(filters.command("retry_download"))
@error_handler
async def retry_download_command(client, message):
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
            await message.reply(f"Lỗi khi tải lại file {file_path}: {e}")
    await message.reply("Đã hoàn thành tải lại các file bị lỗi.")


@app.on_message(filters.command("retry_upload"))
@error_handler
async def retry_upload_command(client, message):
    global failed_uploads, uploading
    if not failed_uploads:
        await message.reply("Không có file upload bị lỗi để tải lại.")
        return

    if uploading:
        await message.reply("Đã có tác vụ đồng bộ hóa đang chạy.")
        return

    rclone_path = r"C:\rclone\rclone.exe"
    if not os.path.exists(rclone_path):
        await message.reply("Rclone không được tìm thấy tại C:\\rclone\\rclone.exe. Vui lòng kiểm tra lại đường dẫn.")
        return

    uploading = True
    await message.reply("Bắt đầu tải lại file upload bị lỗi...")
    album_name = "ONLYFAN"

    try:
        with open("error_log_retry.txt", "w", encoding="utf-8") as log_file:
            for file_path in failed_uploads.copy():
                result = subprocess.run(
                    [
                        rclone_path, "copy", file_path, f"GG PHOTO:album/{album_name}",
                        "--transfers=1", "--drive-chunk-size=128M", "--tpslimit=20", "-P"
                    ],
                    stdout=log_file, stderr=log_file, text=True, encoding="utf-8"
                )
                if result.returncode == 0:
                    failed_uploads.remove(file_path)
                else:
                    logging.error(f"Retry upload thất bại cho {file_path}")
        if not failed_uploads:
            await message.reply("Tất cả file upload lỗi đã được tải lại thành công.")
        else:
            await message.reply("Một số file vẫn chưa upload được. Kiểm tra error_log_retry.txt để biết chi tiết.")
    except Exception as e:
        await message.reply(f"Lỗi khi retry upload: {e}")
    uploading = False


@app.on_message(filters.command("status"))
@error_handler
async def status_command(client, message):
    status = get_system_status()
    status_message = f"CPU: {status['cpu']}\nMemory: {status['memory']}\nDisk: {status['disk']}\n"
    await message.reply(f"Trạng thái hệ thống:\n{status_message}")


@app.on_message(filters.command("delete"))
@error_handler
async def delete_command(client, message):
    confirm_msg = await message.reply("Bạn có chắc chắn muốn xóa tất cả các file trong thư mục tải về? Hãy trả lời 'yes' (trong 30 giây) để xác nhận.")
    try:
        confirmation = await app.listen(message.chat.id, timeout=30)
        if confirmation.text.lower() == "yes":
            deleted = await delete_all_files()
            await message.reply(f"Đã xóa {deleted} file trong thư mục tải về.")
        else:
            await message.reply("Hủy xóa file vì không nhận được 'yes'.")
    except asyncio.TimeoutError:
        await message.reply("Xác nhận hết thời gian. Hủy xóa file.")


@app.on_message(filters.command("cleanup"))
@error_handler
async def cleanup_command(client, message):
    confirm_msg = await message.reply("Bạn có chắc chắn muốn dọn dẹp các file tạm thời trong thư mục tải về? Hãy trả lời 'yes' (trong 30 giây) để xác nhận.")
    try:
        confirmation = await app.listen(message.chat.id, timeout=30)
        if confirmation.text.lower() == "yes":
            deleted = await delete_all_files()
            await message.reply(f"Đã dọn dẹp {deleted} file tạm thời trong thư mục tải về.")
        else:
            await message.reply("Hủy dọn dẹp file vì không nhận được 'yes'.")
    except asyncio.TimeoutError:
        await message.reply("Xác nhận hết thời gian. Hủy dọn dẹp file.")


@app.on_message(filters.command("stats"))
@error_handler
async def stats_command(client, message):
    stats_message = (
        f"Số file tải: {download_stats['files_downloaded']}\n"
        f"Tổng dung lượng: {download_stats['bytes_downloaded'] / (1024*1024):.2f} MB"
    )
    await message.reply(f"Thống kê tải về:\n{stats_message}")


@app.on_message()
@error_handler
async def handle_message(client, message):
    # Xử lý tin nhắn chứa URL ngoài chế độ tải về
    if message.text and message.text.startswith("http"):
        await download_from_url(message, message.text.strip())
        return

    global downloading
    async with download_lock:
        if not downloading:
            return

    # Xử lý tin nhắn forwarded chứa media
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
@error_handler
async def download_with_progress(message, media_type, retry=False):
    global failed_files, download_stats, downloaded_files

    if media_type == "ảnh":
        ext = "jpg"
    elif media_type == "video":
        ext = "mp4"
    elif media_type == "tài liệu":
        ext = "dat"
    else:
        ext = "dat"

    file_name = f"{uuid.uuid4().hex}.{ext}"
    file_path = os.path.join(BASE_DOWNLOAD_FOLDER, file_name)
    start_time = time.time()

    async def progress_callback(current, total):
        elapsed_time = time.time() - start_time
        speed = current / elapsed_time if elapsed_time > 0 else 0
        # Send periodic update if the download is long-running
        if total > 1e6 and current % (1024*1024) < 65536:
            await message.reply(
                f"Đang tải {media_type}: {current*100/total:.2f}% - {current / (1024*1024):.2f} MB đã tải, tốc độ: {speed/1024:.2f} KB/s",
                quote=True
            )
        if current == total:
            await message.reply(
                f"Tải xong {media_type}: 100% ({total / (1024*1024):.2f} MB)\nTốc độ: {speed/1024:.2f} KB/s",
                quote=True
            )

    try:
        async with download_semaphore:
            await app.download_media(
                message.photo or message.video or message.document,
                file_name=file_path,
                progress=progress_callback
            )

        # Check file size limit (10GB)
        if os.path.getsize(file_path) > 10 * 1024**3:
            os.remove(file_path)
            await message.reply("File vượt quá giới hạn 10GB và đã bị xóa.")
            return

        file_md5 = compute_md5(file_path)
        if file_md5 in downloaded_files:
            os.remove(file_path)
            await message.reply("File đã tồn tại (trùng lặp), không tải lại.")
            return
        else:
            downloaded_files[file_md5] = file_path

        download_stats['files_downloaded'] += 1
        download_stats['bytes_downloaded'] += os.path.getsize(file_path)

        lower_file = file_path.lower()
        if any(lower_file.endswith(ext) for ext in ARCHIVE_EXTENSIONS):
            extract_dir = extract_archive(file_path)
            if extract_dir:
                moved = move_media_files(extract_dir, BASE_DOWNLOAD_FOLDER)
                if moved:
                    await message.reply(f"Đã tự động giải nén và di chuyển {len(moved)} file media từ archive.")
                else:
                    await message.reply("Archive giải nén nhưng không tìm thấy file media hợp lệ.")
                shutil.rmtree(extract_dir, ignore_errors=True)
                os.remove(file_path)
    except Exception as e:
        logging.error(f"Download error: {e}")
        failed_files.append({"message": message, "media_type": media_type, "file_path": file_path})
        await message.reply(f"Tải file bị lỗi: {e}\nFile đã được thêm vào danh sách retry.", quote=True)


@error_handler
async def download_from_url(message, url):
    connector = aiohttp.TCPConnector(limit=50)
    async with aiohttp.ClientSession(connector=connector) as session:
        try:
            async with session.get(url) as response:
                content_type = response.headers.get("Content-Type", "")
                if response.status == 200 and "html" in content_type:
                    html_content = await response.text()
                    soup = BeautifulSoup(html_content, "html.parser")
                    if "telegra.ph" in url:
                        media_links = [tag["src"] for tag in soup.find_all("img", src=True)]
                        if not media_links:
                            await message.reply("Không tìm thấy ảnh trong URL telegra.ph.")
                            return
                        for media_url in media_links:
                            if media_url.startswith("/"):
                                media_url = f"https://telegra.ph{media_url}"
                            await download_from_url(message, media_url)
                        return

                    media_links = [tag["src"] for tag in soup.find_all(["img", "video"], src=True)]
                    if media_links:
                        for media_url in media_links:
                            if not media_url.startswith("http"):
                                media_url = os.path.join(os.path.dirname(url), media_url)
                            await download_from_url(message, media_url)
                        return
                    else:
                        await message.reply("Không tìm thấy media trong URL.")
                        return

                if response.status == 500:
                    await message.reply(f"Lỗi server (500) từ URL: {url}")
                    return

                if response.status != 200:
                    await message.reply(f"Không thể tải file từ URL: {url} (Mã lỗi: {response.status})")
                    return

                ext = mimetypes.guess_extension(content_type.split(";")[0]) or ""
                file_name = f"{uuid.uuid4().hex}{ext}"
                file_path = os.path.join(BASE_DOWNLOAD_FOLDER, file_name)
                with open(file_path, "wb") as f:
                    while chunk := await response.content.read(65536):
                        f.write(chunk)
                await message.reply(f"Tải thành công file từ URL: {url}\nĐã lưu tại: {file_path}")

                file_md5 = compute_md5(file_path)
                if file_md5 in downloaded_files:
                    os.remove(file_path)
                    await message.reply("File đã tồn tại (trùng lặp), không tải lại.")
                    return
                else:
                    downloaded_files[file_md5] = file_path
                    download_stats['files_downloaded'] += 1
                    download_stats['bytes_downloaded'] += os.path.getsize(file_path)

                lower_file = file_path.lower()
                if any(lower_file.endswith(ext) for ext in ARCHIVE_EXTENSIONS):
                    extract_dir = extract_archive(file_path)
                    if extract_dir:
                        moved = move_media_files(extract_dir, BASE_DOWNLOAD_FOLDER)
                        if moved:
                            await message.reply(f"Đã tự động giải nén và di chuyển {len(moved)} file media từ archive.")
                        else:
                            await message.reply("Archive giải nén nhưng không tìm thấy file media hợp lệ.")
                        shutil.rmtree(extract_dir, ignore_errors=True)
                        os.remove(file_path)
        except Exception as e:
            logging.error(f"Error downloading from URL {url}: {e}")
            await message.reply(f"Có lỗi khi tải file từ URL: {e}")
            

# ---------------------------
# Main Execution
# ---------------------------
if __name__ == "__main__":
    logging.info("Bot đang chạy...")
    print("Bot đang chạy...")
    app.run()
