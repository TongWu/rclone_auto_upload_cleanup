import os
import subprocess
import json
import sys
from datetime import datetime, timedelta
from collections import OrderedDict
from filelock import FileLock, Timeout

# Global constants for the wait_cleanup file and its lock file
WAIT_CLEANUP_LOG = "/home/tedwu/wait_cleanup.json"
WAIT_CLEANUP_LOCK = WAIT_CLEANUP_LOG + ".lock"

CONFIG_FILE = "config.json"  # 配置文件路径

def load_config(config_file=CONFIG_FILE):
    """Load configuration from a JSON config file."""
    with open(config_file, "r", encoding="utf-8") as f:
        config = json.load(f)
    # 如果 upload_size_gb 未设置或为 0，则默认按照 rclone_drives 数量来确定
    if not config.get("upload_size_gb", 0):
        config["upload_size_gb"] = len(config.get("rclone_drives", [])) * 1024
    return config

def get_folder_size(folder):
    """Get folder size in GB
    # Convert folder size to GB
    """
    result = subprocess.run(["du", "-sb", folder], capture_output=True, text=True)
    size_in_bytes = int(result.stdout.split()[0])
    return size_in_bytes / (1024 ** 3)

def load_mapping_rules(config):
    """Load mapping rules from config dictionary."""
    return config.get("mapping_rules", {})

def load_wait_cleanup():
    """Load wait_cleanup.json using file lock and return its content as a dictionary."""
    lock = FileLock(WAIT_CLEANUP_LOCK)
    with lock:
        if os.path.exists(WAIT_CLEANUP_LOG):
            with open(WAIT_CLEANUP_LOG, "r", encoding="utf-8") as f:
                data = json.load(f)
            return data
        else:
            return {}

def write_wait_cleanup(data):
    """Write the given data to wait_cleanup.json with file lock protection."""
    lock = FileLock(WAIT_CLEANUP_LOCK)
    with lock:
        with open(WAIT_CLEANUP_LOG, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)

def scan_folders_by_mapping(config):
    """Scan folders based on mapping rules, ignoring folders already recorded in wait_cleanup.json."""
    print("Scanning folders by mapping...")
    mapping_rules = load_mapping_rules(config)
    # Load previously uploaded folders using file lock
    ignored_folders = set(load_wait_cleanup().keys())
    folders = OrderedDict()
    skipped_folder_num = 0
    for base_path in mapping_rules.keys():
        if os.path.exists(base_path):
            folder_list = [
                os.path.join(base_path, folder)
                for folder in os.listdir(base_path)
                if os.path.isdir(os.path.join(base_path, folder))
            ]
            folder_list.sort(key=lambda x: os.path.getctime(x))
            for folder_path in folder_list:
                abs_path = os.path.abspath(folder_path)
                if abs_path in ignored_folders:
                    # print(f"Skipping {abs_path} (already in wait_cleanup.json)")
                    skipped_folder_num += 1
                    continue
                creation_time = os.path.getctime(abs_path)
                readable_time = datetime.fromtimestamp(creation_time).strftime('%y-%m-%d %H:%M:%S')
                # print(f"{abs_path} -> Created at {readable_time}")
                folders[abs_path] = get_folder_size(folder_path)
    print("Folder scanning completed.")
    print(f"Skipped {skipped_folder_num} folders.")
    return folders

def select_folders_for_upload(folders, max_size):
    """Select folders to ensure total daily upload size does not exceed max_size (in GB)."""
    print(f"Selecting folders with total {max_size:.2f}G limit for upload from {len(folders)} folders...")
    selected_folders = []
    total_size = 0
    selected_folders_num = 0
    for folder, size in folders.items():
        if total_size + size <= max_size:
            selected_folders.append(folder)
            total_size += size
            selected_folders_num += 1
        else:
            break
    # print(f"Selected folders: {selected_folders}")
    print(f"Selected {selected_folders_num} folders, total size {total_size:.2f} GB")
    return selected_folders

def upload_folders(selected_folders, config):
    """
    Execute rclone upload and record log.
    If an upload error occurs, the current and subsequent folders will be uploaded using the next drive.
    After each folder is successfully uploaded, write its log immediately into wait_cleanup.json.
    All operations on wait_cleanup.json are protected by file lock.
    """
    print("Starting upload process...")
    mapping_rules = load_mapping_rules(config)
    drives = config.get("rclone_drives", [])
    current_drive_index = 0
    # Load current wait_cleanup data
    wait_cleanup_folders = load_wait_cleanup()
    stop_all = False
    for folder in selected_folders:
        for base_path, remote_suffix in mapping_rules.items():
            if folder.startswith(base_path):
                # Construct destination using the current drive name from config and remote_suffix from mapping_rules
                current_drive = drives[current_drive_index]
                destination = folder.replace(base_path, f"{current_drive}:{remote_suffix}")
                while True:
                    print(f"Uploading {folder} to {destination} using drive {current_drive}")
                    command = [
                        "rclone", "copy", "--progress", "--drive-upload-cutoff", "1000T",
                        "--drive-stop-on-upload-limit", folder, destination
                    ]
                    result = subprocess.run(command)
                    if result.returncode == 0:
                        # Upon success, update wait_cleanup.json immediately using file lock
                        wait_cleanup_folders[os.path.abspath(folder)] = str(datetime.now())
                        write_wait_cleanup(wait_cleanup_folders)
                        break
                    else:
                        print(f"Upload error for {folder} using drive {current_drive}, switching to next drive...")
                        current_drive_index += 1
                        if current_drive_index >= len(drives):
                            print("No more drives available, stopping upload.")
                            stop_all = True
                            break
                        current_drive = drives[current_drive_index]
                        destination = folder.replace(base_path, f"{current_drive}:{remote_suffix}")
                if stop_all:
                    break
                break
        if stop_all:
            break
    print("Upload process completed.")

def cleanup_old_uploads(config):
    """
    Cleanup uploaded folders that are older than the offset days specified in config.
    If wait_cleanup.json is locked, the function will wait until the lock is released.
    After each folder deletion, immediately remove the corresponding record from WAIT_CLEANUP_LOG.
    Always use the first drive from rclone_drives to construct the remote path.
    """
    print("Starting cleanup process...")
    cleanup_offset = config.get("cleanup_offset_days", 7)
    # Acquire lock (waiting if necessary)
    lock = FileLock(WAIT_CLEANUP_LOCK)
    while True:
        try:
            lock.acquire(timeout=1)
            print("Lock acquired for cleanup.")
            break
        except Timeout:
            print("wait_cleanup.json is locked, waiting...")
            continue

    try:
        if os.path.exists(WAIT_CLEANUP_LOG):
            with open(WAIT_CLEANUP_LOG, "r", encoding="utf-8") as f:
                uploaded_folders = json.load(f)
        else:
            uploaded_folders = {}

        threshold_time = datetime.now() - timedelta(days=cleanup_offset)
        # Always use the first drive (drives[0]) from config
        drive = config.get("rclone_drives", [])[0]

        # Iterate over a copy of the dictionary items
        for folder, timestamp in list(uploaded_folders.items()):
            upload_time = datetime.fromisoformat(timestamp)
            if upload_time < threshold_time:
                for base_path, remote_suffix in load_mapping_rules(config).items():
                    if folder.startswith(base_path):
                        # Construct remote folder using the default drive (drive) and remote_suffix
                        remote_folder = folder.replace(base_path, f"{drive}:{remote_suffix}")
                        if is_folder_uploaded(folder, remote_folder):
                            print(f"Deleting {folder}...")
                            subprocess.run(["rm", "-rf", folder])
                        # After deletion (or even if no deletion needed), remove record immediately
                        del uploaded_folders[folder]
                        with open(WAIT_CLEANUP_LOG, "w", encoding="utf-8") as f:
                            json.dump(uploaded_folders, f, indent=4, ensure_ascii=False)
                        # Exit inner loop after processing current folder
                        break
    finally:
        lock.release()
        print("Lock released for cleanup.")
    print("Cleanup process completed.")


def is_folder_uploaded(local_folder, remote_folder):
    """Check using rclone if the local folder is fully uploaded to the cloud."""
    print(f"Checking if {local_folder} is fully uploaded to {remote_folder}")
    command = ["rclone", "check", "--one-way" , local_folder, remote_folder]
    result = subprocess.run(command, capture_output=True, text=True)
    return "ERROR" not in result.stdout

def main():
    config = load_config()
    mapping_rules = load_mapping_rules(config)
    if len(sys.argv) > 1 and sys.argv[1] == "--cleanup":
        cleanup_old_uploads(config)
    else:
        folders = scan_folders_by_mapping(config)
        max_upload_size = config["upload_size_gb"]
        selected_folders = select_folders_for_upload(folders, max_upload_size)
        upload_folders(selected_folders, config)
    print("Script execution completed.")

if __name__ == "__main__":
    main()

