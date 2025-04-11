import os
import subprocess
import json
import sys
from datetime import datetime, timedelta
from collections import OrderedDict

def get_folder_size(folder):
    """获取文件夹的大小（以GB为单位）"""
    result = subprocess.run(["du", "-sb", folder], capture_output=True, text=True)
    size_in_bytes = int(result.stdout.split()[0])
    return size_in_bytes / (1024 ** 3)  # Convert size to GB

def load_mapping_rules():
    """加载目录映射规则"""
    # Mapping rules: the remote part starts with drive name, e.g. "paula:"
    return {
        "/home/tedwu/media/completed/个人收集/电影/动画电影": "paula:/剧集/个人收集/电影/动画电影",
        "/home/tedwu/media/completed/个人收集/电影/华语电影": "paula:/剧集/个人收集/电影/华语电影",
        "/home/tedwu/media/completed/个人收集/电影/外语电影": "paula:/剧集/个人收集/电影/外语电影",
        "/home/tedwu/media/completed/个人收集/国漫": "paula:/剧集/个人收集/动漫剧",
        "/home/tedwu/media/completed/个人收集/日番": "paula:/剧集/个人收集/动漫剧",
        "/home/tedwu/media/completed/个人收集/纪录片": "paula:/剧集/个人收集/纪录片",
        "/home/tedwu/media/completed/个人收集/儿童": "paula:/剧集/个人收集/动漫剧",
        "/home/tedwu/media/completed/个人收集/综艺": "paula:/剧集/个人收集/综艺",
        "/home/tedwu/media/completed/个人收集/国产剧": "paula:/剧集/个人收集/国产剧",
        "/home/tedwu/media/completed/个人收集/日韩剧": "paula:/剧集/个人收集/日韩剧",
        "/home/tedwu/media/completed/个人收集/未分类": "paula:/剧集/个人收集/未分类",
        "/home/tedwu/media/completed/个人收集/欧美剧": "paula:/剧集/个人收集/欧美剧"
    }

def load_wait_cleanup():
    """加载 wait_cleanup.json 并返回已上传但尚未清理的文件夹列表"""
    wait_cleanup_log = "/home/tedwu/wait_cleanup.json"
    if os.path.exists(wait_cleanup_log):
        with open(wait_cleanup_log, "r") as f:
            return set(json.load(f).keys())  # Use set for fast lookup
    return set()

def scan_folders_by_mapping(mapping_rules):
    """扫描文件夹，忽略已经记录在 wait_cleanup.json 的文件夹"""
    print("Scanning folders by mapping...")

    # Read wait_cleanup.json to get folders that have already been uploaded but not yet cleaned up
    ignored_folders = load_wait_cleanup()
    folders = OrderedDict()

    for base_path in mapping_rules.keys():
        if os.path.exists(base_path):
            folder_list = [
                os.path.join(base_path, folder)
                for folder in os.listdir(base_path)
                if os.path.isdir(os.path.join(base_path, folder))
            ]
            folder_list.sort(key=lambda x: os.path.getctime(x))  # Sort by creation time

            for folder_path in folder_list:
                abs_path = os.path.abspath(folder_path)

                # Skip folders that are already in wait_cleanup.json
                if abs_path in ignored_folders:
                    print(f"Skipping {abs_path} (already in wait_cleanup.json)")
                    continue

                creation_time = os.path.getctime(abs_path)
                readable_time = datetime.fromtimestamp(creation_time).strftime('%y-%m-%d %H:%M:%S')
                print(f"{abs_path} -> Created at {readable_time}")

                folders[abs_path] = get_folder_size(folder_path)

    print("Folder scanning completed.")
    return folders

def select_folders_for_upload(folders, max_size=2048):
    """选择合适的文件夹以确保每天上传总量不超过 max_size GB"""
    print("Selecting folders for upload...")
    selected_folders = []
    total_size = 0
    for folder, size in folders.items():
        if total_size + size <= max_size:
            selected_folders.append(folder)
            total_size += size
        else:
            break
    print(f"Selected folders: {selected_folders}")
    return selected_folders

def upload_folders(selected_folders, mapping_rules):
    """执行 rclone 上传并记录日志，支持出现上传错误时切换目标 drive
    (Execute rclone upload and record log. If an upload error occurs, the current and subsequent folders will be uploaded using the next drive.)"""
    print("Starting upload process...")
    uploaded_log = "/home/tedwu/uploaded_folders.json"
    wait_cleanup_log = "/home/tedwu/wait_cleanup.json"

    # Define a list of drive names; originally default is "paula", and subsequent drives are used on error
    drives = ["paula", "zyr"] #, "vera", "pics", "codes", "alk", "columbia"]
    current_drive_index = 0

    uploaded_folders = {}
    if os.path.exists(uploaded_log):
        with open(uploaded_log, "r") as f:
            uploaded_folders = json.load(f)

    # Flag to indicate no more available drives
    stop_all = False

    # Iterate over each folder selected for upload
    for folder in selected_folders:
        # Find the corresponding mapping rule for the folder
        for base_path, remote_path in mapping_rules.items():
            if folder.startswith(base_path):
                # Extract the path suffix from remote_path (after the drive name)
                parts = remote_path.split(":", 1)
                if len(parts) != 2:
                    print("Invalid remote path format:", remote_path)
                    continue
                path_suffix = parts[1]  # e.g., "/剧集/个人收集/电影/动画电影"
                # Construct destination using the current drive name, only replace drive name
                destination = folder.replace(base_path, f"{drives[current_drive_index]}:{path_suffix}")
                # Attempt to upload, retrying with next drive if error occurs
                while True:
                    print(f"Uploading {folder} to {destination} using drive {drives[current_drive_index]}")
                    # Execute rclone copy command
                    command = [
                        "rclone", "copy", "--progress", "--drive-upload-cutoff", "1000T",
                        "--drive-stop-on-upload-limit", folder, destination
                    ]
                    result = subprocess.run(command)
                    if result.returncode == 0:
                        # Upload succeeded, record upload time and break the retry loop
                        uploaded_folders[os.path.abspath(folder)] = str(datetime.now())
                        break
                    else:
                        # If an error occurs, switch to next drive and update destination accordingly
                        print(f"Upload error for {folder} using drive {drives[current_drive_index]}, switching to next drive...")
                        current_drive_index += 1
                        if current_drive_index >= len(drives):
                            print("No more drives available, stopping upload.")
                            stop_all = True
                            break
                        destination = folder.replace(base_path, f"{drives[current_drive_index]}:{path_suffix}")
                # 如果没有可用的drive，则退出外层循环
                if stop_all:
                    break
                # Once matched, no need to check other base paths
                break
        if stop_all:
            break

    # Load existing wait_cleanup log and merge the new uploaded folders
    wait_cleanup_folders = {}
    if os.path.exists(wait_cleanup_log):
        with open(wait_cleanup_log, "r") as f:
            wait_cleanup_folders = json.load(f)

    wait_cleanup_folders.update(uploaded_folders)

    # Save updated logs and clear uploaded_folders.json
    with open(wait_cleanup_log, "w") as f:
        json.dump(wait_cleanup_folders, f, indent=4, ensure_ascii=False)

    with open(uploaded_log, "w") as f:
        json.dump({}, f, indent=4, ensure_ascii=False)  # Clear upload log

    print("Upload process completed.")

def cleanup_old_uploads():
    """每周删除已上传的文件夹，确保已完整上传到云端"""
    print("Starting cleanup process...")
    wait_cleanup_log = "/home/tedwu/wait_cleanup.json"

    if not os.path.exists(wait_cleanup_log):
        return

    with open(wait_cleanup_log, "r") as f:
        uploaded_folders = json.load(f)

    one_week_ago = datetime.now()  # Can adjust timedelta if needed
    for folder, timestamp in list(uploaded_folders.items()):
        upload_time = datetime.fromisoformat(timestamp)
        if upload_time < one_week_ago:
            for base_path, remote_path in load_mapping_rules().items():
                if folder.startswith(base_path):
                    remote_folder = folder.replace(base_path, remote_path)
                    if is_folder_uploaded(folder, remote_folder):
                        print(f"Deleting {folder}...")
                        subprocess.run(["rm", "-rf", folder])
                    del uploaded_folders[folder]

    with open(wait_cleanup_log, "w") as f:
        json.dump(uploaded_folders, f, indent=4, ensure_ascii=False)

    print("Cleanup process completed.")

def is_folder_uploaded(local_folder, remote_folder):
    """使用 rclone 检查本地文件夹是否完整上传到云端"""
    print(f"Checking if {local_folder} is fully uploaded...")
    command = ["rclone", "check", local_folder, remote_folder]
    result = subprocess.run(command, capture_output=True, text=True)
    return "ERROR" not in result.stdout  # If no error, assume folder is fully uploaded

def main():
    if len(sys.argv) > 1 and sys.argv[1] == "--cleanup":
        cleanup_old_uploads()
    else:
        mapping_rules = load_mapping_rules()
        folders = scan_folders_by_mapping(mapping_rules)
        selected_folders = select_folders_for_upload(folders)
        upload_folders(selected_folders, mapping_rules)
    print("Script execution completed.")

if __name__ == "__main__":
    main()
