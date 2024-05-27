import os

def get_sorted_subfolders(crrt_path):
    subfolders = sorted([f.path for f in os.scandir(crrt_path) if f.is_dir()])
    return subfolders
