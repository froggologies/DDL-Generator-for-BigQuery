import os
import shutil
import re
import argparse
from datetime import datetime


def move_old_sql_to_trash(root_dir):
    """Moves older .sql files to a trash directory, keeping only the latest."""

    trash_dir = os.path.join(root_dir, "trash")
    os.makedirs(trash_dir, exist_ok=True)

    for dirpath, dirnames, filenames in os.walk(root_dir):
        sql_files = {}
        for filename in filenames:
            if filename.endswith(".sql"):
                base_name = filename.split("_")[0]

                if base_name not in sql_files:
                    sql_files[base_name] = []

                try:
                    timestamp_str = re.search(
                        r"_(\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2})", filename
                    ).group(1)
                    timestamp = datetime.strptime(timestamp_str, "%Y-%m-%dT%H-%M-%S")
                    sql_files[base_name].append((timestamp, filename))
                except (AttributeError, ValueError):
                    print(
                        f"Warning: Skipping file {filename} due to missing or invalid timestamp."
                    )

        for base_name, files in sql_files.items():
            if len(files) > 1:
                files.sort(reverse=True)
                latest_file = files[0][1]

                for timestamp, filename in files[1:]:
                    source_path = os.path.join(dirpath, filename)
                    dest_path = os.path.join(trash_dir, filename)
                    try:
                        shutil.move(source_path, dest_path)
                        print(f"Moved {filename} to trash")
                    except Exception as e:
                        print(f"Error moving {filename}: {e}")


if __name__ == "__main__":  # Makes the script runnable from the command line
    parser = argparse.ArgumentParser(
        description="Move old SQL files to a trash directory."
    )
    parser.add_argument(
        "root_dir",
        nargs="?",
        default=".",
        help="Root directory to process (defaults to current directory)",
    )

    args = parser.parse_args()

    move_old_sql_to_trash(args.root_dir)
