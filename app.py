import pandas as pd
from yt_dlp import YoutubeDL
from concurrent.futures import ThreadPoolExecutor
import os
from pathlib import Path
import time

def download_video(url, output_path, resolution="720"):
    """Download a single YouTube video."""
    try:
        ydl_opts = {
            'format': f'bestvideo[height<={resolution}]+bestaudio/best[height<={resolution}]',
            'outtmpl': str(output_path / '%(title)s.%(ext)s'),
            'quiet': False,
            'no_warnings': True
        }

        with YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=True)
            return f"Downloaded: {info['title']}"
    except Exception as e:
        return f"Error downloading {url}: {str(e)}"

def process_downloads(csv_file, output_folder="youtube_videos", max_threads=2, resolution="720"):
    output_path = Path(output_folder)
    output_path.mkdir(exist_ok=True)

    df = pd.read_csv(csv_file)

    if 'YouTube_Link' not in df.columns:
        raise ValueError("CSV file must contain 'YouTube_Link' column")

    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = [
            executor.submit(download_video, url, output_path, resolution)
            for url in df['YouTube_Link']
        ]

        for future in futures:
            print(future.result())

if __name__ == "__main__":
    process_downloads(
        csv_file="youtube_links.csv",
        output_folder="youtube_videos",
        max_threads=2,
        resolution="720"
      )
