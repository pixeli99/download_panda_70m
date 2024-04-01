import json
import sys, os, os.path as osp
import yt_dlp
import asyncio

import fire
import pandas as pd
from random import random
from concurrent.futures import ProcessPoolExecutor


def ytb_download(uid, url, json_info, output_dir="ytb_videos/"):
    os.makedirs(output_dir, exist_ok=True)
    # uid = url.split("?v=")[-1]
    yt_opts = {
        "format": "best",  # Download the best quality available
        "outtmpl": osp.join(output_dir, f"{uid}.%(ext)s"),  # Set the output template
        "postprocessors": [
            {
                "key": "FFmpegVideoConvertor",
                "preferedformat": "mp4",  # Convert video to mp4 format
            },
        ],
    }

    video_path = osp.join(output_dir, f"{uid}.mp4")
    meta_path = osp.join(output_dir, f"{uid}.json")
    if osp.exists(video_path) and osp.exists(meta_path):
        print(f"{uid} already labeled.")
        return 0

    try:
        with yt_dlp.YoutubeDL(yt_opts) as ydl:
            ydl.download([url])
        with open(osp.join(output_dir, f"{uid}.json"), "w") as fp:
            json.dump(json_info, fp, indent=2)
        return 0
    except:
        return -1


async def main(csv_path, max_workers=256, shards=0, total=-1, limit=False):
    PPE = ProcessPoolExecutor(max_workers=max_workers)
    loop = asyncio.get_event_loop()

    df = pd.read_csv(csv_path)
    output_dir = csv_path.split(".")[0]

    tasks = []

    data_list = list(df.iterrows())

    if total > 0:
        chunk = len(data_list) // total
        begin_idx = shards * chunk
        end_idx = (shards + 1) * chunk
        if shards == total - 1:
            end_idx = len(data_list)
        data_list = data_list[begin_idx:end_idx]
    print(f"download total {len(data_list)} videos")
    
    for idx, (index, row) in enumerate(data_list):
        uid = row["videoID"]
        url = row["url"]

        json_info = {
            "timestamp": eval(row["timestamp"]),
            "caption": eval(row["caption"]),
            "matching_score": eval(row["matching_score"]),
        }

        tasks.append(
            loop.run_in_executor(PPE, ytb_download, uid, url, json_info, output_dir)
        )
        if idx >= 20 and limit:
            break
    res = await asyncio.gather(*tasks)

    print(f"[{sum(res)} / {len(res)}]")


def entry(csv="panda70m_testing.csv", shards=0, total=-1, limit=False):
    asyncio.run(main(csv, shards=shards, total=total, limit=limit))


if __name__ == "__main__":
    fire.Fire(entry)
