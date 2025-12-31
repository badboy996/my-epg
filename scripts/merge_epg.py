#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import gzip
import hashlib
import os
import re
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime
from typing import Set

URLS = [
    "https://epgshare01.online/epgshare01/epg_ripper_US2.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_US_SPORTS1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_US_LOCALS1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_UK1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_CA2.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_AU1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_FR1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_DE1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_ES1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_PT1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_IT1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_NL1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_BE2.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_CH1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_NZ1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_GR1.xml.gz",
]

PLAYLIST_PATH = "playlist.m3u"   # 你把 m3u 放到仓库根目录，命名为 playlist.m3u
OUT_XML = "epg.xml"
TMP_DIR = ".tmp_epg"
UA = "Mozilla/5.0 (GitHubActions EPG Merger)"

ATTR_RE = re.compile(r'(\w[\w\-]*)="([^"]*)"')

def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def download(url: str, out_path: str, retries: int = 5) -> None:
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=120) as resp:
                data = resp.read()
            with open(out_path, "wb") as f:
                f.write(data)
            return
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as e:
            last_err = e
            print(f"Download failed ({attempt}/{retries}): {url} -> {e}")
            time.sleep(3 * attempt)
    raise last_err

def load_allowed_tvg_ids(m3u_path: str) -> Set[str]:
    if not os.path.exists(m3u_path):
        raise FileNotFoundError(f"Missing {m3u_path}. Please commit it to the repo (e.g. root/playlist.m3u).")

    allowed: Set[str] = set()
    with open(m3u_path, "rt", encoding="utf-8", errors="ignore") as f:
        for line in f:
            if line.startswith("#EXTINF"):
                attrs = dict(ATTR_RE.findall(line))
                tvg_id = attrs.get("tvg-id") or attrs.get("tvgid") or attrs.get("tvg_id")
                if tvg_id:
                    allowed.add(tvg_id.strip())

    if not allowed:
        raise ValueError("No tvg-id found in playlist.m3u. Please ensure your m3u lines contain tvg-id=\"...\".")
    return allowed

def iter_xmltv_inner_lines(gz_path: str):
    """逐行读取解压后的 XMLTV，跳过 <tv...> 和 </tv> 外壳，只产出内部行。"""
    with gzip.open(gz_path, "rt", encoding="utf-8", errors="replace") as f:
        in_tv = False
        for line in f:
            if not in_tv:
                if "<tv" in line:
                    in_tv = True
                    after = line.split(">", 1)
                    if len(after) == 2 and after[1].strip():
                        yield after[1]
                continue
            if "</tv>" in line:
                before = line.split("</tv>", 1)[0]
                if before.strip():
                    yield before
                break
            yield line

def main() -> int:
    allowed = load_allowed_tvg_ids(PLAYLIST_PATH)
    print(f"Allowed tvg-id count: {len(allowed)}")

    os.makedirs(TMP_DIR, exist_ok=True)

    gz_files = []
    for i, url in enumerate(URLS, start=1):
        path = os.path.join(TMP_DIR, f"{i:02d}.xml.gz")
        print(f"[{i}/{len(URLS)}] downloading: {url}")
        download(url, path)
        gz_files.append(path)

    out_tmp = OUT_XML + ".tmp"
    with open(out_tmp, "wt", encoding="utf-8", newline="\n") as out:
        out.write(f"<!-- generated: {datetime.utcnow().isoformat()}Z -->\n")
        out.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        out.write('<tv generator-info-name="my-epg">\n')

        buf = []
        block_type = None  # "channel" | "programme"
        channel_id = None
        programme_channel = None

        def flush_block():
            nonlocal buf, block_type, channel_id, programme_channel
            if not buf or not block_type:
                buf = []
                block_type = None
                return

            text = "".join(buf)

            if block_type == "channel" and channel_id in allowed:
                out.write(text)
            elif block_type == "programme" and programme_channel in allowed:
                out.write(text)

            buf = []
            block_type = None
            channel_id = None
            programme_channel = None

        for gz in gz_files:
            for line in iter_xmltv_inner_lines(gz):
                if block_type is None:
                    if "<channel " in line:
                        block_type = "channel"
                        buf = [line]
                        if 'id="' in line:
                            channel_id = line.split('id="', 1)[1].split('"', 1)[0]
                        continue

                    if "<programme " in line:
                        block_type = "programme"
                        buf = [line]
                        if 'channel="' in line:
                            programme_channel = line.split('channel="', 1)[1].split('"', 1)[0]
                        continue

                    continue

                buf.append(line)

                if block_type == "channel" and "</channel>" in line:
                    flush_block()
                    continue
                if block_type == "programme" and "</programme>" in line:
                    flush_block()
                    continue

        flush_block()
        out.write("</tv>\n")

    if os.path.exists(OUT_XML):
        if sha256_file(OUT_XML) == sha256_file(out_tmp):
            print("No change in epg.xml")
            os.remove(out_tmp)
            return 0

    os.replace(out_tmp, OUT_XML)
    size_mb = os.path.getsize(OUT_XML) / (1024 * 1024)
    print(f"Updated epg.xml: {size_mb:.2f} MB")

    if size_mb > 95:
        raise RuntimeError(f"epg.xml too large for GitHub push: {size_mb:.2f} MB")

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
