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
from typing import Iterable, List, Optional, Set, Tuple

# ===== 原始 EPG 源（不变）=====
URLS = [
    "https://epgshare01.online/epgshare01/epg_ripper_US2.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_CA2.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_UK1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_IE1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_FR1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_DE1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_ES1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_IT1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_NL1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_PL1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_PT1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_AU1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_NZ1.xml.gz",
    "https://github.com/matthuisman/i.mjh.nz/raw/master/SamsungTVPlus/all.xml.gz",
    "https://raw.githubusercontent.com/BuddyChewChew/xumo-playlist-generator/main/playlists/xumo_epg.xml.gz",
    "https://github.com/matthuisman/i.mjh.nz/raw/master/Roku/all.xml.gz",
    "https://github.com/matthuisman/i.mjh.nz/raw/master/PlutoTV/all.xml.gz",
    "https://github.com/matthuisman/i.mjh.nz/raw/master/Plex/all.xml.gz",
]

PLAYLIST_PATH = "playlist.m3u"  # 你手动维护上传的
OUT_EPG = "epg.xml"             # IPTV 最终读取的 EPG
# 可选：输出一个当前白名单快照，方便你查看有哪些台（不用于匹配，只是可视化）
WRITE_GUIDE_XML = True
GUIDE_XML = "guide.xml"

TMP_DIR = ".tmp_epg"
UA = "Mozilla/5.0 (GitHubActions EPG Merger)"

ATTR_RE = re.compile(r'(\w[\w\-]*)="([^"]*)"')
CHANNEL_ID_RE = re.compile(r'<channel\s+[^>]*\bid="([^"]+)"', re.IGNORECASE)
PROGRAMME_CH_RE = re.compile(r'<programme\s+[^>]*\bchannel="([^"]+)"', re.IGNORECASE)

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

def load_allowed_from_m3u(m3u_path: str) -> List[str]:
    if not os.path.exists(m3u_path):
        raise FileNotFoundError(f"Missing {m3u_path}. Please commit it to repo root.")

    ids: List[str] = []
    seen: Set[str] = set()
    with open(m3u_path, "rt", encoding="utf-8", errors="ignore") as f:
        for line in f:
            if line.startswith("#EXTINF"):
                attrs = dict(ATTR_RE.findall(line))
                tvg_id = attrs.get("tvg-id") or attrs.get("tvgid") or attrs.get("tvg_id")
                if tvg_id:
                    tvg_id = tvg_id.strip()
                    if tvg_id and tvg_id not in seen:
                        seen.add(tvg_id)
                        ids.append(tvg_id)

    if not ids:
        raise ValueError("No tvg-id found in playlist.m3u. Ensure EXTINF lines contain tvg-id=\"...\".")
    return ids

def xml_escape_text(s: str) -> str:
    return (s.replace("&", "&amp;")
             .replace("<", "&lt;")
             .replace(">", "&gt;"))

def xml_escape_attr(s: str) -> str:
    return (xml_escape_text(s)
            .replace('"', "&quot;")
            .replace("'", "&apos;"))

def write_guide_xml(ids: List[str], out_path: str) -> None:
    tmp = out_path + ".tmp"
    with open(tmp, "wt", encoding="utf-8", newline="\n") as out:
        out.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        out.write('<tv generator-info-name="my-epg-whitelist">\n')
        for cid in ids:
            out.write(f'  <channel id="{xml_escape_attr(cid)}">\n')
            out.write(f'    <display-name lang="en">{xml_escape_text(cid)}</display-name>\n')
            out.write('  </channel>\n')
        out.write('</tv>\n')
    os.replace(tmp, out_path)

def iter_xmltv_inner_lines(path: str) -> Iterable[str]:
    """
    逐行读取 XMLTV（支持 .xml.gz 和 .xml），跳过 <tv...> 与 </tv> 外壳，只产出内部内容行。
    不依赖文件后缀：自动探测是否 gzip。
    """
    # 探测 gzip 魔数：1f 8b
    with open(path, "rb") as bf:
        head = bf.read(2)

    is_gz = (head == b"\x1f\x8b")

    if is_gz:
        opener = lambda p: gzip.open(p, "rt", encoding="utf-8", errors="replace")
    else:
        opener = lambda p: open(p, "rt", encoding="utf-8", errors="replace")

    with opener(path) as f:
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


def filter_epg(gz_files: List[str], allowed: Set[str], out_epg: str) -> Tuple[int, int, int, int]:
    """
    过滤：只保留 allowed 中的 channel/programme。
    返回统计：
      written_channels, written_programmes, missing_channel_in_sources, no_programmes
    """
    tmp = out_epg + ".tmp"

    written_channels = 0
    written_programmes = 0
    seen_channels: Set[str] = set()
    seen_programme_channels: Set[str] = set()

    with open(tmp, "wt", encoding="utf-8", newline="\n") as out:
        out.write(f"<!-- generated: {datetime.utcnow().isoformat()}Z -->\n")
        out.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        out.write('<tv generator-info-name="my-epg">\n')

        buf: List[str] = []
        block_type: Optional[str] = None  # channel|programme
        block_channel_id: Optional[str] = None

        def flush():
            nonlocal buf, block_type, block_channel_id, written_channels, written_programmes
            if not buf or not block_type or not block_channel_id:
                buf.clear()
                block_type = None
                block_channel_id = None
                return
            text = "".join(buf)

            if block_type == "channel":
                seen_channels.add(block_channel_id)
                if block_channel_id in allowed:
                    out.write(text)
                    written_channels += 1
            elif block_type == "programme":
                seen_programme_channels.add(block_channel_id)
                if block_channel_id in allowed:
                    out.write(text)
                    written_programmes += 1

            buf.clear()
            block_type = None
            block_channel_id = None

        for gz in gz_files:
            for line in iter_xmltv_inner_lines(gz):
                if block_type is None:
                    if "<channel " in line:
                        block_type = "channel"
                        buf = [line]
                        m = CHANNEL_ID_RE.search(line)
                        block_channel_id = m.group(1) if m else None
                        continue
                    if "<programme " in line:
                        block_type = "programme"
                        buf = [line]
                        m = PROGRAMME_CH_RE.search(line)
                        block_channel_id = m.group(1) if m else None
                        continue
                    continue

                buf.append(line)
                if block_type == "channel" and "</channel>" in line:
                    flush()
                elif block_type == "programme" and "</programme>" in line:
                    flush()

        flush()
        out.write("</tv>\n")

    os.replace(tmp, out_epg)

    missing = len(allowed - seen_channels)
    no_prog = len((allowed & seen_channels) - seen_programme_channels)
    return written_channels, written_programmes, missing, no_prog

def main() -> int:
    os.makedirs(TMP_DIR, exist_ok=True)

    ids = load_allowed_from_m3u(PLAYLIST_PATH)
    allowed = set(ids)

    print(f"[Whitelist] tvg-id in playlist.m3u: {len(ids)}")

    if WRITE_GUIDE_XML:
        write_guide_xml(ids, GUIDE_XML)
        print(f"[Whitelist] Updated {GUIDE_XML} (snapshot from playlist)")

    gz_files: List[str] = []
    for i, url in enumerate(URLS, start=1):
        path = os.path.join(TMP_DIR, f"{i:02d}.xml.gz")
        print(f"[{i}/{len(URLS)}] downloading: {url}")
        download(url, path)
        gz_files.append(path)

    ch_w, pr_w, missing, no_prog = filter_epg(gz_files, allowed, OUT_EPG)

    size_mb = os.path.getsize(OUT_EPG) / (1024 * 1024)
    print(f"[Result] epg.xml size: {size_mb:.2f} MB")
    print(f"[Result] channels written: {ch_w}, programmes written: {pr_w}")
    print(f"[Result] whitelist missing in sources: {missing}")
    print(f"[Result] whitelist with no programmes: {no_prog}")

    # 防止又生成超大文件导致 GitHub 拒绝
    if size_mb > 95:
        raise RuntimeError(f"epg.xml too large for GitHub push: {size_mb:.2f} MB")

    return 0

if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:
        print("ERROR:", e, file=sys.stderr)
        raise
