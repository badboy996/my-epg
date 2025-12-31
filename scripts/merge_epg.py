#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import gzip
import hashlib
import os
import sys
import urllib.request
from datetime import datetime
from typing import List, Tuple

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

OUT_XML = "epg.xml"
TMP_DIR = ".tmp_epg"

UA = "Mozilla/5.0 (GitHubActions EPG Merger)"

def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def download(url: str, out_path: str) -> None:
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=120) as resp:
        data = resp.read()
    with open(out_path, "wb") as f:
        f.write(data)

def extract_tv_parts_from_gz(gz_path: str) -> Tuple[bytes, bytes, bytes]:
    """
    从一个 gzip 的 XMLTV 中提取：
    - xml declaration(可能有) + <tv ...> opening tag（含结尾 >）
    - tv 内部内容（不含 </tv>）
    - </tv> closing tag（原样，通常就是 </tv>）
    为了省内存，按块扫描，但这里实现用“读取全文 bytes”，一般也够用；
    如果你遇到单个文件特别大（几百MB+），再告诉我我给你换成完全流式版本。
    """
    with gzip.open(gz_path, "rb") as f:
        raw = f.read()

    # 找 <tv ...> 的开头与结束
    tv_start = raw.find(b"<tv")
    if tv_start == -1:
        raise ValueError(f"Missing <tv in {gz_path}")

    open_end = raw.find(b">", tv_start)
    if open_end == -1:
        raise ValueError(f"Unterminated <tv ...> in {gz_path}")

    open_tag = raw[:open_end+1]  # 含 xml declaration + opening tag

    # 找最后一个 </tv>
    close_tag = b"</tv>"
    tv_close = raw.rfind(close_tag)
    if tv_close == -1:
        raise ValueError(f"Missing </tv> in {gz_path}")

    inner = raw[open_end+1:tv_close]  # tv 内部内容
    return open_tag, inner, close_tag

def main() -> int:
    os.makedirs(TMP_DIR, exist_ok=True)

    gz_files: List[str] = []
    for i, url in enumerate(URLS, start=1):
        name = f"{i:02d}.xml.gz"
        path = os.path.join(TMP_DIR, name)
        print(f"[{i}/{len(URLS)}] downloading: {url}")
        download(url, path)
        gz_files.append(path)

    print("Merging...")
    first_open_tag = None
    closing_tag = b"</tv>"

    # 输出到临时文件，避免写一半失败
    out_tmp = OUT_XML + ".tmp"
    with open(out_tmp, "wb") as out:
        out.write(b"<!-- generated: " + datetime.utcnow().isoformat().encode("utf-8") + b"Z -->\n")

        for idx, gz in enumerate(gz_files):
            open_tag, inner, close = extract_tv_parts_from_gz(gz)
            if idx == 0:
                first_open_tag = open_tag
                out.write(first_open_tag)
                if not first_open_tag.endswith(b"\n"):
                    out.write(b"\n")
            # 后续文件只写 inner
            out.write(inner)
            if len(inner) and not inner.endswith(b"\n"):
                out.write(b"\n")
            closing_tag = close  # 通常一致

        out.write(closing_tag)
        if not closing_tag.endswith(b"\n"):
            out.write(b"\n")

    # 如果已有旧文件且内容相同，则不替换
    if os.path.exists(OUT_XML):
        old_hash = sha256_file(OUT_XML)
        new_hash = sha256_file(out_tmp)
        if old_hash == new_hash:
            print("No change in epg.xml")
            os.remove(out_tmp)
            return 0

    os.replace(out_tmp, OUT_XML)
    print("Updated epg.xml")
    return 0

if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:
        print("ERROR:", e, file=sys.stderr)
        raise
