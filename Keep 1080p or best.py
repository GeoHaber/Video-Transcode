"""
Keep 1080p or Best - Anti-Freeze Edition
Rev: 3.1 (Debug & Stability Fixes)

Fixes:
  - Added "Heartbeat" printing during file discovery so it doesn't look dead.
  - Added strict timeouts to FFprobe to kill hanging processes on corrupt files.
  - Added Debug logging to identify exactly which file crashes the scanner.
"""

import os
import sys
import re
import json
import time
import shutil
import asyncio
import signal
import platform
import threading
import subprocess
import atexit
from typing import List, Dict, Optional, Tuple, Set
from pathlib import Path
from datetime import datetime
from collections import defaultdict
from dataclasses import dataclass
import tkinter as tk

# --- TOGGLES ---
DEBUG_MODE = True  # Set to True to see exactly which file is being scanned
STRICT_TIMEOUT = 20.0  # Seconds before killing a stuck ffprobe process

# Try importing third-party libraries
try:
    from tqdm.asyncio import tqdm_asyncio
except ImportError:
    class tqdm_asyncio:
        @staticmethod
        async def gather(*args, **kwargs): return await asyncio.gather(*args)

try:
    import vlc
    HAS_VLC = True
except ImportError:
    HAS_VLC = False

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

@dataclass
class Config:
    source_dirs: List[Path]
    except_dir: Path
    user_recycle_dir: Path
    exclude_keywords: List[str]
    video_extensions: Set[str]
    ffprobe_timeout: float = 30.0

NON_MOVIE_REGEXES: List[re.Pattern] = []
TV_PATTERNS = [
    re.compile(r'^(?P<show_title>.*?)(?:[\s\._]*)(?:[Ss](?:eason)?[\s\._-]*?(?P<season>\d{1,2}))(?:[\s\._-]*(?:[EeXx](?:pisode)?[\s\._-]*?(?P<episode>\d{1,3})))(?P<remaining_title>.*)', re.IGNORECASE),
    re.compile(r'^(?P<show_title>.*?)(?:[\s\._]*)(?:(?P<season>\d{1,2})[xX](?P<episode>\d{1,3}))(?P<remaining_title>.*)', re.IGNORECASE),
    re.compile(r'^(?:[Ss](?:eason)?[\s\._-]*?(?P<season>\d{1,2}))(?:[\s\._-]*(?:[EeXx](?:pisode)?[\s\._-]*?(?P<episode>\d{1,3})))(?:[\s\._]*)(?P<show_title>.*?)(?P<remaining_title>.*)', re.IGNORECASE),
]

FFMPEG_BIN = "ffmpeg"
FFPROBE_BIN = "ffprobe"
PRINT_LOCK = threading.Lock()
VLC_LOCK = threading.Lock()
ACTIVE_VLC_APP = None
IS_WIN = platform.system() == "Windows"

# -----------------------------------------------------------------------------
# Helper Utilities
# -----------------------------------------------------------------------------

def safe_print(*args, **kwargs):
    with PRINT_LOCK:
        print(*args, **kwargs)
        sys.stdout.flush() # FORCE FLUSH to prevent buffer freeze appearance

def load_config() -> Config:
    # HARDCODED DEFAULTS FOR STABILITY - EDIT PATHS HERE IF JSON FAILS
    defaults = {
        "source_dirs": [r"F:\Media\TV", r"F:\Media\Movie"],
        "except_dir": r"C:\_temp\Exceptions",
        "user_recycle_dir": r"C:\_temp\Recycled",
        "exclude_keywords": ["trailer", "sample", "bonus"],
        "video_extensions": ["mp4", "mkv", "avi", "wmv", "mov", "m4v"],
    }

    # Try loading json, fall back to defaults
    try:
        if os.path.exists("config.json"):
            with open("config.json", 'r', encoding='utf-8') as f:
                defaults.update(json.load(f))
    except Exception as e:
        safe_print(f"Config load error: {e}. Using defaults.")

    global NON_MOVIE_REGEXES
    NON_MOVIE_REGEXES = [re.compile(rf'\b{kw}\b', re.IGNORECASE) for kw in defaults["exclude_keywords"]]

    # Create dirs
    for k in ["except_dir", "user_recycle_dir"]:
        Path(defaults[k]).mkdir(parents=True, exist_ok=True)

    return Config(
        source_dirs=[Path(p) for p in defaults["source_dirs"]],
        except_dir=Path(defaults["except_dir"]),
        user_recycle_dir=Path(defaults["user_recycle_dir"]),
        exclude_keywords=defaults["exclude_keywords"],
        video_extensions=set(defaults["video_extensions"]),
        ffprobe_timeout=STRICT_TIMEOUT
    )

CONFIG = load_config()

# -----------------------------------------------------------------------------
# Process Management
# -----------------------------------------------------------------------------

class ProcessManager:
    def __init__(self):
        self._procs = {}
        self._lock = threading.Lock()
    def register(self, proc):
        if proc.pid:
            with self._lock: self._procs[proc.pid] = proc
    def unregister(self, proc):
        if proc.pid:
            with self._lock: self._procs.pop(proc.pid, None)
    def terminate_all(self):
        with self._lock:
            procs = list(self._procs.values())
        for p in procs:
            if p.returncode is None:
                try: p.kill()
                except: pass

PROC_MGR = ProcessManager()
atexit.register(PROC_MGR.terminate_all)

async def run_command_async(cmd: List[str], timeout: float) -> Tuple[int, str, str]:
    proc = None
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if IS_WIN else 0
        )
        PROC_MGR.register(proc)

        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        return proc.returncode, stdout.decode(errors='ignore'), stderr.decode(errors='ignore')

    except asyncio.TimeoutError:
        if proc:
            try: proc.kill()
            except: pass
        return -1, "", "Timeout"
    except Exception as e:
        return -1, "", str(e)
    finally:
        if proc: PROC_MGR.unregister(proc)

async def find_binaries():
    global FFMPEG_BIN, FFPROBE_BIN
    FFMPEG_BIN = shutil.which("ffmpeg") or "ffmpeg"
    FFPROBE_BIN = shutil.which("ffprobe") or "ffprobe"

    # Simple check
    try:
        await run_command_async([FFPROBE_BIN, "-version"], 5)
    except:
        safe_print("CRITICAL: ffprobe not working. Install FFmpeg.")
        sys.exit(1)

# -----------------------------------------------------------------------------
# Media Logic
# -----------------------------------------------------------------------------

@dataclass
class MediaFile:
    path: Path
    name: str
    size: int
    content_type: str
    title: str
    resolution: str
    duration: float
    bitrate: float
    sort_score: tuple

def parse_filename(path: Path) -> Tuple[str, str, str]:
    """Returns (Title, Resolution, Type)"""
    name = path.stem
    clean = re.sub(r'[\._]', ' ', name)

    ctype = 'movie'
    title = clean

    for pat in TV_PATTERNS:
        if m := pat.search(clean):
            ctype = 'tv'
            title = m.group('show_title') or clean
            if s := m.group('season'):
                if e := m.group('episode'):
                    return (title.strip(), f"S{int(s):02d}E{int(e):02d}", ctype)
            break

    res = "Unknown"
    if m := re.search(r'(\b\d{3,4}p\b|\b4K\b)', str(path), re.IGNORECASE):
        res = m.group(0).upper()

    return (title.strip().title(), res, ctype)

async def get_metadata(path: Path) -> Optional[Dict]:
    if DEBUG_MODE:
        safe_print(f"  > Probing: {path.name}...")

    cmd = [
        FFPROBE_BIN, "-v", "error", "-print_format", "json",
        "-show_format", "-show_streams", str(path)
    ]

    # STRICT TIMEOUT HERE
    rc, out, err = await run_command_async(cmd, STRICT_TIMEOUT)

    if rc != 0 or not out:
        if DEBUG_MODE:
            safe_print(f"  X Failed Probe: {path.name}")
        return None

    try:
        data = json.loads(out)
        fmt = data.get("format", {})
        vid = next((s for s in data.get("streams", []) if s["codec_type"] == "video"), {})

        dur = float(fmt.get("duration", 0))
        br = float(vid.get("bit_rate") or fmt.get("bit_rate") or 0)

        if dur > 0 and br == 0: # Fallback calc
            br = (path.stat().st_size * 8) / dur

        return {"duration": dur, "bitrate": br}
    except:
        return None

# -----------------------------------------------------------------------------
# Logic
# -----------------------------------------------------------------------------

async def scan_files(sem: asyncio.Semaphore) -> Dict[Tuple, List[MediaFile]]:
    groups = defaultdict(list)
    media_files = []

    # 1. Discovery Phase (With Heartbeat)
    safe_print("--- Phase 1: Locating Files ---")
    for src in CONFIG.source_dirs:
        if not src.exists(): continue

        def _walk():
            found = []
            count = 0
            for root, _, files in os.walk(src):
                for f in files:
                    if f.split('.')[-1].lower() in CONFIG.video_extensions:
                        if not any(r.search(f) for r in NON_MOVIE_REGEXES):
                            found.append(Path(root) / f)
                            count += 1
                            if count % 100 == 0:
                                safe_print(f"   Found {count} files so far in {src.name}...")
            return found

        media_files.extend(await asyncio.to_thread(_walk))

    safe_print(f"--- Phase 2: Analyzing {len(media_files)} Files ---")

    # 2. Analysis Phase
    async def _process(path: Path):
        async with sem:
            if not path.exists(): return

            # Identify
            title, extra, ctype = parse_filename(path)

            # Probe
            meta = await get_metadata(path)
            if not meta: return

            dur = meta["duration"]
            br = meta["bitrate"]
            size = path.stat().st_size

            # Score
            res_val = 0
            if "1080" in extra: res_val = 1080
            elif "720" in extra: res_val = 720
            elif "4K" in extra: res_val = 2160

            norm_br = (br / 1000.0) / max(1.0, dur ** 0.5)
            score = (res_val, norm_br, size)

            mf = MediaFile(
                path=path, name=path.name, size=size,
                content_type=ctype, title=title, resolution=extra,
                duration=dur, bitrate=br, sort_score=score
            )

            # Key: (Type, Title, Season/Episode/Year identifier)
            # If movie: (movie, Title, Year)
            # If TV: (tv, Title, S01E01)
            key = (ctype, title, extra if ctype == 'tv' else 0)
            groups[key].append(mf)

    # Use tqdm if available, else simple wait
    tasks = [_process(p) for p in media_files]
    try:
        await tqdm_asyncio.gather(*tasks)
    except Exception as e:
        safe_print(f"Crash in gather: {e}")
        # Fallback to simple loop
        for t in tasks: await t

    return groups

async def process_groups(groups: Dict[Tuple, List[MediaFile]]):
    dupes = {k: v for k, v in groups.items() if len(v) > 1}
    safe_print(f"--- Phase 3: Processing {len(dupes)} Duplicate Groups ---")

    if not dupes:
        safe_print("No duplicates found.")
        return

    sorted_keys = sorted(dupes.keys(), key=lambda k: (k[0], k[1]))

    for i, key in enumerate(sorted_keys):
        files = dupes[key]
        files.sort(key=lambda f: f.sort_score, reverse=True)

        ctype, title, extra = key
        header = f"[{i+1}/{len(dupes)}] {title}"
        if ctype == 'tv': header += f" {extra}"

        while True:
            print(f"\n{'='*60}")
            print(f"{header}")
            print(f"{'='*60}")

            for idx, f in enumerate(files):
                dur_str = f"{int(f.duration//60)}m"
                sz_str = f"{f.size / (1024*1024):.1f}MB"
                print(f" {idx+1}. {f.name}")
                print(f"    {f.resolution} | {sz_str} | {dur_str} | {f.bitrate/1000:.0f} kbps")

            print("\n(k #) Keep, (d #) Recycle, (s) Skip group, (q) Quit")

            try:
                raw = await asyncio.to_thread(input, "Choice: ")
            except EOFError: return

            parts = raw.lower().split()
            if not parts: continue

            cmd = parts[0]
            if cmd == 'q': return
            if cmd == 's': break

            if cmd in ['k', 'd'] and len(parts) > 1 and parts[1].isdigit():
                idx = int(parts[1]) - 1
                if 0 <= idx < len(files):
                    target = files[idx]

                    if cmd == 'k':
                        safe_print(f"Keeping {target.name}...")
                        others = [f for j, f in enumerate(files) if j != idx]
                        for o in others:
                            dest = CONFIG.user_recycle_dir / o.name
                            try: shutil.move(str(o.path), str(dest))
                            except Exception as e: safe_print(f"Err moving {o.name}: {e}")
                        break

                    elif cmd == 'd':
                        dest = CONFIG.user_recycle_dir / target.name
                        try: shutil.move(str(target.path), str(dest))
                        except Exception as e: safe_print(f"Err moving {target.name}: {e}")
                        files.pop(idx)
                        if len(files) < 2: break

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

async def main():
    await find_binaries()

    # Safe worker count: Don't choke the CPU/IO
    workers = min(4, os.cpu_count() or 2)
    sem = asyncio.Semaphore(workers)

    groups = await scan_files(sem)
    await process_groups(groups)
    print("\nAll done.")

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nExiting...")
    finally:
        PROC_MGR.terminate_all()
		
