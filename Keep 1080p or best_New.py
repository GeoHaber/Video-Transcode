"""
Keep 1080p or Best - Fixed Player Edition
Rev: 5.0

Features:
  - FULL VLC PLAYER: Dark mode, Seek Slider, Audio Mute, File Info.
  - ANTI-FREEZE: Strict timeouts on ffprobe to prevent hanging on bad files.
  - REAL METADATA: Reads actual resolution (1920x1080) from file headers.
"""

import os
import sys
import re
import json
import time
import shutil
import asyncio
import platform
import threading
import subprocess
import atexit
import traceback
from typing import List, Dict, Optional, Tuple, Set
from pathlib import Path
from collections import defaultdict
from dataclasses import dataclass
import tkinter as tk

# --- CONFIGURATION ---
STRICT_TIMEOUT = 15.0  # Seconds to wait for file analysis (Anti-Freeze)

# Try importing dependencies
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
# Config & Globals
# -----------------------------------------------------------------------------

@dataclass
class Config:
	source_dirs: List[Path]
	except_dir: Path
	user_recycle_dir: Path
	exclude_keywords: List[str]
	video_extensions: Set[str]

# Global Regex Patterns
TV_PATTERNS = [
	re.compile(r'^(?P<show_title>.*?)(?:[\s\._]*)(?:[Ss](?:eason)?[\s\._-]*?(?P<season>\d{1,2}))(?:[\s\._-]*(?:[EeXx](?:pisode)?[\s\._-]*?(?P<episode>\d{1,3})))(?P<remaining_title>.*)', re.IGNORECASE),
	re.compile(r'^(?P<show_title>.*?)(?:[\s\._]*)(?:(?P<season>\d{1,2})[xX](?P<episode>\d{1,3}))(?P<remaining_title>.*)', re.IGNORECASE),
]
NON_MOVIE_REGEXES = []

FFMPEG_BIN = "ffmpeg"
FFPROBE_BIN = "ffprobe"
PRINT_LOCK = threading.Lock()
IS_WIN = platform.system() == "Windows"

# -----------------------------------------------------------------------------
# Setup & Helpers
# -----------------------------------------------------------------------------

def safe_print(*args, **kwargs):
	with PRINT_LOCK:
		print(*args, **kwargs)
		sys.stdout.flush()

def load_config() -> Config:
	# --- DEFAULT PATHS (EDIT HERE IF NEEDED) ---
	defaults = {
		"source_dirs": [r"F:\Media\TV", r"F:\Media\Movie"],
		"except_dir": r"C:\_temp\Exceptions",
		"user_recycle_dir": r"C:\_temp\Recycled",
		"exclude_keywords": ["trailer", "sample"],
		"video_extensions": ["mp4", "mkv", "avi", "wmv", "mov", "m4v", "mpg"],
	}

	# Load from file if exists
	if os.path.exists("config.json"):
		try:
			with open("config.json", 'r') as f: defaults.update(json.load(f))
		except: pass

	global NON_MOVIE_REGEXES
	NON_MOVIE_REGEXES = [re.compile(rf'\b{kw}\b', re.IGNORECASE) for kw in defaults["exclude_keywords"]]

	for k in ["except_dir", "user_recycle_dir"]:
		Path(defaults[k]).mkdir(parents=True, exist_ok=True)

	return Config(
		source_dirs=[Path(p) for p in defaults["source_dirs"]],
		except_dir=Path(defaults["except_dir"]),
		user_recycle_dir=Path(defaults["user_recycle_dir"]),
		exclude_keywords=defaults["exclude_keywords"],
		video_extensions=set(defaults["video_extensions"])
	)

CONFIG = load_config()

# Process Manager (Clean Cleanup)
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
		with self._lock: procs = list(self._procs.values())
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
	try: await run_command_async([FFPROBE_BIN, "-version"], 5)
	except:
		safe_print("CRITICAL: ffprobe not found. Install FFmpeg.")
		sys.exit(1)

# -----------------------------------------------------------------------------
# Media Data
# -----------------------------------------------------------------------------

@dataclass
class MediaFile:
	path: Path
	name: str
	size: int
	content_type: str
	title: str
	# Metadata
	width: int
	height: int
	duration: float
	bitrate: float
	# Sorting
	sort_score: tuple

	@property
	def resolution_str(self) -> str:
		if self.width > 0 and self.height > 0: return f"{self.width}x{self.height}"
		return "Unknown"

	@property
	def nice_size(self) -> str:
		s = self.size
		for u in ['B', 'KB', 'MB', 'GB']:
			if s < 1024: return f"{s:.1f}{u}"
			s /= 1024
		return f"{s:.1f}TB"

	@property
	def info_string(self) -> str:
		"""Returns the text string displayed in the player."""
		br_kb = int(self.bitrate / 1000)
		dur_m = int(self.duration // 60)
		return f"{self.name}\n{self.resolution_str} | {self.nice_size} | {dur_m}m | {br_kb} kbps"

# -----------------------------------------------------------------------------
# THE RESTORED VLC PLAYER APP
# -----------------------------------------------------------------------------

class VLCPlayerApp:
	def __init__(self, media_files: List[MediaFile]):
		self.media_files = media_files
		self.root = None
		self.players = []
		self.is_paused = False
		self.is_muted = False
		self.slider_dragging = False
		self.slider_var = None
		self.duration_sec = 0

	def run(self):
		"""Blocking UI Loop"""
		self.root = tk.Tk()
		self.root.title("Side-by-Side Comparison Player")
		self.root.configure(bg="#1e1e1e") # Dark background
		self.root.protocol("WM_DELETE_WINDOW", self._cleanup)

		# Determine Grid Layout
		count = len(self.media_files)
		cols = 2 if count > 1 else 1
		rows = (count + 1) // 2

		# VLC Instance
		# --no-video-title-show: Don't show filename overlay inside video
		inst = vlc.Instance("--no-xlib", "--quiet", "--no-video-title-show")

		# Create Video Frames
		video_frame_container = tk.Frame(self.root, bg="#1e1e1e")
		video_frame_container.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

		for i, mf in enumerate(self.media_files):
			# Container for 1 video + label
			cell = tk.Frame(video_frame_container, bg="black", bd=1, relief=tk.SUNKEN)
			cell.grid(row=i//cols, column=i%cols, sticky="nsew", padx=2, pady=2)
			video_frame_container.grid_columnconfigure(i%cols, weight=1)
			video_frame_container.grid_rowconfigure(i//cols, weight=1)

			# 1. Video Canvas
			canvas = tk.Canvas(cell, bg="black", highlightthickness=0)
			canvas.pack(fill=tk.BOTH, expand=True)

			# 2. Info Label (Dark Mode)
			lbl = tk.Label(cell, text=mf.info_string, bg="#1e1e1e", fg="#e0e0e0",
						   font=("Consolas", 9), anchor="w", justify=tk.LEFT)
			lbl.pack(fill=tk.X, side=tk.BOTTOM, ipady=3)

			# Initialize Player
			p = inst.media_player_new()
			p.set_media(inst.media_new(str(mf.path)))

			# Embed Player
			wid = canvas.winfo_id()
			if IS_WIN: p.set_hwnd(wid)
			elif platform.system() == "Darwin": p.set_nsobject(wid)
			else: p.set_xwindow(wid)

			p.play()
			self.players.append(p)

			# Update duration from first file (approx)
			if i == 0 and mf.duration > 0:
				self.duration_sec = mf.duration

		# --- CONTROLS AREA ---
		ctrl = tk.Frame(self.root, bg="#2d2d2d", pady=5)
		ctrl.pack(fill=tk.X, side=tk.BOTTOM)

		# Slider
		self.slider_var = tk.DoubleVar()
		self.slider = tk.Scale(ctrl, from_=0, to=100, orient=tk.HORIZONTAL, variable=self.slider_var,
							   showvalue=0, bg="#2d2d2d", fg="white",
							   troughcolor="#404040", activebackground="#007acc",
							   command=self._on_seek)
		self.slider.pack(fill=tk.X, padx=10, pady=(0,5))
		self.slider.bind("<ButtonPress-1>", self._start_drag)
		self.slider.bind("<ButtonRelease-1>", self._end_drag)

		# Buttons
		btn_frame = tk.Frame(ctrl, bg="#2d2d2d")
		btn_frame.pack()

		def mk_btn(txt, cmd, w=8):
			return tk.Button(btn_frame, text=txt, command=cmd, width=w,
							 bg="#3c3c3c", fg="white", activebackground="#505050", activeforeground="white", relief=tk.FLAT)

		mk_btn("Pause", self._toggle_pause).pack(side=tk.LEFT, padx=2)
		mk_btn("Mute", self._toggle_mute).pack(side=tk.LEFT, padx=2)
		mk_btn("Close", self._cleanup, w=10).pack(side=tk.LEFT, padx=10)

		# Start Polling Loop for Slider Update
		self._update_ui_loop()

		self.root.mainloop()

	def _toggle_pause(self):
		self.is_paused = not self.is_paused
		for p in self.players:
			p.set_pause(1 if self.is_paused else 0)

	def _toggle_mute(self):
		self.is_muted = not self.is_muted
		for p in self.players:
			p.audio_set_mute(self.is_muted)

	def _start_drag(self, event): self.slider_dragging = True
	def _end_drag(self, event):
		self.slider_dragging = False
		self._on_seek(self.slider.get())

	def _on_seek(self, value):
		# VLC seeks in percentage if length is known, or 0.0-1.0
		# Here we map 0-100 scale to 0.0-1.0 pos
		pos = float(value) / 100.0
		for p in self.players:
			if p.is_seekable():
				p.set_position(pos)

	def _update_ui_loop(self):
		if not self.root: return
		try:
			if not self.slider_dragging and self.players:
				# Update slider based on first player
				pos = self.players[0].get_position() # 0.0 to 1.0
				if pos >= 0:
					self.slider_var.set(pos * 100)
		except: pass
		self.root.after(250, self._update_ui_loop)

	def _cleanup(self):
		try:
			for p in self.players:
				p.stop()
				p.release()
			self.root.destroy()
			self.root = None
		except: pass

def launch_vlc(media_files: List[MediaFile]):
	if not HAS_VLC:
		safe_print("Error: python-vlc not installed.")
		return
	app = VLCPlayerApp(media_files)
	app.run()

# -----------------------------------------------------------------------------
# Metadata Extraction (Strict Mode)
# -----------------------------------------------------------------------------

def parse_filename_basics(path: Path) -> Tuple[str, str, str]:
	name = path.stem
	clean = re.sub(r'[\._]', ' ', name)
	ctype = 'movie'
	title = clean
	extra_id = "0"
	for pat in TV_PATTERNS:
		if m := pat.search(clean):
			ctype = 'tv'
			title = m.group('show_title') or clean
			if s := m.group('season'):
				if e := m.group('episode'):
					extra_id = f"S{int(s):02d}E{int(e):02d}"
			break
	return (title.strip().title(), ctype, extra_id)

async def get_metadata_strict(path: Path) -> Optional[Dict]:
	cmd = [FFPROBE_BIN, "-v", "error", "-print_format", "json", "-show_format", "-show_streams", str(path)]
	rc, out, _ = await run_command_async(cmd, STRICT_TIMEOUT)
	if rc != 0 or not out: return None
	try:
		data = json.loads(out)
		fmt = data.get("format", {})
		vid = next((s for s in data.get("streams", []) if s["codec_type"] == "video"), {})

		width = int(vid.get("width", 0))
		height = int(vid.get("height", 0))
		dur = float(fmt.get("duration", 0))
		br = float(vid.get("bit_rate") or fmt.get("bit_rate") or 0)

		if dur > 0 and br == 0: br = (path.stat().st_size * 8) / dur
		return {"width": width, "height": height, "duration": dur, "bitrate": br}
	except: return None

# -----------------------------------------------------------------------------
# Scanner Logic
# -----------------------------------------------------------------------------

async def scan_library(sem: asyncio.Semaphore) -> Dict[Tuple, List[MediaFile]]:
	groups = defaultdict(list)
	files_to_scan = []

	safe_print("--- Phase 1: Finding Files ---")
	for src in CONFIG.source_dirs:
		if not src.exists(): continue
		for root, _, files in os.walk(src):
			for f in files:
				if f.split('.')[-1].lower() in CONFIG.video_extensions:
					if not any(r.search(f) for r in NON_MOVIE_REGEXES):
						files_to_scan.append(Path(root) / f)
						if len(files_to_scan) % 10 == 0:
							safe_print(f"\r  Found {len(files_to_scan)} files...", end="")
	print ("")

	safe_print(f"--- Phase 2: Analyzing {len(files_to_scan)} Files (Strict Mode) ---")

	async def _analyze(path: Path):
		async with sem:
			if not path.exists(): return
			meta = await get_metadata_strict(path)
			if not meta: return

			title, ctype, extra = parse_filename_basics(path)

			w, h = meta['width'], meta['height']
			pixels = w * h
			norm_br = (meta['bitrate'] / 1000.0) / max(1.0, meta['duration'] ** 0.5)
			score = (pixels, norm_br, path.stat().st_size)

			mf = MediaFile(
				path=path, name=path.name, size=path.stat().st_size,
				content_type=ctype, title=title,
				width=w, height=h, duration=meta['duration'], bitrate=meta['bitrate'],
				sort_score=score
			)
			key = (ctype, title, extra)
			groups[key].append(mf)

	tasks = [_analyze(p) for p in files_to_scan]
	await tqdm_asyncio.gather(*tasks)
	return groups

# -----------------------------------------------------------------------------
# Interactive Processor
# -----------------------------------------------------------------------------

async def process_groups(groups: Dict[Tuple, List[MediaFile]]):
	dupes = {k: v for k, v in groups.items() if len(v) > 1}

	if not dupes:
		safe_print("No duplicates found.")
		return

	safe_print(f"\n--- Phase 3: Processing {len(dupes)} Groups ---")

	sorted_keys = sorted(dupes.keys(), key=lambda k: (k[0], k[1]))

	for i, key in enumerate(sorted_keys):
		files = dupes[key]
		files.sort(key=lambda f: f.sort_score, reverse=True)

		ctype, title, extra = key
		header = f"[{i+1}/{len(dupes)}] {title}"
		if ctype == 'tv': header += f" ({extra})"

		while True:
			print(f"\n{'='*60}")
			print(f"{header}")
			print(f"{'='*60}")

			for idx, f in enumerate(files):
				print(f" {idx+1}. {f.info_string.replace(chr(10), ' | ')}") # Print info on one line for menu

			print("\nOptions: (k #) Keep, (d #) Recycle, (p) Play All, (s) Skip, (q) Quit")

			try: raw = await asyncio.to_thread(input, "Choice: ")
			except EOFError: return

			parts = raw.lower().split()
			if not parts: continue
			cmd = parts[0]

			if cmd == 'q': return
			if cmd == 's': break

			if cmd == 'p':
				if not HAS_VLC:
					safe_print("!! VLC not installed.")
					continue
				safe_print("Launching Player... (Script paused)")
				await asyncio.to_thread(launch_vlc, files)
				continue

			if cmd in ['k', 'd'] and len(parts) > 1 and parts[1].isdigit():
				idx = int(parts[1]) - 1
				if not (0 <= idx < len(files)):
					print("Invalid index.")
					continue

				target = files[idx]

				if cmd == 'k':
					safe_print(f">> Keeping: {target.name}")
					others = [x for j, x in enumerate(files) if j != idx]
					for o in others:
						dest = CONFIG.user_recycle_dir / o.name
						try: shutil.move(str(o.path), str(dest))
						except Exception as e: safe_print(f"Err moving {o.name}: {e}")
					break

				elif cmd == 'd':
					safe_print(f">> Recycling: {target.name}")
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
	sem = asyncio.Semaphore(min(4, os.cpu_count() or 2))
	groups = await scan_library(sem)
	await process_groups(groups)
	print("\nDone.")

if __name__ == "__main__":
	if sys.platform == 'win32':
		asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
	try:
		asyncio.run(main())
	except KeyboardInterrupt:
		print("\nExiting...")
	finally:
		PROC_MGR.terminate_all()
