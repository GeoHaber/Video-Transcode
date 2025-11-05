# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sys
import time
import atexit
import random
import shutil
import string
import threading
import platform

from typing     import Any, Dict, Optional
from pathlib    import Path
from datetime	import datetime

Rev ="""
  Trans_code.py (Shutdown-safe hardening) 10/30/2025
	- All fixes from design review applied.
	- Standardized logging to use Tee+print_lock and a module logger for critical errors.
	- Fixed hvc1 remux-only bug.
	- Fixed bitrate logic.
	- Fixed spinner bug.
	- Fixed concurrency deadlock risk.
"""

# -----------------------------------------------------------------------------
# Paths & Globals
# -----------------------------------------------------------------------------

def _get_script_dir() -> Path:
	if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
		return Path(sys.executable).resolve().parent
	try:
		return Path(__file__).resolve().parent
	except NameError:
		return Path.cwd()

SCRIPT_DIR = _get_script_dir()
WORK_DIR: Path = Path(os.environ.get("ONE_TRANS_WORK_DIR") or str(SCRIPT_DIR)).resolve()
WORK_DIR.mkdir(parents=True, exist_ok=True)

RUN_TOKEN = "".join(random.choices(string.ascii_lowercase + string.digits, k=4))
RUN_TMP = WORK_DIR / f"__N_tmp_{RUN_TOKEN}"
RUN_TMP.mkdir(parents=True, exist_ok=True)

def _cleanup_run_tmp() -> None:
	try:
		if RUN_TMP.exists():
			shutil.rmtree(RUN_TMP, ignore_errors=True)
	except Exception:
		pass

atexit.register(_cleanup_run_tmp)

print_lock		= threading.Lock()
progress_lock	= threading.Lock()
progress_state: Dict[str, Dict[str, float | str]] = {}

# ------------------------------ Config / Constants (Defined Directly) ------------- #

# --- Script Behavior ---
de_bug				= False # Set to True for verbose debugging output
PAUSE_ON_EXIT		= True  # Set to True to pause console before exiting

# --- File Handling ---
File_extn			= {".av1",".m4v",".mkv",".mov",".mp4",".ts",".mts"}
TMPF_EX				= ".mp4"                                 # Output container format
SKIP_KEY			= "| <¯\\_(ツ)_/¯> |"                    # Metadata comment to mark processed files

# --- Encoding Policy ---
Default_lng				= "eng"     # Default language for audio/subtitle selection
Keep_langua				= ["eng","fre","ger","heb","hun","ita","jpn","rum","rus","spa"] # Languages to keep (case-insensitive in logic)
HEVC_BPP				= 0.05      # Target Bits Per Pixel for HEVC bitrate calculation
SIZE_OK_MARGIN			= 1.15      # Allow source bitrate up to X * ideal before forcing re-encode
ALWAYS_10BIT			= True      # Aim for 10-bit HEVC output when re-encoding
HW_10BIT_ENCOD			= True      # Allow hardware encoder (QSV) to output 10-bit (if ALWAYS_10BIT is True)
TAG_HEVC_AS_HVC1		= True      # Use 'hvc1' tag for HEVC in MP4 for Apple compatibility

# --- Size Validation Guards ---
AUTO_SIZE_GUARD			= True      # Enable checks to prevent excessive file size growth
INFLATE_MAX_BY			= 35        # %: Reject if output grows by more than this percentage (if AUTO_SIZE_GUARD=True)
MAX_ABS_GROW_MB			= None      # MB: Reject if output grows by more than this absolute size (None=disabled)
FORCE_BIGGER			= False     # If True, bypass "Too Large" guards (useful for specific quality targets)
# Intelligent "Too Small" Check Thresholds (Used in clean_up)
MIN_SIZE_RATIO_FLOOR	= 0.10  # Absolute minimum allowed size ratio (e.g., 10% of original)
MIN_SIZE_RATIO_DEFAULT	= 0.25  # Default min ratio if not targeting drastic reduction (e.g., 25%)
# Duration Validation Thresholds (Used in clean_up)
DURATION_TOLERANCE_ABS	= 1.5   # Seconds
DURATION_TOLERANCE_PCT	= 0.05  # 5%

# --- Parallelism ---
CPU_COUNT          	= os.cpu_count() or 4 # Detect CPU cores
SCAN_PARALLEL			= True      # Use threads for scanning/probing files
MAX_SCAN_WORKRS			= max(1, int(CPU_COUNT * 0.7)) # Max threads for scanning
WORK_PARALLEL			= False     # Use threads for processing files (Set to True for parallel encodes)
MAX_WORKERS				= max(1, int(CPU_COUNT * 0.6)) # Max threads for processing (if WORK_PARALLEL=True)

# --- File Lock Retry Logic (Windows) ---
RENAME_ATTEMPTS         = 6     # How many times to retry file rename/move on lock error
RENAME_INITIAL_DELAY    = 3.0   # Seconds to wait after first lock error
RENAME_BACKOFF_FACTOR   = 1.0   # Multiplier for subsequent delays (1.0 = linear, >1.0 = exponential)
# --- Additional Artifacts ---

ADD_ADDITIONAL          = True # Master flag to enable artifact creation
FORCE_ARTIFACTS_ON_SKIP = True # Create artifacts even if main file is skipped
# --- NEW: Per-artifact toggles ---
ADD_ARTIFACT_MATRIX      = True
ADD_ARTIFACT_SPEED       = True
ADD_ARTIFACT_SHORT       = True
# --- END NEW ---

# Settings for specific artifacts
ADDITIONAL_MATRIX_COLS  = 4
ADDITIONAL_MATRIX_ROWS  = 3
ADDITIONAL_MATRIX_WIDTH = 320   # Width of each thumbnail in pixels
# <<< NEW CONSTANT for absolute start time >>>
ADDITIONAL_MATRIX_START_TIME = 30.0 # Seconds: Time into video to start taking thumbnails
ADDITIONAL_MATRIX_SKIP_PCT_START = 7.0 # Skip % start for matrix
ADDITIONAL_MATRIX_SKIP_PCT_END = 7.0 # Skip % end for matrix# <<< Keep Percentage for end skip >>>
ADDITIONAL_SHORT_DUR    = 25.0  # Duration in seconds for short version
ADDITIONAL_SHORT_SKP_STRT = 5.0 # Skip % start for short clip
ADDITIONAL_SPEED_FACTOR = 2.5   # Speed multiplier for speed_up version
ADDITIONAL_QUALITY_CRF  = 24    # CRF for speed_up video re-encode
ADDITIONAL_PRESET       = "faster" # Preset for speed_up video re-encode
ALLOW_GROWTH_SAME_RES_PCT = 35.0

CHECK_CORRUPTION	= False     # Enable slower, more thorough corruption check during scan
IS_WIN              = platform.system() == "Windows"
CREATE_NEW_PROCESS_GROUP = 0x00000200 if IS_WIN else 0

FFMPEG  = shutil.which("ffmpeg")  or r"C:\Program Files\ffmpeg\bin\ffmpeg.exe"
FFPROBE = shutil.which("ffprobe") or r"C:\Program Files\ffmpeg\bin\ffprobe.exe"
# Fail-fast check moved to Trans_code.py

ERROR_LOGS_ENABLED		= True
ERROR_LOG_MAX_LINES		= 400
_ERRLOG_LAST_NOTICE		= {"printed": False}

PROBE_TIMEOUT_S		= 300
STAGE_TIMEOUT_S		= 12 * 60 * 60
REMUX_TIMEOUT_S		= max(1800, STAGE_TIMEOUT_S // 4)

# Timeout used by the secondary ffprobe corruption check (promote from magic 30s)
CORRUPTION_CHECK_TIMEOUT_S = 30
# XXX --------------------------- Utils ---------------------------------- XXX #
def copy_move(src: str, dst_dir: str, move: bool):
	"""Copy or move file with debug."""
	# XXX: !! USER REQUESTED TO LEAVE THIS DEBUG BLOCK AS-IS !! # XXX:
	de_bug = True
	if de_bug and move :
		delay = 5
		print(f"Debug in {delay} Sec\n File: {src}\n Copied to:{dst_dir}")
		move = False
		time.sleep( delay ) # Pause for 5 seconds
	op = "Moved" if move else "Copied"
	try:
		dst_file = Path(dst_dir) / Path(src).name
		print(f" File:	{src}\n {op} to: {dst_file}")
		if move:
			shutil.move(src, dst_file)
		else:
			shutil.copy2(src, dst_file)
		return True
	except Exception as e:
		print(f" ERROR: Could not {op.lower()} '{src}' to '{dst_dir}': {e}")
		return False


def hm_sz(nbyte: Optional[int | float], unit: str = "B") -> str:
	"""Human-readable size formatting."""
	suffix = ["", "K", "M", "G", "T", "P", "E"]
	if not nbyte:
		return f"0 {unit}"
	sign = "-" if float(nbyte) < 0 else ""
	value = abs(float(nbyte))
	idx = 0
	while value >= 1024.0 and idx < len(suffix) - 1:
		value /= 1024.0
		idx += 1
	return f"{sign}{round(value, 1)} {suffix[idx]}{unit}"

def hm_tm(sec: float) -> str:
	"""Human-readable time formatting."""
	sec = float(sec or 0)
	if sec < 60: return f"{sec:.1f} sec"
	elif sec < 3600: return f"{sec/60:.1f} min"
	else: return f"{sec/3600:.2f} hr"

# XXX --------------------------- Classes -------------------------------- XXX #

class Tee:
	def __init__(self, name: str, mode: str = "a", encoding: str = "utf-8"):
		self.file = open(name, mode, encoding=encoding)
		self.stdout = sys.stdout
		self.stderr = sys.stderr
		sys.stdout = self
		sys.stderr = self # Redirect stderr as well

	def __enter__(self) -> "Tee":
		return self

	def __exit__(self, *_: Any) -> None:
		sys.stdout = self.stdout
		sys.stderr = self.stderr # Restore stderr
		self.file.close()

	def write(self, data: str) -> None:
		self.file.write(data)
		self.stdout.write(data)

	def flush(self) -> None:
		self.file.flush()
		self.stdout.flush()

class Spinner:
	def __init__(self, spin_text: str = r"|/-\o+", delay: float = 0.08):
		self.spin_text = spin_text
		self.delay = delay
		self.last_len = 0
		self.count = 0
		self.last_update = 0.0

	def print_spin(self, extra: str = "") -> None:
		if (time.time() - self.last_update) < self.delay:
			return
		self.last_update = time.time()
		term_width = shutil.get_terminal_size(fallback=(120, 25)).columns

		# --- FIX: Added 'else' block ---
		if len(extra) > term_width - 12:
			left = (term_width - 15) // 2
			extra = f"{extra[:left]}...{extra[-left:]}"
			msg = f"\r| {self.spin_text[self.count % len(self.spin_text)]} | {extra}"
			with print_lock:
				sys.stderr.write(msg + " " * max(self.last_len - len(msg), 0))
				sys.stderr.flush()
			self.last_len = len(msg)
			self.count += 1
		else:
			# Normal path (no truncation): still print the spinner/status line
			msg = f"\r| {self.spin_text[self.count % len(self.spin_text)]} | {extra}"
			with print_lock:
				sys.stderr.write(msg + " " * max(self.last_len - len(msg), 0))
				sys.stderr.flush()
			self.last_len = len(msg)
			self.count += 1
		# --- END FIX ---

	def stop(self) -> None:
		with print_lock:
			sys.stderr.write("\r" + " " * self.last_len + "\r") # Clear final line
			sys.stderr.flush()

def _errlog_target_path(input_file: str) -> Path:
	return WORK_DIR / f"_{Path(input_file).stem}_errors.log"

def errlog_block(input_file: str, header: str, body: str, max_lines: int = ERROR_LOG_MAX_LINES) -> None:
	if not ERROR_LOGS_ENABLED:
		return
	ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
	lines = (body or "").splitlines()
	if max_lines and len(lines) > max_lines:
		lines = ["...[truncated]..."] + lines[-max_lines:]
	text = "\n".join([f"==== {ts} :: {header} ====\n"] + lines + ["\n==== end ====\n", ""])
	try:
		p = _errlog_target_path(input_file)
		p.parent.mkdir(parents=True, exist_ok=True)
		with open(p, "a", encoding="utf-8", errors="replace") as f:
			f.write(text)
		with print_lock:
			if not _ERRLOG_LAST_NOTICE["printed"]:
				print(f"\033[96m[log]\033[0m wrote error details to: {p}")
				_ERRLOG_LAST_NOTICE["printed"] = True
	except Exception:
		with print_lock: print("\033[93m[warn]\033[0m could not write error log to script folder.")


def retry_with_lock_info( action_desc: str, func: callable,	args: tuple,de_bug: bool = False,
	attempts:    int = RENAME_ATTEMPTS,
	base_wait: float = RENAME_INITIAL_DELAY,
	backoff:   float = RENAME_BACKOFF_FACTOR
	) -> None:
	"""
	Retries a function (like rename/move) with exponential backoff.
	Includes logging and debug info about file locks.
	"""
	wait = base_wait
	for i in range(attempts):
		try:
			func(*args)
			if de_bug:
				with print_lock: print(f"DEBUG: Successfully performed action: {action_desc}")
			return  # Success
		except (OSError, PermissionError) as e:
			with print_lock:
				print(f"\n   [WARN] Attempt {i+1}/{attempts} failed for: {action_desc}")
				print(f"   Error: {e}")

			if i < attempts - 1:
				if de_bug:
					print(f"   ...will retry in {wait:.2f} seconds.")
				time.sleep(wait)
				wait *= backoff
			else:
				with print_lock:
					print(f"   [ERROR] All {attempts} attempts failed.")
				raise  # Re-raise the last exception
		except Exception as e:
			with print_lock:
				print(f"\n   [ERROR] Non-retryable error during '{action_desc}': {e}")
			raise # Re-raise unexpected errors
