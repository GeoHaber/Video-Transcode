# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sys
import time
import stat
import atexit
try: import psutil
except ImportError: psutil = None
import random
import shutil
import string
import platform
import threading
import subprocess as sp
import importlib

from typing     import Any, Dict, Optional
from pathlib    import Path
from datetime   import datetime

# ---------------------------------------------------------------------------
# Configuration loader – bridging config.yaml → module-level constants
# ---------------------------------------------------------------------------
try:
    from config_loader import load_config, get_config, Config   # noqa: F401
    _cfg = load_config()
except Exception:
    _cfg = None

# ---------------------------------------------------------------------------
# Local LLM integration  (from sister project Local_LLM)
# ---------------------------------------------------------------------------
LLM_AVAILABLE = False
_llm_engine   = None

def _try_import_local_llm():
    """Attempt to import Local_LLM engine with graceful fallback."""
    global LLM_AVAILABLE
    try:
        from local_llm import FIFOLlamaCppInference, LocalLLMManager  # noqa: F401
        LLM_AVAILABLE = True
    except ImportError:
        LLM_AVAILABLE = False

_try_import_local_llm()


def get_llm_engine():
    """Return a ready Local_LLM inference engine (singleton, lazy init)."""
    global _llm_engine
    if _llm_engine is not None:
        return _llm_engine
    if not LLM_AVAILABLE:
        return None
    try:
        from local_llm import FIFOLlamaCppInference, LocalLLMManager
        mgr = LocalLLMManager()
        models = mgr.list_models()
        if not models:
            return None
        _llm_engine = FIFOLlamaCppInference(models[0].file_path)
        return _llm_engine
    except Exception:
        return None


def llm_query(prompt: str, system: str = "", max_tokens: int = 512) -> Optional[str]:
    """Synchronous convenience wrapper around Local_LLM inference.

    Returns the model reply as a string, or None on failure.
    """
    engine = get_llm_engine()
    if engine is None:
        return None
    try:
        import asyncio
        loop = asyncio.new_event_loop()
        try:
            result = loop.run_until_complete(
                engine.query(prompt, system_prompt=system, max_tokens=max_tokens)
            )
        finally:
            loop.close()
        return result
    except Exception:
        return None


# -----------------------------------------------------------------------------
# Paths & Globals
# -----------------------------------------------------------------------------

def _get_script_dir() -> Path:
	if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
		return Path(sys.executable).resolve().parent
	try:                return Path(__file__).resolve().parent
	except NameError:   return Path.cwd()

SCRIPT_DIR = _get_script_dir()
WORK_DIR: Path = Path(os.environ.get("ONE_TRANS_WORK_DIR") or str(SCRIPT_DIR)).resolve()
WORK_DIR.mkdir(parents=True, exist_ok=True)

RUN_TOKEN = "".join(random.choices(string.ascii_lowercase + string.digits, k=4))
RUN_TMP = WORK_DIR / f"__N_tmp_{RUN_TOKEN}"
RUN_TMP.mkdir(parents=True, exist_ok=True)

def _cleanup_run_tmp() -> None:
	try:
		if RUN_TMP.exists():    shutil.rmtree(RUN_TMP, ignore_errors=True)
	except Exception:           pass

atexit.register(_cleanup_run_tmp)

print_lock      = threading.Lock()
progress_lock   = threading.Lock()
progress_state: Dict[str, Dict[str, float | str]] = {}

# ------------------------------ Config / Constants ------------------------------ #

# --- Script Behavior ---
de_bug                  = False # Set to True for verbose debugging output
PAUSE_ON_EXIT           = True  # Set to True to pause console before exiting

# --- File Handling ---
File_extn               = { ".avi", ".flv", ".av1",".m4v",".mkv",".mov",".mp4",".ts",".mts", ".webm", ".wmv"}
Ignore_fils             = { ".7z", ".mp3", ".pdf", ".epub", ".ini", ".jpg", ".jpeg", ".txt" ,".png", ".rar", ".xml",
							".nfo", ".srt", ".ssa", ".metathumb", ".plexmatch", ".url", ".zip", ".exe", ".msi"}

# --- Local LLM settings ---
SMART_RENAME            = False                        # If True, use LLM to rename files after processing


TMPF_EX                 = ".mp4"                                # Output container format
SKIP_KEY                = "| <¯\\_(ツ)_/¯> |"                   # Metadata comment to mark processed files

# --- Portability Fix: Use cross-platform default for exceptions ---
EXCEPT_DIR: Path = WORK_DIR / "Exceptions"
EXCEPT_DIR.mkdir(parents=True, exist_ok=True)

# --- Encoding Policy ---
Default_lng             = "eng"  # Default language for audio/subtitle selection
Keep_langua             = ["eng","fre","ger","heb","hun","ita","jpn","rum","rus","spa"] # Languages to keep (case-insensitive in logic)
HEVC_BPP                = 0.045     # Target Bits Per Pixel for HEVC bitrate calculation
SIZE_OK_MARGIN          = 1.2       # Allow source bitrate up to X * ideal before forcing re-encode
ALWAYS_10BIT            = True      # Aim for 10-bit HEVC output when re-encoding
HW_10BIT_ENCOD          = True      # Allow hardware encoder (QSV) to output 10-bit (if ALWAYS_10BIT is True)
TAG_HEVC_AS_HVC1        = True      # Use 'hvc1' tag for HEVC in MP4 for Apple compatibility
ENSURE_ENG_SUB          = True      # Force processing if English subtitle is missing
ADD_AI_SUB_LANGS        = []        # List: Languages to generate via AI translation (e.g. ['fra', 'spa'])
ADD_AI_AUD_LANGS        = []        # Reserved for future AI audio translation

# --- Size Validation Guards ---
AUTO_SIZE_GUARD         = True      # Enable checks to prevent excessive file size growth
INFLATE_MAX_BY          = 35        # %: Reject if output grows by more than this percentage (if AUTO_SIZE_GUARD=True)
MAX_ABS_GROW_MB         = None      # MB: Reject if output grows by more than this absolute size (None=disabled)
FORCE_BIGGER            = False     # If True, bypass "Too Large" guards (useful for specific quality targets)

# Intelligent "Too Small" Check Thresholds (Used in clean_up)
MIN_SIZE_RATIO_FLOOR    = 0.05      # Absolute minimum allowed size ratio (e.g., 0.05% of original)

# Duration Validation Thresholds (Used in clean_up)
DURATION_TOLERANCE_ABS  = 1.5       # Seconds
DURATION_TOLERANCE_PCT  = 0.05      # 5%

# --- Parallelism ---
CPU_COUNT               = os.cpu_count() or 4           # Detect CPU cores
SCAN_PARALLEL           = True      # Use threads for scanning/probing files
MAX_SCAN_WORKRS         = max(1, int(CPU_COUNT * 0.7))  # Max threads for scanning
WORK_PARALLEL           = False     # Use threads for processing files (Set to True for parallel encodes)
MAX_WORKERS             = max(1, int(CPU_COUNT * 0.6))  # Max threads for processing (if WORK_PARALLEL=True)

# --- File Lock Retry Logic (Windows) ---
RENAME_ATTEMPTS         = 20        # Increased for better robustness on slow disks/AV scans
RENAME_INITIAL_DELAY    = 2.0       # Seconds to wait after first lock error
RENAME_BACKOFF_FACTOR   = 1.5       # Exponential backoff (1.5x each time)

# --- Additional Artifacts ---
ADD_ADDITIONAL          = False      # Master flag to enable artifact creation
FORCE_ARTIFACTS_ON_SKIP = True      # Create artifacts even if main file is skipped
ADD_ARTIFACT_MATRIX     = True
ADD_ARTIFACT_SPEED      = True
ADD_ARTIFACT_SHORT      = True

# Settings for specific artifacts
ADDITIONAL_MATRIX_COLS  = 4
ADDITIONAL_MATRIX_ROWS  = 3
ADDITIONAL_MATRIX_WIDTH = 320       # Width of each thumbnail in pixels

# <<< NEW CONSTANT for absolute start time >>>
ADDITIONAL_MATRIX_START_TIME        = 30.0      # Seconds: Time into video to start taking thumbnails
ADDITIONAL_MATRIX_SKIP_PCT_START    = 9.0       # Skip % start for matrix
ADDITIONAL_MATRIX_SKIP_PCT_END      = 18.0      # Skip % end for matrix# <<< Keep Percentage for end skip >>>
ADDITIONAL_SHORT_SKP_STRT           = 9.0       # Skip % start for short clip
ADDITIONAL_SHORT_DUR                = 33.0      # Duration in seconds for short version
ADDITIONAL_SPEED_FACTOR             = 2         # Speed multiplier for speed_up version
ADDITIONAL_QUALITY_CRF              = 24        # CRF for speed_up video re-encode
ADDITIONAL_PRESET                   = "fast"    # Preset for speed_up video re-encode
ALLOW_GROWTH_SAME_RES_PCT           = 33.0

CHECK_CORRUPTION            = False  # Enable slower, more thorough corruption check during scan
IS_WIN                      = platform.system() == "Windows"
CREATE_NEW_PROCESS_GROUP    = 0x00000200 if IS_WIN else 0

FFMPEG  = shutil.which("ffmpeg")  or "ffmpeg"
FFPROBE = shutil.which("ffprobe") or "ffprobe"

ERROR_LOGS_ENABLED      = True
ERROR_LOG_MAX_LINES     = 400
_ERRLOG_LAST_NOTICE     = {"printed": False}

PROBE_TIMEOUT_S     = 300
STAGE_TIMEOUT_S     = 12 * 60 * 60
REMUX_TIMEOUT_S     = max(1800, STAGE_TIMEOUT_S // 4)

# Timeout used by the secondary ffprobe corruption check (promote from magic 30s)
CORRUPTION_CHECK_TIMEOUT_S = 30

# XXX --------------------------- Utils ---------------------------------- XXX #
def copy_move(src: str, dst_dir: str, move: bool):
	"""Copy or Move file *** if de_bug = True do not move just copy."""
	# FIX: Use global de_bug rather than hardcoding it to True
	local_debug = globals().get('de_bug', False)
	if local_debug and move :
		delay = 5
		print(f"Debug in {delay} Sec\n File: {src}\n Copied to:{dst_dir}")
		move = False
		time.sleep( delay ) # Pause for 5 seconds
	op = "Moved" if move else "Copied"
	try:
		dst_file = Path(dst_dir) / Path(src).name
		print(f" File:  {src}\n {op} to: {dst_file}")
		if move:    shutil.move(src, dst_file)
		else:       shutil.copy2(src, dst_file)
		return True
	except Exception as e:
		print(f" ERROR: Could not {op.lower()} '{src}' to '{dst_dir}': {e}")
		return False


def hm_sz(nbyte: Optional[int | float], unit: str = "B") -> str:
	"""Human-readable size formatting."""
	suffix = ["", "K", "M", "G", "T", "P", "E"]
	if not nbyte:	return f"0 {unit}"
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
	"""
	A helper to redirect stdout/stderr to both the console AND a log file.
	CRITICAL FIX: Forces flush on carriage returns (\r) so spinners animate.
	"""
	def __init__(self, name: str, mode: str = "a", encoding: str = "utf-8"):
		self.file			= open(name, mode, encoding=encoding)
		self.stdout			= sys.stdout
		self.stderr			= sys.stderr
		sys.stdout			= self
		sys.stderr			= self # Redirect stderr as well
		self._suppress_file	= False

	def __enter__(self) -> "Tee":
		return self

	def __exit__(self, *_: Any) -> None:
		sys.stdout	= self.stdout
		sys.stderr	= self.stderr # Restore stderr
		self.file.close()

	def write(self, data: str) -> None:
		# Console: Always write
		try:
			self.stdout.write(data)
		except Exception:
			pass

		# File: Filter out ephemeral console updates (containing \r)
		# Track state: if we see a \r, suppress writing to file until a \n is seen
		if '\r' in data:
			self._suppress_file = True

		if not self._suppress_file:
			try:
				self.file.write(data)
			except Exception:
				pass

		if '\n' in data:
			self._suppress_file = False

		# [CRITICAL FIX] Flush if data contains newline OR carriage return
		# This allows the "Spinner" to update live in the terminal
		if '\n' in data or '\r' in data:
			self.flush()

	def flush(self) -> None:
		try: self.file.flush()
		except Exception: pass
		try: self.stdout.flush()
		except Exception: pass

class Spinner:
	"""
	A simple text spinner for long-running operations (scanning).
	"""
	def __init__(self, spin_text: str = r"|/-\o+", delay: float = 0.08):
		self.spin_text	= spin_text
		self.delay		= delay
		self.last_len	= 0
		self.count		= 0
		self.last_update	= 0.0

	def print_spin(self, extra: str = "") -> None:
		if (time.time() - self.last_update) < self.delay:	return
		self.last_update	= time.time()

		try: term_width	= shutil.get_terminal_size(fallback=(120, 25)).columns
		except Exception: term_width	= 120

		if len(extra) > term_width - 12:
			left = (term_width - 15) // 2
			extra = f"{extra[:left]}...{extra[-left:]}"

		msg = f"\r| {self.spin_text[self.count % len(self.spin_text)]} | {extra}"

		with print_lock:
			sys.stderr.write(msg + " " * max(self.last_len - len(msg), 0))
			sys.stderr.flush()

		self.last_len	= len(msg)
		self.count		+= 1

	def stop(self) -> None:
		with print_lock:
			sys.stderr.write("\r" + " " * self.last_len + "\r") # Clear final line
			sys.stderr.flush()


# XXX: =========================================================== #
# Thread safe print
def safe_print( msg: str ) -> None:
	with print_lock:    print(msg, flush=True)

def errlog_block(input_file: str, header: str, body: str, max_lines: int = ERROR_LOG_MAX_LINES) -> None:
	"""Appends a structured error block to the log file for the given input file."""
	if not ERROR_LOGS_ENABLED:  return
	try:
		# Define log directory and path
		log_dir = WORK_DIR / "Log_files"
		log_dir.mkdir(parents=True, exist_ok=True)
		log_path = log_dir / f"_{Path(input_file).stem}_err.log"

		# Prepare timestamp and format log block
		ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
		lines = (body or "").splitlines()
		if max_lines and len(lines) > max_lines:
			lines = ["...[truncated]..."] + lines[-max_lines:]
		text = "\n".join([
							f"==== {ts} :: {header} ====\n",
							*lines,
							"\n==== end ====\n",
							""
						])
		# Write to file using pathlib
		with log_path.open("a", encoding="utf-8", errors="replace") as f:
			f.write(text)
		# Notify once per file
		with print_lock:
			if "_ERRLOG_PRINTED_PATHS" not in globals():
				globals()["_ERRLOG_PRINTED_PATHS"] = set()
			if log_path not in _ERRLOG_PRINTED_PATHS:
	#           print(f"\033[96m[log]\033[0m wrote error details to: {log_path}")
				_ERRLOG_PRINTED_PATHS.add(log_path)
	except Exception:
		safe_print("\033[93m[warn]\033[0m could not write error log to Log_files subdirectory.")

def retry_with_lock_info(
	action_desc:	str,
	func:			callable,
	args:			tuple,
	de_bug:			bool = False,
	attempts:		int = RENAME_ATTEMPTS,
	base_wait:		float = RENAME_INITIAL_DELAY,
	backoff:		float = RENAME_BACKOFF_FACTOR
) -> None:
	"""
	Retries a function (like rename/move) with exponential backoff.
	Logs debug info, file status, and fallback copy if all attempts fail.
	"""

	def log_file_status(path: str, label: str = "File") -> None:
		if not os.path.exists(path):
			safe_print(f"   [ERROR] {label} does not exist: {path}")
			return
		if not os.access(path, os.W_OK):
			safe_print(f"   [WARN] {label} is not writable: {path}")
		
		# Try to find who is locking the file (Windows only)
		try:
			if not psutil: return
			locking_procs = []
			target_abs = os.path.abspath(path).lower()
			for proc in psutil.process_iter(['pid', 'name', 'open_files']):
				try:
					for f in (proc.info['open_files'] or []):
						if os.path.abspath(f.path).lower() == target_abs:
							locking_procs.append(f"{proc.info['name']} (PID: {proc.info['pid']})")
				except Exception: pass
			if locking_procs:
				safe_print(f"   [LOCK] {label} currently locked by: {', '.join(locking_procs)}")
		except Exception: pass

		try:
			mode = os.stat(path).st_mode
			if not (mode & stat.S_IWRITE):
				safe_print(f"   [ATTR] {label} is marked as read-only.")
		except Exception as e:
			safe_print(f"   [WARN] Could not read {label} attributes: {e}")

	def try_fallback_copy(src_path: str) -> None:
		try:
			os.makedirs(EXCEPT_DIR, exist_ok=True)
			timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
			base_name = os.path.basename(src_path)
			dest_path = os.path.join(EXCEPT_DIR, f"{base_name}.FAILED_{timestamp}")
			shutil.copy2(src_path, dest_path)
			safe_print(f"   [FALLBACK] Copied original to: {dest_path}")
		except Exception as copy_err:
			safe_print(f"   [ERROR] Fallback copy failed: {copy_err}")

	wait = base_wait
	src_path = args[0] if args else None

	if de_bug:
		safe_print(f"[DEBUG] Starting retry loop for: {action_desc}")
		safe_print(f"[DEBUG] Max attempts: {attempts}, base_wait: {base_wait}, backoff: {backoff}")
		if src_path:        safe_print(f"[DEBUG] Source path: {src_path}")
		if len(args) > 1:   safe_print(f"[DEBUG] Target path: {args[1]}")

	for i in range(attempts):
		try:
			if de_bug:  safe_print(f"[DEBUG] Attempt {i+1}/{attempts} for: {action_desc}")
			func(*args)
			if de_bug:  safe_print(f"[DEBUG] Success on attempt {i+1}: {action_desc}")
			return  # Success

		except (OSError, PermissionError) as e:
			safe_print(f"\n   [WARN] Attempt {i+1}/{attempts} failed for: {action_desc}\n   Error: {e}")
			if i < attempts - 1:
				if de_bug:      safe_print(f"[DEBUG] Retrying in {wait:.2f} seconds...")
				time.sleep(wait)
				wait = min(10.0, wait * backoff) # Cap wait at 10 seconds
			else:
				safe_print(f"   [ERROR] All {attempts} attempts failed.")
				if src_path:
					log_file_status(src_path, label="Source")
					try_fallback_copy(src_path)
				raise  # Re-raise the original error

		except Exception as e:
			safe_print(f"\n   [ERROR] Non-retryable error during '{action_desc}': {e}")
			raise

# -----------------------------------------------------------------------------
# Power Management
# -----------------------------------------------------------------------------
class PowerManagement:
	# ES_CONTINUOUS | ES_SYSTEM_REQUIRED (0x80000000 | 0x00000001)
	# We do NOT use ES_DISPLAY_REQUIRED (0x00000002) so monitor can sleep.
	ES_CONTINUOUS       = 0x80000000
	ES_SYSTEM_REQUIRED  = 0x00000001

	@staticmethod
	def prevent_sleep():
		if sys.platform == 'win32':
			try:
				import ctypes
				ctypes.windll.kernel32.SetThreadExecutionState(
					PowerManagement.ES_CONTINUOUS | PowerManagement.ES_SYSTEM_REQUIRED
				)
				safe_print("• Power Management: Sleep Disabled (Transcode Active)")
			except Exception as e:
				safe_print(f"Warning: Could not set wake lock: {e}")
	@staticmethod
	def allow_sleep():
		if sys.platform == 'win32':
			try:
				import ctypes
				ctypes.windll.kernel32.SetThreadExecutionState(PowerManagement.ES_CONTINUOUS)
				safe_print("• Power Management: Sleep Restored")
			except Exception: # Added explicit Exception for consistency
				pass


def wait_for_enter():
	"""Waits for the user to press Enter."""
	input("Press Enter to continue...")


# -----------------------------------------------------------------------------
# Dependency Management
# -----------------------------------------------------------------------------
def ensure_libs(lib_map: Dict[str, str]) -> None:
	"""
	Ensures that required libraries are installed.
	lib_map: { "import_name": "pip_package_name" }
	"""
	missing = []
	for imp_name, pkg_name in lib_map.items():
		try:
			importlib.import_module(imp_name)
		except ImportError:
			missing.append((imp_name, pkg_name))
	
	if not missing:
		return

	safe_print("\n" + "!" * 80)
	safe_print(" [DEPENDENCY CHECK] Missing required Python libraries!")
	for imp_name, pkg_name in missing:
		safe_print(f"  - {imp_name} (package: {pkg_name})")
	
	if os.environ.get("ANTIGRAVITY_AGENT"):
		safe_print("\n Skipping auto-install: Running in agent environment.")
		return

	safe_print("\n Attempting automatic installation...")
	for imp_name, pkg_name in missing:
		try:
			safe_print(f" Installing {pkg_name}...")
			sp.check_call([sys.executable, "-m", "pip", "install", pkg_name])
			importlib.invalidate_caches()
			importlib.import_module(imp_name)
			safe_print(f" [OK] {pkg_name} installed successfully.")
		except Exception as e:
			safe_print(f" [ERROR] Failed to install {pkg_name}: {e}")
			safe_print(f" Please install it manually: pip install {pkg_name}")
	
	safe_print("!" * 80 + "\n")

# Default required libraries for the toolkit
REQUIRED_LIBS = {
	"psutil": "psutil",
	"charset_normalizer": "charset-normalizer",
}

# Optional libraries (installed on demand or handled via try-except)
GUI_LIBS = {"PySide6": "PySide6"}
AI_LIBS = {
	"torch": "torch",
	"transformers": "transformers",
	"sentencepiece": "sentencepiece"
}

# ---------------------------------------------------------------------------
# Config.yaml  →  module-constant overrides  (if config loaded successfully)
# ---------------------------------------------------------------------------
if _cfg is not None:
    # Encoding
    HEVC_BPP            = _cfg.encoding.bits_per_pixel
    SIZE_OK_MARGIN      = _cfg.encoding.size_ok_margin
    ALWAYS_10BIT        = _cfg.encoding.force_10bit
    TAG_HEVC_AS_HVC1    = _cfg.encoding.tag_hevc_as_hvc1

    # Languages
    Default_lng         = _cfg.languages.default_language
    Keep_langua         = _cfg.languages.keep_languages
    ENSURE_ENG_SUB      = _cfg.languages.ensure_english_subtitle
    ADD_AI_SUB_LANGS    = _cfg.languages.ai_subtitle_languages
    ADD_AI_AUD_LANGS    = _cfg.languages.ai_audio_languages

    # Scanning
    File_extn           = set(_cfg.scanning.file_extensions) if _cfg.scanning.file_extensions else File_extn
    SCAN_PARALLEL       = _cfg.scanning.parallel_scan
    MAX_SCAN_WORKRS     = _cfg.scanning.max_scan_workers

    # Artifacts
    ADD_ADDITIONAL          = _cfg.artifacts.enabled
    FORCE_ARTIFACTS_ON_SKIP = _cfg.artifacts.force_on_skip
    ADDITIONAL_SHORT_DUR    = _cfg.artifacts.short_duration
    ADDITIONAL_SPEED_FACTOR = _cfg.artifacts.speed_factor
    ADDITIONAL_QUALITY_CRF  = _cfg.artifacts.quality_crf

    # Processing
    SMART_RENAME        = _cfg.processing.smart_rename
    CHECK_CORRUPTION    = _cfg.processing.check_corruption
    PAUSE_ON_EXIT       = _cfg.processing.pause_on_exit

    # LLM – use_local_llm already handled via LLM_AVAILABLE + get_llm_engine()
