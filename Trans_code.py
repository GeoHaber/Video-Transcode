# -*- coding: utf-8 -*-
from __future__ import annotations

Rev = """
  Trans_code.py (Production - Optimized + Reporting)
	- Orchestrates FFMpeg.py with improved performance.
	- Enhanced scanning and caching mechanisms.
	- Optimized threading and resource management.
	- NEW: Simple Concise Resume at exit.
"""
import os
import sys
import time
import json
import shutil
import traceback

from typing import Any, Dict, List, Tuple, Collection, Optional, Set, Iterator, Callable
from hashlib import sha1
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import Utils
from Utils import (
    WORK_DIR, EXCEPT_DIR, SCRIPT_DIR, RUN_TMP,
    File_extn, Ignore_fils, SKIP_KEY,
    SMART_RENAME, PAUSE_ON_EXIT,
    SCAN_PARALLEL, MAX_SCAN_WORKRS, WORK_PARALLEL, MAX_WORKERS,
    ADD_ADDITIONAL, FORCE_ARTIFACTS_ON_SKIP,
    de_bug, print_lock, progress_lock, progress_state,
    REQUIRED_LIBS,
    safe_print, errlog_block, copy_move, hm_sz, hm_tm,
    ensure_libs, wait_for_enter, Tee, Spinner, PowerManagement,
    LLM_AVAILABLE,
)
# Ensure all required libraries are installed
ensure_libs(REQUIRED_LIBS)

try:
	import FFMpeg
	import Subtitle_Manager
except ImportError:
	print("Warning: FFMpeg or Subtitle_Manager module not found. Ensure they are in PYTHONPATH.")

try:
	from tkinter import Tk
	from tkinter.filedialog import askdirectory
	HAS_TKINTER = True
except ImportError:
	HAS_TKINTER = False

# --- Configuration ---
from config_loader import get_config as _get_config

_cfg = _get_config()

DEFAULTS = {
	"Root_Dir": _cfg.scanning.root_directories[0] if _cfg.scanning.root_directories else str(WORK_DIR),
}

Log_File = str(Utils.WORK_DIR / f"__{Path(sys.argv[0]).stem}_{time.strftime('%Y_%j_%H-%M-%S')}.log")

# Multiple directories to scan (loaded from config.yaml)
ROOT_DIRS = list(_cfg.scanning.root_directories) if _cfg.scanning.root_directories else [str(WORK_DIR)]

if 'EXCEPT_DIR' not in globals(): EXCEPT_DIR = Utils.WORK_DIR / "_temp"
BAD_FILES_DIR = Utils.WORK_DIR / "_temp" / "Bad_Files"
sort_keys_cfg = [("size", True), ("date", False)]


def pick_directory(title: str = "Select Directory to Scan", initial_dir: Optional[str] = None) -> Optional[str]:
	"""Open a GUI directory picker dialog."""
	if not HAS_TKINTER:
		return None
	try:
		print("📁 Opening directory picker...")
		# Validate initial_dir - if it doesn't exist, Tkinter might behave unpredictably (e.g. last used)
		if initial_dir and not os.path.exists(initial_dir):
			initial_dir = None
			
		root_tk = Tk()
		root_tk.withdraw()  # Hide main window
		root_tk.attributes('-topmost', True)  # Bring to front
		root_tk.lift()
		root_tk.focus_force()
		selected = askdirectory(parent=root_tk, title=title, initialdir=initial_dir)
		root_tk.destroy()
		return selected if selected else None
	except Exception as e:
		print(f"⚠️  Failed to open GUI directory picker: {e}")
		return None


def handle_bad_file(file_path: str, reason: str, root_dir: Optional[str] = None, move: bool = True) -> None:
	"""Logs error and optionally moves file to Bad_Files within root_dir.

	Args:
		file_path: Path to the problematic file
		reason: Description of the error
		root_dir: Root directory being scanned (Bad_Files created here). Falls back to WORK_DIR.
		move: Whether to move the file or just log
	"""
	try:
		# Create Bad_Files at the DRIVE/MOUNT ROOT level (e.g., D:\_temp\Bad_Files)
		if root_dir:
			root_path = Path(root_dir)
			# Get drive root (C:\, D:\) or UNC root (\\server\share\)
			drive_root = Path(root_path.anchor)
			# For UNC paths, anchor is just \\, so we need the first two parts
			if root_path.anchor == "\\\\":
				parts = root_path.parts
				if len(parts) >= 2:
					drive_root = Path(parts[0]) / parts[1]
			bad_files_dir = drive_root / "_temp" / "Bad_Files"
		else:
			bad_files_dir = Utils.WORK_DIR / "_temp" / "Bad_Files"
		bad_files_dir.mkdir(parents=True, exist_ok=True)

		# 1. Log the error
		log_file = bad_files_dir / "errors.log"
		timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
		with log_file.open("a", encoding="utf-8") as f:
			f.write(f"[{timestamp}] {reason} | File: {file_path}\n")

		# 2. Move the file (if requested and exists)
		if move and os.path.exists(file_path):
			dest_path = bad_files_dir / Path(file_path).name
			# Handle duplicate names by renaming
			if dest_path.exists():
				stem = dest_path.stem
				suffix = dest_path.suffix
				dest_path = bad_files_dir / f"{stem}_{int(time.time())}{suffix}"

			shutil.move(file_path, dest_path)
			safe_print(f"   \033[91m-> Moved to Bad_Files: {dest_path.name}\033[0m")

	except Exception as e:
		safe_print(f"   \033[91m! Failed to handle bad file: {e}\033[0m")


def validate_directories(dirs_to_check: List[str]) -> List[str]:
	"""Validates that directories exist. Prompts user for missing ones."""
	validated = []
	last_picked_dir = None # Track the last successfully picked directory for smart defaults

	for idx, dir_path in enumerate(dirs_to_check):
		if os.path.exists(dir_path):
			validated.append(dir_path)
			print(f"[OK] Directory {idx+1}/{len(dirs_to_check)}: '{dir_path}' exists.")
		else:
			print(f"\n[MISSING] Directory {idx+1}/{len(dirs_to_check)}: '{dir_path}' does NOT exist.")
			remaining = len(dirs_to_check) - idx - 1

			# Build options menu
			options = []
			if remaining > 0:
				options.append("(S)kip to next")
			if HAS_TKINTER:
				options.append("(P)ick another folder")
			options.append("(E)xit")

			print(f"   Options: {' | '.join(options)}")

			while True:
				try:
					choice = input("   Your choice: ").strip().upper()

					if choice == 'S' and remaining > 0:
						print(f"[SKIP] Skipping to next directory...\n")
						break
					elif choice == 'P':
						if HAS_TKINTER:
							# Use a loop to ensure valid selection or explicit cancel
							new_dir = None # Initialize new_dir outside the loop
							
							# Smart Default: Use last picked dir if available, else parent of missing dir
							start_dir = last_picked_dir if last_picked_dir else os.path.dirname(dir_path)
							if start_dir: start_dir = os.path.abspath(start_dir)

							while True:
								new_dir = pick_directory(title=f"Find replacement for '{dir_path}'", initial_dir=start_dir)
								if new_dir:
									new_dir = new_dir.replace('\\', '/') # Ensure consistent separators
									if os.path.exists(new_dir):
										print(f"✅ Selected: '{new_dir}'")
										validated.append(new_dir)
										last_picked_dir = new_dir # Update smart history
										break # Exit inner while loop (valid dir found)
									else:
										print(f"⚠️  Selected directory '{new_dir}' does not exist. Please try again.")
								else:
									print("   [CANCEL] Selection cancelled.")
									# Ask again if they want to retry picking or exit/skip
									retry = input("   Retry picking? (Y/n): ").strip().lower()
									if retry == 'n':
										break # Exit inner while loop (user chose not to retry)
							if new_dir and os.path.exists(new_dir): # Exit the outer choice loop if we found a valid dir
								break
							elif not new_dir: # If new_dir is None (cancelled) or didn't exist, don't break outer loop, let user choose again
								print("⚠️  No valid folder selected. Try again.")
						else:
							print("   [ERROR] GUI not available.")
							# If GUI not available, user can't pick, so let them choose again or exit/skip
							print("⚠️  No valid folder selected. Try again.")
					elif choice == 'E':
						print("🛑 Exiting...")
						sys.exit(0)
					else:
						print("⚠️  Invalid choice. Please try again.")
				except (KeyboardInterrupt, EOFError):
					print("\n🛑 Interrupted by user. Exiting...")
					sys.exit(0)

	return validated


def faster_scandir(dir_path: str, ext_set: Set[str]) -> Iterator[Tuple[str, os.stat_result]]:
	"""Recursively scan directories yielding (path, stat_result) for matching files.
	Uses os.scandir for performance. Skips hidden/temp folders.
	"""
	try:
		with os.scandir(dir_path) as it:
			for entry in it:
				# SKIP hidden directories and _temp folders (loops)
				if entry.is_dir(follow_symlinks=False):
					if entry.name.startswith(".") or entry.name.startswith("_") or "Bad_Files" in entry.name:
						continue
					yield from faster_scandir(entry.path, ext_set)
				elif entry.is_file(follow_symlinks=False):
					if Path(entry.name).suffix.lower() in ext_set:
						try:
							# On Windows, this is cached from the directory listing
							yield entry.path, entry.stat()
						except OSError:
							pass
	except (OSError, PermissionError):
		return


def scan_folder(
	root: str,
	xtnsio: Collection[str],
	sort_keys_cfg: Collection,
	use_threads: bool,
	max_workers: int,
	on_file_found: Optional[Callable[[Dict[str, Any]], None]] = None
) -> Tuple[List[Dict[str, Any]], List[Dict[str, str]]]:
	"""Scans the root directory for media files, caching probe results.

	Returns:
		(valid_file_list, scan_error_list)
	"""
	print(f"Scan: {root}\n Scanning folder Sort: {sort_keys_cfg} Start: {time.strftime('%H:%M:%S')}")
	spinner = Spinner()
	file_list = []
	scan_errors = [] # List to track scan failures
	TOUCH_DATE = datetime(2000, 1, 1)
	CACHE_FILE = Utils.WORK_DIR / "scan_cache.json"
	IGNORE_SCAN_CACHE = os.getenv("IGNORE_SCAN_CACHE", "0") == "1"
	CLEAR_SCAN_CACHE = os.getenv("CLEAR_SCAN_CACHE", "0") == "1"

	# Clear cache if requested
	if CLEAR_SCAN_CACHE and CACHE_FILE.exists():
		try:
			CACHE_FILE.unlink()
		except OSError:
			pass

	# Load existing cache
	cache: Dict[str, Dict[str, Any]] = {}
	if not IGNORE_SCAN_CACHE and CACHE_FILE.exists():
		try:
			with CACHE_FILE.open("r", encoding="utf-8") as f:
				cache = json.load(f)
		except Exception as e:
			print(f"s,?  Warning: Failed to load cache: {e}")
			cache = {}

	# OPTIMIZATION: Use set for faster extension lookup
	xtnsio_set: Set[str] = set(xtnsio) if not isinstance(xtnsio, set) else xtnsio

	# OPTIMIZATION: Single pass scan + stat using os.scandir
	stat_results: Dict[str, os.stat_result] = {}

	try:
		scan_count = 0
		for f_path, f_stat in faster_scandir(root, xtnsio_set):
			stat_results[f_path] = f_stat
			scan_count += 1
			# Update spinner (Time-based for smoothness)
			spinner.print_spin(f"[scan] Discovering files... {scan_count} found")
		spinner.print_spin(f"[scan] Discovering files... {scan_count} found")
		spinner.stop()
		print(f" [OK] Discovered {scan_count} file(s)")
	except Exception as e:
		print(f"Error scanning folder: {e}")

	if not stat_results:
		print("   No media files found.")
		return [], []

	# OPTIMIZATION: Pre-compute cache keys and separate cached/uncached files
	cached_results: Dict[str, Tuple[Any, bool, Optional[str]]] = {}
	futures: Dict[Any, str] = {}

	# Helper to add files to list
	def add_to_list(
		f_path: str,
		metadata: Any,
		is_bad: bool,
		error_msg: Optional[str],
		from_cache: bool = False
	) -> None:
		file_stat = stat_results.get(f_path)
		if not file_stat or file_stat.st_size < 10:
			return

		if is_bad or not metadata:
			err_msg = error_msg or "No metadata found"
			# Only print fatal if not from cache or if we want it noisy
			safe_print(f"\n\033[91m[Fatal Probe] '{f_path}': {err_msg}\033[0m")
			handle_bad_file(f_path, f"Probe Failed: {err_msg}", root_dir=root, move=True)
			scan_errors.append({
				"path": f_path, "error": err_msg,
				"name": Path(f_path).name, "status": "FATAL_SCAN"
			})
			return

		if error_msg:
			safe_print(f"\n\033[93m[Warning Probe] '{f_path}': {error_msg} -> Retrying...\033[0m")

		# Parse file modification time
		try:
			mtime = file_stat.st_mtime
			if mtime < 0: file_mtime = TOUCH_DATE
			else:
				file_mtime = datetime.fromtimestamp(mtime)
				if file_mtime.year < 1970: file_mtime = TOUCH_DATE
		except (OSError, ValueError, OverflowError):
			file_mtime = TOUCH_DATE

		# Extract duration from metadata
		duration = 0.0
		meta_dict = metadata or {}
		try:
			duration = float(meta_dict.get("duration", 0.0) or (meta_dict.get("format", {}) or {}).get("duration", 0.0) or 0.0)
		except (ValueError, TypeError):
			duration = 0.0

		# Add to file list
		file_info = {
			"name":      Path(f_path).name,
			"size":      file_stat.st_size,
			"path":      f_path,
			"metadata":  meta_dict,
			"date":      file_mtime,
			"duration":  duration,
			"root_dir":  root,
			"is_bad":    is_bad,
			"error_msg": error_msg
		}
		file_list.append(file_info)

		# Update cache for new entries
		if not from_cache:
			f_key = sha1(f"{f_path}|{file_stat.st_size}|{file_stat.st_mtime}".encode()).hexdigest()
			cache[f_key] = {"metadata": meta_dict, "is_bad": is_bad, "error_msg": error_msg}

		# CALLBACK: Notify main thread immediately (This starts transcoding)
		if on_file_found:
			on_file_found(file_info)

	# --- PROBE PHASE (With Streaming) ---
	probe_workers = max_workers if use_threads else 1
	print(f" [PROBE] Analyzing {scan_count} file(s)... (Streaming results)")
	spinner = Spinner()

	with ThreadPoolExecutor(max_workers=probe_workers) as executor:
		# 1. Start processing cache hits immediately (Top Priority)
		current_cache_count = 0
		for f_path, stat in stat_results.items():
			f_key = sha1(f"{f_path}|{stat.st_size}|{stat.st_mtime}".encode()).hexdigest()
			if not IGNORE_SCAN_CACHE and f_key in cache:
				entry = cache[f_key]
				add_to_list(f_path, entry.get("metadata"), entry.get("is_bad", False), entry.get("error_msg"), from_cache=True)
				current_cache_count += 1
		
		# 2. Submit remaining for Fresh Probing
		futures: Dict[Any, str] = {}
		for f_path, stat in stat_results.items():
			f_key = sha1(f"{f_path}|{stat.st_size}|{stat.st_mtime}".encode()).hexdigest()
			if IGNORE_SCAN_CACHE or f_key not in cache:
				fut = executor.submit(FFMpeg.probe_worker, f_path, de_bug)
				futures[fut] = f_path

		# 3. Process probe results as they complete
		total_probes = len(futures)
		fresh_start_t = time.time()
		if total_probes > 0:
			for i, fut in enumerate(as_completed(futures)):
				try:
					f_path = futures[fut]
					meta, is_bad, emsg = fut.result()
					add_to_list(f_path, meta, is_bad, emsg, from_cache=False)
				except Exception: pass

				# Update feedback
				fresh_count = i + 1
				elapsed = time.time() - fresh_start_t
				fps = fresh_count / elapsed if elapsed > 0 else 0
				progress_pct = 100 * (current_cache_count + fresh_count) / len(stat_results)
				spinner.print_spin(
					f"Probing: {progress_pct:>5.1f}% ({current_cache_count + fresh_count}/{len(stat_results)}) "
					f"| {fps:.1f} probes/s | Fresh: {fresh_count} | Cached: {current_cache_count}"
				)

	spinner.stop()
	print(f" [OK] Analysis complete. Sorting files...")

	# OPTIMIZATION: Save cache with better error handling
	if not IGNORE_SCAN_CACHE and cache:
		try:
			# Ensure all metadata is serializable
			final_cache = {}
			for k, v in cache.items():
				m = v.get('metadata', {})
				if hasattr(m, '__dict__'):
					m = m.__dict__
				final_cache[k] = {**v, 'metadata': m}

			# Write cache atomically
			temp_cache = CACHE_FILE.with_suffix('.tmp')
			with temp_cache.open("w", encoding="utf-8") as f:
				json.dump(final_cache, f, indent=2, default=str)
			temp_cache.replace(CACHE_FILE)
		except Exception as e:
			print(f"s,?  Warning: Failed to save cache: {e}")

	# OPTIMIZATION: Use single-pass sorting with stable sort
	Sort_key = {
		"size": lambda x: x["size"],
		"date": lambda x: x["date"],
		"name": lambda x: x["name"]
	}

	for key, descending in reversed(sort_keys_cfg):
		if key in Sort_key:
			file_list.sort(key=Sort_key[key], reverse=descending)

	return file_list, scan_errors


def process_file(
	file_info: Dict[str, Any],
	idx: int,
	total: int,
	task_id: str
) -> Dict[str, Any]:
	"""Orchestrates the transcoding process for a single file.

	Returns: dict containing stats and details for the final report.
	"""
	str_t = datetime.now()
	saved = procs = skipt = errod = 0
	file_p = file_info["path"]
	file_name = file_info["name"]
	root_dir = file_info.get("root_dir")

	result = {
		"file": file_name,
		"path": file_p,
		"status": "UNKNOWN",
		"reason": "",
		"error_msg": None,
		"saved": 0, "procs": 0, "skipt": 0, "errod": 0
	}

	# Skip temp files
	if Path(file_p).stem.endswith(".temp"):
		result.update({"status": "SKIPPED", "reason": "Is Temp File", "skipt": 1})
		return result

	safe_print(f"\n{file_p}\n +Start: [{str_t.strftime('%H:%M:%S')}]  File: {idx} of {total}, {hm_sz(file_info['size'])}")

	try:
		# Reconstruct MediaFile object from dictionary info
		mf = FFMpeg.MediaFile.from_dict(file_info)

		# Helper to run artifacts
		def run_artifacts(is_skip: bool, target_path: Path):
			info = {
				"dur_out": getattr(mf, 'duration', 0.0),
				"w_enc": getattr(mf, 'width', 0),
				"h_enc": getattr(mf, 'height', 0),
				"ach_out": len([s for s in getattr(mf, 'streams', []) if s.get("codec_type") == "audio"])
			}
			FFMpeg.post_encode_artifacts(
				target_path,
				info,
				task_id,
				main_was_skipped=is_skip
			)

		# Helper to run subtitle management
		def run_subtitle_mgmt(target_path: Path):
			if not (Utils.ENSURE_ENG_SUB or Utils.ADD_AI_SUB_LANGS):
				return
			
			from types import SimpleNamespace
			sm_args = SimpleNamespace(
				path       = str(target_path),
				clean      = True, # Always clean duplicates when running through Trans_code
				ensure_eng = Utils.ENSURE_ENG_SUB,
				add_langs  = Utils.ADD_AI_SUB_LANGS,
				llm        = True, # Assume LLM use if AI langs are requested
				model      = "",   # Uses Local_LLM engine (no model name needed)
				dry_run    = False
			)
			try:
				safe_print(f"   [{task_id}] Triggering Subtitle Manager for: {target_path.name}")
				Subtitle_Manager.process_path(str(target_path), sm_args)
			except Exception as sme:
				safe_print(f"   [{task_id}] ERROR in Subtitle Manager: {sme}")

		# PLAN
		ff_cmd, skip_it, logs = mf.plan(de_bug)
		
		if not skip_it:
			for line in logs: safe_print(line)
		else:
			# For skipped files, only print the final summary skip message
			skip_msg = next((l for l in reversed(logs) if ".Skip:" in l), None)
			if skip_msg: safe_print(skip_msg)

		# Capture reasons for reporting
		reason_str = ", ".join(getattr(mf, 'reasons', [])) or "Compliant"

		if skip_it:
			skipt = 1
			# Attempt artifacts even if skipped (honors FORCE_ARTIFACTS_ON_SKIP)
			run_artifacts(True, Path(file_p))

			# Even if skipped, we might want to run subtitle management if it was processed by an older version
			if FORCE_ARTIFACTS_ON_SKIP:
				run_subtitle_mgmt(Path(file_p))

			result.update({"status": "SKIPPED", "reason": reason_str or "No changes needed", "skipt": 1})
		else:
			# RUN
			out_temp = mf.run(task_id, de_bug)

			if out_temp:
				# CLEANUP
				res = mf.cleanup(out_temp, False, de_bug, task_id)
				if res != -1:
					saved = res
					procs = 1
					dest = Path(file_p).with_suffix(".mp4")

					# Generate artifacts for the newly processed file
					run_artifacts(False, dest)

					# Run Subtitle Management
					run_subtitle_mgmt(dest)

					# --- SMART RENAME ---
					if SMART_RENAME:
						current_path = dest # From cleanup's move result
						suggested = FFMpeg.suggest_clean_name_with_llm(current_path.stem)
						if suggested and suggested != current_path.stem:
							new_path = current_path.with_name(f"{suggested}{current_path.suffix}")
							if not new_path.exists():
								try:
									shutil.move(str(current_path), str(new_path))
									safe_print(f"   [LLM] Renamed: {current_path.name} -> {new_path.name}")
									result["file"] = new_path.name # Update result for summary
									# Note: We don't update file_p because it's technically finished
								except Exception as re:
									safe_print(f"   [LLM-ERROR] Rename failed: {re}")

					result.update({"status": "PROCESSED", "reason": reason_str, "saved": saved, "procs": 1})
				else:
					errod = 1
					err_msg = "Cleanup Failed (Size check or Move failed)"
					result.update({"status": "ERROR", "reason": reason_str, "error_msg": err_msg, "errod": 1})
			else:
				errod = 1
				err_msg = "Transcode Failed (No Output)"
				handle_bad_file(file_p, err_msg, root_dir=root_dir, move=True)
				result.update({"status": "FATAL", "reason": reason_str, "error_msg": err_msg, "errod": 1})

	except Exception as e:
		errod = 1
		err_msg = f"Crash: {e}"
		safe_print(f"\n[CRITICAL] {e}\n{traceback.format_exc()}")
		handle_bad_file(file_p, err_msg, root_dir=root_dir, move=True)
		result.update({"status": "FATAL", "error_msg": err_msg, "errod": 1})

	# If it was a generic error falling through
	if errod == 1 and result["status"] == "UNKNOWN":
		# Treat as fatal if not skipt
		if not skipt:
			err_msg = "Generic Error"
			handle_bad_file(file_p, err_msg, root_dir=root_dir, move=True)
			result.update({"status": "FATAL", "error_msg": err_msg, "errod": 1})

	safe_print(f" -End: [{datetime.now().strftime('%H:%M:%S')}]\tTotal: {hm_tm((datetime.now()-str_t).total_seconds())}")
	return result


def print_final_summary(results: List[Dict[str, Any]], scan_errors: List[Dict[str, Any]], total_scanned: int):
	"""Prints a simple, concise resume of the batch job."""

	processed = [r for r in results if r["procs"] > 0]
	skipped   = [r for r in results if r["skipt"] > 0]
	errors    = [r for r in results if r["errod"] > 0] # Non-fatal process errors
	fatal     = [r for r in results if r["status"] == "FATAL"]

	# Scan errors are effectively fatal
	all_fatal = fatal + scan_errors
	total_saved = sum(r["saved"] for r in results)

	print("\n" + "="*80)
	print(f" FINAL SESSION RESUME")
	print("="*80)

	print(f" [SCANNED] Total Files: {total_scanned} | Valid: {len(results)} | Rejected: {len(scan_errors)}")

	if processed:
		print(f"\n [PROCESSED] ({len(processed)} files)")
		for p in processed:
			print(f"   * {p['file']}")
			print(f"     Reason: {p['reason']}")
			print(f"     Status: Saved {hm_sz(p['saved'])}")

	if all_fatal or errors:
		print(f"\n [ERRORS] ({len(all_fatal) + len(errors)} files)")
		# List Scan Errors
		for e in scan_errors:
			print(f"   * {e['name']} (FATAL - Scan)")
			print(f"     Error:  {e['error']}")
			print(f"     Action: Moved to Bad_Files")
		# List Process Fatal Errors
		for f in fatal:
			print(f"   * {f['file']} (FATAL - Process)")
			print(f"     Error:  {f.get('error_msg', 'Unknown')}")
			print(f"     Action: Moved to Bad_Files")
		# List Minor Errors
		for e in errors:
			if e["status"] != "FATAL":
				print(f"   * {e['file']} (MINOR)")
				print(f"     Error:  {e.get('error_msg', 'Check logs')}")
				print(f"     Action: Flagged as Error")

	print(f"\n [STATS]")
	print(f"   Processed: {len(processed)}")
	print(f"   Skipped:   {len(skipped)}")
	print(f"   Errors:    {len(all_fatal) + len(errors)} (Fatal: {len(all_fatal)}, Minor: {len(errors)})")
	print(f"   Storage:   Saved {hm_sz(total_saved)}")
	print("-" * 80 + "\n")


def main(argv=None) -> int:
	"""Main entry point for the transcoding batch job."""
	print(f"\n+Main Start: [{time.strftime('%H:%M:%S')}]")

	if not shutil.which("ffmpeg"):
		print(f"Error: ffmpeg not found.")
		return 1

	# Choose target directories
	to_validate = [sys.argv[1]] if len(sys.argv) > 1 else ROOT_DIRS
	
	# Validate all directories
	print(f"\n[ACTION] Validating {len(to_validate)} director{'y' if len(to_validate)==1 else 'ies'}...")
	valid_dirs = validate_directories(to_validate)
	
	# Deduplicate directories (in case user selected the same replacement for multiple bad paths)
	# Also normalize paths
	valid_dirs = sorted(list(set(os.path.normpath(d).replace('\\', '/') for d in valid_dirs)))

	if not valid_dirs:
		print(f"\n[WARNING] No valid directories found.")
		if HAS_TKINTER:
			new_dir = pick_directory(title="Select Scan Directory")
			if new_dir:
				valid_dirs = [new_dir]
			else:
				print("[Info] No folder selected via GUI. Using current directory.")
				valid_dirs = ["."]
		else:
			print("[Info] GUI not available. Using current directory.")
			valid_dirs = ["."]

	print(f"\n[OK] Processing {len(valid_dirs)} valid director{'y' if len(valid_dirs)==1 else 'ies'}\n")

	# Ensure directories exist
	os.makedirs(EXCEPT_DIR, exist_ok=True)

	# Clean up temp files
	for p in Utils.RUN_TMP.glob("*"):
		try:
			p.unlink()
		except OSError:
			pass

	# --- CONCURRENT ORCHESTRATION ---
	
	import threading
	
	# Shared State
	file_queue: List[Dict[str, Any]] = []
	queue_lock = threading.Lock()
	scan_active = True
	scan_errors_global: List[Dict[str, Any]] = []
	
	# Results
	final_results: List[Dict[str, Any]] = []
	
	# Stats for live summary
	stats = {"saved": 0, "procs": 0, "skipt": 0, "errod": 0}

	def scan_worker(target_dirs: List[str]):
		nonlocal scan_active
		for d_idx, r_dir in enumerate(target_dirs, 1):
			safe_print(f"\n{'='*80}")
			safe_print(f"================================================================================\n[PROCESS] Directory {d_idx}/{len(target_dirs)}: {r_dir}\n================================================================================")
			safe_print(f"{'='*80}")
			
			def on_found(f_info):
				with queue_lock:
					file_queue.append(f_info)
			
			_, s_err = scan_folder(
				r_dir, 
				Utils.File_extn, 
				sort_keys_cfg, 
				Utils.SCAN_PARALLEL, 
				Utils.MAX_SCAN_WORKRS,
				on_file_found=on_found
			)
			with queue_lock:
				scan_errors_global.extend(s_err)
		
		scan_active = False

	# Start Scanner Thread
	scanner_thread = threading.Thread(target=scan_worker, args=(valid_dirs,), daemon=True)
	scanner_thread.start()

	# Main Processor Loop
	processed_count = 0
	has_sorted_after_scan = False
	
	while True:
		# Check if we should exit
		with queue_lock:
			is_empty = len(file_queue) == 0
			is_scan_done = not scan_active
		
		if is_empty and is_scan_done:
			break
		
		# Sort Logic: "When scan and probe are done then do sorting"
		if is_scan_done and not has_sorted_after_scan:
			safe_print("\n [ORCHESTRATOR] Scan complete. Sorting remaining files by Size (Biggest First)...")
			with queue_lock:
				# Sort by size descending
				file_queue.sort(key=lambda x: x["size"], reverse=True)
			has_sorted_after_scan = True
		
		# Buffer Logic: Wait for a few files before starting, to give breadth-first scan a chance?
		# User said: "after a few files were probed start the reencode"
		# We'll just define "a few" as > 0. The simple fact that we are polling achieves this.
		# But to avoid constant locking on empty queue, we wait a bit.
		
		file_to_process = None
		with queue_lock:
			if len(file_queue) > 0:
				# If we haven't sorted yet (scan still active), we just take the first available (usually FIFO)
				# If we HAVE sorted (scan done), we take from top (Biggest)
				file_to_process = file_queue.pop(0)

		if not file_to_process:
			# Queue empty but scan still running
			time.sleep(0.5)
			continue
			
		# [FIX] Check if file still exists (it might have been processed/replaced in a previous pass)
		if not os.path.exists(file_to_process["path"]):
			# Silent skip - likely a duplicate or replaced file
			continue

		# PROCESS THE FILE
		processed_count += 1
		
		# For total count, we only know the "current known total". 
		with queue_lock:
			current_total = processed_count + len(file_queue)
			if scan_active: cur_tot_str = f"{current_total}+"
			else: cur_tot_str = str(current_total)

		# Run processing (We do this sequentially in the main thread)
		res = process_file(file_to_process, processed_count, cur_tot_str, "T1")
		final_results.append(res)
		
		stats["saved"] += res["saved"]
		stats["procs"] += res["procs"]
		stats["skipt"] += res["skipt"]
		stats["errod"] += res["errod"]

		# Summary
		lbl = "Lost" if stats["saved"] < 0 else "Saved"
		with queue_lock: 
			remaining = len(file_queue)
			
		safe_print(f"  |Queue: {remaining}|OK: {stats['procs']}|Errors: {stats['errod']}|Skipt: {stats['skipt']}|{lbl}: {hm_sz(stats['saved'])} |")


	# Ensure scanner thread joined (should be done if loop broke)
	scanner_thread.join()

	print(f"\n-Main Done: [{time.strftime('%H:%M:%S')}] Processed:{stats['procs']} Skipped:{stats['skipt']} Errors:{stats['errod']}")

	# --- NEW: Print Final Resume ---
	total_scanned_final = len(final_results) + len(scan_errors_global)
	print_final_summary(final_results, scan_errors_global, total_scanned_final)

	if Utils.PAUSE_ON_EXIT:
		input("All Done :)")
	return 0


if __name__ == "__main__":
	# Define Log File with Timestamp
	Log_File = str(Utils.WORK_DIR / f"Trans_code_{time.strftime('%Y-%m-%d_%H-%M-%S')}.log")

	# Wrap execution in Tee for logging
	with Tee(Log_File):
		print(Rev)
		sys.exit(main())
