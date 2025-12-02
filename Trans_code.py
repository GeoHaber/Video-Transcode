# -*- coding: utf-8 -*-
from __future__ import annotations

Rev = """
  Trans_code.py (Production - Optimized)
	- Orchestrates FFMpeg.py with improved performance.
	- Enhanced scanning and caching mechanisms.
	- Optimized threading and resource management.
"""
import os
import sys
import time
import json
import shutil
import traceback

from typing import Any, Dict, List, Tuple, Collection, Optional, Set
from hashlib import sha1
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
	from tkinter import Tk
	from tkinter.filedialog import askdirectory
	HAS_TKINTER = True
except ImportError:
	HAS_TKINTER = False

import FFMpeg
from Utils import *

Log_File = str(WORK_DIR / f"__{Path(sys.argv[0]).stem}_{time.strftime('%Y_%j_%H-%M-%S')}.log")

# Multiple directories to scan (add more as needed)
ROOT_DIRS = [
	r"D:\_Fix_them",
#	r"C:\_txa_tmp",
#	r"D:\Media\Videos",  # Add more folders here
]

if 'EXCEPT_DIR' not in globals(): EXCEPT_DIR = Path("C:/_temp")
sort_keys_cfg = [("size", True), ("date", False)]


def validate_directories(dirs_to_check: List[str]) -> List[str]:
	"""Validates that directories exist. Prompts user for missing ones."""
	validated = []
	for idx, dir_path in enumerate(dirs_to_check):
		if os.path.exists(dir_path):
			validated.append(dir_path)
			print(f"✅ Directory {idx+1}/{len(dirs_to_check)}: '{dir_path}' exists.")
		else:
			print(f"\n❌ Directory {idx+1}/{len(dirs_to_check)}: '{dir_path}' does NOT exist.")
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
						print(f"⏭️  Skipping to next directory...\n")
						break
					elif choice == 'P' and HAS_TKINTER:
						print("📁 Opening file picker...")
						root_tk = Tk()
						root_tk.withdraw()  # Hide main window
						root_tk.attributes('-topmost', True)  # Bring to front
						new_dir = askdirectory(title=f"Select replacement for: {dir_path}")
						root_tk.destroy()
						
						if new_dir and os.path.exists(new_dir):
							validated.append(new_dir)
							print(f"✅ Selected: '{new_dir}'\n")
							break
						else:
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


def scan_folder(
	root: str, 
	xtnsio: Collection[str], 
	sort_keys_cfg: Collection, 
	use_threads: bool, 
	max_workers: int
) -> List[Dict[str, Any]]:
	"""Scans the root directory for media files, caching probe results.
	
	Optimizations:
	- Batch file system operations
	- Smart cache invalidation
	- Reduced redundant calls
	- Progress tracking improvements
	"""
	print(f"Scan: {root}\n Scanning folder Sort: {sort_keys_cfg} Start: {time.strftime('%H:%M:%S')}")
	spinner = Spinner()
	file_list = []
	TOUCH_DATE = datetime(2000, 1, 1)
	CACHE_FILE = WORK_DIR / "scan_cache.json"
	IGNORE_SCAN_CACHE = os.getenv("IGNORE_SCAN_CACHE", "0") == "1"
	CLEAR_SCAN_CACHE = os.getenv("CLEAR_SCAN_CACHE", "0") == "1"
	
	# Clear cache if requested
	if CLEAR_SCAN_CACHE and CACHE_FILE.exists():
		try: 
			CACHE_FILE.unlink()
		except: 
			pass
	
	# Load existing cache
	cache: Dict[str, Dict[str, Any]] = {}
	if not IGNORE_SCAN_CACHE and CACHE_FILE.exists():
		try:
			with CACHE_FILE.open("r", encoding="utf-8") as f:
				cache = json.load(f)
		except Exception as e:
			print(f"⚠️  Warning: Failed to load cache: {e}")
			cache = {}

	# OPTIMIZATION: Use set for faster extension lookup
	xtnsio_set: Set[str] = set(xtnsio) if not isinstance(xtnsio, set) else xtnsio
	
	# OPTIMIZATION: Batch collect all candidates first
	candidates: List[str] = []
	for dirpath, _, files in os.walk(root):
		# OPTIMIZATION: Filter in batch using list comprehension
		candidates.extend(
			os.path.join(dirpath, one_file)
			for one_file in files
			if Path(one_file).suffix.lower() in xtnsio_set
		)

	if not candidates:
		spinner.stop()
		print("   No media files found.")
		return []

	# OPTIMIZATION: Batch stat operations with better error handling
	stat_results: Dict[str, os.stat_result] = {}
	with ThreadPoolExecutor(max_workers=max_workers) as stat_pool:
		stat_futures = {stat_pool.submit(os.stat, f): f for f in candidates}
		for fut in as_completed(stat_futures):
			try:
				stat_results[stat_futures[fut]] = fut.result()
			except Exception:
				# Skip files we can't stat (permissions, deleted, etc.)
				continue

	# OPTIMIZATION: Pre-compute cache keys and separate cached/uncached files
	cached_results: Dict[str, Tuple[Any, bool, Optional[str]]] = {}
	futures: Dict[Any, str] = {}
	
	with ThreadPoolExecutor(max_workers=max_workers if use_threads else 1) as executor:
		for f_path, stat in stat_results.items():
			# Generate cache key
			f_key = sha1(f"{f_path}|{stat.st_size}|{stat.st_mtime}".encode()).hexdigest()
			
			# Check cache
			if not IGNORE_SCAN_CACHE and f_key in cache:
				entry = cache[f_key]
				cached_results[f_path] = (
					entry.get("metadata"),
					entry.get("is_corrupted", False),
					entry.get("error_msg", None)
				)
			else:
				# Submit probe task
				fut = executor.submit(
					FFMpeg.ffprobe_run, 
					f_path, 
					FFMpeg.FFPROBE, 
					de_bug, 
					CHECK_CORRUPTION
				)
				futures[fut] = f_path

	# Helper to add files to list
	def add_to_list(
		f_path: str, 
		metadata: Any, 
		is_corrupted: bool, 
		error_msg: Optional[str], 
		from_cache: bool = False
	) -> None:
		file_stat = stat_results.get(f_path)
		if not file_stat or file_stat.st_size < 10:
			return
		
		if error_msg or is_corrupted:
			safe_print(f"\n\033[93m Warning/Error probing '{f_path}': {error_msg or 'Corrupt'}\033[0m")
			return
		
		# Parse file modification time
		try:
			file_mtime = datetime.fromtimestamp(file_stat.st_mtime)
			if file_mtime.year < 1970:
				file_mtime = TOUCH_DATE
		except:
			file_mtime = TOUCH_DATE

		# Extract duration from metadata
		if hasattr(metadata, 'duration'):
			meta_dict = metadata.__dict__
			duration = metadata.duration
		else:
			meta_dict = metadata or {}
			duration = float((meta_dict.get("format", {}) or {}).get("duration", 0.0) or 0.0)

		# Add to file list
		file_list.append({
			"path": f_path,
			"metadata": meta_dict,
			"size": file_stat.st_size,
			"name": Path(f_path).name,
			"date": file_mtime,
			"duration": duration
		})

		# Update cache for new entries
		if not from_cache:
			f_key = sha1(f"{f_path}|{file_stat.st_size}|{file_stat.st_mtime}".encode()).hexdigest()
			# Ensure meta_dict is serializable
			if hasattr(meta_dict, '__dict__'):
				meta_dict = meta_dict.__dict__
			cache[f_key] = {
				"metadata": meta_dict,
				"is_corrupted": is_corrupted,
				"error_msg": error_msg
			}

	# Process cached results first (instant)
	for f_path, (meta, corrupt, emsg) in cached_results.items():
		add_to_list(f_path, meta, corrupt, emsg, from_cache=True)

	# Process probe futures with progress tracking
	total_probes = len(futures)
	for i, fut in enumerate(as_completed(futures)):
		try:
			add_to_list(futures[fut], *fut.result(), from_cache=False)
		except Exception:
			pass
		
		# OPTIMIZATION: Update progress less frequently for better performance
		if i % 5 == 0 or i == total_probes - 1:
			progress_pct = 100 * (i + 1) / total_probes if total_probes else 100
			spinner.print_spin(f"[scan] {progress_pct:>3.1f}% ({len(cached_results) + i + 1}/{len(candidates)})")
	
	spinner.stop()

	# OPTIMIZATION: Save cache with better error handling
	if not IGNORE_SCAN_CACHE:
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
			print(f"⚠️  Warning: Failed to save cache: {e}")

	# OPTIMIZATION: Use single-pass sorting with stable sort
	Sort_key = {
		"size": lambda x: x["size"],
		"date": lambda x: x["date"],
		"name": lambda x: x["name"]
	}
	
	for key, descending in reversed(sort_keys_cfg):
		if key in Sort_key:
			file_list.sort(key=Sort_key[key], reverse=descending)
	
	return file_list


def process_file(file_info: Dict[str, Any], idx: int, total: int, task_id: str) -> Tuple[int, int, int, int]:
	"""Orchestrates the transcoding process for a single file.
	
	Returns: (saved_bytes, processed_count, skipped_count, error_count)
	"""
	str_t = datetime.now()
	saved = procs = skipt = errod = 0
	file_p = file_info["path"]
	
	# Skip temp files
	if Path(file_p).stem.endswith(".temp"):
		return 0, 0, 1, 0

	safe_print(f"\n{file_p}\n +Start: [{str_t.strftime('%H:%M:%S')}]  File: {idx} of {total}, {hm_sz(file_info['size'])}")
	
	try:
		ff_cmd, skip_it, logs = FFMpeg.parse_finfo(file_p, file_info["metadata"], de_bug)
		for line in logs:
			safe_print(line)

		if skip_it:
			skipt = 1
		else:
			out_temp = FFMpeg.ffmpeg_run(file_p, ff_cmd, file_info["duration"], skip_it, de_bug, task_id)
			if out_temp:
				res = FFMpeg.clean_up(file_p, out_temp, False, de_bug, task_id)
				if res != -1:
					saved = res
					procs = 1
				else:
					errod = 1
			else:
				errod = 1
	except Exception as e:
		errod = 1
		safe_print(f"\n[CRITICAL] {e}\n{traceback.format_exc()}")
	
	safe_print(f" -End: [{datetime.now().strftime('%H:%M:%S')}]\tTotal: {hm_tm((datetime.now()-str_t).total_seconds())}")
	return saved, procs, skipt, errod


def main(argv=None) -> int:
	"""Main entry point for the transcoding batch job."""
	print(f"\n+Main Start: [{time.strftime('%H:%M:%S')}]")
	
	if not shutil.which("ffmpeg"):
		print(f"Error: ffmpeg not found.")
		return 1
	
	# Validate all directories
	print(f"\n📂 Validating {len(ROOT_DIRS)} director{'y' if len(ROOT_DIRS)==1 else 'ies'}...")
	valid_dirs = validate_directories(ROOT_DIRS)
	
	if not valid_dirs:
		print(f"\n❌ No valid directories to process. Exiting.")
		return 1
	
	print(f"\n✅ Processing {len(valid_dirs)} valid director{'y' if len(valid_dirs)==1 else 'ies'}\n")

	# Ensure directories exist
	os.makedirs(EXCEPT_DIR, exist_ok=True)
	
	# Clean up temp files
	for p in RUN_TMP.glob("*"):
		try:
			p.unlink()
		except:
			pass

	# Process each validated directory
	all_files: List[Dict[str, Any]] = []
	for dir_idx, root_dir in enumerate(valid_dirs, 1):
		print(f"\n{'='*80}")
		print(f"📁 Directory {dir_idx}/{len(valid_dirs)}: {root_dir}")
		print(f"{'='*80}")
		
		fl_lst = scan_folder(root_dir, File_extn, sort_keys_cfg, SCAN_PARALLEL, MAX_SCAN_WORKRS)
		all_files.extend(fl_lst)
		print(f"   Found {len(fl_lst)} file(s) in this directory.")
	
	fl_nmb = len(all_files)
	print(f"\n📊 Total files to process across all directories: {fl_nmb}\n")
	saved = procs = skipt = errod = 0

	# Process files (parallel or sequential)
	if WORK_PARALLEL and fl_nmb > 0 and MAX_WORKERS >= 1:
		with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
			futures = {
				ex.submit(process_file, fi, i+1, fl_nmb, f"T{(i%MAX_WORKERS)+1}"): fi 
				for i, fi in enumerate(all_files)
			}
			for f in as_completed(futures):
				s, p, sk, e = f.result()
				saved += s
				procs += p
				skipt += sk
				errod += e
				
				# Summary for Parallel (Thread-safe print)
				lbl = "Lost" if saved < 0 else "Saved"
				safe_print(f"  |To_do: {fl_nmb-(procs+skipt+errod)}|OK: {procs}|Errors: {errod}|Skipt: {skipt}|{lbl}: {hm_sz(saved)} |")
	else:
		for i, each in enumerate(all_files):
			s, p, sk, e = process_file(each, i+1, fl_nmb, "T1")
			saved += s
			procs += p
			skipt += sk
			errod += e
			
			# Summary for Sequential
			lbl = "Lost" if saved < 0 else "Saved"
			safe_print(f"  |To_do: {fl_nmb-(procs+skipt+errod)}|OK: {procs}|Errors: {errod}|Skipt: {skipt}|{lbl}: {hm_sz(saved)} |")

	print(f"\n-Main Done: [{time.strftime('%H:%M:%S')}] Processed:{procs} Skipped:{skipt} Errors:{errod}")
	if PAUSE_ON_EXIT:
		input("All Done :)")
	return 0


if __name__ == "__main__":
	# Define Log File with Timestamp
	Log_File = str(WORK_DIR / f"Trans_code_{time.strftime('%Y-%m-%d_%H-%M-%S')}.log")
	
	# Wrap execution in Tee for logging
	with Tee(Log_File):
		print(Rev)
		sys.exit(main())
