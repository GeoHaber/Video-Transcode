# -*- coding: utf-8 -*-
from __future__ import annotations

Rev ="""
  Trans_code.py (Production)
	- Orchestrates FFMpeg.py.
	- Relies on FFMpeg.py for live progress visualization.
"""
import os
import sys
import time
import json
import traceback
import threading
import shutil
from typing import Any, Dict, List, Tuple, Collection, Optional
from hashlib import sha1
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import FFMpeg
from Utils import *

Log_File = str(WORK_DIR / f"__{Path(sys.argv[0]).stem}_{time.strftime('%Y_%j_%H-%M-%S')}.log")
ROOT_DIR = r"C:/Users/Geo/Desktop/downloads"

if 'EXCEPT_DIR' not in globals(): EXCEPT_DIR = Path("C:/_temp")
sort_keys_cfg = [("size", True ), ("date", False )]

def scan_folder(root: str, xtnsio: Collection[str], sort_keys_cfg: Collection, use_threads: bool, max_workers: int) -> List[Dict[str, Any]]:
	"""
	Recursively scans the root directory for media files.
	Uses threading to fetch file stats and probe metadata efficiently.
	Implements JSON caching to skip re-probing unchanged files.
	"""
	print(f"Scan: {root}\n Scanning folder Sort: {sort_keys_cfg} Start: {time.strftime('%H:%M:%S')}")
	spinner = Spinner()
	candidates = []
	file_list = []
	TOUCH_DATE = datetime(2000, 1, 1)
	CACHE_FILE = WORK_DIR / "scan_cache.json"
	IGNORE_SCAN_CACHE = os.getenv("IGNORE_SCAN_CACHE", "0") == "1"
	CLEAR_SCAN_CACHE = os.getenv("CLEAR_SCAN_CACHE", "0") == "1"
	cache = {}
	if CLEAR_SCAN_CACHE and CACHE_FILE.exists():
		try: CACHE_FILE.unlink()
		except: pass
	if not IGNORE_SCAN_CACHE and CACHE_FILE.exists():
		try:
			with CACHE_FILE.open("r", encoding="utf-8") as f: cache = json.load(f)
		except: pass

	for dirpath, _, files in os.walk(root):
		for one_file in files:
			ext = Path(one_file).suffix.lower()
			if ext in xtnsio: candidates.append(os.path.join(dirpath, one_file))

	stat_results = {}
	with ThreadPoolExecutor(max_workers=max_workers) as stat_pool:
		stat_futures = {stat_pool.submit(os.stat, f): f for f in candidates}
		for fut in as_completed(stat_futures):
			try: stat_results[stat_futures[fut]] = fut.result()
			except: continue

	cached_results, futures = {}, {}
	with ThreadPoolExecutor(max_workers=max_workers if use_threads else 1) as executor:
		for f_path, stat in stat_results.items():
			f_key = sha1(f"{f_path}|{stat.st_size}|{stat.st_mtime}".encode()).hexdigest()
			if not IGNORE_SCAN_CACHE and f_key in cache:
				entry = cache[f_key]
				cached_results[f_path] = (entry.get("metadata"), entry.get("is_corrupted", False), entry.get("error_msg", None))
			else:
				fut = executor.submit(FFMpeg.ffprobe_run, f_path, FFMpeg.FFPROBE, de_bug, CHECK_CORRUPTION)
				futures[fut] = f_path

	def add_to_list(f_path, metadata, is_corrupted, error_msg, from_cache=False):
		file_stat = stat_results.get(f_path)
		if not file_stat or file_stat.st_size < 10: return
		if error_msg or is_corrupted:
			safe_print(f"\n\033[93m Warning/Error probing '{f_path}': {error_msg or 'Corrupt'}\033[0m")
			return
		try:
			file_mtime = datetime.fromtimestamp(file_stat.st_mtime)
			if file_mtime.year < 1970: file_mtime = TOUCH_DATE
		except: file_mtime = TOUCH_DATE

		if hasattr(metadata, 'duration'):
			meta_dict = metadata.__dict__
			duration = metadata.duration
		else:
			meta_dict = metadata or {}
			duration = float((meta_dict.get("format", {}) or {}).get("duration", 0.0) or 0.0)

		file_list.append({"path": f_path, "metadata": meta_dict, "size": file_stat.st_size, "name": Path(f_path).name, "date": file_mtime, "duration": duration})

		if not from_cache:
			f_key = sha1(f"{f_path}|{file_stat.st_size}|{file_stat.st_mtime}".encode()).hexdigest()
			if hasattr(meta_dict, '__dict__'): meta_dict = meta_dict.__dict__
			cache[f_key] = {"metadata": meta_dict, "is_corrupted": is_corrupted, "error_msg": error_msg}

	for f_path, (meta, corrupt, emsg) in cached_results.items(): add_to_list(f_path, meta, corrupt, emsg, from_cache=True)
	for i, fut in enumerate(as_completed(futures)):
		try: add_to_list(futures[fut], *fut.result(), from_cache=False)
		except: pass
		spinner.print_spin(f"[scan] {100*(i+1)/len(candidates) if candidates else 0:>3.1f}%")
	spinner.stop()

	if not IGNORE_SCAN_CACHE:
		try:
			final_cache = {}
			for k, v in cache.items():
				 m = v.get('metadata', {})
				 if hasattr(m, '__dict__'): m = m.__dict__
				 final_cache[k] = {**v, 'metadata': m}
			with CACHE_FILE.open("w", encoding="utf-8") as f: json.dump(final_cache, f, indent=2, default=str)
		except: pass

	Sort_key = {"size": lambda x: x["size"], "date": lambda x: x["date"], "name": lambda x: x["name"]}
	for key, descending in reversed(sort_keys_cfg):
		if key in Sort_key: file_list.sort(key=Sort_key[key], reverse=descending)
	return file_list

def process_file(file_info, idx, total, task_id):
	"""
	Worker function:
	1. Parses file info (parse_finfo).
	2. Runs FFmpeg (ffmpeg_run).
	3. Cleans up and verifies size (clean_up).
	"""
	str_t = datetime.now()
	saved = procs = skipt = errod = 0
	file_p = file_info["path"]
	if Path(file_p).stem.endswith(".temp"): return 0,0,1,0

	safe_print(f"\n{file_p}\n +Start: [{str_t.strftime('%H:%M:%S')}]  File: {idx} of {total}, {hm_sz(file_info['size'])}")
	try:
		ff_cmd, skip_it, logs = FFMpeg.parse_finfo(file_p, file_info["metadata"], de_bug)
		for line in logs: safe_print(line)

		if skip_it: skipt = 1
		else:
			out_temp = FFMpeg.ffmpeg_run(file_p, ff_cmd, file_info["duration"], skip_it, de_bug, task_id)
			if out_temp:
				res = FFMpeg.clean_up(file_p, out_temp, False, de_bug, task_id)
				if res != -1:
					saved = res; procs = 1
				else: errod = 1
			else: errod = 1
	except Exception as e:
		errod = 1
		safe_print(f"\n[CRITICAL] {e}\n{traceback.format_exc()}")

	safe_print(f" -End: [{datetime.now().strftime('%H:%M:%S')}]\tTotal: {hm_tm((datetime.now()-str_t).total_seconds())}")
	return saved, procs, skipt, errod

def main(argv=None):
	"""
	Main Entry Point:
	1. Checks environment/dependencies.
	2. Scans folders.
	3. Dispatches files to process_file (Sequential or Parallel).
	"""
	print(f"\n+Main Start: [{time.strftime('%H:%M:%S')}]")
	if not shutil.which("ffmpeg"):
		print(f"Error: no ffmpeg not exist.")
		return 1
	if not os.path.exists(ROOT_DIR):
		print(f"\n❌ Error: Directory '{ROOT_DIR}' does not exist.")
		input ("waiting ")
	os.makedirs(EXCEPT_DIR, exist_ok=True)
	for p in RUN_TMP.glob("*"):
		try: p.unlink()
		except: pass

	fl_lst = scan_folder(ROOT_DIR, File_extn, sort_keys_cfg, SCAN_PARALLEL, MAX_SCAN_WORKRS)
	fl_nmb = len(fl_lst)
	saved=procs=skipt=errod=0

	if WORK_PARALLEL and fl_nmb>0 and MAX_WORKERS>=1:
		with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
			futures = {ex.submit(process_file, fi, i+1, fl_nmb, f"T{(i%MAX_WORKERS)+1}"): fi for i,fi in enumerate(fl_lst)}
			for f in as_completed(futures):
				s,p,sk,e = f.result()
				saved+=s; procs+=p; skipt+=sk; errod+=e
				# Summary for Parallel (Thread-safe print)
				lbl = "Lost" if saved < 0 else "Saved"
				safe_print(f"  |To_do: {fl_nmb-(procs+skipt+errod)}|OK: {procs}|Errors: {errod}|Skipt: {skipt}|{lbl}: {hm_sz(saved)} |")
	else:
		for i,each in enumerate(fl_lst):
			s,p,sk,e = process_file(each, i+1, fl_nmb, "T1")
			saved+=s; procs+=p; skipt+=sk; errod+=e
			# Summary for Sequential
			lbl = "Lost" if saved < 0 else "Saved"
			safe_print(f"  |To_do: {fl_nmb-(procs+skipt+errod)}|OK: {procs}|Errors: {errod}|Skipt: {skipt}|{lbl}: {hm_sz(saved)} |")

	print(f"\n-Main Done: [{time.strftime('%H:%M:%S')}] Processed:{procs} Skipped:{skipt} Errors:{errod}")
	if PAUSE_ON_EXIT: input("All Done :)")
	return 0

if __name__ == "__main__":
	with Tee(Log_File):
		print (Rev)
		sys.exit(main())
