# -*- coding: utf-8 -*-
from __future__ import annotations

Rev ="""
  Trans_code.py (Shutdown-safe hardening) 10/30/2025
	- All fixes from design review applied.
	- Standardized logging to use Tee+print_lock and a module logger for critical errors.
	- Fixed hvc1 remux-only bug.
	- Fixed bitrate logic.
	- Fixed spinner bug.
	- Fixed concurrency deadlock risk.
"""
# --- Standard Library Imports ---
import os
import re
import sys
import time
import traceback
import threading

from typing				import Any, Dict, List, Tuple
from pathlib			import Path
from datetime			import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- Our Project Imports ---
import FFMpeg			# Imports the entire FFmpeg engine
from Utils import *		# Imports all constants, globals (locks, etc.), and basic helpers


Log_File = str(WORK_DIR / f"__{Path(sys.argv[0]).stem}_{time.strftime('%Y_%j_%H-%M-%S')}.log")

ROOT_DIR			= r"C:/Users/Geo/Desktop/downloads"	# Main directory to scan


### EXCEPT_DIR			= r"C:\\_temp"							# Directory for failed/corrupt files
sort_keys_cfg		= [("size", True ), ("date", False )]		# Example: Largest first, oldest
#sort_keys_cfg		= [("size", False), ("date", False )]		# Example: Smalles first, oldest
# -----------------------------------------------------------------------------
# File Scanning
# -----------------------------------------------------------------------------

def scan_folder(root: str, xtnsio: Collection[str], sort_keys_cfg: Collection, use_threads: bool, max_workers: int
	) -> List[Dict[str, Any]]:

	print(f"Scan: {root}\n Scaning folder Sort: {sort_keys_cfg} Start: {time.strftime('%H:%M:%S')}")
	spinner = Spinner()
	candidates: List[str] = []
	file_list: List[Dict[str, Any]] = []
	for dirpath, _, files in os.walk(root):
		for one_file in files:
			if not (ext := Path(one_file).suffix.lower()):
				continue
			if ext in xtnsio:
				candidates.append(os.path.join(dirpath, one_file))
			elif ext not in Ignore_fils:
				safe_print(f"\033[93m :) File {ext}: not Video {one_file}\033[0m")

	total = len(candidates)
	err = 0

	with ThreadPoolExecutor(max_workers=max_workers if use_threads else 1) as executor:
		# Use the ffprobe_run from FFMpeg
		futures = {executor.submit(FFMpeg.ffprobe_run, p, FFPROBE, de_bug, CHECK_CORRUPTION): p for p in candidates}
		for i, fut in enumerate(as_completed(futures)):
			f_path = futures[fut]
			try:
				file_stat = os.stat(f_path)
				if file_stat.st_size < 10:
					try:
						os.remove(f_path)
						safe_print(f"\033[93m :) Removed empty file: {f_path}\033[0m")
					except Exception as e:
						safe_print(f"\033[93m !! Failed to remove file {f_path}: {e}\033[0m")
					continue
			except Exception:
				continue
			try:
				metadata, is_corrupted, error_msg = fut.result()
				if error_msg:
					err += 1
					safe_print(f"\n\033[93m Warning: Could not probe '{f_path}':\nMeta Data{metadata}\nErr: {error_msg}. Skipping.\033[0m")
				elif is_corrupted:
					err += 1
					safe_print(f"\n\033[93m Error: File '{f_path}'\n{metadata}\n Moving to{EXCEPT_DIR}.\033[0m")
					# copy_move is in utils.py
					copy_move(f_path, EXCEPT_DIR, move=True)
				else:
					try:
						file_mtime = datetime.fromtimestamp(file_stat.st_mtime)
					except Exception as ts_err:
						safe_print(f"\n\033[93m[WARNING] Invalid timestamp for {f_path}: {ts_err}. Using default date.\033[0m")
						file_mtime = datetime.fromtimestamp(0)
					file_list.append({
							"path": 	f_path,
							"metadata":	metadata,
							"size":		file_stat.st_size,
							"name":		Path(f_path).name,
							"date":		file_mtime,
							"duration": float((metadata.get("format", {}) or {}).get("duration", 0.0) or 0.0)
							})
			except Exception as e:
				err += 1
				error_details = traceback.format_exc()
				safe_print(f"\n\033[91m[CRITICAL] Unhandled error scanning {f_path}: {e}\033[0m \n {error_details}")
				# errlog_block is in utils.py
				try:						errlog_block(f_path, "scan_folder CRITICAL error", error_details)
				except Exception as log_e:	safe_print(f"  (Additionally, failed to write to error log: {log_e})")
				copy_move(f_path, EXCEPT_DIR, move=True)

			spinner.print_spin(f"[scan] {100*(i+1)/total if total > 0 else 0:>3.1f}% Done - {err:>3} Err ✓ {os.path.basename(f_path)}")

	spinner.stop()
	if err > 0:
		safe_print(f"\033[93mWarning:\033[0m {err} files failed scanning (see per-file logs in script folder).")

	Sort_key = {"size":		lambda x: x["size"],
				"date":		lambda x: x["date"],
				"name":		lambda x: x["name"],
				"duration": lambda x: x["duration"],
				}
	for key, descending in reversed(sort_keys_cfg):
		if key in Sort_key:
			file_list.sort(key=Sort_key[key], reverse=descending)

	return file_list

# -----------------------------------------------------------------------------
# Single File Orchestrator
# -----------------------------------------------------------------------------

def process_file(file_info: Dict[str, Any], idx: int, total: int, task_id: str
	) -> Tuple[int, int, int, int]:
	"""
	Processes a single media file: parse, execute FFmpeg, validate/replace,
	and optionally create additional versions (even on skip).
	Returns: (saved_bytes, processed, skipped, errors)
	"""
	# Use datetime.now()
	de_bug	= False
	str_t	= datetime.now()
	saved	= procs = skipt = errod = 0
	file_p	= file_info["path"]
	input_path_obj = Path(file_p) # Keep original path object
	out_file_temp: Optional[str] = None # Path from ffmpeg_run
	final_file_path: Optional[Path] = None # Path after clean_up moves it
	# --- <<< ADD THIS FIX TO STOP INFINITE LOOP >>> ---
	file_stem = input_path_obj.stem
	# Check if the file stem ends with one of our artifact suffixes
	if file_stem.endswith("_matrix") or \
		file_stem.endswith(f"_short_{int(ADDITIONAL_SHORT_DUR)}s") or \
		re.search(r"_fast_[\d\.]+x$", file_stem): # Matches _fast_2.0x, _fast_3.0x etc.
		safe_print(f"\nSkipping file (already an artifact): {input_path_obj.name}")
		skipt = 1
		# This is a clean skip, so we return immediately
		end_t = datetime.now(); duration_sec=(end_t-str_t).total_seconds()
		safe_print(f" -End: [{end_t.strftime('%H:%M:%S')}]\tTotal: {hm_tm(duration_sec)}")
		return saved, procs, skipt, errod
	safe_print(f"\n{file_p}\n +Start: [{str_t.strftime('%H:%M:%S')}]  File: {idx} of {total}, {hm_sz(file_info['size'])}")
	try: # --- Main Try Block ---
		# 1. Parse file info
		try:
			ff_run_cmd, skip_it, captured_output_lines = FFMpeg.parse_finfo(file_p, file_info["metadata"], de_bug)
		except Exception as e:
			# If parse fails, treat as skipped with error
			skip_it = True
			errod = 1 # Mark error during parse
			captured_output_lines = [f"CRITICAL ERROR IN parse_finfo for {file_p}: {e}", traceback.format_exc()]
			try:	errlog_block(file_p, "parse_finfo CRITICAL error", traceback.format_exc())
			except Exception: pass

		for ln in captured_output_lines:
			safe_print(ln)
		# 2. Handle Skip (or Parse Error)
		if skip_it:
			# Determine status based on whether parse failed
			if errod == 0: # Normal skip
				skipt = 1
			# else: errod remains 1 from parse failure
			# --- ARTIFACTS ON SKIP (Optional) ---
			if ADD_ADDITIONAL and FORCE_ARTIFACTS_ON_SKIP and errod == 0: # Only run if ADD_ADDITIONAL is on, force is on, and parse didn't fail
			#	safe_print("\033[96m  .Artifacts- from source.\033[0m")
				# We need info from the source file
				source_info_for_artifacts = {}
				probe_success = False
				try:
					# Probe the original file to get info needed for artifacts
					# Call ffprobe_run from FFMpeg
					meta, is_corrupted, probe_err = FFMpeg.ffprobe_run(file_p, check_corruption=False) # Probe the source 'file_p'
					if not probe_err and not is_corrupted and meta:
						fmt = meta.get("format", {})
						streams = meta.get("streams", [])
						v_stream = next((s for s in streams if s.get("codec_type") == "video"), {})
						a_stream = next((s for s in streams if s.get("codec_type") == "audio"), {})
						source_info_for_artifacts = {
							"dur_out":	float(fmt.get("duration", 0.0)), # Use source duration
							"w_enc":	  int(v_stream.get("width", 0)),     # Use source dimensions
							"h_enc":	  int(v_stream.get("height", 0)),
							"ach_out":	  int(a_stream.get("channels", 0)),# Check if source has audio channels
							# Add "has_audio_on_source" for compatibility if needed by _post_encode_artifacts
							"has_audio_on_source": int(a_stream.get("channels", 0)) > 0
						}
						probe_success = True
					else:
						safe_print(f"   [WARN] Could not probe source '{input_path_obj.name}' for skip-artifact info: {probe_err or 'No meta/Corrupt'}")
					# Call artifact generation using the SOURCE path and probed info (if probe worked)
					if probe_success:
						safe_print("\n[post] Start Artifact generation from source...")
						# Call _post_encode_artifacts from FFMpeg
						FFMpeg._post_encode_artifacts(input_path_obj, source_info_for_artifacts, f"{task_id}_SKIP", de_bug )
					else:
						safe_print("\n[post] (skip) Skipping Artifact source probe failure.")
				except Exception as art_e:
					# Log errors during artifact generation but don't fail the main skip
					safe_print(f"\n   [WARN] Error during skip-artifact generation: {art_e}")
					try: errlog_block(file_p, "Skip Artifact Generation Error", f"{art_e}\n{traceback.format_exc()}")
					except Exception: pass
			# --- END ARTIFACTS ON SKIP ---
			# Clean up SRIK for skipped file
			try: FFMpeg.srik_clear(file_p)
			except Exception: pass
			# Return directly from skip block
			# (Finally block will still execute for timing)
			return saved, procs, skipt, errod
		# 3. Main processing logic (only runs if not skip_it)
		# Call ffmpeg_run from FFMpeg
		out_file_temp = FFMpeg.ffmpeg_run(file_p, ff_run_cmd, file_info["duration"], skip_it, de_bug, task_id)
		if not out_file_temp:
			# 3a. FFmpeg failed
			errod = 1
			safe_print(f"ffmpeg_run failed for: {file_p}")
			copy_move(file_p, EXCEPT_DIR, move=True)
			# Return directly after handling failure
			# (Finally block will still execute)
			return saved, procs, skipt, errod
		else:
			# 3b. FFmpeg succeeded, run cleanup/validation
			# Call clean_up from FFMpeg
			savings_or_err = FFMpeg.clean_up(file_p, out_file_temp, skip_it=False, de_bug=de_bug, task_id=task_id)
			if savings_or_err == -1:
				# clean_up reported an error/rejection
				errod = 1
				# (Finally block will still execute)
				return saved, procs, skipt, errod
			else:
				# clean_up was successful!
				saved = savings_or_err
				procs = 1
				# Return final success state
				# (Finally block will still execute)
				return saved, procs, skipt, errod

	except Exception as e:
		# 4. Catch-all for any *unexpected* errors in the main try block
		safe_print(f"\n[CRITICAL] Unhandled worker error for {file_p}: {e}\n{traceback.format_exc()}")
		saved, procs, skipt, errod = 0, 0, 0, 1 # Force error state
		try:	errlog_block(file_p, "process_file CRITICAL", traceback.format_exc())
		except Exception: pass
		# Cleanup potentially created temp file
		if out_file_temp and Path(out_file_temp).exists():
			try: Path(out_file_temp).unlink(missing_ok=True)
			except Exception: pass
		# Try to move original to exceptions dir
		try: copy_move(file_p, EXCEPT_DIR, move=True)
		except Exception: pass
		# Return error state directly
		# (Finally block will still execute)
		return saved, procs, skipt, errod

	finally:
		# 5. This block *always* runs AFTER the try or except block finishes (and before returning)
		# Use datetime.now()
		end_t = datetime.now()
		duration_sec = (end_t - str_t).total_seconds()
		safe_print(f" -End: [{end_t.strftime('%H:%M:%S')}]\tTotal: {hm_tm(duration_sec)}")
		# NOTE: SRIK clear is now handled within the specific paths (skip, cleanup success/fail)

	# This line should ideally not be reached if all paths return correctly above,
	# but serves as a fallback. It might indicate a logic error if hit.
	# It will return the state as it was left by the try/except block.
	return saved, procs, skipt, errod

# -----------------------------------------------------------------------------
# Progress Reporter Thread
# -----------------------------------------------------------------------------

class ProgressReporter(threading.Thread):
		def __init__(self, get_summary, interval: float = 2.0):
				super().__init__(daemon=True)
				self._get_summary = get_summary
				self._interval = interval
				self._stop = threading.Event()
				self.last_lines_printed = 0

		def stop(self) -> None:
				self._stop.set()
				clear_sequence = self.last_lines_printed * ("\033[F" + "\033[K")
				try:
					with print_lock:
						sys.stderr.write(clear_sequence)
						sys.stderr.flush()
				except Exception:
					pass  # Suppress errors if stderr is closed

		def run(self) -> None:
				while not self._stop.wait(self._interval):
					try:
						# Call progress_get_snapshot from FFMpeg
						snap = FFMpeg.progress_get_snapshot()
						q, d, ok, sk, er = self._get_summary()
						ts = time.strftime("%H:%M:%S")
						lines = [f"{ts} [Work Queue] ToDo:{q:<4d} Done:{d:<4d} OK:{ok:<4d} Skip:{sk:<4d} Err:{er:<4d}"]
						for tid, st in sorted(snap.items()):
								lines.append(f"-=>[{tid}]|Frames:{st.get('frame',0):>7}|Speed:{st.get('speed',0):>5.2f}x|{st.get('percent',0):>5.1f}%  ")

						clear_sequence = self.last_lines_printed * ("\033[F" + "\033[K")
						with print_lock:
							sys.stderr.write(clear_sequence + "\n".join(lines) + "\n")
							sys.stderr.flush()

					except Exception as e:
						# If reporter fails, just print error and stop thread
						safe_print(f"[FATAL] ProgressReporter failed: {e}")
						self.last_lines_printed = 0 # Reset to avoid clearing error
						break # Stop thread

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def main(argv: Optional[List[str]] = None
	) -> int:
	strt_m = datetime.now()
	print(f"\n+Main Start: [{time.strftime('%H:%M:%S')}]")

	# --- NEW: Fail-fast check for binaries ---
	if not os.path.isfile(FFMPEG) or not os.path.isfile(FFPROBE):
		print("\033[91m[CRITICAL ERROR]\033[0m FFmpeg or FFprobe binaries not found.")
		print(f"  FFMPEG Path: {FFMPEG}")
		print(f"  FFPROBE Path: {FFPROBE}")
		print("  Please install FFmpeg and ensure they are in your system PATH")
		print("  or set the correct paths in Utils.py.")
		if PAUSE_ON_EXIT:
			try: input("Press Enter to exit.")
			except EOFError: pass
		return 1 # Exit with an error code
	# --- END NEW ---

	os.makedirs(EXCEPT_DIR, exist_ok=True)
	print("-" * 70, flush=True)

	for p in RUN_TMP.glob("*"):
		try: p.unlink()
		except Exception: pass

	fl_lst = scan_folder(ROOT_DIR, File_extn, sort_keys_cfg, SCAN_PARALLEL, MAX_SCAN_WORKRS)
	fl_nmb = len(fl_lst)

	print("-" * 70, flush=True)
	saved = 0
	procs = 0
	skipt = 0
	errod = 0

	def get_summary() -> Tuple[int, int, int, int, int]:
		completed = procs + skipt + errod
		todo = fl_nmb - completed
		return (todo, completed, procs, skipt, errod)

	if WORK_PARALLEL and fl_nmb > 0 and MAX_WORKERS >= 1:
		reporter = ProgressReporter(get_summary)
		reporter.start()
		try:
			with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
				futures = {
					executor.submit(process_file, fi, i + 1, fl_nmb, f"T{(i % MAX_WORKERS) + 1}"): fi
						for i, fi in enumerate(fl_lst)
				}
				for future in as_completed(futures):
					try:
						s, p, skc, e = future.result()
						saved += s
						procs += p
						skipt += skc
						errod += e

						label	= "Lost" if saved < 0 else "Saved"
						msg		= f":):|To_do: {fl_nmb - (procs + skipt + errod)}|OK: {procs}|Errors: {errod}|Skipt: {skipt}|{label}: {hm_sz(saved)} |"
						safe_print(msg)
					except Exception as exc:
						errod += 1
						print(f"\n[CRITICAL] Worker for {futures[future]['path']} generated: {exc}\n{traceback.format_exc()}")
		finally:
			reporter.stop()
			reporter.join(timeout=5)
	else:
		for i, each in enumerate(fl_lst):
			s, p, skc, e = process_file(each, i + 1, fl_nmb, "T1")
			saved += s
			procs += p
			skipt += skc
			errod += e
			label	= "Lost" if saved < 0 else "Saved"
			msg		= f"  |To_do: {fl_nmb - (procs + skipt + errod)}|OK: {procs}|Errors: {errod}|Skipt: {skipt}|{label}: {hm_sz(saved)} |"
			safe_print(msg)

	end_t = datetime.now()
	total = (end_t - strt_m).total_seconds()
	print(f"\n-Main Done: [{time.strftime('%H:%M:%S')}]\tTotal Time: [{hm_tm(total)}]")
	print(f" Files: {fl_nmb}\tProcessed: {procs}\tSkipped : {skipt}\tErrors : {errod}\n Saved in Total: {hm_sz(saved)}\n")

	if PAUSE_ON_EXIT:
		try: input("All Done :)")
		except EOFError: pass
	return 0

if __name__ == "__main__":
	with Tee(Log_File):
		print (Rev)
		sys.exit(main())
