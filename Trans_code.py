# -*- coding: utf-8 -*-
Rev = "Trans_code.py Rev: 22.18 (Removed verbose_debug, fixed parse_finfo call, removed dormant code (math import, unused sort keys), restored color-coded output and spinner)"

import os
import re
import sys
import stat
import time
import shutil
import psutil
import datetime
import itertools
import threading
import traceback
import subprocess as SP

import os
import sys
sys.path.append(os.path.dirname(__file__) or ".")

from io import StringIO
from typing import List, Optional, Dict, Tuple, Any
from functools import cmp_to_key
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

from My_utils import copy_move, hm_sz, hm_time, Tee

from FFMpeg import (
	ffprobe_run, ffmpeg_run, parse_finfo, clean_up,
	File_extn, Skip_key, Default_lng, Keep_langua,
	FFMPEG, FFPROBE, glb_vidolen, progress_get_snapshot, ALWAYS_10BIT
)

# ---------------------------- Paths & Config ----------------------------
Root = r"C:\Users\Geo\Desktop\downloads"

Excepto = r"C:\_temp"

CPU_COUNT			= os.cpu_count() or 4
MAX_PROBE_WORKERS	= max(2, min(12, int(CPU_COUNT * 0.75)))
MAX_WORKERS 		= max(1, min( 3, int(CPU_COUNT * 0.5    ) if not ALWAYS_10BIT else 2))

SCAN_PARALLEL		= True
WORK_PARALLEL		= False #CPU_COUNT >= 4
CHECK_CORRUPTION	= True

de_bug = False  # Keep debug enabled for testing

Log_File = f"__{os.path.basename(sys.argv[0]).replace('.py','')}_{time.strftime('%Y_%j_%H-%M-%S')}.log"

valid_sort_keys = {
	'size':		 lambda x: x['size'],
	'date':		 lambda x: x['date'],
	'name':      lambda x: x['name'],
	'duration':  lambda x: x['duration'],
	'extension': lambda x: x['extension'],
}

sort_keys = [
	('size', True),
	('date', False),
]

print_lock = threading.Lock()

# ---------- Per-thread capture for tidy per-file detail blocks ----------
class _ThreadCaptureStdout:
	def __init__(self, downstream):
		self._down = downstream
		self._bufs = {}
		self._lock = threading.Lock()
		self.encoding = getattr(downstream, 'encoding', 'utf-8')

	def start_capture(self):
		tid = threading.get_ident()
		with self._lock:
			self._bufs[tid] = []

	def end_capture(self) -> str:
		tid = threading.get_ident()
		with self._lock:
			buf = self._bufs.pop(tid, [])
		return ''.join(buf)

	def write(self, data):
		tid = threading.get_ident()
		with self._lock:
			if tid in self._bufs:
				self._bufs[tid].append(data)
			else:
				self._down.write(data)

	def flush(self):
		self._down.flush()

	def isatty(self):
		try:
			return self._down.isatty()
		except Exception:
			return False

	def fileno(self):
		try:
			return self._down.fileno()
		except Exception:
			return 1

# ---------- Aggregated multi-task progress reporter ----------

class ProgressReporter(threading.Thread):
	def __init__(self, get_summary, interval: float = 4.0):
		super().__init__(daemon=True)
		self._stop_event = threading.Event()
		self._get_summary = get_summary
		self._interval = interval

	def stop(self):
		self._stop_event.set()

	def run(self):
		while not self._stop_event.is_set():
			snap = progress_get_snapshot()
			q, d, ok, sk, er = self._get_summary()
			ts = time.strftime('%H:%M:%S')
			with print_lock:
				sys.stderr.write(f"\\n{ts}  [work] queued: {q:4d}  done: {d:4d}  ok: {ok:4d}  skip: {sk:4d}  err: {er:4d}\\n")
				for tid, st in sorted(snap.items()):
					size = hm_sz(int(st.get('size_kb', 0.0) * 1024))
					fps = st.get('fps', 0.0) or 0.0
					spd = st.get('speed', 0.0) or 0.0
					br  = f"{float(st.get('bitrate_kbps', 0.0)):>6.2f} kbps"
					eta = st.get('eta', '--:--:--')
					pct = st.get('percent', 0.0) or 0.0
					sys.stderr.write(f"          [{tid}] | FFmpeg |Size: {size:>12}|Frames:{int(st.get('frame',0)):>8}|Fps:{fps:>7.1f}|BitRate:{br:>12}|Speed:{spd:>6.2f}x|ETA:{eta:>9}|{pct:>6.1f}%|\\n")
				sys.stderr.flush()
			self._stop.wait(self._interval)


# ---------------------------- Helpers ----------------------------

def ordinal(n: int) -> str:
	if 10 <= (n % 100) <= 20:
		suff = 'th'
	else:
		suff = {1: 'st', 2: 'nd', 3: 'rd'}.get(n % 10, 'th')
	return f"{n}{suff}"

class Spinner:
	def __init__(self, spin_text: str = r"|/-\o+", indent: int = 0, delay: float = 0.08):
		self.spinner_count = 0
		self.spin_text = spin_text
		self.spin_length = len(spin_text)
		self.prefix = " " * indent
		self.last_message_length = 0
		self.cursor_hidden = False
		self.delay = delay
		self.last_update_time = 0

	def hide_cursor(self):
		if not self.cursor_hidden and sys.stderr.isatty():
			with print_lock:
				sys.stderr.write("\033[?25l")
				sys.stderr.flush()
			self.cursor_hidden = True

	def show_cursor(self):
		if self.cursor_hidden and sys.stderr.isatty():
			with print_lock:
				sys.stderr.write("\033[?25h")
				sys.stderr.flush()
			self.cursor_hidden = False

	def abbreviate_path(self, path: str, max_length: int) -> str:
		if len(path) <= max_length:
			return path
		return f"{path[:max_length//2]}...{path[-max_length//2:]}"

	def print_spin(self, extra: str = "") -> None:
		current_time = time.time()
		if current_time - self.last_update_time < self.delay:
			return
		self.last_update_time = current_time
		self.hide_cursor()
		terminal_width = shutil.get_terminal_size(fallback=(120, 25)).columns
		extra = self.abbreviate_path(extra, max(10, terminal_width - 12))
		spin_char = self.spin_text[self.spinner_count % self.spin_length]
		message = f"\r{self.prefix}| {spin_char} | {extra}"
		clear_spaces = max(self.last_message_length - len(message), 0)
		with print_lock:
			sys.stderr.write(f"{message}{' ' * clear_spaces}")
			sys.stderr.flush()
		self.last_message_length = len(message)
		self.spinner_count += 1

	def stop(self):
		self.show_cursor()
		with print_lock:
			sys.stderr.write("\n")
			sys.stderr.flush()

def get_tree_size(path: str) -> int:
	total_size = 0
	try:
		for entry in os.scandir(path):
			if entry.is_file(follow_symlinks=False):
				total_size += entry.stat(follow_symlinks=False).st_size
			elif entry.is_dir(follow_symlinks=False):
				total_size += get_tree_size(entry.path)
	except (OSError, ValueError) as e:
		with print_lock:
			print(f"get_tree_size exception: {e} path: {path}")
			if de_bug:
				print(traceback.format_exc())
	return total_size

# ---------------------------- Scanner ----------------------------

def scan_folder(root: str,
				xtnsio: List[str],
				sort_keys: Optional[List[Tuple[str, bool]]] = None,
				use_threads: bool = SCAN_PARALLEL,
				max_workers: int = MAX_PROBE_WORKERS) -> Optional[List[Dict]]:
	if not root or not isinstance(root, str) or not os.path.isdir(root):
		with print_lock:
			print(f"Invalid root directory: {root}")
		return []
	if not isinstance(xtnsio, (tuple, list)):
		with print_lock:
			print(f"Invalid extension list: {xtnsio}")
		return []

	str_t = time.perf_counter()
	with print_lock:
		print(f"Scan: {root}\tSize: {hm_sz(get_tree_size(root))}\nscan_folder Start: {time.strftime('%H:%M:%S')}", flush=True)

	spinner = Spinner(indent=0)

	candidates: List[Tuple[str, int, str]] = []

	def is_file_accessible(file_path: str) -> bool:
		try:
			with open(file_path, 'rb') as f:
				f.read(1)
			return True
		except (IOError, OSError):
			return False

	for dirpath, _, files in os.walk(root):
		for one_file in files:
			f_path = os.path.join(dirpath, one_file)
			_, ext = os.path.splitext(one_file)
			if ext.lower() not in xtnsio:
				continue
			try:
				if not is_file_accessible(f_path):
					with print_lock:
						print(f"\033[93mSkipping inaccessible file: {f_path}\033[0m")
					continue
				file_s = os.path.getsize(f_path)
				if file_s < 10:
					try:
						os.remove(f_path)
						with print_lock:
							print(f"\033[93mRemoved empty file: {f_path}\033[0m")
					except Exception as e:
						with print_lock:
							print(f"\033[93mFailed to remove empty file {f_path}: {e}\033[0m")
					continue
				if not os.access(f_path, os.W_OK) or not (os.stat(f_path).st_mode & stat.S_IWUSR):
					with print_lock:
						print(f"\033[93mSkipping non-writable file: {f_path}\033[0m")
					continue
				if len(f_path) >= 333:
					with print_lock:
						print(f"\033[93mSkipping file with path too long: {f_path}\033[0m")
					continue
				candidates.append((f_path, file_s, ext.lower()))
			except Exception as e:
				with print_lock:
					print(f"\033[93mError collecting candidate {f_path}: {e}\033[0m")
					if de_bug:
						print(traceback.format_exc())
				continue

	total = len(candidates)
	done = 0
	err = 0
	file_list: List[Dict] = []

	def handle_result(f_path: str, file_s: int, ext: str, metadata: Dict[str, Any], is_corrupted: bool) -> bool:
		if is_corrupted:
			with print_lock:
				print(f"\033[93mError: File '{f_path}' is corrupted (invalid data detected). Moving to {Excepto}.\033[0m")
			try:
				copy_move(f_path, Excepto, False, True)
				with print_lock:
					print(f"\033[93mMoved {f_path} to {Excepto} due to corruption\033[0m")
			except Exception as e:
				with print_lock:
					print(f"\033[93mFailed to move {f_path} to {Excepto}: {e}\033[0m")
			return False
		try:
			mod_time = os.path.getmtime(f_path)
			mod_datetime = datetime.datetime.fromtimestamp(mod_time)
		except Exception:
			mod_datetime = datetime.datetime(1970, 1, 1)
		duration = float(metadata.get("format", {}).get("duration", 0.0) or 0.0)
		file_info = {
			'path':      f_path,
			'name':      os.path.basename(f_path),
			'extension': ext,
			'size':      file_s,
			'date':      mod_datetime,
			'duration':  duration,
			'metadata':  metadata,
		}
		file_list.append(file_info)
		return True

	if use_threads and total > 0:
		with ThreadPoolExecutor(max_workers=max_workers) as executor:
			future_map = {executor.submit(ffprobe_run, p, FFPROBE, de_bug, CHECK_CORRUPTION): (p, s, e) for (p, s, e) in candidates}
			for fut in as_completed(future_map):
				f_path, file_s, ext = future_map[fut]
				ok = False
				try:
					metadata, is_corrupted = fut.result()
					ok = handle_result(f_path, file_s, ext, metadata, is_corrupted)
				except Exception as e:
					with print_lock:
						print(f"\033[93mError probing {f_path}: {e}\033[0m")
						if de_bug:
							print(traceback.format_exc())
					err += 1
					try:
						copy_move(f_path, Excepto, False, True)
						with print_lock:
							print(f"\033[93mMoved {f_path} to {Excepto} due to probe error\033[0m")
					except Exception as move_e:
						with print_lock:
							print(f"\033[93mFailed to move {f_path} to {Excepto}: {move_e}\033[0m")
				if ok:
					done += 1
				else:
					err += 1
				pct = (done + err) * 100.0 / total if total else 100.0
				spinner.print_spin(f"[scan] {pct:>3.1f}% Done - {err:>3} Err ✓ {os.path.basename(f_path)}")
	else:
		for (f_path, file_s, ext) in candidates:
			try:
				metadata, is_corrupted = ffprobe_run(f_path, FFPROBE, de_bug, CHECK_CORRUPTION)
				ok = handle_result(f_path, file_s, ext, metadata, is_corrupted)
			except Exception as e:
				with print_lock:
					print(f"\033[93mError probing {f_path}: {e}\033[0m")
				err += 1
				try:
					copy_move(f_path, Excepto, False, True)
					with print_lock:
						print(f"\033[93mMoved {f_path} to {Excepto} due to probe error\033[0m")
				except Exception as move_e:
					with print_lock:
						print(f"\033[93mFailed to move {f_path} to {Excepto}: {move_e}\033[0m")
				ok = False
			if ok:
				done += 1
			else:
				err += 1
			pct = (done + err) * 100.0 / total if total else 100.0
			spinner.print_spin(f"[scan] {pct:>3.1f}% Done - {err:>3} Err ✓ {os.path.basename(f_path)}")

	spinner.stop()

	if sort_keys:
		sort_keys = [(key, desc) for key, desc in sort_keys if key in valid_sort_keys]
		if not sort_keys:
			sort_keys = [('size', True)]
	else:
		sort_keys = [('size', True)]

	key_funcs = [valid_sort_keys[key] for key, desc in sort_keys]
	sort_orders = [desc for _, desc in sort_keys]

	def compare_items(a, b):
		for key_func, descending in zip(key_funcs, sort_orders):
			a_key = key_func(a); b_key = key_func(b)
			if a_key != b_key:
				if descending:
					return -1 if a_key > b_key else 1
				else:
					return -1 if a_key < b_key else 1
		return 0

	sorted_list = sorted(file_list, key=cmp_to_key(compare_items))
	order_str = ', '.join([f"{key} ({'Desc' if desc else 'Asc'})" for key, desc in sort_keys])

	end_t = time.perf_counter()
	with print_lock:
		print(f"\nSort by: {order_str}\nScan Done: {time.strftime('%H:%M:%S')}\tTotal: {hm_time(end_t - str_t)}\n")
		if err > 0:
			print(f"\033[93mWarning: {err} files failed to scan and were moved to {Excepto}\033[0m")
	return sorted_list

# ---------------------------- File processing ----------------------------

def process_file(file_info: Dict, idx: int, total: int, task_id: str) -> Tuple[int, int, int, int]:
	msj = sys._getframe().f_code.co_name
	str_t = time.perf_counter()
	output_buffer = StringIO()

	saved = procs = skipt = errod = 0
	skip_it = False

	file_p = file_info['path']
	file_s = file_info['size']
	ext = file_info['extension']
	jsn_ou = file_info['metadata']

	if not os.path.isfile(file_p):
		with print_lock:
			print(f' -Missing: {hm_sz(file_s)}\t{file_p}')
		return (0, 0, 0, 1)

	header = f"\n{file_p}\n {ordinal(idx)} of {total}, {ext}, {hm_sz(file_s)}\n"

	try:
		free_disk_space = shutil.disk_usage(os.path.splitdrive(file_p)[0] + os.sep).free
		free_temp_space = shutil.disk_usage(r"C:").free
		if free_disk_space < (1.5 * file_s) or free_temp_space < (1.5 * file_s):
			output_buffer.write(f'\nNot enough space. Disk: {hm_sz(free_disk_space)}  Temp(C:): {hm_sz(free_temp_space)}\n')
			with print_lock:
				print(output_buffer.getvalue(), flush=True)
			output_buffer.close()
			return (0, 0, 0, 1)
	except Exception as e:
		output_buffer.write(f"Space check error: {e}\n")
		if de_bug:
			output_buffer.write(f"{traceback.format_exc()}\n")
		with print_lock:
			print(output_buffer.getvalue(), flush=True)
		output_buffer.close()
		return (0, 0, 0, 1)

	try:
		# Check corruption first
		metadata, is_corrupted = ffprobe_run(file_p, FFPROBE, de_bug, CHECK_CORRUPTION)
		if is_corrupted:
			output_buffer.write(f"\033[93mError: File '{file_p}' is corrupted (invalid data detected). Moving to {Excepto}.\033[0m\n")
			try:
				copy_move(file_p, Excepto, False, True)
				output_buffer.write(f"Moved to {Excepto} due to corruption\n")
			except Exception as e:
				output_buffer.write(f"\033[93mFailed to move {file_p} to {Excepto}: {e}\033[0m\n")
			with print_lock:
				print(output_buffer.getvalue(), flush=True)
			output_buffer.close()
			return (0, 0, 0, 1)

		sys.stdout.start_capture()
		all_good, skip_it = parse_finfo(file_p, metadata, de_bug)
		captured = sys.stdout.end_capture()
		with print_lock:
			print(header, end='')
			print(output_buffer.getvalue(), end='')
			print(captured, end='')
		output_buffer.close()
		output_buffer = StringIO()

		if skip_it:
			with print_lock:
				print(f"\033[91m   .Skip: >| {msj} |<\033[0m")
			skipt += 1
		else:
			all_good = ffmpeg_run(file_p, all_good, skip_it, FFMPEG, de_bug, task_id)
			if all_good:
				saved += (clean_up(file_p, all_good, de_bug) or 0)
				procs += 1
			elif not skip_it:
				output_buffer.write(f"Fmpeg failed for: {file_p}\n")
				errod += 1
				if de_bug:
					output_buffer.write(f"{traceback.format_exc()}\n")
				try:
					if copy_move(file_p, Excepto, False, True):
						output_buffer.write(f"Moved to {Excepto} due to FFmpeg failure\n")
				except Exception as e:
					output_buffer.write(f"\033[93mFailed to move {file_p} to {Excepto}: {e}\033[0m\n")

	except ValueError as e:
		if e.args and e.args[0] == "Skip It":
			output_buffer.write(f"Go on: {e}\n")
			skipt += 1
		else:
			errod += 1
			output_buffer.write(f" +: ValueError: {e}\n{os.path.dirname(file_p)}\n\t{os.path.basename(file_p)}\t{hm_sz(file_s)}\nMoved\n")
			if de_bug:
				output_buffer.write(f"{traceback.format_exc()}\n")
			try:
				if copy_move(file_p, Excepto, False, True):
					output_buffer.write("Handled ValueError\n")
			except Exception as e:
				output_buffer.write(f"\033[93mFailed to move {file_p} to {Excepto}: {e}\033[0m\n")

	except Exception as e:
		errod += 1
		output_buffer.write(f" +: Exception: {e}\n{os.path.dirname(file_p)}\n\t{os.path.basename(file_p)}\t{hm_sz(file_s)}\nCopied\n")
		if de_bug:
			output_buffer.write(f"{traceback.format_exc()}\n")
		try:
			if copy_move(file_p, Excepto, False, True):
				output_buffer.write("Handled Exception\n")
		except Exception as e:
			output_buffer.write(f"\033[93mFailed to move {file_p} to {Excepto}: {e}\033[0m\n")

	end_t = time.perf_counter()
	output_buffer.write(f"  -End: {time.strftime('%H:%M:%S')}\tTotal: {hm_time(end_t - str_t)}\n")

	with print_lock:
		print(output_buffer.getvalue(), flush=True)
	output_buffer.close()
	return saved, procs, skipt, errod

# ---------------------------- Main ----------------------------

def main():
	print(Rev)

	str_t = time.perf_counter()
	with print_lock:
		print("sys._getframe().f_code.co_name")
	with Tee(Log_File) as _tee:
		sys.stdout = _ThreadCaptureStdout(sys.stdout)
		if not Root:
			with print_lock:
				print("Root directory not provided", flush=True)
			return
		if not os.path.exists(Excepto):
			try:
				os.makedirs(Excepto, exist_ok=True)
			except Exception as e:
				with print_lock:
					print(f"Error creating Excepto dir: {e}")
					if de_bug:
						print(traceback.format_exc())

		with print_lock:
			print(f"{psutil.cpu_count()} CPU's\t\t\t ¯\\_(%)_/¯", flush=True)
			print(f"Python version: {sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}", flush=True)
			print(f"Script absolute path: {os.path.abspath(__file__)}", flush=True)
			print(f"Main Start: {time.strftime('%Y:%m:%d-%H:%M:%S')}", flush=True)
			print("-" * 70, flush=True)

		try:
			if not os.path.isfile(FFMPEG):
				raise FileNotFoundError(f"FFmpeg not found at '{FFMPEG}'.")
			if not os.path.isfile(FFPROBE):
				raise FileNotFoundError(f"FFprobe not found at '{FFPROBE}'.")

			result = SP.run([FFMPEG, "-version"], stdout=SP.PIPE, stderr=SP.PIPE)
			if result.returncode == 0:
				version_info = result.stdout.decode("utf-8", errors="ignore")
				m = re.search(r"ffmpeg version ([^\s]+)", version_info)
				if m:
					with print_lock:
						print(f"Fmpeg version: {m.group(1)}", flush=True)
				else:
					with print_lock:
						print("Warning: could not parse ffmpeg version.", flush=True)
			else:
				with print_lock:
					print(f"Error running ffmpeg -version:\n{result.stderr.decode('utf-8', errors='ignore')}", flush=True)
		except Exception as e:
			with print_lock:
				print(f"FFmpeg check error: {e}", flush=True)
				if de_bug:
					print(traceback.format_exc())

		fl_lst = scan_folder(Root, list(map(str.lower, File_extn)), sort_keys=sort_keys)
		if fl_lst is None:
			with print_lock:
				print("Scan returned None; exiting.")
			return
		fl_nmb = len(fl_lst)
		counter = itertools.count(1)

		saved = procs = skipt = errod = 0

		work_spin = Spinner(indent=0)

		def update_work_spinner(q, d, ok, sk, er):
			work_spin.print_spin(
				f"[Stats] Tbd: {q:4d}|Done: {d:4d}|Skip: {sk:4d}|Ok: {ok:4d}|Err: {er:3d}|"
				f"{'Saved' if saved <= 0 else 'Lost'}: {hm_sz(abs(saved))}\n")

		if WORK_PARALLEL and fl_nmb > 0:
			d = ok = sk = er = 0
			update_work_spinner(fl_nmb - d, d, ok, sk, er)
			with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
				fut2file = {}
				for each in fl_lst:
					n = next(counter)
					fut = executor.submit(process_file, each, n, fl_nmb, f"T{n}")
					fut2file[fut] = each
				for fut in as_completed(fut2file):
					each = fut2file[fut]
					try:
						s, p, skc, e = fut.result()
						saved += s; procs += p; skipt += skc; errod += e
						ok += p; sk += skc; er += e; d += 1
						update_work_spinner(fl_nmb - d, d, ok, sk, er)
					except Exception as exc:
						er += 1; d += 1
						errod += 1
						update_work_spinner(fl_nmb - d, d, ok, sk, er)
						with print_lock:
							print(f"Worker error for {each['path']}: {exc}")
							if de_bug:
								print(traceback.format_exc())
			work_spin.stop()
		else:
			d = ok = sk = er = 0
			for each in fl_lst:
				cnt = next(counter)
				s, p, skc, e = process_file(each, cnt, fl_nmb, f"T{cnt}")
				saved += s; procs += p; skipt += skc; errod += e
				ok += p; sk += skc; er += e; d += 1
				update_work_spinner(fl_nmb - d, d, ok, sk, er)
			work_spin.stop()

		end_t = time.perf_counter()
		with print_lock:
			print(f"\n Done: {time.strftime('%H:%M:%S')}\t Total Time: {hm_time(end_t - str_t)}", flush=True)
			print(f" Files: {fl_nmb}\tProcessed: {procs}\tSkipped : {skipt}\tErrors : {errod}\n Saved in Total: {hm_sz(abs(saved))}\n", flush=True)

	with print_lock:
		input('All Done :)')
	sys.exit(0)

if __name__ == "__main__":
	main()
