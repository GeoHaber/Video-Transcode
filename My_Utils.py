from __future__ import annotations

import os
import re
import sys
import time
import queue
import shutil
import random
import string
import filecmp
import logging
import platform
import traceback
import threading
import tracemalloc

import datetime as TM
from pathlib import Path
from functools import wraps
from typing import Optional

# ========================  DECORATORS  ========================

def color_print(fg: int = 37, bg: int | None = None):
	"""
	Decorator factory that prints the wrapped function's returned text with ANSI colors.

	Parameters
	----------
	fg : int
		ANSI foreground color code (e.g., 31 red, 32 green, 33 yellow, 34 blue, 37 white/default).
	bg : int | None
		ANSI background color code (e.g., 40 black, 41 red, 44 blue). None = no background.

	Notes
	-----
	- The wrapped function should return a string (or something convertible to str).
	- The wrapper *prints* the colored text and also returns the plain string result.
	- Safe to use when ANSI is supported (most terminals). Windows 10+ supports this; on older
	  terminals the escape sequences will simply print literally.
	"""
	def decorator(func):
		@wraps(func)
		def wrapper(*args, **kwargs):
			text = func(*args, **kwargs)
			s = "" if text is None else str(text)
			pre = f"\033[{fg}m" + (f"\033[{bg}m" if bg is not None else "")
			print(f"{pre}{s}\033[0m")
			return text
		return wrapper
	return decorator


def name_time(func):
	"""
	Decorator that prints the function name with start time and total duration.

	It does **not** swallow exceptions; it re-raises after timing so failures
	are still visible to the caller.
	"""
	@wraps(func)
	def wrapper(*args, **kwargs):
		start_perf = time.perf_counter()
		start_hms = time.strftime('%H:%M:%S')
		print(f" +{func.__name__} Start: {start_hms}")
		try:
			return func(*args, **kwargs)
		finally:
			dur = time.perf_counter() - start_perf
			h, r = divmod(dur, 3600)
			m, s = divmod(r, 60)
			print(f" -{func.__name__} Done in {int(h)}h:{int(m)}m:{s:05.2f}s")
	return wrapper


def debug(func):
	"""
	Decorator that logs a full traceback if the wrapped function raises.

	The exception is re-raised (safe default) so calling code can handle it.
	"""
	@wraps(func)
	def wrapper(*args, **kwargs):
		try:
			return func(*args, **kwargs)
		except Exception:
			logging.exception("Unhandled exception in %s", func.__name__)
			raise
	return wrapper

from functools import wraps
import logging
import time
import tracemalloc

def perf_monitor(enabled: bool = True):
	"""
	Decorator factory to measure wall time, memory (via tracemalloc), and
	process CPU time around the wrapped function. Falls back gracefully if
	psutil is not available.

	Parameters
	----------
	enabled : bool
		If False, returns the original function (no overhead).
	"""
	if not enabled:
		def passthrough(func):
			return func
		return passthrough

	def decorator(func):
		@wraps(func)
		def wrapper(*args, **kwargs):
			# optional dependency
			proc = None
			cpu_time_before = None
			try:
				import psutil  # type: ignore
				proc = psutil.Process()
				cpu_time_before = proc.cpu_times()
			except Exception:
				psutil = None  # noqa: F841

			tracemalloc.start()
			t0 = time.perf_counter()
			try:
				return func(*args, **kwargs)
			finally:
				wall = time.perf_counter() - t0
				cur, peak = tracemalloc.get_traced_memory()
				tracemalloc.stop()

				cpu_time = None
				if proc is not None and cpu_time_before is not None:
					try:
						ct = proc.cpu_times()
						cpu_time = (getattr(ct, "user", 0.0) + getattr(ct, "system", 0.0)) - (
							getattr(cpu_time_before, "user", 0.0) + getattr(cpu_time_before, "system", 0.0)
						)
					except Exception:
						pass

				# hm_sz is assumed to exist in your module; if not, this falls back to raw bytes.
				def _fmt_bytes(n: int) -> str:
					try:
						return hm_sz(n, "B")  # type: ignore[name-defined]
					except Exception:
						return f"{n} B"

				logging.info(
					"%s: wall=%0.3fs, cpu_time=%s, mem(cur=%s, peak=%s)",
					func.__name__,
					wall,
					f"{cpu_time:.3f}s" if cpu_time is not None else "n/a",
					_fmt_bytes(cur),
					_fmt_bytes(peak),
				)
		return wrapper
	return decorator


def get_tree_size(path: str | os.PathLike[str]) -> int:
	"""
	Recursively compute the total size (bytes) of `path`.

	- Follows neither symlinks nor special files.
	- Skips unreadable entries without raising.
	"""
	total = 0
	try:
		with os.scandir(path) as it:
			for entry in it:
				try:
					if entry.is_file(follow_symlinks=False):
						total += entry.stat(follow_symlinks=False).st_size
					elif entry.is_dir(follow_symlinks=False):
						total += get_tree_size(entry.path)
				except (OSError, ValueError):
					continue
	except (OSError, ValueError):
		pass
	return total


def perf_monitor_temp(func):
	"""
	Measure performance like `perf_monitor`, plus attempt to read CPU temperature.

	Notes
	-----
	- Temperature reading via psutil.sensors_temperatures() is platform/driver
	  dependent and may return empty or None; this function degrades gracefully.
	- Logs metrics via `logging.info`.
	"""
	@wraps(func)
	def wrapper(*args, **kwargs):
		try:
			import psutil  # type: ignore
		except Exception:
			psutil = None  # noqa: F841

		tracemalloc.start()
		t0 = time.perf_counter()
		try:
			return func(*args, **kwargs)
		except Exception as e:
			logging.exception("Exception in %s: %s", func.__name__, e)
			raise
		finally:
			cur, peak = tracemalloc.get_traced_memory()
			tracemalloc.stop()
			wall = time.perf_counter() - t0

			cpu_temp = None
			try:
				import psutil as _ps  # type: ignore
				temps = _ps.sensors_temperatures() or {}
				# best-effort: pick any available entry
				for _, entries in temps.items():
					if entries:
						cpu_temp = getattr(entries[0], "current", None)
						break
			except Exception:
				pass

			logging.info(
				"%s: wall=%s mem(cur=%s, peak=%s) temp=%s°C",
				func.__name__,
				hm_time(wall),
				hm_sz(cur, "B"),
				hm_sz(peak, "B"),
				f"{cpu_temp:.1f}" if isinstance(cpu_temp, (int, float)) else "n/a",
			)
	return wrapper


def measure_cpu_time(func):
	"""
	Decorator that reports wall time and process CPU time (user+sys) after a call.

	Prints a concise line to stdout (does not log). Exceptions are not swallowed.
	"""
	@wraps(func)
	def wrapper(*args, **kwargs):
		t0 = time.perf_counter()
		cpu0 = None
		try:
			import psutil  # type: ignore
			cpu0 = psutil.Process(os.getpid()).cpu_times()
		except Exception:
			pass

		result = func(*args, **kwargs)

		wall = time.perf_counter() - t0
		cpu_used = "n/a"
		try:
			import psutil as _ps  # type: ignore
			cpu1 = _ps.Process(os.getpid()).cpu_times()
			if cpu0:
				cpu_used = f"{(cpu1.user - cpu0.user) + (cpu1.system - cpu0.system):.3f}s"
		except Exception:
			pass
		print(f"{func.__name__} used CPU={cpu_used} over {wall:.2f}s")
		return result
	return wrapper


def logit(logfile: str = "out.log", de_bug: bool = False):
	"""
	Decorator factory that appends (args, kwargs, result) to a text logfile.

	Parameters
	----------
	logfile : str
		Path to a log file (created if missing).
	de_bug : bool
		If True, also prints the same line to stdout.

	Notes
	-----
	- Uses UTF-8 with replace to avoid encoding crashes.
	- Serializes arguments and results with `repr(...)`.
	"""
	def logging_decorator(func):
		@wraps(func)
		def wrapper(*args, **kwargs):
			res = func(*args, **kwargs)
			line = f"{func.__name__}{args} {kwargs} = {repr(res)}\n"
			try:
				with open(logfile, "a", encoding="utf-8", errors="replace") as f:
					f.write(line)
			except Exception:
				logging.exception("Failed to write to %s", logfile)
			if de_bug:
				print(line, end="")
			return res
		return wrapper
	return logging_decorator


def handle_exception(func):
	"""
	Decorator that logs exceptions with traceback and re-raises them.

	Rationale
	---------
	The original version swallowed errors and continued; that makes debugging
	much harder and can corrupt downstream state. Here we log and re-raise.
	"""
	@wraps(func)
	def wrapper(*args, **kwargs):
		try:
			return func(*args, **kwargs)
		except Exception:
			logging.exception("Exception in %s", func.__name__)
			raise
	return wrapper


def measure_cpu_utilization(func):
	"""
	Decorator that returns (result, avg_cpu_percent, per_cpu_list).

	Notes
	-----
	- Uses a short sampling window. Values are coarse and primarily for
	  comparative/diagnostic output, not strict accounting.
	"""
	@wraps(func)
	def wrapper(*args, **kwargs):
		per_cpu_before = None
		try:
			import psutil  # type: ignore
			per_cpu_before = psutil.cpu_percent(interval=0.05, percpu=True)
		except Exception:
			pass

		result = func(*args, **kwargs)

		avg = 0.0
		per = []
		try:
			import psutil as _ps  # type: ignore
			per = _ps.cpu_percent(interval=0.1, percpu=True)
			avg = sum(per) / max(1, len(per))
		except Exception:
			pass
		return result, avg, per
	return wrapper


def log_exceptions(func):
	"""Decorator that logs any exception from the wrapped function, then re-raises."""
	@wraps(func)
	def wrapper(*args, **kwargs):
		try:
			return func(*args, **kwargs)
		except Exception:
			logging.exception("Exception in %s", func.__name__)
			raise
	return wrapper


def measure_execution_time(func):
	"""Decorator that prints wall-clock execution time (seconds) for the call."""
	@wraps(func)
	def wrapper(*args, **kwargs):
		t0 = time.perf_counter()
		res = func(*args, **kwargs)
		print(f"{func.__name__}: Execution time: {time.perf_counter() - t0:.5f} sec")
		return res
	return wrapper


def measure_memory_usage(func):
	"""Decorator that prints current and peak memory usage using tracemalloc."""
	@wraps(func)
	def wrapper(*args, **kwargs):
		tracemalloc.start()
		try:
			return func(*args, **kwargs)
		finally:
			cur, peak = tracemalloc.get_traced_memory()
			tracemalloc.stop()
			print(
				f"{func.__name__}: Mem usage: {cur / 10**6:.3f} MB (cur), {peak / 10**6:.3f} MB (peak)"
			)
	return wrapper


def performance_check(func):
	"""
	Composite decorator combining:
	  - log_exceptions
	  - measure_execution_time
	  - measure_memory_usage
	  - measure_cpu_utilization

	Returns
	-------
	tuple
		(result, avg_cpu_percent, per_cpu_list)
	"""
	@log_exceptions
	@measure_execution_time
	@measure_memory_usage
	@measure_cpu_utilization
	@wraps(func)
	def wrapper(*args, **kwargs):
		return func(*args, **kwargs)
	return wrapper

def temperature():
	"""Print a summary of available temperature sensors (best-effort).

	On many Windows systems, temperature sensors are not exposed; in that case this
	function will simply print nothing (and is considered successful).

	Relies on ``psutil.sensors_temperatures()`` when available and degrades
	gracefully if psutil is missing or unsupported on the host.

	Returns
	-------
	None

	Examples
	--------
	>>> temperature()
	acpitz:
	  temp: 47.0°C
	"""
	try:
		import psutil  # type: ignore
		if hasattr(psutil, "sensors_temperatures"):
			data = psutil.sensors_temperatures()
			if not data:
				return "N/A (no sensors exposed)"
			# pick first sensor family with readings
			fam, readings = next(((k, v) for k, v in data.items() if v), (None, None))
			if readings:
				r = readings[0]
				return f"{fam}: {getattr(r, 'current', 'n/a')}°C"
		return "N/A (unsupported on this system)"
	except Exception:
		return "N/A (psutil not available)"


# ========================  HELPERS / CLASSES  ========================

class Tee:
	"""
	Tee-like stream that duplicates writes to multiple file-like objects.

	Usage
	-----
	with Tee(sys.stdout, open("log.txt","a"), error_on=True) as t:
		print("hello")  # goes to both

	Parameters
	----------
	*files : file-like
		Destinations to write to. Do not pass the same stream twice.
	error_on : bool
		If True, also route sys.stderr through this Tee context.
	"""
	def __init__(self, *files, error_on: bool = False):
		self.files = files
		self.error_on = error_on
		self._orig_out = sys.stdout
		self._orig_err = sys.stderr

	def write(self, obj):
		s = obj if isinstance(obj, str) else str(obj)
		for f in self.files:
			try:
				f.write(s)
				f.flush()
			except Exception:
				# Never explode on logging output
				pass

	def flush(self):
		for f in self.files:
			try:
				f.flush()
			except Exception:
				pass

	def isatty(self):
		# Heuristic: return True if *any* target is a tty
		for f in self.files:
			try:
				if hasattr(f, "isatty") and f.isatty():
					return True
			except Exception:
				continue
		return False

	def __enter__(self):
		sys.stdout = self
		if self.error_on:
			sys.stderr = self
		return self

	def __exit__(self, exc_type, exc_value, tb):
		sys.stdout = self._orig_out
		if self.error_on:
			sys.stderr = self._orig_err
		# Do not suppress exceptions:
		return False


class Spinner:
	"""
	Lightweight stderr spinner for CLI tasks.

	- Non-threaded; you call `print_spin(...)` periodically from your loop.
	- Hides cursor while active and restores it on `stop()`.
	- Abbreviates long messages to terminal width.

	Parameters
	----------
	spin_text : str
		Characters to animate through.
	indent : int
		Indentation spaces before the spinner.
	delay : float
		Minimum seconds between spinner frames.
	"""
	def __init__(self, spin_text: str = "|/-o+\\", indent: int = 0, delay: float = 0.1):
		self.spinner_count = 0
		self.spin_text = spin_text
		self.spin_length = max(1, len(spin_text))
		self.prefix = " " * max(0, indent)
		self.last_message_length = 0
		self.cursor_hidden = False
		self.delay = max(0.02, float(delay))
		self.last_update_time = 0.0

	def hide_cursor(self):
		if not self.cursor_hidden:
			sys.stderr.write("\033[?25l")
			sys.stderr.flush()
			self.cursor_hidden = True

	def show_cursor(self):
		if self.cursor_hidden:
			sys.stderr.write("\033[?25h")
			sys.stderr.flush()
			self.cursor_hidden = False

	def abbreviate_path(self, path: str, max_length: int) -> str:
		if max_length <= 0 or len(path) <= max_length:
			return path
		half = max_length // 2
		return f"{path[:half]}...{path[-(max_length - half - 3):]}"

	def print_spin(self, extra: str = "") -> None:
		now = time.time()
		if now - self.last_update_time < self.delay:
			return
		self.last_update_time = now
		self.hide_cursor()

		width = shutil.get_terminal_size(fallback=(80, 20)).columns
		extra = self.abbreviate_path(str(extra), max(10, width - 10))
		spin_char = self.spin_text[self.spinner_count % self.spin_length]
		message = f"\r{self.prefix}| {spin_char} | {extra}"
		clear_spaces = max(self.last_message_length - len(message), 0)
		sys.stderr.write(f"{message}{' ' * clear_spaces}")
		sys.stderr.flush()

		self.last_message_length = len(message)
		self.spinner_count += 1

	def stop(self):
		self.show_cursor()
		sys.stderr.write("\n")
		sys.stderr.flush()


class RunningAverage:
	"""Compute a simple running average over a stream of numeric values."""
	def __init__(self):
		self.n = 0
		self.avg = 0.0

	def update(self, x: float) -> None:
		self.avg = (self.avg * self.n + float(x)) / (self.n + 1)
		self.n += 1

	def get_avg(self) -> float:
		return float(self.avg)

	def reset(self) -> None:
		self.n = 0
		self.avg = 0.0


class RunningStats:
	"""
	Track running count, sum, min, and max with a convenience printer.

	Methods
	-------
	update(num): add a new value and print current stats
	"""
	def __init__(self):
		self.total = 0.0
		self.count = 0
		self.min_val = float("inf")
		self.max_val = float("-inf")

	def update(self, num: float) -> None:
		num = float(num)
		self.total += num
		self.count += 1
		self.min_val = min(self.min_val, num)
		self.max_val = max(self.max_val, num)
		self.print_stats(num)

	def print_stats(self, num: float) -> None:
		avg = self.total / self.count if self.count else 0.0
		print(f"Current number: {num}, Running average: {avg}")
		print(f"Minimum value: {self.min_val}")
		print(f"Maximum value: {self.max_val}")


class Color:
	"""
	Simple ANSI color constants and helper.

	Usage
	-----
	print(Color.wrap("Hello", Color.RED, bright=True))
	"""
	BLACK   = "\033[30m"
	RED     = "\033[31m"
	GREEN   = "\033[32m"
	YELLOW  = "\033[33m"
	BLUE    = "\033[34m"
	MAGENTA = "\033[35m"
	CYAN    = "\033[36m"
	WHITE   = "\033[37m"
	RESET   = "\033[0m"

	@staticmethod
	def wrap(text: str, color: str, bright: bool = False) -> str:
		pre = "\033[1m" if bright else ""
		return f"{pre}{color}{text}{Color.RESET}"


# ========================  STRING / FORMAT UTILS  ========================

def visual_compare_advanced(string1: str, string2: str) -> None:
	"""
	Print a colorized diff between two strings (line-by-line).

	- Green lines: additions
	- Red lines: deletions
	- Plain lines: unchanged
	"""
	import difflib
	s1 = "" if string1 is None else str(string1)
	s2 = "" if string2 is None else str(string2)
	differ = difflib.Differ()
	diff = list(differ.compare(s1.splitlines(True), s2.splitlines(True)))
	has_diff = any(line[:2] in ("+ ", "- ", "? ") for line in diff)
	if not has_diff:
		print("✅ Strings are identical.")
		return
	print("--- String Differences ---")
	for line in diff:
		if line.startswith("+ "):
			print(f"\033[92m{line}\033[0m", end="")
		elif line.startswith("- "):
			print(f"\033[91m{line}\033[0m", end="")
		elif line.startswith("? "):
			# omit intraline hints for cleaner view
			pass
		else:
			print(line, end="")
	print("--------------------------")


def hm_sz(nbyte: Optional[int | float], unit: str = "B") -> str:
	"""
	Format a byte count (or any unit-scaled number) into a human string, base 1024.

	Examples
	--------
	>>> hm_sz(1536)
	'1.5 KB'
	"""
	if not nbyte:
		return f"0 {unit}"
	sign = "-" if float(nbyte) < 0 else ""
	value = abs(float(nbyte))
	suffix = ["", "K", "M", "G", "T", "P", "E", "Z", "Y"]
	idx = 0
	while value >= 1024.0 and idx < len(suffix) - 1:
		value /= 1024.0
		idx += 1
	return f"{sign}{value:.1f} {suffix[idx]}{unit}"


def hm_time(seconds: float) -> str:
	"""
	Format seconds as 'HH:MM:SS'. Negative inputs are clamped to 0.

	Examples
	--------
	>>> hm_time(65)
	'00:01:05'
	"""
	seconds = int(max(0, float(seconds)))
	h = seconds // 3600
	m = (seconds % 3600) // 60
	s = seconds % 60
	return f"{h:02d}:{m:02d}:{s:02d}"


def ordinal(n: int) -> str:
	"""
	Return the ordinal representation of an integer (e.g., '1st', '2nd', '3rd').
	"""
	if 10 <= (n % 100) <= 20:
		suf = "th"
	else:
		suf = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
	return f"{n}{suf}"


def flatten_list_of_lists(lst):
	"""
	Flatten a list that may contain lists/tuples into a single list (one level).
	"""
	out = []
	for item in lst:
		if isinstance(item, list):
			out.extend(item)
		elif isinstance(item, tuple):
			out.extend(item)
		else:
			out.append(item)
	return out


def divd_strn(val: str) -> float:
	"""
	Parse 'n/d' or float string to a float; rounds to 3 decimals.

	Examples
	--------
	'30000/1001' -> 29.970
	'23.976'     -> 23.976
	"""
	val = str(val).strip()
	try:
		if "/" in val:
			n, d = val.split("/", 1)
			n = float(n)
			d = float(d)
			return round(n / d if d else 0.0, 3)
		return round(float(val), 3)
	except Exception:
		return 0.0


def vis_compr(string1: str, string2: str, no_match_c: str = "|", match_c: str = "="):
	"""
	Visualize character-level differences using simple markers.

	Returns
	-------
	tuple[str, int]
		(graph line, number of differing positions)
	"""
	start_t = TM.datetime.now()
	print(f"     +{vis_compr.__name__}=: Start: {start_t:%T}")

	s1, s2 = str(string1), str(string2)
	if len(s2) < len(s1):
		s1, s2 = s2, s1

	graph = []
	n_diff = 0
	for c1, c2 in zip(s1, s2):
		if c1 == c2:
			graph.append(match_c)
		else:
			graph.append(no_match_c)
			n_diff += 1
	delta = len(s2) - len(s1)
	if delta > 0:
		graph.append(no_match_c * delta)
		n_diff += delta
	graphx = "".join(graph)
	if n_diff:
		print(f"{n_diff} Differences \n1: {s1}\n {graphx}\n2: {s2}\n")
	return graphx, n_diff


def print_alighned(rows: list[list[str]]) -> None:
	"""
	Print a table where each column is left-aligned based on its widest cell.

	Parameters
	----------
	rows : list[list[str]]
		A list of rows; each row is a list of stringifiable values.
	"""
	if not rows:
		return
	# normalize all to strings
	rows = [[str(c) for c in r] for r in rows]
	widths = [max(len(r[i]) for r in rows) for i in range(len(rows[0]))]
	fmt = "  ".join("{:<" + str(w) + "}" for w in widths)
	for r in rows:
		print(fmt.format(*r))


def test_filename(filename: str) -> None:
	"""
	Test whether `filename` matches a conservative legal pattern and suggest a sanitized version.
	"""
	legal = re.compile(r"^[A-Za-z0-9._-]+$")
	if legal.fullmatch(filename):
		print(f"{filename} is a legal filename.")
	else:
		print(f"{filename} is NOT a legal filename.")
	sanitized = re.sub(r"[^\w\s._-]+", "", filename).strip().replace(" ", "_")
	print(f"Suggested rename: {sanitized}")


def stmpd_rad_str(length: int = 13, prefix: str = "") -> str:
	"""
	Generate <prefix>_<MM_SS_><random> using letters+digits.

	Parameters
	----------
	length : int
		Number of random characters.
	prefix : str
		Optional prefix.

	Returns
	-------
	str
	"""
	now = TM.datetime.now().strftime("_%M_%S_")
	pool = string.ascii_letters + string.digits
	rnd = "".join(random.choice(pool) for _ in range(max(1, length)))
	return f"{prefix}{now}{rnd}"


def safe_options(strm, opts: dict) -> dict:
	"""
	Return a sanitized copy of an options dict.

	Behavior
	--------
	- If `opts` contains a special key `__schema__` mapping allowed keys to types,
	  cast values accordingly and drop unknown keys.
	- Else, keep only keys whose values are of simple JSON-friendly types
	  (str, int, float, bool, None).
	- `strm` is accepted for backward-compatibility (ignored unless you later
	  extend it to carry a schema).

	Returns
	-------
	dict
		The sanitized options.
	"""
	SCALAR = (str, int, float, bool, type(None))
	schema = opts.get("__schema__") if isinstance(opts, dict) else None
	out: dict = {}
	if isinstance(schema, dict):
		for k, typ in schema.items():
			if k in opts and opts[k] is not None:
				try:
					out[k] = typ(opts[k])
				except Exception:
					continue
		return out

	# no schema present -> keep only simple types
	for k, v in (opts or {}).items():
		if k == "__schema__":
			continue
		if isinstance(v, SCALAR):
			out[k] = v
	return out


def Trace(message: str, exception: BaseException | None = None, debug: bool = False) -> None:
	"""
	Print and log a formatted traceback diagnostic.

	Parameters
	----------
	message : str
		A short message describing the context.
	exception : Exception | None
		The exception instance (if you caught one). If None, uses the active
		exception (if any).
	debug : bool
		If True, also prints the full stack trace to stdout.
	"""
	print("-" * 84)
	print(f"Msg: {message}")
	if exception is not None:
		print(f"Exc: {exception!r}")
	print("-" * 84)

	if debug:
		traceback.print_exc()

	logging.exception("Trace: %s", message, exc_info=True)


def res_chk(folder: str = ".", ffmpath: str | None = None, log_file: str | None = None) -> bool:
	"""
	Print basic environment diagnostics and perform quick sanity checks.

	Checks
	------
	- Current working directory and script metadata (size & times).
	- If `ffmpath` is given, verifies that the path exists.
	- Reports total/free disk space for the drive containing `folder`.

	Returns
	-------
	bool
		True if the checks pass, False otherwise.
	"""
	ok = True
	print("=" * 60)
	print(TM.datetime.now().strftime("\n%a:%b:%Y %T %p"))
	print("\n:> res_chk")
	print("cwd:", os.getcwd())

	try:
		script = Path(__file__)
		print("\nFile       :", script)
		print("Access time:", time.ctime(script.stat().st_atime))
		print("Modified   :", time.ctime(script.stat().st_mtime))
		print("Created    :", time.ctime(script.stat().st_ctime))
		print("Size       :", hm_sz(script.stat().st_size, "B"))
	except Exception:
		pass

	sys_is = platform.uname()
	print(
		"\nSystem :",
		sys_is.node,
		sys_is.system,
		sys_is.release,
		f"({sys_is.version})",
		sys_is.processor,
	)
	if ffmpath:
		print("FFmpeg   :", ffmpath)
		if not Path(ffmpath).exists():
			print("res_chk: ffmpeg path does not exist!")
			ok = False
	if log_file:
		print("Log File :", log_file)

	try:
		usage = shutil.disk_usage(Path(folder).resolve())
		pct = 100.0 * usage.free / max(1, usage.total)
		print(
			f"\nTotal: {hm_sz(usage.total,'B')}  Free: {hm_sz(usage.free,'B')}  ({pct:.0f}%)"
		)
		if pct < 30.0:
			print("Warning: Less than 30% free space on disk.")
	except Exception as e:
		logging.exception("disk_usage error: %s", e)
		ok = False

	print("\nResources OK\n" if ok else "\nResources have issues\n")
	return ok


def calculate_total_bits(width: int, height: int, pixel_format: str, *, verbose: bool = False) -> int:
	"""
	Compute total **bits per frame** for a given pixel format.

	Supported formats (common):
	  - 'yuv420p'/'yuv420p8' -> 12 bpp
	  - 'p010le'/'yuv420p10le' -> 15 bpp
	  - 'yuv422p'/'yuv422p8' -> 16 bpp
	  - 'yuv422p10le' -> 20 bpp
	  - 'yuv444p'/'yuv444p8' -> 24 bpp
	  - 'yuv444p10le' -> 30 bpp

	Returns
	-------
	int
		Total bits for a single frame (width * height * bpp).
	"""
	fmt = pixel_format.lower()
	bpp_map = {
		"yuv420p": 12, "yuv420p8": 12,
		"p010le": 15, "yuv420p10le": 15,
		"yuv422p": 16, "yuv422p8": 16,
		"yuv422p10le": 20,
		"yuv444p": 24, "yuv444p8": 24,
		"yuv444p10le": 30,
	}
	bpp = bpp_map.get(fmt)
	if bpp is None:
		raise ValueError(f"Unsupported pixel format: {pixel_format}")
	total = int(width) * int(height) * int(bpp)
	if verbose:
		print(f"Total bits for {width}x{height} {pixel_format}: {total}")
	return total


def copy_move(
	src: str | os.PathLike,
	dst: str | os.PathLike,
	move: bool = False,
	overwrite: bool = False,
	timeout: int = 180,
) -> bool:
	"""
	Safely copy or move a file/dir.

	Behavior
	--------
	- If `dst` is a directory, resolves final path as `dst / src.name`.
	- If `dst` exists and is identical to `src` (deep compare), deletes `src` and returns True.
	- If `dst` exists but differs:
		* If `overwrite=True`, overwrite without prompt.
		* Else prompt the user with timeout (S/D/N). Default is 'N' (keep both, abort).
	- Ensures parent directory exists before copying/moving.

	Returns
	-------
	bool
		True on a completed copy/move or intended delete; False on abort.
	"""
	src_path = Path(src)
	dst_path = Path(dst)
	if not src_path.exists():
		print(f"Error: Source '{src_path}' does not exist.")
		return False

	try:
		if dst_path.is_dir():
			dst_path = dst_path / src_path.name

		if dst_path.exists():
			# identical?
			try:
				if src_path.is_file() and dst_path.is_file() and filecmp.cmp(
					str(src_path), str(dst_path), shallow=False
				):
					print(f"Files identical -> deleting source '{src_path}'")
					src_path.unlink(missing_ok=True)
					return True
			except Exception:
				pass  # if compare fails, treat as different

			if overwrite:
				if dst_path.is_dir():
					shutil.rmtree(dst_path, ignore_errors=True)
				else:
					try:
						dst_path.unlink()
					except Exception:
						pass
			else:
				# prompt with timeout
				q: queue.Queue[str] = queue.Queue()

				def prompt():
					ans = input(
						"The files are different. Which version to keep?\n"
						"  (S)ource - Overwrite destination.\n"
						"  (D)estination - Keep destination, delete source.\n"
						"  (N)either - Abort.\n"
						f"Your choice [S/D/N] (default 'N' in {timeout}s): "
					).strip().lower()
					q.put(ans)

				th = threading.Thread(target=prompt, daemon=True)
				th.start()
				try:
					choice = q.get(timeout=max(1, timeout))
				except queue.Empty:
					choice = "n"

				if choice == "s":
					if dst_path.is_dir():
						shutil.rmtree(dst_path, ignore_errors=True)
					else:
						try:
							dst_path.unlink()
						except Exception:
							pass
				elif choice == "d":
					src_path.unlink(missing_ok=True)
					return True
				else:
					print("Aborting; kept both.")
					return False

		# carry out copy/move
		dst_path.parent.mkdir(parents=True, exist_ok=True)
		if move:
			shutil.move(str(src_path), str(dst_path))
			print(f"Moved '{src_path.name}' -> '{dst_path}'")
		else:
			if src_path.is_dir():
				shutil.copytree(src_path, dst_path, dirs_exist_ok=False)
			else:
				shutil.copy2(src_path, dst_path)
			print(f"Copied '{src_path.name}' -> '{dst_path}'")
		return True

	except Exception as e:
		print(f"Error during {'move' if move else 'copy'}: {e}")
		logging.exception("copy_move failed")
		return False

def main() -> None:
	"""
	Self-test battery for the helpers / decorators in this module.

	- Creates a temp folder `_utils_selftest` with a couple of files.
	- Runs quick checks for formatters, math helpers, file ops, table printing, etc.
	- Exercises every decorator on tiny functions (fast, non-interactive).
	- Leaves behind a few logs for inspection and then cleans up most temp files.
	"""
	print("\n=== My_utils self-test ===")
	base = Path("_utils_selftest")
	base.mkdir(exist_ok=True)
	tmp = base / "tmp_utils_test.txt"
	tmp.write_text("ok\n", encoding="utf-8")
	print(f"Temp dir: {base.resolve()}")

	# --- Simple formatters
	print("ordinal(21):", ordinal(21))
	print("hm_sz(123456789):", hm_sz(123456789))
	print("hm_time(3661):", hm_time(3661))
	print("divd_strn('30000/1001'):", divd_strn("30000/1001"))
	print("divd_strn('23.976'):", divd_strn("23.976"))
	print("flatten_list_of_lists:", flatten_list_of_lists([[1, 2], (3, 4), 5, [6]]))
	print("stmpd_rad_str(6,'X'):", stmpd_rad_str(6, "X"))

	# --- Size of current folder (human-readable)
	print("get_tree_size(.):", hm_sz(get_tree_size("."), "B"))

	# --- Visual/string utilities
	visual_compare_advanced("line1\nline2\n", "line1\nLINE-2\n")
	rows = [
		["Name", "Frames", "Bitrate"],
		["ClipA", "1234", "2.5 Mbps"],
		["ClipB", "   8", "640 Kbps"],
	]
	print_alighned(rows)
	test_filename("My File?.txt")

	# --- Color helper
	print(Color.wrap("This is bright red", Color.RED, bright=True))
	print(Color.wrap("This is cyan", Color.CYAN))

	# --- Spinner quick demo
	sp = Spinner(indent=2, delay=0.05)
	for i in range(12):
		sp.print_spin(f"spinner demo {i+1}/12 ...")
		time.sleep(0.05)
	sp.stop()

	# --- Running stats
	ra = RunningAverage()
	rs = RunningStats()
	for x in [1, 2, 3, 4, 5]:
		ra.update(x)
		rs.update(x)
	print(f"RunningAverage avg: {ra.get_avg():.3f}")

	# --- Tee logging (duplicate output to a log file)
	tee_log = base / "tmp_utils_test.log"
	with open(tee_log, "a", encoding="utf-8") as lf, Tee(sys.stdout, lf, error_on=True):
		print("tee works to both console and log")

	# --- Safe options
	opts = {
		"__schema__": {"crf": int, "preset": str, "faststart": bool},
		"crf": "23",
		"preset": "medium",
		"faststart": "1",
		"unknown": 999,
	}
	print("safe_options ->", safe_options(None, opts))

	# --- Trace and res_chk (best-effort)
	try:
		raise ValueError("demo exception")
	except Exception as e:
		Trace("Demonstrating Trace()", e, debug=True)

	print("res_chk('.') ->", res_chk(".", ffmpath=None, log_file=str(tee_log)))

	# --- Bits per frame
	print(
		"calculate_total_bits(3840x2160, yuv420p10le):",
		calculate_total_bits(3840, 2160, "yuv420p10le", verbose=True),
	)

	# --- File copy/move tests (non-interactive)
	copy_to = base / "tmp_utils_test_copy.txt"
	print("copy_move(copy) ->", copy_move(tmp, copy_to, move=False, overwrite=True))
	# Make a different file and attempt overwrite
	copy_to.write_text("DIFFERENT\n", encoding="utf-8")
	print("copy_move(overwrite True) ->", copy_move(tmp, copy_to, move=False, overwrite=True))
	# Move test
	moved = base / "tmp_utils_moved.txt"
	print("copy_move(move) ->", copy_move(copy_to, moved, move=True, overwrite=True))

	# ------------- Decorators exercise -------------
	@color_print(32)  # green
	def colored_msg():
		"""Return a short message to be printed in green."""
		return "color_print decorator says hello"

	@name_time
	def timed_sleep():
		"""Sleep briefly to demonstrate timing output."""
		time.sleep(0.05)

	@debug
	def may_raise(ok=True):
		"""Raise to see debug logging."""
		if not ok:
			raise RuntimeError("boom")
		return "ok"

	@perf_monitor(enabled=True)
	def cpu_work():
		"""Tiny CPU-bound loop."""
		return sum(i * i for i in range(50_000))

	@measure_cpu_time
	def compute_cpu():
		"""Compute a quick sum and report CPU used."""
		return sum(range(100_000))

	@logit(logfile=str(base / "decorators.log"), de_bug=True)
	def add(a, b):
		"""Add two numbers."""
		return a + b

	@handle_exception
	def safe_div(a, b):
		"""Divide (will raise if b==0, but gets logged)."""
		return a / b

	@measure_cpu_utilization
	def tiny():
		"""Return a quick sum and CPU util snapshot."""
		return sum(range(10_000))

	@log_exceptions
	def oops():
		"""Raises a value error; decorator logs it."""
		raise ValueError("bad value")

	@measure_execution_time
	def sleepy():
		"""Short sleep to show timing."""
		time.sleep(0.02)

	@measure_memory_usage
	def alloc():
		"""Allocate a small list to show mem stats."""
		x = [0] * 100_000
		return len(x)

	@performance_check
	def checked():
		"""Composite perf check demo."""
		_ = [i for i in range(50_000)]
		return "ok"

	# Run the decorated tests
	colored_msg()
	timed_sleep()
	print("may_raise(ok=True) ->", may_raise(ok=True))
	try:
		may_raise(ok=False)
	except RuntimeError:
		print("may_raise raised as expected (and was logged).")
	print("cpu_work ->", cpu_work())
	print("compute_cpu ->", compute_cpu())
	print("add(2,3) ->", add(2, 3))
	try:
		safe_div(1, 0)
	except ZeroDivisionError:
		print("safe_div raised ZeroDivisionError (and was logged).")

	tiny_res, tiny_avg, tiny_per = tiny()
	print(f"tiny -> {tiny_res}, avg_cpu={tiny_avg:.1f}%, per_cpu={tiny_per}")

	try:
		oops()
	except ValueError:
		print("oops raised ValueError (and was logged).")

	sleepy()
	print("alloc() len ->", alloc())

	chk_res, chk_avg, chk_per = checked()
	print(f"checked -> {chk_res}, avg_cpu={chk_avg:.1f}%, per_cpu_n={len(chk_per)}")

	# Temperatures (best-effort; may be n/a)
	print("temperature() ->")
	temperature()

	# --- Cleanup (keep logs, remove temp files)
	for p in [tmp, moved]:
		try:
			p.unlink()
		except Exception:
			pass

	print("\n=== Self-test complete. Logs in:", base.resolve(), "===\n")


if __name__ == "__main__":
	main()
