# -*- coding: utf-8 -*-
"""
My_utils_clean.py
-----------------
Small, editor-friendly helpers used by Trans_code_clean.py and FFMpeg.py.
No logging frameworks, just print/Tee; color is left to the caller.
"""

from __future__ import annotations

import os
import sys
import shutil
import traceback
from pathlib import Path
from typing import Optional

__all__ = [
	"ordinal",
	"get_tree_size",
	"copy_move",
	"hm_sz",
	"hm_time",
	"Tee",
]

def ordinal(n: int) -> str:
	"""Return integer with English ordinal suffix (1st, 2nd, 3rd, 4th...)."""
	if 10 <= (n % 100) <= 20:
		suff = "th"
	else:
		suff = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
	return f"{n}{suff}"

def get_tree_size(path: str | os.PathLike[str]) -> int:
	"""
	Recursively compute total size of a directory tree (bytes).
	Follows no symlinks and skips paths we cannot stat/read.
	"""
	total = 0
	try:
		for entry in os.scandir(path):
			try:
				if entry.is_file(follow_symlinks=False):
					total += entry.stat(follow_symlinks=False).st_size
				elif entry.is_dir(follow_symlinks=False):
					total += get_tree_size(entry.path)
			except (OSError, ValueError):
				continue
	except (OSError, ValueError) as e:
		print(f"get_tree_size exception: {e} path: {path}")
		print(traceback.format_exc())
	return total

def copy_move(src: str | os.PathLike[str], dst: str | os.PathLike[str], *, move: bool = False, overwrite: bool = False) -> bool:
	"""
	Copy or move a file from src to dst.

	Args:
		src: Source file path
		dst: Destination file path (file path or directory path)
		move: If True, move the file; otherwise copy it
		overwrite: If True, overwrite destination if it exists

	Returns:
		True on success, False on failure.
	"""
	src_path = Path(src)
	dst_path = Path(dst)

	if not src_path.is_file():
		print(f"Error: Source file {src} does not exist")
		return False

	if dst_path.exists() and dst_path.is_dir():
		dst_path = dst_path / src_path.name

	if dst_path.exists() and not overwrite:
		print(f"Error: Destination {dst_path} already exists and overwrite is False")
		return False

	try:
		dst_path.parent.mkdir(parents=True, exist_ok=True)
		if move:
			shutil.move(str(src_path), str(dst_path))
			print(f"Moved {src_path} -> {dst_path}")
		else:
			shutil.copy2(str(src_path), str(dst_path))
			print(f"Copied {src_path} -> {dst_path}")
		return True
	except Exception as e:
		print(f"Error during {'move' if move else 'copy'} from {src_path} to {dst_path}: {e}")
		print(traceback.format_exc())
		return False

def hm_sz(nbyte: Optional[int | float], unit: str = "B") -> str:
	"""
	Format bytes (or unit-scaled number) into a human string like '1.2 GB'.
	Accepts negative values; returns '0 B' for falsy inputs.
	"""
	suffix = ["", "K", "M", "G", "T", "P", "E", "Z", "Y"]
	if not nbyte:
		return f"0 {unit}"
	sign = "-" if float(nbyte) < 0 else ""
	value = abs(float(nbyte))
	idx = 0
	while value >= 1024.0 and idx < len(suffix) - 1:
		value /= 1024.0
		idx += 1
	return f"{sign}{round(value, 1)} {suffix[idx]}{unit}"

def hm_time(seconds: float) -> str:
	"""Convert seconds to 'HH:MM:SS'."""
	seconds = int(max(0, seconds))
	h = seconds // 3600
	m = (seconds % 3600) // 60
	s = seconds % 60
	return f"{h:02d}:{m:02d}:{s:02d}"

class Tee:
	"""
	Minimal stdout tee: duplicates writes to a file and the original stdout.
	Usage:
		with Tee("run.log"):
			print("this goes to console and log")
	"""
	def __init__(self, path: str | os.PathLike[str], mode: str = "w", encoding: str = "utf-8"):
		self._file = open(path, mode, encoding=encoding)
		self._stdout = sys.stdout

	def __enter__(self) -> "Tee":
		sys.stdout = self
		return self

	def __exit__(self, exc_type, exc, tb) -> bool:
		sys.stdout = self._stdout
		try:
			self._file.close()
		except Exception:
			pass
		return False

	def __del__(self):
		try:
			if hasattr(self, "_file") and not self._file.closed:
				self._file.close()
		except Exception:
			pass

	def write(self, data: str) -> int:
		try:
			self._file.write(data)
		except Exception:
			pass
		return self._stdout.write(data)

	def flush(self) -> None:
		try:
			self._file.flush()
		except Exception:
			pass
		try:
			self._stdout.flush()
		except Exception:
			pass

if __name__ == "__main__":
	print("Testing My_utils...")
	tmp = Path("tmp_utils_test.txt")
	tmp.write_text("ok", encoding="utf-8")
	print("ordinal(21):", ordinal(21))
	print("hm_sz(123456789):", hm_sz(123456789))
	print("hm_time(3661):", hm_time(3661))
	print("get_tree_size(.):", hm_sz(get_tree_size(".")))
	with Tee("tmp_utils_test.log"):
		print("tee works")
	copy_move(tmp, "tmp_utils_test_copy.txt", move=False, overwrite=True)
	for p in [tmp, Path("tmp_utils_test_copy.txt"), Path("tmp_utils_test.log")]:
		try: p.unlink()
		except Exception: pass
