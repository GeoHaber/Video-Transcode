#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

Rev = """
	Scan • Select • Play (VLC Matrix)

	- Scans SOURCE_DIRS for videos with a calm console spinner (rate + ETA)
	- Shows a filterable/sortable list as Title (Year)
	- Multi-select with Ctrl/Shift OR right-click context menu:
		Toggle Select, Select All Visible, Clear Selection, Open Selected in Matrix
	- Plays 1..12 items in a VLC matrix (1x1, 1x2, 2x2, near-square for 5+)
	- One pane outputs audio (choose via combo)
	- Global playback rate controls (−, reset, +), hotkeys: [ ] \
	- Seek slider at bottom; Volume slider at bottom (global)

	Req: pip install PySide6 python-vlc
	Tested on Windows 11; should work on macOS/Linux with VLC installed.
"""

import os
import re
import sys
import vlc
import time
import math

import threading
import traceback

from PySide6 import QtCore, QtGui, QtWidgets
from pathlib import Path
from typing import List, Tuple, Optional

# ========================== User-configurable constants ==========================

# Folders to scan (add more if needed)
SOURCE_DIRS: List[str] = [
		r"F:\Media\Movie",   # <-- change/add folders as you like
		]

# Skip files containing these keywords (case-insensitive)
EXCLUDE_KEYWORDS: List[str] = [
		"trailer", "biography", "deleted scenes", "making of", "featurette",
		"behind the scenes", "director", "gallery", "introduction", "profile",
		"sample", "preview", "extras", "bonus", "featurettes"
		]

# Allowed video extensions
VIDEO_EXTENSIONS: List[str] = ["mp4", "mkv", "avi", "wmv", "flv", "mov", "mpg", "mpeg", "webm"]

# Max items playable at once in the matrix
MAX_PREVIEW_PANES: int = 12

# VLC args — keep quiet OSD
VLC_ARGS: Tuple[str, ...] = ("--quiet", "--no-video-title-show", "--no-osd")

# Spinner frames for scanning
SPIN_FRAMES = ("⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏")

# ===============================================================================

IS_WIN		= sys.platform.startswith("win")
IS_MAC		= sys.platform == "darwin"
IS_LINUX	= sys.platform.startswith("linux")

print_lock = threading.Lock()
def safe_print(msg: str) -> None:
	with print_lock:	print(msg, flush=True)

def ensure_deps() -> None:
	"""Exit nicely if PySide6 or python-vlc are missing."""
	try:		import vlc  # noqa
	except Exception as e:
		safe_print("python-vlc is required. Install: pip install python-vlc")
		safe_print(f"Import error: {e}")
		sys.exit(1)
	try:		from PySide6 import QtCore, QtGui, QtWidgets  # noqa
	except Exception as e:
		safe_print("PySide6 is required. Install: pip install PySide6")
		safe_print(f"Import error: {e}")
		sys.exit(1)

ensure_deps()


# ===============================================================================

YEAR_RE = re.compile(r"(19\d{2}|20\d{2})")

def parse_title_year(path: Path) -> Tuple[str, Optional[str]]:
	"""
	Extract a clean Title and Year from the filename.
	"Rebel.Without.A.Cause.1955.2160p.UHD.BluRay.x265..." -> ("Rebel Without A Cause", "1955")
	"Avatar_2009_Extended.mkv" -> ("Avatar", "2009")
	"Some.Movie.mkv" -> ("Some Movie", None)
	"""
	name = path.stem
	clean = re.sub(r"[._\-]+", " ", name).strip()
	m = YEAR_RE.search(clean)
	year = None
	title_part = clean
	if m:
		year = m.group(1)
		title_part = clean[:m.start()].strip()
	if not title_part:
		title_part = clean
	title = re.sub(r"\s{2,}", " ", title_part).strip()
	if title and title.islower():
		title = title.capitalize()
	return title, year

# ===============================================================================

def iter_video_files(root: Path) -> List[Path]:
	exts = tuple("." + e.lower() for e in VIDEO_EXTENSIONS)
	files: List[Path] = []
	for dirpath, _dirnames, filenames in os.walk(root):
		for fn in filenames:
			if fn.lower().endswith(exts):
				files.append(Path(dirpath) / fn)
	return files

def exclude_match(path: Path) -> bool:
	name = path.name.lower()
	for kw in EXCLUDE_KEYWORDS:
		if kw in name:	return True
	return False

def scan_sources_with_progress(dirs: List[str]) -> List[Path]:
	"""Scan all dirs and print a single-line spinner with rate & ETA."""
	roots = [Path(d) for d in dirs if d]
	candidates: List[Path] = []
	for r in roots:
		if not r.exists():
			safe_print(f"Warning: source dir does not exist: {r}")
			continue
		candidates.extend(iter_video_files(r))
	total = len(candidates)
	safe_print(f"\nFound {total} media files in {len(roots)} folder(s).")
	if total == 0:
		return []

	kept: List[Path] = []
	start = time.time()
	last = start
	frame = 0
	for idx, p in enumerate(candidates, 1):
		if not exclude_match(p):	kept.append(p)
		now = time.time()
		if (now - last) >= 0.05 or idx == total:
			frame = (frame + 1) % len(SPIN_FRAMES)
			rate = idx / max(0.001, now - start)
			rem = total - idx
			eta = int(rem / max(0.1, rate))
			with print_lock:
				sys.stdout.write(f"\r {SPIN_FRAMES[frame]} Scanning {idx}/{total} ({rate:0.1f}/s, ETA {eta}s)   ")
				sys.stdout.flush()
			last = now
	with print_lock:
		sys.stdout.write("\r ✓ Scanning complete. " + " " * 36 + "\n")
		sys.stdout.flush()
	safe_print(f"Kept {len(kept)} file(s) after keyword filter.")
	return kept

# ===============================================================================

class Pane(QtWidgets.QWidget):
	def __init__(self, label: str, vlc_args: Tuple[str, ...] = VLC_ARGS):
		super().__init__()
		self.label_text = label
		self.vlc_instance = vlc.Instance(*vlc_args)
		self.player = self.vlc_instance.media_player_new()
		self._media_obj = None
		self._video_output_set = False

		self.vbox = QtWidgets.QVBoxLayout(self)
		self.vbox.setContentsMargins(0, 0, 0, 0)

		self.title = QtWidgets.QLabel(label)
		self.title.setStyleSheet("QLabel{background:#111;color:#eee;font-weight:bold;padding:4px;}")
		self.vbox.addWidget(self.title)

		self.video_frame = QtWidgets.QFrame()
		self.video_frame.setStyleSheet("QFrame{background:#000;}")
		self.vbox.addWidget(self.video_frame, 1)

	def ensure_video_output(self):
		if self._video_output_set or not self.video_frame.isVisible():
			return
		try:
			wid = int(self.video_frame.winId())
			if IS_WIN:		self.player.set_hwnd(wid)
			elif IS_MAC:	self.player.set_nsobject(wid)
			else:			self.player.set_xwindow(wid)
			self._video_output_set = True
		except Exception as e:	safe_print(f"[Pane] set output error: {e}")

	def clear_media(self):
		try:
			self.player.stop()
			m = self.player.get_media()
			if m: 	m.release()
			self._media_obj = None
			self.title.setText(self.label_text)
		except Exception as e:	safe_print(f"[Pane] clear_media error: {e}")

	def set_media(self, path: Optional[Path]):
		self.clear_media()
		if not path or not path.exists():
			self.title.setText(self.label_text + " (no media)")
			return
		try:
			m = self.vlc_instance.media_new_path(str(path))
			self.player.set_media(m)
			self._media_obj = m
			self.title.setText(self.label_text + f"  —  {path.name}")
		except Exception as e:
			safe_print(f"[Pane] set_media error: {e}")
			self.title.setText(self.label_text + " (error)")

	def play(self):
		try:		self.player.play()
		except Exception as e:	safe_print(f"[Pane] play error: {e}")

	def pause(self):
		try:		self.player.set_pause(1)
		except Exception as e:	safe_print(f"[Pane] pause error: {e}")

	def set_position(self, pos: float):
		try:			self.player.set_position(max(0.0, min(1.0, pos)))
		except Exception as e:	safe_print(f"[Pane] position error: {e}")

	def get_position(self) -> float:
		try:				return float(self.player.get_position())
		except Exception:	return 0.0

	def set_muted(self, mute: bool):
		try:			self.player.audio_set_mute(bool(mute))
		except Exception as e:	safe_print(f"[Pane] mute error: {e}")

	def set_rate(self, rate: float):
		try:			self.player.set_rate(float(rate))
		except Exception as e:	safe_print(f"[Pane] rate error: {e}")

	def set_volume(self, vol: int):
		try:
			v = max(0, min(100, int(vol)))
			self.player.audio_set_volume(v)
		except Exception as e:		safe_print(f"[Pane] volume error: {e}")

# ===============================================================================

def grid_for_count(n: int) -> Tuple[int, int]:
	if n <= 1:		return (1, 1)
	if n == 2:		return (1, 2)
	if n in (3, 4):	return (2, 2)
	cols = math.ceil(math.sqrt(n))
	rows = math.ceil(n / cols)
	return (rows, cols)

class MatrixViewer(QtWidgets.QMainWindow):
	def __init__(self, paths: List[Path], settings: Optional[QtCore.QSettings] = None):
		super().__init__()
		self.setWindowTitle("VLC Matrix Viewer")
		self.resize(1720, 980)

		self._settings = settings or QtCore.QSettings("GeoTools", "ScanSelectPlayVLC")

		cw = QtWidgets.QWidget()
		self.setCentralWidget(cw)
		v = QtWidgets.QVBoxLayout(cw)

		# --- Top Controls: Play/Pause, Mute, Audio source, Rate ---
		top					= QtWidgets.QHBoxLayout()
		self.btn_playpause	= QtWidgets.QPushButton("⏯ Play/Pause (Space)")
		self.btn_mute_all	= QtWidgets.QPushButton("Mute All")
		self.lbl_audio		= QtWidgets.QLabel("Audio:")
		self.combo_audio	= QtWidgets.QComboBox()

		# Rate controls
		self.btn_rate_dn	= QtWidgets.QPushButton("−")
		self.btn_rate_dn.setToolTip("Slower 5%  ( [ )")
		self.btn_rate_rst	= QtWidgets.QPushButton("1.00×")
		self.btn_rate_rst.setToolTip("Reset rate  ( \\ )")
		self.btn_rate_up	= QtWidgets.QPushButton("+")
		self.btn_rate_up.setToolTip("Faster 5%  ( ] )")
		self.spin_rate		= QtWidgets.QDoubleSpinBox()
		self.spin_rate.setDecimals(2)
		self.spin_rate.setMinimum(0.25)
		self.spin_rate.setMaximum(4.00)
		self.spin_rate.setSingleStep(0.05)
		self.spin_rate.setValue(float(self._settings.value("rate", 1.00)))

		for w in (self.btn_playpause, self.btn_mute_all, self.lbl_audio, self.combo_audio,
				  QtWidgets.QLabel("Rate:"), self.btn_rate_dn, self.btn_rate_rst, self.btn_rate_up, self.spin_rate):
			top.addWidget(w)
		top.addStretch(1)
		v.addLayout(top)

		# --- Grid ---
		self.grid_wrap	= QtWidgets.QWidget()
		self.grid		= QtWidgets.QGridLayout(self.grid_wrap)
		self.grid.setContentsMargins(0, 0, 0, 0)
		self.grid.setSpacing(4)
		v.addWidget(self.grid_wrap, 1)

		# --- Bottom Bar: Seek + Volume ---
		bottom_bar		= QtWidgets.QHBoxLayout()
		self.slider_seek= QtWidgets.QSlider(QtCore.Qt.Orientation.Horizontal)
		self.slider_seek.setRange(0, 1000)
		bottom_bar.addWidget(self.slider_seek, 1)

		bottom_bar.addSpacing(12)
		self.lbl_volume	= QtWidgets.QLabel("Vol:")
		self.slider_volume = QtWidgets.QSlider(QtCore.Qt.Orientation.Horizontal)
		self.slider_volume.setRange(0, 100)
		self.slider_volume.setFixedWidth(160)
		self.slider_volume.setValue(int(self._settings.value("volume", 80)))
		bottom_bar.addWidget(self.lbl_volume)
		bottom_bar.addWidget(self.slider_volume)
		v.addLayout(bottom_bar)

		# wiring
		self.btn_playpause.clicked.connect(self._toggle_play_pause)
		self.btn_mute_all.clicked.connect(self._mute_all)
		self.combo_audio.currentIndexChanged.connect(self._apply_audio_idx)

		# rate controls
		self.btn_rate_dn.clicked.connect(lambda: self._nudge_rate(-0.05))
		self.btn_rate_up.clicked.connect(lambda: self._nudge_rate(+0.05))
		self.btn_rate_rst.clicked.connect(lambda: self._set_rate_all(1.00))
		self.spin_rate.valueChanged.connect(lambda val: self._set_rate_all(float(val)))

		# hotkeys
		QtGui.QShortcut(QtGui.QKeySequence(QtCore.Qt.Key_Space), self).activated.connect(self._toggle_play_pause)
		QtGui.QShortcut(QtGui.QKeySequence("["), self).activated.connect(lambda: self._nudge_rate(-0.05))
		QtGui.QShortcut(QtGui.QKeySequence("]"), self).activated.connect(lambda: self._nudge_rate(+0.05))
		QtGui.QShortcut(QtGui.QKeySequence("\\"), self).activated.connect(lambda: self._set_rate_all(1.00))

		self.slider_seek.sliderMoved.connect(self._seek_all)
		self.slider_seek.sliderPressed.connect(lambda: setattr(self, "_slider_user_drag", True))
		self.slider_seek.sliderReleased.connect(self._slider_released)
		self.slider_volume.valueChanged.connect(self._set_volume_all)

		# timer for slider sync
		self._slider_user_drag = False
		self._updating_slider = False
		self.tmr		= QtCore.QTimer(self)
		self.tmr.setInterval(250)
		self.tmr.timeout.connect(self._tick)
		self.tmr.start()

		self._panes: List[Pane] = []
		self._paths: List[Path] = []

		self.set_videos(paths)

	# --- Rate/Volume helpers ---
	def _nudge_rate(self, delta: float):
		new_val = float(self.spin_rate.value()) + float(delta)
		new_val = max(0.25, min(4.0, new_val))
		self.spin_rate.blockSignals(True)
		self.spin_rate.setValue(new_val)
		self.spin_rate.blockSignals(False)
		self._set_rate_all(new_val)

	def _set_rate_all(self, rate: float):
		for p in self._panes:	p.set_rate(rate)
		self._settings.setValue("rate", float(rate))

	def _set_volume_all(self, vol: int):
		for p in self._panes:	p.set_volume(int(vol))
		self._settings.setValue("volume", int(vol))

	# --- Matrix population ---
	def set_videos(self, paths: List[Path]):
		# clear old
		for p in self._panes:
			p.clear_media()
			p.setParent(None)
		self._panes = []
		self._paths = [p for p in paths if p and p.exists()][:MAX_PREVIEW_PANES]

		n = len(self._paths) or 1
		r, c = grid_for_count(n)
		for rr in range(max(r, 2)):		self.grid.setRowStretch(rr, 1)
		for cc in range(max(c, 2)):		self.grid.setColumnStretch(cc, 1)

		for i in range(n):
			pane = Pane(f"Pane {i+1}")
			self._panes.append(pane)
			R, C = divmod(i, c)
			self.grid.addWidget(pane, R, C)
		if n == 3:
			filler = QtWidgets.QWidget()
			filler.setStyleSheet("background:#111;")
			self.grid.addWidget(filler, 1, 1)

		for i, pth in enumerate(self._paths):
			self._panes[i].set_media(pth)

		# audio dropdown
		self._populate_audio()
		self._apply_audio_idx(0)

		names = "  |  ".join([p.name for p in self._paths])
		self.setWindowTitle(f"VLC Matrix Viewer — {names}" if names else "VLC Matrix Viewer")

		QtCore.QTimer.singleShot(120, self._ensure_and_play)
		# apply saved rate/volume on start
		QtCore.QTimer.singleShot(200, lambda: self._set_rate_all(float(self._settings.value("rate", 1.00))))
		QtCore.QTimer.singleShot(220, lambda: self._set_volume_all(int(self._settings.value("volume", 80))))

	def showEvent(self, e: QtGui.QShowEvent) -> None:
		super().showEvent(e)
		QtCore.QTimer.singleShot(100, self._ensure_and_play)

	def _ensure_and_play(self):
		for p in self._panes:		p.ensure_video_output()
		QtCore.QTimer.singleShot(100, lambda: [pn.play() for pn in self._panes if pn.player.get_media()])

	# --- playback/ui wiring ---
	def _toggle_play_pause(self):
		if not self._panes:		return
		p0 = self._panes[0].player
		st = p0.get_state()
		playing = (vlc.State.Playing, vlc.State.Buffering, vlc.State.Opening)
		if st in playing:
			for p in self._panes:
				try:
					if p.player.get_state() in playing:
						p.pause()
				except Exception:	pass
		else:
			for p in self._panes:
				try:
					if p.player.get_media():
						p.play()
				except Exception:	pass

	def _mute_all(self):
		for p in self._panes:	p.set_muted(True)

	def _populate_audio(self):
		self.combo_audio.blockSignals(True)
		self.combo_audio.clear()
		for i, p in enumerate(self._paths):
			title, year = parse_title_year(p)
			label = f"{i+1}: {title} ({year})" if year else f"{i+1}: {title}"
			self.combo_audio.addItem(label)
		self.combo_audio.setCurrentIndex(0 if self._paths else -1)
		self.combo_audio.blockSignals(False)

	def _apply_audio_idx(self, idx: int):
		for i, p in enumerate(self._panes):
			p.set_muted(i != idx)
		if 0 <= idx < len(self._paths):
			title, year = parse_title_year(self._paths[idx])
			_ = (title, year)  # keep concise; no status bar spam

	def _tick(self):
		if self._slider_user_drag or not self._panes:	return
		try:
			pos = self._panes[0].get_position()
			self._updating_slider = True
			self.slider_seek.setValue(int(max(0.0, min(1.0, pos)) * 1000))
			self._updating_slider = False
		except Exception:	pass

	def _slider_released(self):
		self._slider_user_drag = False
		self._seek_all(self.slider_seek.value())

	def _seek_all(self, val: int):
		if getattr(self, "_updating_slider", False):	return
		pos = max(0.0, min(1.0, val / 1000.0))
		for p in self._panes:		p.set_position(pos)

	def closeEvent(self, e: QtGui.QCloseEvent) -> None:
		# persist last rate/volume
		self._settings.setValue("rate", float(self.spin_rate.value()))
		self._settings.setValue("volume", int(self.slider_volume.value()))
		for p in self._panes:
			try:				p.clear_media()
			except Exception:	pass
			try:				p.player.release()
			except Exception:	pass
			try:				p.vlc_instance.release()
			except Exception:	pass
		super().closeEvent(e)

# ===============================================================================

class MainWindow(QtWidgets.QMainWindow):
	def __init__(self, files: List[Path]):
		super().__init__()
		self.setWindowTitle("Keep 1080p or Best — Scanner & Preview")
		self.resize(1280, 840)

		self.settings = QtCore.QSettings("GeoTools", "ScanSelectPlayVLC")

		cw = QtWidgets.QWidget()
		self.setCentralWidget(cw)
		v = QtWidgets.QVBoxLayout(cw)

		# Top row
		top = QtWidgets.QHBoxLayout()
		self.btn_rescan		= QtWidgets.QPushButton("Rescan")
		self.btn_open_sel	= QtWidgets.QPushButton("Open Selected in Matrix")
		self.btn_open_sel.setToolTip(f"Pick up to {MAX_PREVIEW_PANES} files")
		self.edit_filter	= QtWidgets.QLineEdit()
		self.edit_filter.setPlaceholderText("Filter by title substring…")
		self.combo_sort		= QtWidgets.QComboBox()
		self.combo_sort.addItems(["Sort: Title ↑", "Sort: Title ↓", "Sort: Year ↑", "Sort: Year ↓"])
		# restore last sort/filter
		self.combo_sort.setCurrentIndex(int(self.settings.value("sort_index", 0)))
		self.edit_filter.setText(str(self.settings.value("last_filter", "")))
		self.lbl_info		= QtWidgets.QLabel("")
		for w in (self.btn_rescan, self.btn_open_sel, self.edit_filter, self.combo_sort, self.lbl_info):
			top.addWidget(w)
		v.addLayout(top)

		# List
		self.list = QtWidgets.QListWidget()
		self.list.setSelectionMode(QtWidgets.QAbstractItemView.ExtendedSelection)
		self.list.setContextMenuPolicy(QtCore.Qt.CustomContextMenu)
		v.addWidget(self.list, 1)

		# Status
		self.status = QtWidgets.QLabel("Ready")
		self.status.setStyleSheet("QLabel{color:#6cf;}")
		v.addWidget(self.status)

		# Data
		self._all_files: List[Path] = files[:]

		# Hooks
		self.btn_rescan.clicked.connect(self._rescan)
		self.btn_open_sel.clicked.connect(self._open_selected)
		self.edit_filter.textChanged.connect(self._apply_filter)
		self.combo_sort.currentIndexChanged.connect(self._resort)
		self.list.customContextMenuRequested.connect(self._open_context_menu)

		self._populate(files)
		# apply initial filter/sort
		if self.edit_filter.text().strip():
			self._apply_filter(self.edit_filter.text())
		else:
			self._resort(self.combo_sort.currentIndex())

	# ------ Context Menu ------
	def _open_context_menu(self, pos: QtCore.QPoint):
		menu = QtWidgets.QMenu(self)
		act_toggle		= menu.addAction("Toggle Select")
		act_select_all	= menu.addAction("Select All Visible")
		act_clear_sel	= menu.addAction("Clear Selection")
		menu.addSeparator()
		act_open		= menu.addAction("Open Selected in Matrix")

		global_pos = self.list.viewport().mapToGlobal(pos)
		action = menu.exec(global_pos)
		if action is None:	return

		if action == act_toggle:
			item = self.list.itemAt(pos)
			if item:			item.setSelected(not item.isSelected())
		elif action == act_select_all:	self.list.selectAll()
		elif action == act_clear_sel:	self.list.clearSelection()
		elif action == act_open:		self._open_selected()

	# ------ Sorting/Filtering ------
	def _sort_key(self, p: Path, mode: int):
		title, year = parse_title_year(p)
		year_val = int(year) if (year and year.isdigit()) else -1
		if mode in (0, 1):  # Title
			return (title.lower(), year_val)
		else:               # Year
			return (year_val, title.lower())

	def _resort(self, idx: int):
		self.settings.setValue("sort_index", int(idx))
		if not self._all_files:
			return
		reverse = idx in (1, 3)
		mode = idx  # 0 T↑, 1 T↓, 2 Y↑, 3 Y↓
		files = sorted(self._all_files, key=lambda p: self._sort_key(p, mode), reverse=reverse)
		# keep current filter
		filt = self.edit_filter.text().strip().lower()
		if filt:
			show = []
			for p in files:
				t, y = parse_title_year(p)
				if filt in f"{t} {y or ''}".lower():
					show.append(p)
			self._populate(show)
		else:
			self._populate(files)

	def _apply_filter(self, text: str):
		text = (text or "").strip().lower()
		self.settings.setValue("last_filter", text)
		base = self._all_files[:]
		# keep current sort
		idx = self.combo_sort.currentIndex()
		reverse = idx in (1, 3)
		base = sorted(base, key=lambda p: self._sort_key(p, idx), reverse=reverse)
		if not text:
			self._populate(base)
			return
		filtered: List[Path] = []
		for p in base:
			title, year = parse_title_year(p)
			hay = f"{title} {year or ''}".lower()
			if text in hay:
				filtered.append(p)
		self._populate(filtered)

	# ------ Populate list ------
	def _populate(self, files: List[Path]):
		self.list.clear()
		for p in files:
			title, year = parse_title_year(p)
			txt = f"{title} ({year})" if year else title
			it = QtWidgets.QListWidgetItem(txt)
			it.setData(QtCore.Qt.UserRole, str(p))
			self.list.addItem(it)
		self.lbl_info.setText(f"{self.list.count()} item(s)")
		self.status.setText("Loaded.")

	# ------ Scan / Open ------
	def _rescan(self):
		self.status.setText("Scanning… (see console for progress)")
		QtWidgets.QApplication.processEvents()
		files = scan_sources_with_progress(SOURCE_DIRS)
		self._all_files = files[:]
		self._resort(self.combo_sort.currentIndex())
		self.status.setText("Scan complete.")

	def _open_selected(self):
		items = self.list.selectedItems()
		if not items:
			QtWidgets.QMessageBox.information(self, "Open", "Select 1 or more items.")
			return
		if len(items) > MAX_PREVIEW_PANES:
			QtWidgets.QMessageBox.information(self, "Open", f"Please select ≤ {MAX_PREVIEW_PANES} items.")
			return
		paths = [Path(it.data(QtCore.Qt.UserRole)) for it in items]
		mv = MatrixViewer(paths, settings=self.settings)
		mv.show()

# ===============================================================================

def main(argv: List[str]) -> int:
	# If file paths passed as args, open matrix directly
	safe_print(Rev)
	cli_paths = [Path(a) for a in argv if a and not a.startswith("-")]
	if cli_paths:
		app = QtWidgets.QApplication(sys.argv)
		mv = MatrixViewer([p for p in cli_paths if p.exists()][:MAX_PREVIEW_PANES])
		mv.show()
		return app.exec()

	safe_print("=== Media Scanner Starting ===")
	files = scan_sources_with_progress(SOURCE_DIRS)

	app = QtWidgets.QApplication(sys.argv)
	w = MainWindow(files)
	# restore window geometry/state
	settings = QtCore.QSettings("GeoTools", "ScanSelectPlayVLC")
	if (geo := settings.value("main_geo")) is not None:
		w.restoreGeometry(geo)
	if (state := settings.value("main_state")) is not None:
		w.restoreState(state)
	w.show()
	ret = app.exec()
	# save geometry/state
	settings.setValue("main_geo", w.saveGeometry())
	settings.setValue("main_state", w.saveState())
	return ret

if __name__ == "__main__":
	try:	sys.exit(main(sys.argv[1:]))
	except Exception as e:
		safe_print(f"Fatal error: {e}\n{traceback.format_exc()}")
		sys.exit(1)
