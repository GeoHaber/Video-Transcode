#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Scan • Select • Play – VLC Matrix (Designer Edition + Player Settings)

- Modern dark UI
- Top command bar: Scan Library, Open in Matrix, Settings
- Library panel:
	* Source folder picker (with Browse)
	* Rescan button
	* Settings button
	* Filter + Sort + Open Selected + item count
- Background scanning (non-blocking UI)
- Multi-select list → opens up to MAX_PREVIEW_PANES in VLC matrix

Matrix window:
	- Up to 12 panes
	- Single audio source selector
	- Global playback speed (with hotkeys)
	- Global seek
	- Global volume
	- All player defaults configurable in Settings:
		* Default playback speed
		* Default volume
		* Default audio pane (which pane has sound)
		* Autoplay on/off when matrix opens

Req:
	pip install PySide6 python-vlc
"""

import os
import sys
import time
import math
import json
import re
import threading
import traceback
from pathlib import Path
from typing import List, Tuple, Optional

import vlc
from PySide6 import QtCore, QtGui, QtWidgets

# ---------------- Configuration persistence ----------------

CONFIG_FILE = "scan_select_play_config.json"

# Default values (editable via Settings dialog)
SOURCE_DIRS: List[str] = [
	r"F:\Media\Movie",
]

EXCLUDE_KEYWORDS: List[str] = [
	"trailer", "biography", "deleted scenes", "making of", "featurette",
	"behind the scenes", "director", "gallery", "introduction", "profile",
	"sample", "preview", "extras", "bonus", "featurettes",
]

VIDEO_EXTENSIONS: List[str] = [
	"mp4", "mkv", "avi", "wmv", "flv", "mov", "mpg", "mpeg", "webm",
]

MAX_PREVIEW_PANES: int = 12

# Player defaults (also editable in Settings)
DEFAULT_PLAYBACK_RATE: float = 1.0     # 1.0x
DEFAULT_VOLUME: int = 80               # 0–100
DEFAULT_AUDIO_SOURCE_INDEX: int = 0    # 0 = first pane
DEFAULT_AUTOPLAY: bool = True          # auto-start playback when matrix opens

VLC_ARGS: Tuple[str, ...] = ("--quiet", "--no-video-title-show", "--no-osd")

SPIN_FRAMES = ("⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏")

IS_WIN = sys.platform.startswith("win")
IS_MAC = sys.platform == "darwin"
IS_LINUX = sys.platform.startswith("linux")

print_lock = threading.Lock()


def safe_print(msg: str) -> None:
	with print_lock:
		print(msg, flush=True)


def load_config() -> None:
	"""Load config from JSON into globals."""
	global SOURCE_DIRS, EXCLUDE_KEYWORDS, VIDEO_EXTENSIONS, MAX_PREVIEW_PANES
	global DEFAULT_PLAYBACK_RATE, DEFAULT_VOLUME, DEFAULT_AUDIO_SOURCE_INDEX, DEFAULT_AUTOPLAY

	try:
		if not os.path.isfile(CONFIG_FILE):
			return
		with open(CONFIG_FILE, "r", encoding="utf-8") as f:
			data = json.load(f)

		src = data.get("source_dirs")
		if isinstance(src, list):
			SOURCE_DIRS = [str(s) for s in src if str(s).strip()]

		excl = data.get("exclude_keywords")
		if isinstance(excl, list):
			EXCLUDE_KEYWORDS = [str(s) for s in excl if str(s).strip()]

		exts = data.get("video_extensions")
		if isinstance(exts, list):
			VIDEO_EXTENSIONS = [str(s).lstrip(".") for s in exts if str(s).strip()]

		mp = data.get("max_preview_panes")
		if mp is not None:
			try:
				MAX_PREVIEW_PANES = int(mp)
			except ValueError:
				pass

		dr = data.get("default_rate")
		if dr is not None:
			try:
				DEFAULT_PLAYBACK_RATE = float(dr)
			except ValueError:
				pass

		dv = data.get("default_volume")
		if dv is not None:
			try:
				DEFAULT_VOLUME = max(0, min(100, int(dv)))
			except ValueError:
				pass

		da = data.get("default_audio_source_index")
		if da is not None:
			try:
				idx = int(da)
				DEFAULT_AUDIO_SOURCE_INDEX = max(0, min(MAX_PREVIEW_PANES - 1, idx))
			except ValueError:
				pass

		ap = data.get("default_autoplay")
		if isinstance(ap, bool):
			DEFAULT_AUTOPLAY = ap

	except Exception as e:
		safe_print(f"Error loading config {CONFIG_FILE}: {e}")


def save_config() -> None:
	"""Save config globals to JSON."""
	try:
		data = {
			"source_dirs": SOURCE_DIRS,
			"exclude_keywords": EXCLUDE_KEYWORDS,
			"video_extensions": VIDEO_EXTENSIONS,
			"max_preview_panes": MAX_PREVIEW_PANES,
			"default_rate": DEFAULT_PLAYBACK_RATE,
			"default_volume": DEFAULT_VOLUME,
			"default_audio_source_index": DEFAULT_AUDIO_SOURCE_INDEX,
			"default_autoplay": DEFAULT_AUTOPLAY,
		}
		with open(CONFIG_FILE, "w", encoding="utf-8") as f:
			json.dump(data, f, indent=2)
	except Exception as e:
		safe_print(f"Error saving config {CONFIG_FILE}: {e}")


# ---------------- Filename parsing & scanning ----------------

YEAR_RE = re.compile(r"(19\d{2}|20\d{2})")


def parse_title_year(path: Path) -> Tuple[str, Optional[str]]:
	"""
	Extract a title and year from a filename.
	Example:  "Avatar.2009.1080p.mkv" -> ("Avatar", "2009")
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
		if kw.lower() in name:
			return True
	return False


def scan_sources_with_progress(dirs: List[str]) -> List[Path]:
	"""
	Scan SOURCE_DIRS for media files (blocking, but we run it in a QThread).
	Shows a spinner + stats in the console, returns filtered list of Paths.
	"""
	roots = [Path(d) for d in dirs if d]
	candidates: List[Path] = []
	for r in roots:
		if not r.exists():
			safe_print(f"Warning: source dir does not exist: {r}")
			continue
		candidates.extend(iter_video_files(r))
	total = len(candidates)
	safe_print(f"Found {total} media files in {len(roots)} folder(s).")
	if total == 0:
		return []
	kept: List[Path] = []
	start = time.time()
	last = start
	frame = 0
	for idx, p in enumerate(candidates, 1):
		if not exclude_match(p):
			kept.append(p)
		now = time.time()
		if (now - last) >= 0.05 or idx == total:
			frame = (frame + 1) % len(SPIN_FRAMES)
			rate = idx / max(0.001, now - start)
			rem = total - idx
			eta = int(rem / max(0.1, rate))
			with print_lock:
				sys.stdout.write(
					f"\r {SPIN_FRAMES[frame]} Scanning {idx}/{total} "
					f"({rate:0.1f}/s, ETA {eta}s)   "
				)
				sys.stdout.flush()
			last = now
	with print_lock:
		sys.stdout.write("\r ✓ Scanning complete. " + " " * 36 + "\n")
		sys.stdout.flush()
	safe_print(f"Kept {len(kept)} file(s) after keyword filter.")
	return kept


# ---------------- VLC Pane & Matrix ----------------

class Pane(QtWidgets.QWidget):
	"""
	One VLC video pane: title label + video widget + controls via python-vlc.
	We avoid aggressive .release() calls to reduce crashes on close.
	"""

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
		self.title.setStyleSheet(
			"QLabel{background:#141414;color:#f5f5f5;font-weight:bold;padding:4px 8px;}"
		)
		self.vbox.addWidget(self.title)

		self.video_frame = QtWidgets.QFrame()
		self.video_frame.setStyleSheet("QFrame{background:#000; border:1px solid #222;}")
		self.vbox.addWidget(self.video_frame, 1)

	def ensure_video_output(self):
		if self._video_output_set or not self.video_frame.isVisible():
			return
		try:
			wid = int(self.video_frame.winId())
			if IS_WIN:
				self.player.set_hwnd(wid)
			elif IS_MAC:
				self.player.set_nsobject(wid)
			else:
				self.player.set_xwindow(wid)
			self._video_output_set = True
		except Exception as e:
			safe_print(f"[Pane] set output error: {e}")

	def clear_media(self):
		"""Stop playback and reset label, but do NOT aggressively release VLC objects."""
		try:
			self.player.stop()
			self._media_obj = None
			self.title.setText(self.label_text)
		except Exception as e:
			safe_print(f"[Pane] clear_media error: {e}")

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
		try:
			self.player.play()
		except Exception as e:
			safe_print(f"[Pane] play error: {e}")

	def pause(self):
		try:
			self.player.set_pause(1)
		except Exception as e:
			safe_print(f"[Pane] pause error: {e}")

	def set_position(self, pos: float):
		try:
			self.player.set_position(max(0.0, min(1.0, pos)))
		except Exception as e:
			safe_print(f"[Pane] position error: {e}")

	def get_position(self) -> float:
		try:
			return float(self.player.get_position())
		except Exception:
			return 0.0

	def set_muted(self, mute: bool):
		try:
			self.player.audio_set_mute(bool(mute))
		except Exception as e:
			safe_print(f"[Pane] mute error: {e}")

	def set_rate(self, rate: float):
		try:
			self.player.set_rate(float(rate))
		except Exception as e:
			safe_print(f"[Pane] rate error: {e}")

	def set_volume(self, vol: int):
		try:
			v = max(0, min(100, int(vol)))
			self.player.audio_set_volume(v)
		except Exception as e:
			safe_print(f"[Pane] volume error: {e}")


def grid_for_count(n: int) -> Tuple[int, int]:
	if n <= 1:
		return (1, 1)
	if n == 2:
		return (1, 2)
	if n in (3, 4):
		return (2, 2)
	cols = math.ceil(math.sqrt(n))
	rows = math.ceil(n / cols)
	return (rows, cols)


class MatrixViewer(QtWidgets.QMainWindow):
	"""
	Window with up to MAX_PREVIEW_PANES video panes (VLC), plus global controls.
	"""

	def __init__(self, paths: List[Path]):
		super().__init__()
		self.setWindowTitle("VLC Matrix Viewer")
		self.resize(1720, 980)

		cw = QtWidgets.QWidget()
		self.setCentralWidget(cw)
		v = QtWidgets.QVBoxLayout(cw)
		v.setContentsMargins(8, 8, 8, 8)
		v.setSpacing(6)

		# --- Top Controls ---
		top = QtWidgets.QHBoxLayout()
		self.btn_playpause = QtWidgets.QPushButton("⏯  Play / Pause")
		self.btn_playpause.setIcon(self.style().standardIcon(QtWidgets.QStyle.SP_MediaPlay))
		self.btn_mute_all = QtWidgets.QPushButton("🔇  Mute All")
		self.lbl_audio = QtWidgets.QLabel("Audio from:")
		self.combo_audio = QtWidgets.QComboBox()

		self.btn_rate_dn = QtWidgets.QToolButton()
		self.btn_rate_dn.setText("−")
		self.btn_rate_dn.setToolTip("Slower 5%  ([)")
		self.btn_rate_rst = QtWidgets.QToolButton()
		self.btn_rate_rst.setText("1.00×")
		self.btn_rate_rst.setToolTip("Reset rate  (\\)")
		self.btn_rate_up = QtWidgets.QToolButton()
		self.btn_rate_up.setText("+")
		self.btn_rate_up.setToolTip("Faster 5%  (])")
		self.spin_rate = QtWidgets.QDoubleSpinBox()
		self.spin_rate.setDecimals(2)
		self.spin_rate.setMinimum(0.25)
		self.spin_rate.setMaximum(4.00)
		self.spin_rate.setSingleStep(0.05)
		self.spin_rate.setValue(DEFAULT_PLAYBACK_RATE)

		for w in (
			self.btn_playpause,
			self.btn_mute_all,
			self.lbl_audio,
			self.combo_audio,
			QtWidgets.QLabel("Rate:"),
			self.btn_rate_dn,
			self.btn_rate_rst,
			self.btn_rate_up,
			self.spin_rate,
		):
			top.addWidget(w)
		top.addStretch(1)
		v.addLayout(top)

		# --- Grid of panes ---
		self.grid_wrap = QtWidgets.QWidget()
		self.grid = QtWidgets.QGridLayout(self.grid_wrap)
		self.grid.setContentsMargins(0, 0, 0, 0)
		self.grid.setSpacing(6)
		v.addWidget(self.grid_wrap, 1)

		# --- Bottom bar: seek + volume ---
		bottom_bar = QtWidgets.QHBoxLayout()
		self.slider_seek = QtWidgets.QSlider(QtCore.Qt.Orientation.Horizontal)
		self.slider_seek.setRange(0, 1000)
		self.slider_seek.setToolTip("Global seek")

		bottom_bar.addWidget(QtWidgets.QLabel("Position:"))
		bottom_bar.addWidget(self.slider_seek, 1)

		bottom_bar.addSpacing(12)
		self.lbl_volume = QtWidgets.QLabel("Volume:")
		self.slider_volume = QtWidgets.QSlider(QtCore.Qt.Orientation.Horizontal)
		self.slider_volume.setRange(0, 100)
		self.slider_volume.setFixedWidth(160)
		self.slider_volume.setValue(DEFAULT_VOLUME)
		bottom_bar.addWidget(self.lbl_volume)
		bottom_bar.addWidget(self.slider_volume)

		v.addLayout(bottom_bar)

		# Wiring
		self.btn_playpause.clicked.connect(self._toggle_play_pause)
		self.btn_mute_all.clicked.connect(self._mute_all)
		self.combo_audio.currentIndexChanged.connect(self._apply_audio_idx)
		self.btn_rate_dn.clicked.connect(lambda: self._nudge_rate(-0.05))
		self.btn_rate_up.clicked.connect(lambda: self._nudge_rate(+0.05))
		self.btn_rate_rst.clicked.connect(lambda: self._set_rate_all(DEFAULT_PLAYBACK_RATE))
		self.spin_rate.valueChanged.connect(lambda val: self._set_rate_all(float(val)))

		QtGui.QShortcut(QtGui.QKeySequence(QtCore.Qt.Key_Space), self).activated.connect(
			self._toggle_play_pause
		)
		QtGui.QShortcut(QtGui.QKeySequence("["), self).activated.connect(
			lambda: self._nudge_rate(-0.05)
		)
		QtGui.QShortcut(QtGui.QKeySequence("]"), self).activated.connect(
			lambda: self._nudge_rate(+0.05)
		)
		QtGui.QShortcut(QtGui.QKeySequence("\\"), self).activated.connect(
			lambda: self._set_rate_all(DEFAULT_PLAYBACK_RATE)
		)

		self.slider_seek.sliderMoved.connect(self._seek_all)
		self.slider_seek.sliderPressed.connect(
			lambda: setattr(self, "_slider_user_drag", True)
		)
		self.slider_seek.sliderReleased.connect(self._slider_released)
		self.slider_volume.valueChanged.connect(self._set_volume_all)

		self._slider_user_drag = False
		self._updating_slider = False
		self.tmr = QtCore.QTimer(self)
		self.tmr.setInterval(250)
		self.tmr.timeout.connect(self._tick)
		self.tmr.start()

		self._panes: List[Pane] = []
		self._paths: List[Path] = []

		self.set_videos(paths)

	# Rate/volume helpers
	def _nudge_rate(self, delta: float):
		new_val = float(self.spin_rate.value()) + float(delta)
		new_val = max(0.25, min(4.0, new_val))
		self.spin_rate.blockSignals(True)
		self.spin_rate.setValue(new_val)
		self.spin_rate.blockSignals(False)
		self._set_rate_all(new_val)

	def _set_rate_all(self, rate: float):
		for p in self._panes:
			p.set_rate(rate)

	def _set_volume_all(self, vol: int):
		for p in self._panes:
			p.set_volume(int(vol))

	# Matrix setup
	def set_videos(self, paths: List[Path]):
		for p in self._panes:
			p.clear_media()
			p.setParent(None)
		self._panes = []
		self._paths = [p for p in paths if p and p.exists()][:MAX_PREVIEW_PANES]

		n = len(self._paths) or 1
		r, c = grid_for_count(n)
		for rr in range(max(r, 2)):
			self.grid.setRowStretch(rr, 1)
		for cc in range(max(c, 2)):
			self.grid.setColumnStretch(cc, 1)

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

		self._populate_audio()

		# Apply default audio pane from settings
		if self._paths:
			idx = max(0, min(DEFAULT_AUDIO_SOURCE_INDEX, len(self._paths) - 1))
			self.combo_audio.setCurrentIndex(idx)
			self._apply_audio_idx(idx)

		names = "  |  ".join([p.name for p in self._paths])
		self.setWindowTitle(f"VLC Matrix Viewer — {names}" if names else "VLC Matrix Viewer")

		QtCore.QTimer.singleShot(150, self._ensure_outputs_and_maybe_play)
		QtCore.QTimer.singleShot(
			250, lambda: self._set_rate_all(float(self.spin_rate.value()))
		)
		QtCore.QTimer.singleShot(
			270, lambda: self._set_volume_all(int(self.slider_volume.value()))
		)

	def showEvent(self, e: QtGui.QShowEvent) -> None:
		super().showEvent(e)
		QtCore.QTimer.singleShot(100, self._ensure_outputs_and_maybe_play)

	def _ensure_outputs_and_maybe_play(self):
		for p in self._panes:
			p.ensure_video_output()
		if DEFAULT_AUTOPLAY:
			QtCore.QTimer.singleShot(
				120, lambda: [pn.play() for pn in self._panes if pn.player.get_media()]
			)

	# Playback UI
	def _toggle_play_pause(self):
		if not self._panes:
			return
		p0 = self._panes[0].player
		st = p0.get_state()
		playing = (vlc.State.Playing, vlc.State.Buffering, vlc.State.Opening)
		if st in playing:
			for p in self._panes:
				try:
					if p.player.get_state() in playing:
						p.pause()
				except Exception:
					pass
		else:
			for p in self._panes:
				try:
					if p.player.get_media():
						p.play()
				except Exception:
					pass

	def _mute_all(self):
		for p in self._panes:
			p.set_muted(True)

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

	def _tick(self):
		if self._slider_user_drag or not self._panes:
			return
		try:
			pos = self._panes[0].get_position()
			self._updating_slider = True
			self.slider_seek.setValue(int(max(0.0, min(1.0, pos)) * 1000))
			self._updating_slider = False
		except Exception:
			pass

	def _slider_released(self):
		self._slider_user_drag = False
		self._seek_all(self.slider_seek.value())

	def _seek_all(self, val: int):
		if getattr(self, "_updating_slider", False):
			return
		pos = max(0.0, min(1.0, val / 1000.0))
		for p in self._panes:
			p.set_position(pos)

	def closeEvent(self, e: QtGui.QCloseEvent) -> None:
		"""Stop timer + stop players, but avoid over-releasing VLC objects."""
		self.tmr.stop()
		for p in self._panes:
			try:
				p.player.stop()
			except Exception:
				pass
		super().closeEvent(e)


# ---------------- Settings dialog for scan constants + player defaults ----------------

class SettingsDialog(QtWidgets.QDialog):
	"""
	Edits:
	  - SOURCE_DIRS
	  - EXCLUDE_KEYWORDS
	  - VIDEO_EXTENSIONS
	  - MAX_PREVIEW_PANES
	  - DEFAULT_PLAYBACK_RATE
	  - DEFAULT_VOLUME
	  - DEFAULT_AUDIO_SOURCE_INDEX
	  - DEFAULT_AUTOPLAY
	"""

	def __init__(self, parent: Optional[QtWidgets.QWidget] = None):
		super().__init__(parent)
		self.setWindowTitle("Settings")
		self.setMinimumWidth(520)

		layout = QtWidgets.QFormLayout(self)
		layout.setLabelAlignment(QtCore.Qt.AlignRight | QtCore.Qt.AlignVCenter)
		layout.setSpacing(8)

		# Library settings
		self.sources_edit = QtWidgets.QPlainTextEdit()
		self.sources_edit.setPlainText("\n".join(SOURCE_DIRS))
		self.sources_edit.setMinimumHeight(80)

		self.exclude_edit = QtWidgets.QLineEdit(", ".join(EXCLUDE_KEYWORDS))
		self.ext_edit = QtWidgets.QLineEdit(", ".join(VIDEO_EXTENSIONS))

		self.max_panes_spin = QtWidgets.QSpinBox()
		self.max_panes_spin.setRange(1, 32)
		self.max_panes_spin.setValue(MAX_PREVIEW_PANES)

		layout.addRow("Source folders (one per line):", self.sources_edit)
		layout.addRow("Exclude keywords:", self.exclude_edit)
		layout.addRow("Video extensions:", self.ext_edit)
		layout.addRow("Max preview panes:", self.max_panes_spin)

		# Player defaults
		self.rate_spin = QtWidgets.QDoubleSpinBox()
		self.rate_spin.setDecimals(2)
		self.rate_spin.setRange(0.25, 4.0)
		self.rate_spin.setSingleStep(0.05)
		self.rate_spin.setValue(DEFAULT_PLAYBACK_RATE)

		self.volume_spin = QtWidgets.QSpinBox()
		self.volume_spin.setRange(0, 100)
		self.volume_spin.setValue(DEFAULT_VOLUME)

		self.audio_index_spin = QtWidgets.QSpinBox()
		self.audio_index_spin.setRange(1, MAX_PREVIEW_PANES)
		self.audio_index_spin.setValue(DEFAULT_AUDIO_SOURCE_INDEX + 1)

		self.autoplay_check = QtWidgets.QCheckBox("Start playback automatically when matrix opens")
		self.autoplay_check.setChecked(DEFAULT_AUTOPLAY)

		layout.addRow("Default playback speed:", self.rate_spin)
		layout.addRow("Default volume (0–100):", self.volume_spin)
		layout.addRow("Default audio pane (1 = first):", self.audio_index_spin)
		layout.addRow("Autoplay matrix:", self.autoplay_check)

		buttons = QtWidgets.QDialogButtonBox(
			QtWidgets.QDialogButtonBox.Ok | QtWidgets.QDialogButtonBox.Cancel,
			QtCore.Qt.Orientation.Horizontal,
			self,
		)
		layout.addRow(buttons)
		buttons.accepted.connect(self.accept)
		buttons.rejected.connect(self.reject)

	def get_values(
		self,
	) -> Tuple[List[str], List[str], List[str], int, float, int, int, bool]:
		dirs = [ln.strip() for ln in self.sources_edit.toPlainText().splitlines() if ln.strip()]
		exclude = [x.strip() for x in self.exclude_edit.text().split(",") if x.strip()]
		exts = [x.strip().lstrip(".") for x in self.ext_edit.text().split(",") if x.strip()]
		max_panes = int(self.max_panes_spin.value())
		rate = float(self.rate_spin.value())
		volume = int(self.volume_spin.value())
		audio_1_based = int(self.audio_index_spin.value())
		autoplay = bool(self.autoplay_check.isChecked())
		return dirs, exclude, exts, max_panes, rate, volume, audio_1_based, autoplay


# ---------------- Background scanner worker (QThread) ----------------

class ScannerWorker(QtCore.QObject):
	finished = QtCore.Signal(list)   # list[str] as paths
	error = QtCore.Signal(str)

	def __init__(self, dirs: List[str]):
		super().__init__()
		self._dirs = dirs

	@QtCore.Slot()
	def run(self):
		try:
			files = scan_sources_with_progress(self._dirs)
			self.finished.emit([str(p) for p in files])
		except Exception as e:
			self.error.emit(str(e))


# ---------------- App-wide theme ----------------

def apply_dark_theme(app: QtWidgets.QApplication):
	"""Global dark theme with orange accent."""
	palette = QtGui.QPalette()

	base = QtGui.QColor(20, 20, 20)
	window = QtGui.QColor(24, 24, 24)
	alt = QtGui.QColor(32, 32, 32)
	text = QtGui.QColor(235, 235, 235)
	disabled_text = QtGui.QColor(130, 130, 130)
	accent = QtGui.QColor(255, 140, 0)

	palette.setColor(QtGui.QPalette.Window, window)
	palette.setColor(QtGui.QPalette.WindowText, text)
	palette.setColor(QtGui.QPalette.Base, base)
	palette.setColor(QtGui.QPalette.AlternateBase, alt)
	palette.setColor(QtGui.QPalette.ToolTipBase, base)
	palette.setColor(QtGui.QPalette.ToolTipText, text)
	palette.setColor(QtGui.QPalette.Text, text)
	palette.setColor(QtGui.QPalette.Button, alt)
	palette.setColor(QtGui.QPalette.ButtonText, text)
	palette.setColor(QtGui.QPalette.BrightText, QtCore.Qt.red)
	palette.setColor(QtGui.QPalette.Highlight, accent)
	palette.setColor(QtGui.QPalette.HighlightedText, QtCore.Qt.black)
	palette.setColor(QtGui.QPalette.Disabled, QtGui.QPalette.Text, disabled_text)
	palette.setColor(QtGui.QPalette.Disabled, QtGui.QPalette.ButtonText, disabled_text)

	app.setPalette(palette)

	app.setStyleSheet("""
		QWidget {
			font-family: Segoe UI, Arial, sans-serif;
			font-size: 10pt;
		}
		QGroupBox {
			margin-top: 6px;
			border: 1px solid #444;
			border-radius: 6px;
			padding: 6px;
		}
		QGroupBox::title {
			subcontrol-origin: margin;
			subcontrol-position: top left;
			padding: 0 8px;
			color: #ffb347;
			font-weight: 600;
		}
		QPushButton, QToolButton {
			padding: 5px 10px;
			border-radius: 4px;
			border: 1px solid #555;
			background-color: #333;
			color: #f0f0f0;
		}
		QPushButton:hover, QToolButton:hover {
			background-color: #3d3d3d;
		}
		QPushButton:disabled, QToolButton:disabled {
			background-color: #222;
			color: #777;
			border-color: #333;
		}
		QLineEdit, QPlainTextEdit {
			border: 1px solid #555;
			border-radius: 4px;
			padding: 4px 6px;
			background: #222;
			color: #eee;
		}
		QListWidget {
			border: 1px solid #444;
			border-radius: 4px;
			padding: 4px;
			background: #181818;
		}
		QListWidget::item {
			padding: 4px 6px;
		}
		QListWidget::item:selected {
			background: #ff8c00;
			color: #000;
		}
		QComboBox {
			border: 1px solid #555;
			border-radius: 4px;
			padding: 3px 6px;
			background: #222;
			color: #eee;
		}
		QSlider::groove:horizontal {
			height: 6px;
			background: #333;
			border-radius: 3px;
		}
		QSlider::handle:horizontal {
			width: 14px;
			margin: -4px 0;
			border-radius: 7px;
			background: #ff8c00;
		}
		QLabel#StatusLabel {
			background: #111;
			border-top: 1px solid #333;
		}
	""")


# ---------------- Main window: scan UI + list + matrix launcher ----------------

class MainWindow(QtWidgets.QMainWindow):
	def __init__(self, files: List[Path]):
		super().__init__()
		self.setWindowTitle("Scan • Select • Play – Media Matrix")
		self.resize(1300, 850)

		self.settings = QtCore.QSettings("GeoTools", "ScanSelectPlayVLC")
		self._scan_thread: Optional[QtCore.QThread] = None
		self._scan_worker: Optional[ScannerWorker] = None

		cw = QtWidgets.QWidget()
		self.setCentralWidget(cw)
		main_layout = QtWidgets.QVBoxLayout(cw)
		main_layout.setContentsMargins(8, 8, 8, 8)
		main_layout.setSpacing(6)

		# --- Command bar ---
		cmd_bar = QtWidgets.QHBoxLayout()
		cmd_bar.setSpacing(8)

		icon_style = self.style()
		self.btn_cmd_rescan = QtWidgets.QPushButton("Scan Library")
		self.btn_cmd_rescan.setIcon(icon_style.standardIcon(QtWidgets.QStyle.SP_BrowserReload))
		self.btn_cmd_open = QtWidgets.QPushButton("Open in Matrix")
		self.btn_cmd_open.setIcon(icon_style.standardIcon(QtWidgets.QStyle.SP_MediaPlay))
		self.btn_cmd_settings = QtWidgets.QPushButton("Settings")
		self.btn_cmd_settings.setIcon(icon_style.standardIcon(QtWidgets.QStyle.SP_FileDialogDetailedView))

		self.btn_cmd_rescan.setMinimumHeight(30)
		self.btn_cmd_open.setMinimumHeight(30)
		self.btn_cmd_settings.setMinimumHeight(30)

		title_label = QtWidgets.QLabel("Media Library")
		title_font = title_label.font()
		title_font.setPointSize(title_font.pointSize() + 2)
		title_font.setBold(True)
		title_label.setFont(title_font)
		title_label.setStyleSheet("color:#ffb347;")

		cmd_bar.addWidget(title_label)
		cmd_bar.addSpacing(20)
		cmd_bar.addWidget(self.btn_cmd_rescan)
		cmd_bar.addWidget(self.btn_cmd_open)
		cmd_bar.addWidget(self.btn_cmd_settings)
		cmd_bar.addStretch(1)

		main_layout.addLayout(cmd_bar)

		# --- Library / Filter panel ---
		library_group = QtWidgets.QGroupBox("Library")
		lib_layout = QtWidgets.QVBoxLayout(library_group)
		lib_layout.setSpacing(6)

		# Row 1: Source selection
		row1 = QtWidgets.QHBoxLayout()
		lbl_source = QtWidgets.QLabel("Source folder:")
		self.source_edit = QtWidgets.QLineEdit()
		self.source_edit.setPlaceholderText("Folder to scan…")
		if SOURCE_DIRS:
			self.source_edit.setText(SOURCE_DIRS[0])
		self.btn_browse_source = QtWidgets.QPushButton("Browse…")
		self.btn_browse_source.setFixedWidth(90)
		self.btn_rescan = QtWidgets.QPushButton("Rescan")
		self.btn_rescan.setFixedWidth(80)
		self.btn_settings = QtWidgets.QPushButton("Settings…")
		self.btn_settings.setFixedWidth(85)

		row1.addWidget(lbl_source)
		row1.addWidget(self.source_edit, 3)
		row1.addWidget(self.btn_browse_source)
		row1.addSpacing(6)
		row1.addWidget(self.btn_rescan)
		row1.addWidget(self.btn_settings)

		# Row 2: Filter + sort + open
		row2 = QtWidgets.QHBoxLayout()
		lbl_filter = QtWidgets.QLabel("Filter:")
		self.edit_filter = QtWidgets.QLineEdit()
		self.edit_filter.setPlaceholderText("Type to filter by title or year…")
		self.combo_sort = QtWidgets.QComboBox()
		self.combo_sort.addItems(["Title ↑", "Title ↓", "Year ↑", "Year ↓"])
		self.combo_sort.setCurrentIndex(int(self.settings.value("sort_index", 0)))
		self.edit_filter.setText(str(self.settings.value("last_filter", "")))
		self.btn_open_sel = QtWidgets.QPushButton("Open Selected in Matrix")
		self.btn_open_sel.setToolTip(f"Play up to {MAX_PREVIEW_PANES} videos at once")
		self.lbl_info = QtWidgets.QLabel("0 item(s)")

		row2.addWidget(lbl_filter)
		row2.addWidget(self.edit_filter, 3)
		row2.addWidget(self.combo_sort)
		row2.addSpacing(12)
		row2.addWidget(self.btn_open_sel, 2)
		row2.addStretch(1)
		row2.addWidget(self.lbl_info)

		lib_layout.addLayout(row1)
		lib_layout.addLayout(row2)

		main_layout.addWidget(library_group)

		# --- Library list ---
		self.list = QtWidgets.QListWidget()
		self.list.setSelectionMode(QtWidgets.QAbstractItemView.ExtendedSelection)
		self.list.setContextMenuPolicy(QtCore.Qt.CustomContextMenu)
		self.list.setAlternatingRowColors(True)
		self.list.setHorizontalScrollBarPolicy(QtCore.Qt.ScrollBarAsNeeded)
		main_layout.addWidget(self.list, 1)

		# --- Status bar-like label ---
		self.status = QtWidgets.QLabel("Ready")
		self.status.setObjectName("StatusLabel")
		self.status.setMinimumHeight(22)
		self.status.setAlignment(QtCore.Qt.AlignLeft | QtCore.Qt.AlignVCenter)
		main_layout.addWidget(self.status)

		# Data
		self._all_files: List[Path] = files[:]

		# Hooks – command bar + main controls share handlers
		self.btn_rescan.clicked.connect(self._rescan)
		self.btn_open_sel.clicked.connect(self._open_selected)
		self.btn_settings.clicked.connect(self._open_settings)

		self.btn_cmd_rescan.clicked.connect(self._rescan)
		self.btn_cmd_open.clicked.connect(self._open_selected)
		self.btn_cmd_settings.clicked.connect(self._open_settings)

		self.edit_filter.textChanged.connect(self._apply_filter)
		self.combo_sort.currentIndexChanged.connect(self._resort)
		self.list.customContextMenuRequested.connect(self._open_context_menu)
		self.btn_browse_source.clicked.connect(self._browse_source)
		self.source_edit.editingFinished.connect(self._source_changed)

		# Populate with initial files
		self._populate(files)
		if self.edit_filter.text().strip():
			self._apply_filter(self.edit_filter.text())
		else:
			self._resort(self.combo_sort.currentIndex())

	# ---- Settings & source handling ----

	def _open_settings(self):
		dlg = SettingsDialog(self)
		if dlg.exec() == QtWidgets.QDialog.Accepted:
			(
				dirs,
				excl,
				exts,
				max_panes,
				rate,
				volume,
				audio_1_based,
				autoplay,
			) = dlg.get_values()

			global SOURCE_DIRS, EXCLUDE_KEYWORDS, VIDEO_EXTENSIONS, MAX_PREVIEW_PANES
			global DEFAULT_PLAYBACK_RATE, DEFAULT_VOLUME, DEFAULT_AUDIO_SOURCE_INDEX, DEFAULT_AUTOPLAY

			SOURCE_DIRS = dirs or []
			EXCLUDE_KEYWORDS = excl or []
			VIDEO_EXTENSIONS = exts or []
			MAX_PREVIEW_PANES = max_panes

			DEFAULT_PLAYBACK_RATE = float(rate)
			DEFAULT_VOLUME = max(0, min(100, int(volume)))
			DEFAULT_AUDIO_SOURCE_INDEX = max(
				0, min(MAX_PREVIEW_PANES - 1, int(audio_1_based) - 1)
			)
			DEFAULT_AUTOPLAY = bool(autoplay)

			save_config()

			if SOURCE_DIRS:
				self.source_edit.setText(SOURCE_DIRS[0])
			else:
				self.source_edit.clear()

			self.status.setText("Settings updated. Click Scan Library / Rescan to apply.")

	def _browse_source(self):
		start_dir = self.source_edit.text().strip()
		if not start_dir and SOURCE_DIRS:
			start_dir = SOURCE_DIRS[0]
		if not start_dir:
			start_dir = os.path.expanduser("~")

		path = QtWidgets.QFileDialog.getExistingDirectory(
			self,
			"Select source folder",
			start_dir,
		)
		if path:
			self.source_edit.setText(path)
			self._source_changed()

	def _source_changed(self):
		global SOURCE_DIRS
		text = self.source_edit.text().strip()
		if text:
			SOURCE_DIRS = [text]
		else:
			SOURCE_DIRS = []
		save_config()
		self.status.setText("Source folder updated. Click Scan Library / Rescan to scan.")

	# ---- Context menu on list ----

	def _open_context_menu(self, pos: QtCore.QPoint):
		menu = QtWidgets.QMenu(self)
		act_toggle = menu.addAction("Toggle Select")
		act_select_all = menu.addAction("Select All Visible")
		act_clear_sel = menu.addAction("Clear Selection")
		menu.addSeparator()
		act_open = menu.addAction("Open Selected in Matrix")

		global_pos = self.list.viewport().mapToGlobal(pos)
		action = menu.exec(global_pos)
		if action is None:
			return

		if action == act_toggle:
			item = self.list.itemAt(pos)
			if item:
				item.setSelected( not item.isSelected())
		elif action == act_select_all:
			self.list.selectAll()
		elif action == act_clear_sel:
			self.list.clearSelection()
		elif action == act_open:
			self._open_selected()

	# ---- Sorting / filtering ----

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
			self._populate([])
			return
		reverse = idx in (1, 3)
		mode = idx
		files = sorted(self._all_files, key=lambda p: self._sort_key(p, mode), reverse=reverse)
		filt = self.edit_filter.text().strip().lower()
		if filt:
			show: List[Path] = []
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

	def _populate(self, files: List[Path]):
		self.list.clear()
		for p in files:
			title, year = parse_title_year(p)
			txt = f"{title} ({year})" if year else title
			it = QtWidgets.QListWidgetItem(txt)
			it.setData(QtCore.Qt.UserRole, str(p))
			self.list.addItem(it)
		self.lbl_info.setText(f"{self.list.count()} item(s)")
		if files:
			self.status.setText("Library loaded.")
		else:
			self.status.setText("No items. Click Scan Library to scan source folder.")

	# ---- Scanning in background ----

	def _rescan(self):
		if self._scan_thread is not None:
			QtWidgets.QMessageBox.information(self, "Scan",
											  "A scan is already running.")
			return
		if not SOURCE_DIRS:
			QtWidgets.QMessageBox.warning(
				self, "Scan", "No source folders configured.\nUse Settings or Source to set one."
			)
			return

		self.status.setText("Scanning… (see console for progress)")
		self.btn_rescan.setEnabled(False)
		self.btn_open_sel.setEnabled(False)
		self.btn_cmd_rescan.setEnabled(False)
		self.btn_cmd_open.setEnabled(False)

		self._scan_thread = QtCore.QThread(self)
		self._scan_worker = ScannerWorker(SOURCE_DIRS)
		self._scan_worker.moveToThread(self._scan_thread)

		self._scan_thread.started.connect(self._scan_worker.run)
		self._scan_worker.finished.connect(self._scan_finished)
		self._scan_worker.error.connect(self._scan_error)

		self._scan_worker.finished.connect(self._scan_thread.quit)
		self._scan_worker.finished.connect(self._scan_worker.deleteLater)
		self._scan_thread.finished.connect(self._scan_thread_finished)

		self._scan_thread.start()

	@QtCore.Slot(list)
	def _scan_finished(self, files_as_str: List[str]):
		self.btn_rescan.setEnabled(True)
		self.btn_open_sel.setEnabled(True)
		self.btn_cmd_rescan.setEnabled(True)
		self.btn_cmd_open.setEnabled(True)
		self._all_files = [Path(s) for s in files_as_str]
		self._resort(self.combo_sort.currentIndex())
		self.status.setText("Scan complete.")

	@QtCore.Slot(str)
	def _scan_error(self, msg: str):
		self.btn_rescan.setEnabled(True)
		self.btn_open_sel.setEnabled(True)
		self.btn_cmd_rescan.setEnabled(True)
		self.btn_cmd_open.setEnabled(True)
		QtWidgets.QMessageBox.critical(self, "Scan error", msg)
		self.status.setText(f"Scan error: {msg}")

	@QtCore.Slot()
	def _scan_thread_finished(self):
		self._scan_thread = None
		self._scan_worker = None

	# ---- Open in matrix ----

	def _open_selected(self):
		items = self.list.selectedItems()
		if not items:
			QtWidgets.QMessageBox.information(self, "Open", "Select 1 or more items.")
			return
		if len(items) > MAX_PREVIEW_PANES:
			QtWidgets.QMessageBox.information(
				self, "Open", f"Please select ≤ {MAX_PREVIEW_PANES} items."
			)
			return
		paths = [Path(it.data(QtCore.Qt.UserRole)) for it in items]
		mv = MatrixViewer(paths)
		mv.show()


# ---------------- Entry point ----------------

def main(argv: List[str]) -> int:
	safe_print("Scan • Select • Play – starting")
	load_config()

	cli_paths = [Path(a) for a in argv if a and not a.startswith("-")]
	app = QtWidgets.QApplication(sys.argv)
	apply_dark_theme(app)

	if cli_paths:
		mv = MatrixViewer([p for p in cli_paths if p.exists()][:MAX_PREVIEW_PANES])
		mv.show()
		return app.exec()

	files: List[Path] = []  # start empty; user controls Scan
	w = MainWindow(files)

	qs = QtCore.QSettings("GeoTools", "ScanSelectPlayVLC")
	geo = qs.value("main_geo")
	if geo is not None:
		w.restoreGeometry(geo)
	state = qs.value("main_state")
	if state is not None:
		w.restoreState(state)

	w.show()
	ret = app.exec()

	qs.setValue("main_geo", w.saveGeometry())
	qs.setValue("main_state", w.saveState())
	return ret


if __name__ == "__main__":
	try:
		sys.exit(main(sys.argv[1:]))
	except Exception as e:
		safe_print(f"Fatal error: {e}\n{traceback.format_exc()}")
		sys.exit(1)
