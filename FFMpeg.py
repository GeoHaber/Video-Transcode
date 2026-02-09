# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import re
import gc
import sys
import json
import time
import shlex

import atexit
import signal
import tempfile
import threading
import traceback
import subprocess as sp

import random
import string
import shutil
import platform

from typing 		import Any, Dict, List, Optional, Tuple, Callable, Iterable, Union
from pathlib 		import Path
from fractions 		import Fraction
from dataclasses 	import dataclass, field
from collections 	import defaultdict

import Utils
from Utils import (
    WORK_DIR, RUN_TMP, SCRIPT_DIR, EXCEPT_DIR,
    FFMPEG, FFPROBE, SKIP_KEY, TMPF_EX,
    HEVC_BPP, SIZE_OK_MARGIN, ALWAYS_10BIT, HW_10BIT_ENCOD, TAG_HEVC_AS_HVC1,
    Default_lng, Keep_langua, ENSURE_ENG_SUB, ADD_AI_SUB_LANGS, ADD_AI_AUD_LANGS,
    AUTO_SIZE_GUARD, INFLATE_MAX_BY, MAX_ABS_GROW_MB, FORCE_BIGGER,
    MIN_SIZE_RATIO_FLOOR, DURATION_TOLERANCE_ABS, DURATION_TOLERANCE_PCT,
    ALLOW_GROWTH_SAME_RES_PCT,
    ADD_ADDITIONAL, FORCE_ARTIFACTS_ON_SKIP,
    ADD_ARTIFACT_MATRIX, ADD_ARTIFACT_SPEED, ADD_ARTIFACT_SHORT,
    ADDITIONAL_MATRIX_COLS, ADDITIONAL_MATRIX_ROWS, ADDITIONAL_MATRIX_WIDTH,
    ADDITIONAL_MATRIX_START_TIME, ADDITIONAL_MATRIX_SKIP_PCT_START,
    ADDITIONAL_MATRIX_SKIP_PCT_END,
    ADDITIONAL_SHORT_SKP_STRT, ADDITIONAL_SHORT_DUR,
    ADDITIONAL_SPEED_FACTOR, ADDITIONAL_QUALITY_CRF, ADDITIONAL_PRESET,
    SMART_RENAME, CHECK_CORRUPTION,
    PROBE_TIMEOUT_S, STAGE_TIMEOUT_S, REMUX_TIMEOUT_S,
    CORRUPTION_CHECK_TIMEOUT_S,
    CREATE_NEW_PROCESS_GROUP,
    ERROR_LOGS_ENABLED, ERROR_LOG_MAX_LINES,
    SCAN_PARALLEL, MAX_SCAN_WORKRS,
    de_bug, print_lock, progress_lock, progress_state,
    REQUIRED_LIBS,
    safe_print, errlog_block, retry_with_lock_info, copy_move,
    hm_sz, hm_tm, ensure_libs,
    LLM_AVAILABLE, llm_query,
)

# Ensure required libraries are installed
ensure_libs(REQUIRED_LIBS)

try:
	import charset_normalizer
	CHARSET_NORMALIZER_AVAILABLE = True
except ImportError:
	CHARSET_NORMALIZER_AVAILABLE = False

IS_WIN = sys.platform.startswith("win")

# =============================================================================
# 1. HARDWARE & CONFIG
# =============================================================================

def _test_encoder(encoder: str) -> bool:
	"""Tests if a hardware encoder actually works on this system."""
	try:
		# Use 1920x1080 - AMD AMF requires minimum resolution
		cmd = [
			"ffmpeg", "-hide_banner", "-f", "lavfi", "-i", "color=black:s=1920x1080:d=0.1",
			"-c:v", encoder, "-frames:v", "1", "-f", "null", "-"
		]
		result = sp.run(cmd, capture_output=True, timeout=15)
		return result.returncode == 0
	except Exception:
		return False

def detect_hardware_encoder() -> str:
	"""Detects available hardware encoder by testing each in priority order."""
	# Priority order: NVIDIA > AMD > Intel QSV > Software
	hw_priority = [
		("hevc_nvenc", "NVIDIA"),
		("hevc_amf", "AMD"),
		("hevc_qsv", "Intel QSV"),
	]

	for encoder, name in hw_priority:
		if _test_encoder(encoder):
			print(f"[HW Encoder] Using {name}: {encoder}")
			return encoder

	print("[HW Encoder] No working hardware encoder, using software: libx265")
	return "libx265"

CURRENT_ENCODER = detect_hardware_encoder()
BIT_PER_PIX     = Utils.HEVC_BPP

# --- UNIFIED CONFIGURATION FROM UTILS ---
FFMPEG          = Utils.FFMPEG
FFPROBE         = Utils.FFPROBE
PROBE_TIMEOUT_S = Utils.PROBE_TIMEOUT_S
CORRUPTION_CHECK_TIMEOUT_S = Utils.CORRUPTION_CHECK_TIMEOUT_S

USE_TWO_PASS    = True  # Enables 2-pass encoding for supported encoders (libx265)

# Resolution and scaling constants
MAX_WIDTH_BEFORE_DOWNSCALE = 2600   # 4K+ down to 1080p threshold
MAX_HEIGHT_BEFORE_DOWNSCALE = 1188  # 1200p+ down to 1080p threshold
TARGET_HEIGHT_1080P = 1080          # Standard 1080p target

# Artifact generation timeouts
MATRIX_GENERATION_TIMEOUT_S = 300   # 5 minutes
SPEEDUP_GENERATION_TIMEOUT_S = 600  # 10 minutes

# File size and validation
MIN_OUTPUT_FILE_SIZE = 1024  # Minimum viable output file size in bytes

# =============================================================================
# 2. PROCESS MANAGEMENT
# =============================================================================

if IS_WIN:
	import ctypes
	kernel32 = ctypes.WinDLL('kernel32', use_last_error=True)
	PROCESS_ALL_ACCESS = 0x1F0FFF

	class JOBOBJECT_BASIC_LIMIT_INFORMATION(ctypes.Structure):
		_fields_ = [("PerProcessUserTimeLimit",	ctypes.c_longlong),
					("PerJobUserTimeLimit",		ctypes.c_longlong),
					("LimitFlags",				ctypes.c_uint32),
					("MinimumWorkingSetSize",	ctypes.c_size_t),
					("MaximumWorkingSetSize",	ctypes.c_size_t),
					("ActiveProcessLimit",		ctypes.c_uint32),
					("Affinity",				ctypes.c_size_t),
					("PriorityClass",			ctypes.c_uint32),
					("SchedulingClass",			ctypes.c_uint32),
				]

	class IO_COUNTERS(ctypes.Structure):
		_fields_ = [("ReadOperationCount",		ctypes.c_ulonglong),
					("WriteOperationCount",		ctypes.c_ulonglong),
					("OtherOperationCount",		ctypes.c_ulonglong),
					("ReadTransferCount",		ctypes.c_ulonglong),
					("WriteTransferCount",		ctypes.c_ulonglong),
					("OtherTransferCount",		ctypes.c_ulonglong),
				]

	class JOBOBJECT_EXTENDED_LIMIT_INFORMATION(ctypes.Structure):
		_fields_ = [("BasicLimitInformation",	JOBOBJECT_BASIC_LIMIT_INFORMATION),
					("IoInfo",					IO_COUNTERS),
					("ProcessMemoryLimit",		ctypes.c_size_t),
					("JobMemoryLimit",			ctypes.c_size_t),
					("PeakProcessMemoryUsed",	ctypes.c_size_t),
					("PeakJobMemoryUsed",		ctypes.c_size_t),
				]

	class _WinJob:
		JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x00002000
		JobObjectExtendedLimitInformation = 9

		def __init__(self) -> None:
			self.hJob = kernel32.CreateJobObjectW(None, None)
			if not self.hJob:		raise OSError("CreateJobObjectW failed")

			info = JOBOBJECT_EXTENDED_LIMIT_INFORMATION()
			info.BasicLimitInformation.LimitFlags = self.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE

			ok = kernel32.SetInformationJobObject(
				self.hJob,
				self.JobObjectExtendedLimitInformation,
				ctypes.byref(info),
				ctypes.sizeof(info),
			)
			if not ok:
				kernel32.CloseHandle(self.hJob)
				self.hJob = None
				raise OSError("SetInformationJobObject failed")

		def add_pid(self, pid: int) -> None:
			if not self.hJob:	return
			hProc = kernel32.OpenProcess(PROCESS_ALL_ACCESS, False, pid)
			if not hProc:		return
			try:				kernel32.AssignProcessToJobObject(self.hJob, hProc)
			finally:			kernel32.CloseHandle(hProc)

		def close(self) -> None:
			if self.hJob:
				kernel32.CloseHandle(self.hJob)
				self.hJob = None
else:
	_WinJob = None

class ChildProcessManager:
	"""Registry for subprocesses to ensure cleanup on exit using OS-level jobs."""
	def __init__(self) -> None:
		self._procs: List[sp.Popen] = []
		self._lock = threading.Lock()
		self._stopping = threading.Event()
		self._win_job = _WinJob() if IS_WIN else None
		
		# Register cleanup handlers
		atexit.register(self.terminate_all)
		if IS_WIN and self._win_job:
			atexit.register(self._win_job.close)

		def signal_handler(signum, frame):
			if not self._stopping.is_set():
				self._stopping.set()
				self.terminate_all()
			sys.exit(1)

		try:
			signal.signal(signal.SIGINT, signal_handler)
			signal.signal(signal.SIGTERM, signal_handler)
		except Exception:
			pass

	def register(self, proc: sp.Popen) -> None:
		with self._lock:
			self._procs.append(proc)
		if IS_WIN and self._win_job:
			try:
				self._win_job.add_pid(proc.pid)
			except Exception:
				pass

	def unregister(self, proc: sp.Popen) -> None:
		with self._lock:
			if proc in self._procs:
				self._procs.remove(proc)

	def terminate_all(self) -> None:
		with self._lock:
			procs = list(self._procs)
			self._procs.clear()
		
		if not procs:
			return

		# Try gentle termination first
		for p in procs:
			try:
				if IS_WIN:
					p.send_signal(signal.CTRL_BREAK_EVENT)
				else:
					os.killpg(os.getpgid(p.pid), signal.SIGTERM)
			except Exception:
				pass

		# Brief wait for graceful exit
		time.sleep(0.5)

		# Force kill any survivors
		for p in procs:
			try:
				if p.poll() is None:
					p.kill()
			except Exception:
				pass

PROC_MGR = ChildProcessManager()

def _popen_managed(cmd: List[str], **kwargs) -> sp.Popen:
	"""Starts a subprocess and registers it for cleanup with Job Object support."""
	if IS_WIN:
		flags = kwargs.pop("creationflags", 0) | sp.CREATE_NEW_PROCESS_GROUP
		kwargs["creationflags"] = flags
	else:
		kwargs.setdefault("preexec_fn", os.setsid)

	kwargs.setdefault("text", True)
	kwargs.setdefault("encoding", "utf-8")
	kwargs.setdefault("errors", "replace")

	proc = sp.Popen(cmd, **kwargs)
	PROC_MGR.register(proc)
	return proc

# =============================================================================
# 3. HELPERS
# =============================================================================

def probe_worker(path_str: str, de_bug: bool = False) -> Dict:
	"""Worker function for concurrent probing."""
	mf = MediaFile(Path(path_str))
	# RESPECT CONFIG: Use Utils.CHECK_CORRUPTION (False by default in Utils.py usually)
	# This allows disabling the slow corruption check for speed.
	mf.probe(de_bug, check_corruption=Utils.CHECK_CORRUPTION)
	# Return dict representation for cache/serialization compatibility
	d = mf.to_dict()
	# Add these explicitly	# Return dict representation for cache/serialization compatibility
	return (d.get("metadata"), d.get("is_corrupted"), d.get("error_msg"))

def analyze_error_with_llm(cmd: List[str], log_tail: List[str]) -> str:
	"""Uses Local_LLM to explain why an FFmpeg command failed."""
	if not LLM_AVAILABLE:
		return "LLM not available (local_llm not installed)."

	full_log = "".join(log_tail)
	full_cmd = " ".join(cmd)

	prompt = (
		"I am an FFmpeg Transcoding script. One of my commands failed.\n"
		f"Command executed:\n{full_cmd}\n\n"
		f"FFmpeg Output (end of log):\n{full_log}\n\n"
		"Please explain:\n"
		"1. What exactly went wrong?\n"
		"2. How can I fix it?\n"
		"Keep the explanation technical but concise."
	)
	system = "You are an FFmpeg expert. Analyze the failure and provide a concise explanation and solution."

	result = llm_query(prompt, system=system, max_tokens=512)
	if result is None:
		return "LLM Analysis unavailable (engine returned None)."
	return result.strip()

def suggest_clean_name_with_llm(filename: str) -> str:
	"""Uses Local_LLM to suggest a human-friendly filename."""
	if not LLM_AVAILABLE:
		return filename

	prompt = (
		f'I have a technical media filename:\n"{filename}"\n\n'
		"Please extract the human-friendly title and year.\n"
		"Rules:\n"
		'1. For movies: "Title (Year)"\n'
		'2. For TV shows: "Title - SxxExx - Optional Episode Name"\n'
		"3. Remove all release tags (e.g., 1080p, x264, 10bit, PSA, RARBG).\n"
		"4. Capitalize appropriately.\n"
		"5. Output ONLY the cleaned name, no introduction or quotes.\n\n"
		"Clean Name:"
	)
	system = "You are a media metadata expert. Your job is to clean technical filenames into human-readable titles."

	try:
		clean_name = llm_query(prompt, system=system, max_tokens=128)
		if clean_name is None:
			return filename
		clean_name = clean_name.strip()
		# Basic sanitization for filesystem
		for char in '<>:"/\\|?*':
			clean_name = clean_name.replace(char, '')
		return clean_name
	except Exception:
		return filename

# =============================================================================
# 4. METADATA & PROBING
# =============================================================================

@dataclass
class MediaFile:
	"""The Source of Truth for a single media file's lifecycle."""
	path: Path
	# Metadata
	width: int = 0
	height: int = 0
	duration: float = 0.0
	bitrate: int = 0
	size: int = 0
	codec_v: str = ""
	src_fps: float = 0.0
	streams: List[Dict] = field(default_factory=list)
	format_tags: Dict = field(default_factory=dict)
	estimated_video_bitrate: int = 0

	# Processing state
	cmd: List[str] = field(default_factory=list)
	skip: bool = False
	reasons: List[str] = field(default_factory=list)
	logs: List[str] = field(default_factory=list)
	is_corrupted: bool = False
	error_msg: str = ""
	metadata: Dict = field(default_factory=dict) # For caching/serialization

	def __post_init__(self):
		if isinstance(self.path, str):
			self.path = Path(self.path)

	def to_dict(self) -> Dict:
		"""Converts the MediaFile object to a dictionary for serialization."""
		return {
			"path": str(self.path),
			"width": self.width,
			"height": self.height,
			"duration": self.duration,
			"bitrate": self.bitrate,
			"size": self.size,
			"codec_v": self.codec_v,
			"src_fps": self.src_fps,
			"streams": self.streams,
			"format_tags": self.format_tags,
			"is_corrupted": self.is_corrupted,
			"error_msg": self.error_msg,
			"metadata": self.metadata, # This will contain the raw ffprobe output
		}

	@classmethod
	def from_dict(cls, d: Dict) -> 'MediaFile':
		"""Reconstructs from cached dictionary."""
		m = cls(path=Path(d.get("path", "")))

		# Try to load from "serialized" fields first
		m.width = d.get("width", 0)
		m.height = d.get("height", 0)
		m.duration = d.get("duration", 0.0)
		m.bitrate = d.get("bitrate", 0)
		m.size = d.get("size", 0)
		m.codec_v = d.get("codec_v", "")
		m.src_fps = d.get("src_fps", 23.976)
		m.streams = d.get("streams", [])
		m.format_tags = d.get("format_tags", {})
		m.is_corrupted = d.get("is_corrupted", False)
		m.error_msg = d.get("error_msg", "")
		m.metadata = d.get("metadata", {})

		# CRITICAL FIX: If fields are empty but we have raw metadata (from scan_folder cache structure), repopulate
		if not m.streams and m.metadata:
			data = m.metadata
			fmt = data.get("format", {})
			m.duration = float(fmt.get("duration", 0.0)) or m.duration
			m.bitrate = int(fmt.get("bit_rate", 0)) or m.bitrate
			m.size = int(fmt.get("size", 0)) or m.size
			m.format_tags = fmt.get("tags", {})

			m.streams = data.get("streams", [])
			video_streams = [s for s in m.streams if s.get("codec_type") == "video"]
			if video_streams:
				vid = video_streams[0]
				m.width = int(vid.get("width", 0))
				m.height = int(vid.get("height", 0))
				m.codec_v = vid.get("codec_name", "")
				try:
					r_fps = vid.get("r_frame_rate", "24/1")
					if "/" in r_fps:
						num, den = map(int, r_fps.split("/"))
						m.src_fps = num / den if den > 0 else 23.976
					else:
						m.src_fps = float(r_fps)
				except Exception:
					m.src_fps = 23.976
		return m

	def probe(self, de_bug: bool = False, check_corruption: bool = False):
		"""Probes the media file to extract metadata."""
		self.logs.append(f"   .Probing: {self.path.name}")
		cmd = [FFPROBE, "-v", "quiet", "-print_format", "json", "-show_format", "-show_streams", str(self.path)]

		try:
			p = _popen_managed(cmd, stdout=sp.PIPE, stderr=sp.PIPE)
			out, err = p.communicate(timeout=PROBE_TIMEOUT_S)
			PROC_MGR.unregister(p)

			if p.returncode != 0:
				self.is_corrupted = True
				self.error_msg = f"FFprobe failed with code {p.returncode}: {err.strip()}"
				self.logs.append(f"\033[91m   !Error: {self.error_msg}\033[0m")
				return

			data = json.loads(out)
			self.metadata = data # Store raw metadata

			# Extract format info
			fmt = data.get("format", {})
			self.duration = float(fmt.get("duration", 0.0))
			self.bitrate = int(fmt.get("bit_rate", 0))
			self.size = int(fmt.get("size", 0))
			self.format_tags = fmt.get("tags", {})

			# Extract stream info
			self.streams = data.get("streams", [])
			video_streams = [s for s in self.streams if s.get("codec_type") == "video"]
			if video_streams:
				vid = video_streams[0]
				self.width = int(vid.get("width", 0))
				self.height = int(vid.get("height", 0))
				self.codec_v = vid.get("codec_name", "")
				try:
					r_fps = vid.get("r_frame_rate", "24/1")
					if "/" in r_fps:
						num, den = map(int, r_fps.split("/"))
						self.src_fps = num / den if den > 0 else 23.976
					else:
						self.src_fps = float(r_fps)
				except Exception:
					self.src_fps = 23.976

			# Check for corruption (if duration is 0 but size is not)
			if self.duration == 0.0 and self.size > 1024:
				self.is_corrupted = True
				self.error_msg = "File has size but 0 duration (likely corrupted or unplayable)."
				self.logs.append(f"\033[91m   !Error: {self.error_msg}\033[0m")

			# --- SNIFF CHECK (Restored from FFMpeg_o.py) ---
			# If probe succeeded, check if we can actually decode a bit of it.
			if not self.is_corrupted and check_corruption:
				p2 = None
				try:
					# Attempt to decode 10 seconds. '-xerror' aborts on error.
					sniff = [FFMPEG, "-v", "error", "-xerror", "-i", str(self.path), "-t", "10", "-f", "null", "-"]
					p2 = _popen_managed(sniff, stdout=sp.DEVNULL, stderr=sp.PIPE)
					try:
						_, err2 = p2.communicate(timeout=CORRUPTION_CHECK_TIMEOUT_S)
					finally:
						PROC_MGR.unregister(p2)

					if p2.returncode != 0:
						self.is_corrupted = True
						err_text = err2.strip()
						self.error_msg = f"Corruption detected during sniffing: {err_text}"
						self.logs.append(f"\033[91m   !Error: {self.error_msg}\033[0m")

				except sp.TimeoutExpired:
					self.is_corrupted = True
					self.error_msg = "Corruption check timed out (stall)."
					self.logs.append(f"\033[91m   !Error: {self.error_msg}\033[0m")
					if p2:
						p2.kill()
						PROC_MGR.unregister(p2)
				except Exception as e:
					self.is_corrupted = True
					self.error_msg = f"Corruption check failed: {e}"
					self.logs.append(f"\033[91m   !Error: {self.error_msg}\033[0m")
					if p2: PROC_MGR.unregister(p2)


		except sp.TimeoutExpired:
			self.is_corrupted = True
			self.error_msg = f"FFprobe timed out after {Utils.PROBE_TIMEOUT_S} seconds."
			self.logs.append(f"\033[91m   !Error: {self.error_msg}\033[0m")
			if p:
				p.kill()
				PROC_MGR.unregister(p)
		except json.JSONDecodeError:
			self.is_corrupted = True
			self.error_msg = "FFprobe output was not valid JSON."
			self.logs.append(f"\033[91m   !Error: {self.error_msg}\033[0m")
		except Exception as e:
			self.is_corrupted = True
			self.error_msg = f"An unexpected error occurred during probing: {e}"
			self.logs.append(f"\033[91m   !Error: {self.error_msg}\033[0m")

	def get_pixel_stats(self, duration_check: float = 3.0) -> Tuple[float, float, float]:
		"""
		Scans the video to measure Saturation stats (Min, Max, Avg) using FFmpeg signalstats.
		Returns (SatMin, SatMax, SatAvg). Returns (0,0,0) on failure.
		Uses a random seek to avoid black intro frames.
		"""
		try:
			# Seek to a safe point (30s or 10% if short)
			seek_time = "00:00:30"
			if self.duration > 0 and self.duration < 100:
				seek_time = f"00:00:{int(self.duration * 0.1):02d}"

			cmd = [
				FFMPEG, "-hide_banner", "-v", "error", 
				"-ss", str(seek_time),
				"-t", "3",
				"-i", str(self.path),
				"-vf", "signalstats=stat=brng+vrep,metadata=print:key=lavfi.signalstats.SATAVG:file=-",
				"-f", "null", "-"
			]
			
			# metadata=print writes to STDOUT when file=- is used
			p = _popen_managed(cmd, stdout=sp.PIPE, stderr=sp.PIPE)
			out, err = p.communicate(timeout=15)
			PROC_MGR.unregister(p)
			
			output = out if isinstance(out, str) else out.decode("utf-8", errors="ignore")
			sat_values = []
			
			for line in output.splitlines():
				if "lavfi.signalstats.SATAVG=" in line:
					try:
						val = float(line.split("=")[1])
						sat_values.append(val)
					except Exception: pass
			
			if sat_values:
				return min(sat_values), max(sat_values), sum(sat_values) / len(sat_values)
			
		except Exception as e:
			pass
		
		return 0.0, 0.0, 0.0

	def plan(self, de_bug=False) -> Tuple[List[str], bool, List[str]]:
		"""Analyzes file metadata and plans the transcoding process."""
		fmt, streams, fmt_tags = {}, [], {}
		tot_br, dur, sz, src_fps = 0, 0.0, 0, 23.976

		# Use self's attributes directly
		streams = self.streams
		fmt_tags = self.format_tags
		tot_br = self.bitrate
		dur = self.duration
		sz = self.size

		# Extract Source FPS
		vid = next((s for s in streams if s.get('codec_type') == 'video'), {})
		try:
			r_fps = vid.get("r_frame_rate", "24/1")
			if "/" in r_fps:
				num, den = map(int, r_fps.split("/"))
				src_fps = num / den if den > 0 else 23.976
			else:
				src_fps = float(r_fps)
		except Exception: pass
		self.src_fps = src_fps # Update MediaFile object

		if not streams and sz == 0:
			self.logs.append("\033[93m !Error: Unreadable Metadata\033[0m")
			self.skip = True
			return [], True, self.logs

		vc = len([s for s in streams if s.get('codec_type') ==    'video'])
		ac = len([s for s in streams if s.get('codec_type') ==    'audio'])
		sc = len([s for s in streams if s.get('codec_type') == 'subtitle'])
		
		# --- Robust Skip Key Detection ---
		# Check multiple tag fields and use case-insensitive matching
		tags_to_check = []
		if fmt_tags: tags_to_check.extend(fmt_tags.values())
		for s in streams:
			if "tags" in s: tags_to_check.extend(s["tags"].values())
		
		skip_found = False
		# Match any of these keys (Focus on the unique parts of the original key)
		# We use substrings to be tolerant of encoding issues (e.g. mangled face characters)
		skip_markers = [
			"¯\\_(ツ)_/¯", 
			"ãƒ„",          # Katakana 'tsu' often survives as-is or as this sequence
			"\\_(ツ)_/",
			"â¯\\_(ãƒ„)_/â¯" # Mangled variant seen in user logs
		]
		
		for val in tags_to_check:
			v_lower = str(val).lower()
			if any(m.lower() in v_lower for m in skip_markers):
				skip_found = True
				break

		header = (	f"   |=Title|{self.path.stem}|\n"
					f"   |<FRMT>|Size: {hm_sz(sz)}|Bitrate: {hm_sz(tot_br,'bps')}|"
					f"Length: {hm_tm(dur)}|Streams: V:{vc} A:{ac} S:{sc} |" )
		if skip_found: header 	+= "Comment: SKIP_KEY Found"
		else: header 			+= "Comment: Add SKIP_KEY"

		# DEFER PRINTING HEADER: Store it in self for conditional printing later
		self._header_line = header

		aud_br = sum(int(s.get('bit_rate', 0) or 0) for s in streams if s.get('codec_type') == 'audio')
		vid_br = max(tot_br - aud_br, 0)

		# Pass self to parsing functions
		self.estimated_video_bitrate = vid_br # Store for use in parse_video
		v_cmd, v_skip, v_logs = parse_video(self)
		a_cmd, a_skip, a_logs = parse_audio(self)
		s_cmd, s_skip, s_logs, side_in, side_lang, side_disp = parse_subtl(self)
		
		self.logs.extend(v_logs + a_logs + s_logs)
		
		needs_cont = (self.path.suffix.lower() != ".mp4")
		needs_key  = not skip_found
		
		reasons = []
		if not v_skip: reasons.append("Video needs changes")
		if not a_skip: reasons.append("Audios need changes")
		if not s_skip: reasons.append("Subtitles need changes")
		if needs_cont: reasons.append("Container convert")
		if needs_key:  reasons.append("Add skip key")
		
		# --- Final Decision ---
		# A file is ONLY skipped if ALL streams are optimal AND it already has the SKIP_KEY
		final_skip = (v_skip and a_skip and s_skip and not needs_cont and not needs_key)
		
		if final_skip:
			# If skipped, we only add a concise skip note to the logs (which Trans_code can toggle)
			self.logs.append("\033[96m  .Skip: File fully compliant and tagged.\033[0m")
		else:
			# If NOT skipped, we print the header now and then show why we are processing
			Utils.safe_print(f"\033[96m{self._header_line}\033[0m")
			self.logs.append(f"\033[96m  .Processing ({', '.join(reasons)})\033[0m")

		self.reasons = reasons # Store decision reasons
#		else: self.logs.append(f"\033[96m  .Processing ({', '.join(reasons)}) for: {self.path.name}\033[0m")

		cmd = [FFMPEG, "-y", "-hide_banner", "-i", str(self.path)]

		# 1. ADD SIDE INPUT (But do NOT map it yet)
		if side_in:
			cmd.extend(side_in)

		# 2. ADD STANDARD MAPS (Video -> Audio -> Internal Subs)
		cmd.extend(v_cmd)
		cmd.extend(a_cmd)
		cmd.extend(s_cmd)

		# 3. ADD SIDE MAPS (Last, so calculations align)
		if side_in:
			# Calculate the NEXT available subtitle index (after all internal ones)
			out_idx = len([x for x in s_cmd if "-c:s:" in x])
			cmd.extend([
				"-map", "1:0",
				f"-c:s:{out_idx}", "mov_text",
				f"-metadata:s:s:{out_idx}", f"language={side_lang}",
				f"-disposition:s:{out_idx}", side_disp
			])

		cmd.extend(["-metadata", f"comment={SKIP_KEY}"]) # 5. EXECUTE
		self.cmd = cmd
		str_cmd = " ".join(shlex.quote(c) for c in cmd)
#		Utils.safe_print(f"   [CMD] {str_cmd}") # DEBUG: Print exact command
		self.skip = final_skip
		return cmd, final_skip, self.logs

	def run(self, task_id: str, de_bug: bool = False) -> Optional[str]:
		"""Executes the FFmpeg command."""
		if self.skip or not self.cmd: return None

		# Use RUN_TMP for temp file
		temp = str(RUN_TMP / f"{self.path.stem}_{random.randint(1000,9999)}.mp4")

		do_2pass = (USE_TWO_PASS and "libx265" in CURRENT_ENCODER)
		passes = []

		if do_2pass:
			log_prefix = str(Path(tempfile.gettempdir()) / f"ffmpeg_pass_{int(time.time())}_{random.randint(1000,9999)}")
			dev_null = "NUL" if IS_WIN else "/dev/null"

			p1_cmd = [x for x in self.cmd if x not in ("-stats", "-nostats")]
			p1_cmd.extend(["-pass", "1", "-passlogfile", log_prefix, "-f", "null", dev_null])
			passes.append((1, p1_cmd, log_prefix))

			p2_cmd = [x for x in self.cmd if x not in ("-stats", "-nostats")]
			p2_cmd.extend(["-pass", "2", "-passlogfile", log_prefix, "-progress", "pipe:1", "-stats_period", "0.5", "-movflags", "+faststart", temp])
			passes.append((2, p2_cmd, log_prefix))
		else:
			p1_cmd = [x for x in self.cmd if x not in ("-stats", "-nostats")]
			p1_cmd.extend(["-progress", "pipe:1", "-stats_period", "0.5", "-movflags", "+faststart", temp])
			passes.append((0, p1_cmd, None))

		final_success = False
		for p_num, p_cmd, p_log in passes:
			label = "Encode"
			if p_num == 1: label = "Analysis (Pass 1/2)"
			if p_num == 2: label = "Encode (Pass 2/2)"

			Utils.safe_print(f"   [{task_id}] Stage-1 {label} -> {'MP4' if p_num != 1 else 'Null'}")

			p = _popen_managed(p_cmd, stdout=sp.PIPE, stderr=sp.PIPE, bufsize=1)
			stderr_log = []

			def _read_stderr(pipe, log_list):
				try:
					for line in iter(pipe.readline, ''):
						log_list.append(line)
				except Exception: pass

			# Pass src_fps from self
			t_out = threading.Thread(target=_read_pipe1_progress, args=(p.stdout, task_id, self.duration, self.src_fps))
			t_err = threading.Thread(target=_read_stderr, args=(p.stderr, stderr_log))

			t_out.start()
			t_err.start()

			try:
				p.wait(timeout=STAGE_TIMEOUT_S)
			except sp.TimeoutExpired:
				Utils.safe_print(f"\033[91m   [Error] FFmpeg process exceeded {STAGE_TIMEOUT_S}s timeout, killing...\033[0m")
				p.kill()
				p.wait() # Reap zombie process

			t_out.join(timeout=2)
			t_err.join(timeout=2)
			PROC_MGR.unregister(p)

			if p.returncode == 0:
				final_success = True
			else:
				final_success = False
				err_text = "".join(stderr_log[-15:])
				analysis = ""
				if LLM_AVAILABLE:
					try:
						Utils.safe_print(f"   [{task_id}] !Failure detected. Requesting LLM analysis...")
						analysis = analyze_error_with_llm(p_cmd, stderr_log[-50:])
						self.logs.append(f"\033[93m   [LLM Analysis] {analysis}\033[0m")
					except Exception:
						pass
				
				self.logs.append(f"\033[91m   !FFmpeg Error (Pass {p_num}): {err_text}\033[0m")
				if p_log: # Cleanup pass logs
					try:
						for f in Path(tempfile.gettempdir()).glob(f"{Path(p_log).name}*"): f.unlink()
					except Exception: pass
				break

		if final_success and Path(temp).exists() and Path(temp).stat().st_size > MIN_OUTPUT_FILE_SIZE:
			return temp
		return None

	def cleanup(self, temp_file: str, keep_orig: bool = False, de_bug: bool = False, task_id: str = "Txx") -> int:
		"""Moves temp file to final destination and cleans up."""
		if not temp_file or not Path(temp_file).exists(): return -1

		src = self.path
		tmp = Path(temp_file)

		# 1. Verification
		try:
			s_sz = src.stat().st_size
			t_sz = tmp.stat().st_size
			if t_sz < (s_sz * 0.10) and t_sz < 50_000_000:
				Utils.safe_print(f"\033[91m   !Error: Output too small ({hm_sz(t_sz)}). Keeping original.\033[0m")
				tmp.unlink()
				return -1
		except Exception as e:
			Utils.safe_print(f"   !Error verifying size: {e}")
			return -1

		# 2. Cooldown & GC (WinError 32 fix)
		time.sleep(2.5)
		gc.collect()

		# 3. Transactional Replace (Windows Safe)
		# Goal: Overwrite dest with tmp, but safely via backup
		
		dest = src.with_suffix(".mp4")
		is_same_file = (src == dest) or (src.resolve() == dest.resolve())
		
		backup = src.with_name(f"{src.stem}_ORIG{src.suffix}")
		
		try:
			# Step A: Secure the original (Move aside)
			# Even if different extension, we keep backup until success
			if src.exists():
				retry_with_lock_info(f"Backup original {src.name}", shutil.move, (str(src), str(backup)), de_bug=de_bug)
			
			# Step B: Move temp to destination
			# Use os.replace for POSIX atomic / Windows rename-replace (Python 3.3+)
			# shutil.move is safer across drives, but less atomic. 
			# We assume TMP and DEST are likely same drive for speed, but shutil.move is safest general fallback.
			retry_with_lock_info(f"Move result to {dest.name}", shutil.move, (str(tmp), str(dest)), de_bug=de_bug)
			
			# Step C: Verify Success
			if dest.exists() and dest.stat().st_size > 0:
				# Success -> Delete backup
				try: 
					backup.unlink(missing_ok=True)
				except Exception:
					Utils.safe_print(f"   [Warn] Could not delete backup {backup.name} (locked?), leaving it.")
				
				gain = s_sz - t_sz
				Utils.safe_print(f"   [{task_id}].Success: Replaced {src.name} | Saved {hm_sz(gain)}")
				return gain
			else:
				raise OSError("Destination file missing or empty after move")

		except Exception as e:
			Utils.safe_print(f"\033[91m   [CRITICAL] Failed to finalize output: {e}\033[0m")
			# ROLLBACK
			if backup.exists() and not src.exists():
				try:
					Utils.safe_print(f"   [Rollback] Restoring original from {backup.name}...")
					shutil.move(str(backup), str(src))
				except Exception as re:
					Utils.safe_print(f"   [FATAL] Rollback failed: {re}")
			return -1

# =============================================================================
# 5. SUBTITLE HELPERS
# =============================================================================

_TEXT_SIDE_EXTS = {".srt", ".ass", ".ssa", ".vtt"}
_LANG_ALIASES = {
	"eng": {"en", "eng", "english"},
	"spa": {"es", "spa", "spanish"},
	"fra": {"fr", "fre", "fra", "french"},
}
_ALIAS_TO_LANG3 = {v.lower(): k for k, vals in _LANG_ALIASES.items() for v in vals}

def _detect_encoding(file_path: Path) -> Optional[str]:
	if not CHARSET_NORMALIZER_AVAILABLE:
		return None  # Graceful fallback if library not installed
	try:
		data = file_path.read_bytes()[:102400]
		match = charset_normalizer.from_bytes(data).best()
		if match and match.encoding not in ('utf-8', 'ascii'): return str(match.encoding).lower()
	except Exception: pass
	return None

def _get_subtitle_hash(input_file: str, stream_index: int) -> Optional[str]:
	"""Calculates MD5 hash of a subtitle stream's bitstream content."""
	cmd = [FFMPEG, "-hide_banner", "-i", input_file, "-map", f"0:{stream_index}", "-c:s", "copy", "-f", "hash", "-hash", "md5", "-"]
	try:
		p = _popen_managed(cmd, stdout=sp.PIPE, stderr=sp.DEVNULL)
		# Increase timeout to 75s for slow network shares
		out, _ = p.communicate(timeout=75)
		PROC_MGR.unregister(p)
		if p.returncode == 0 and out:
			# FFmpeg hash output format: MD5=c4103f122d276...
			match = re.search(r"MD5=([a-fA-F0-9]+)", out)
			if match: return match.group(1).lower()
	except sp.TimeoutExpired:
		try: p.kill()
		except Exception: pass
		PROC_MGR.unregister(p)
		Utils.safe_print(f"   [Warning] Subtitle hashing timed out for stream {stream_index} (Slow I/O)")
		return "TIMEOUT_ERR"
	except Exception: pass
	return None

def _tokens_after_stem(sub_path: Path, video_stem: str) -> List[str]:
	remainder = sub_path.stem[len(video_stem):].lstrip(".-_ ")
	return [t for t in re.split(r"[.\-_ ]+", remainder) if t]

def _guess_lang3_from_filename(sub_path: Path, video_stem: str) -> Optional[str]:
	for tok in _tokens_after_stem(sub_path, video_stem):
		key = tok.lower()
		if key in _ALIAS_TO_LANG3: return _ALIAS_TO_LANG3[key]
	return None

def _score_sidecar(video_stem: str, path: Path, default_lng: str) -> int:
	score = {".srt": 30, ".ass": 20, ".vtt": 10}.get(path.suffix.lower(), 0)
	tokens = {t.lower() for t in _tokens_after_stem(path, video_stem)}
	lang = _guess_lang3_from_filename(path, video_stem)
	if lang == default_lng: score += 1200
	if "forced" in tokens: score -= 600
	if "sdh" in tokens or "cc" in tokens: score += 25
	return score

def add_subtl_from_file(input_file: str, existing_subs: List[Dict] = None) -> Tuple[List[str], bool, List[str], str, str]:
	"""Scans for and processes external subtitle files with intelligent sanitization and matching.

	Args:
		input_file: Path to the video file
		existing_subs: List of existing subtitle stream dicts from ffprobe

	Returns:
		(cmd_part, skip, logs, lang3, disposition)
	"""
	p = Path(input_file)
	stem = p.stem
	parent = p.parent
	candidates = []
	logs = []
	existing_subs = existing_subs or []

	try:
		for e in parent.iterdir():
			if e.is_file() and e.suffix.lower() in _TEXT_SIDE_EXTS and e.stem.startswith(stem):
				candidates.append(e)
	except Exception as e:
		logs.append(f"\033[93m  ?!Warning: Failed to scan subtitles: {e}\033[0m")

	if not candidates: return [], True, logs, "eng", "default"

	default_lng = (globals().get("Default_lng", "eng") or "eng").lower()
	scored = [(_score_sidecar(stem, c, default_lng), c) for c in candidates]
	scored.sort(key=lambda x: x[0], reverse=True)
	best_path = scored[0][1]

	lang3 = _guess_lang3_from_filename(best_path, stem) or default_lng

	# **NEW: Check if this external subtitle is already embedded**
	for sub in existing_subs:
		if sub.get("codec_type") != "subtitle":
			continue
		sub_lang = sub.get("tags", {}).get("language", "und").lower()

		# Match if: same language AND codec is mov_text (typical for embedded .srt)
		if sub_lang == lang3 and sub.get("codec_name") == "mov_text":
			logs.append(f"\033[93m  .Info: External subtitle '{best_path.name}' (Lang: {lang3}) appears already embedded (Stream {sub.get('index')}). Skipping.\033[0m")
			return [], True, logs, lang3, "default"

	tokens = {t.lower() for t in _tokens_after_stem(best_path, stem)}
	disposition = "default"
	if "forced" in tokens: disposition = "forced"
	elif "sdh" in tokens or "cc" in tokens: disposition = "0"

	enc = _detect_encoding(best_path) or "utf-8"
	sanitized = False
	tmp_path = None

	try:
		with open(best_path, "r", encoding=enc, errors="ignore") as f:
			content = f.readlines()
		clean_lines = []
		suffix = best_path.suffix.lower()

		if suffix in {".srt", ".vtt"}:
			import re as _re
			i = 0
			while i < len(content):
				line = content[i].strip()
				if line.isdigit():
					clean_lines.append(content[i])
					i += 1
					if i < len(content):
						if _re.match(r"\d{2}:\d{2}:\d{2}", content[i].strip()):
							clean_lines.append(content[i])
							i += 1
							while i < len(content) and content[i].strip():
								clean_text = "".join(c for c in content[i] if ord(c) >= 32 or c in "\n\r\t")
								clean_lines.append(clean_text)
								i += 1
							clean_lines.append("\n")
						else: i += 1
				else: i += 1
		else:
			for line in content:
				clean_lines.append("".join(c for c in line if ord(c) >= 32 or c in "\n\r\t"))

		if len(clean_lines) > 5:
			with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", suffix=suffix, delete=False) as tmp:
				tmp.writelines(clean_lines)
				tmp_path = Path(tmp.name)
			sanitized = True
			logs.append(f"\033[93m  .Info: Sanitized subtitle '{best_path.name}' to UTF-8 temp file.\033[0m")

	except Exception as e:
		logs.append(f"\033[93m   !Warning: Sanitization failed: {e}. Using original.\033[0m")

	use_path = tmp_path if sanitized else best_path
	log_msg = f"\033[94m  .Adding subtitle: {best_path.name} (Lang: {lang3}, Disp: {disposition})"
	if sanitized: log_msg += " (sanitized)"
	logs.append(log_msg + "\033[0m")

	cmd_part = ["-i", str(use_path)]
	return cmd_part, False, logs, lang3, disposition

# =============================================================================
# 6. PLANNING HELPERS
# =============================================================================

def _ideal_hevc_bps(w, h, fps, source_br):
	if w > 0 and h > 0:
		return max(250_000, int(w * h * fps * BIT_PER_PIX))
	if source_br > 0:
		return max(250_000, int(source_br * 0.65))
	return 1_000_000

def _get_aspect_ratio(w, h):
	if not h: return "?"
	r = w/h
	if abs(r - 1.77) < 0.05: return "16:9"
	if abs(r - 1.33) < 0.05: return "4:3"
	return f"{w}:{h}"


# =============================================================================
# 6. STREAM PARSING
# =============================================================================

def parse_video(media: MediaFile) -> Tuple[List[str], bool, List[str]]:
	"""Generates FFmpeg arguments for video streams.

	Args:
		media: The MediaFile object containing streams and context.

	Returns:
		Tuple of (command_args, skip_all, log_messages)
	"""
	logs, cmd = [], []
	skip_all = True
	out_idx = 0
	streams = media.streams

	for s in streams:
		if s.get("codec_type") != "video" or s.get("disposition", {}).get("attached_pic"):
			continue

		w = int(s.get("width", 0))
		h = int(s.get("height", 0))
		fps_str = s.get("avg_frame_rate", "24/1")
		try: fps = float(Fraction(fps_str))
		except Exception: fps = 24.0
		codec = s.get("codec_name", "")
		pix = s.get("pix_fmt", "")

		scale_trigger = (w > MAX_WIDTH_BEFORE_DOWNSCALE or h > MAX_HEIGHT_BEFORE_DOWNSCALE)
		tgt_h = TARGET_HEIGHT_1080P if scale_trigger else h
		tgt_w = int(tgt_h * (w/h)) if h > 0 else w
		if tgt_w % 2: tgt_w += 1

		br = media.estimated_video_bitrate
		ideal = _ideal_hevc_bps(tgt_w, tgt_h, fps, br)

		# Improved Logging: Calculate % difference relative to ideal for more intuitive margin check
		pct = int(((br / ideal) - 1) * 100)	if ideal > 0 else 0
		change = "larger than ideal"		if pct > 0 else "smaller than ideal"
		method = "(Pixel-Math)"				if (w > 0 and h > 0) else "(Source-Ratio)"
		br_log = f"|Source: {hm_sz(br, 'bps')} <=> Ideal: {hm_sz(ideal, 'bps')} {method} => App: {abs(pct)}% {change}"
#		logs.append(f"   {br_log}")

		# Hoisted metadata for encoder decisions
		is_10bit		= ("10" in pix) or ("p010" in pix)
		color_transfer	= s.get("color_transfer", "unknown")
		primaries		= s.get("color_primaries", "unknown")
		
		# Robust HDR Detection (Backported from Fix_Redshift_Safe.py)
		# Check both transfer characteristics AND side data for HDR hints
		side_data = s.get("side_data_list", [])
		has_hdr_side_data = any(sd.get("side_data_type") in ["Mastering display metadata", "Content light level metadata"] for sd in side_data)
		is_hdr			= (color_transfer in ["smpte2084", "arib-std-b67"]) or has_hdr_side_data

		is_hevc			= (codec.lower() in ('hevc', 'h265'))
		is_bloated		= (br > ideal * SIZE_OK_MARGIN)
		needs_scale		= (w != tgt_w or h != tgt_h) and (w > tgt_w)
		# Red_Shift: HEVC files often have incorrect color tags (primaries 2/9/unknown instead of 1/bt709)
		# Fix: Strictly check Primaries, Transfer, AND Color Space
		def _is_bt709(v): return str(v).lower().strip() in ("bt709", "1")
		
		# Get raw metadata (default to 'unknown' to fail strict check)
		p_prim = s.get("color_primaries", "unknown")
		p_trc  = s.get("color_transfer", "unknown")
		p_spc  = s.get("color_space", "unknown")

		is_bt709_complete = _is_bt709(p_prim) and _is_bt709(p_trc) and _is_bt709(p_spc)
		needs_red_fix = (is_hevc and not is_hdr and not is_bt709_complete)
		
		# Tag Fix: Chromecast/Apple Compatibility (hvc1 in MP4)
		v_tag_str = s.get("codec_tag_string", "").lower()
		needs_tag_fix = (is_hevc and Utils.TAG_HEVC_AS_HVC1 and v_tag_str != "hvc1")

		# --- PHASE 13: SMART PIXEL PROBE ---
		# If tags are ambiguous (SDR tags on HEVC 10-bit), check if it's actually HDR
		sat_stats = (0,0,0)
		is_hidden_hdr = False
		is_pseudo_sdr = False
		
		# Trigger probe if: HEVC + 10-bit + Claimed SDR (BT.709/Unknown)
		should_probe = is_hevc and is_10bit and not is_hdr and (p_prim in ["bt709", "unknown"])
		
		if should_probe:
			# logs.append(f"   |SmartProbe: Checking pixels for Hidden HDR...|")
			sat_stats = media.get_pixel_stats()
			sat_avg = sat_stats[2]
			
			if sat_avg > 95.0:
				is_hidden_hdr = True
				logs.append(f"   |SmartProbe: HIDDEN HDR DETECTED (Sat:{sat_avg:.1f}). Force Repair.|")
			
			# Safety: If we treat as BT.2020 input, we must ensure we don't apply PQ curve to Gamma content
			if sat_avg < 88.0:
				is_pseudo_sdr = True

		cmd.extend(["-map", f"0:{s['index']}"])

		status = ""
		# Force Re-encode if Hidden HDR is found (Need to burn-in color fix)
		can_remux = (is_hevc and not needs_scale and not is_bloated and w > 0 and not is_hidden_hdr)
		
		if can_remux:
			cmd.extend([f"-c:v:{out_idx}", "copy"])
			if needs_red_fix or needs_tag_fix:
				skip_all = False
				status = "=> Remux (Fix Metadata)|#REMUX"
				if needs_red_fix:
					# Apply bitstream filter to fix metadata WITHOUT re-encoding
					bsf = "hevc_metadata=colour_primaries=1:transfer_characteristics=1:matrix_coefficients=1"
					cmd.extend([f"-bsf:v:{out_idx}", bsf])
					logs.append(f"   |Repair: HEVC Fix Color Tagging (BT.709 Shift)|")
				if needs_tag_fix:
					logs.append(f"   |Repair: HEVC tag '{v_tag_str}' -> 'hvc1' (Chromecast Compatibility)|")
			else:
				status = "=> Copy (HEVC + Efficient Bitrate)|#COPY"
		else:
			skip_all = False
			# Determine specific reason for re-encoding for better status logging
			re_reasons = []
			if not is_hevc:	re_reasons.append(f"Codec:{codec}")
			if needs_scale:	re_reasons.append("Scale")
			if is_bloated:	re_reasons.append(f"Bitrate: {hm_sz(br)} => {pct}% Saving")
			reason_tag = f"[{', '.join(re_reasons)}]" if re_reasons else ""

			c_enc = CURRENT_ENCODER
			cmd.extend([f"-c:v:{out_idx}", c_enc])

			# --- AMD HEVC OPTIMIZATION (hevc_amf) ---
			# Research findings: HQVBR, VBAQ, Pre-Analysis, and Mandatory 10-bit P010 are best for quality.
			if c_enc == "hevc_amf":
				# 1. Force 10-bit Output (User Request: "safe and thorough ... into 10 Bit")
				cmd.extend([f"-pix_fmt:v:{out_idx}", "p010le"])
				cmd.extend([f"-profile:v:{out_idx}", "main10"])

				# 2. Rate Control: Use 'vbr_peak' (Safe Mode)
				# Notes: 'hqvbr' and 'preanalysis' caused crashes on specific hardware (Error 10).
				# 'vbr_peak' is robust and supports 10-bit.
				cmd.extend([f"-rc:v:{out_idx}", "vbr_peak"])
				cmd.extend([f"-quality:v:{out_idx}", "quality"]) 
				
				# 3. Bitrate Config
				cmd.extend([f"-b:v:{out_idx}", f"{ideal//1000}k"])
				cmd.extend([f"-maxrate:v:{out_idx}", f"{int(ideal*1.5)//1000}k"])
				cmd.extend([f"-bufsize:v:{out_idx}", f"{int(ideal*2)//1000}k"])

				# 4. Advanced Features: DISABLED for stability
				# cmd.extend([f"-vbaq:v:{out_idx}", "true"])      
				# cmd.extend([f"-preanalysis:v:{out_idx}", "true"]) 
				# cmd.extend([f"-high_motion_quality_boost_enable:v:{out_idx}", "true"])
				
				logs.append(f"   [AMD-Opt] 10-bit P010 | VBR_Peak | Safe-Mode")

			else:
				# Generic HEVC / Software / NVENC Fallback
				cmd.extend([f"-b:v:{out_idx}", f"{ideal//1000}k"])
				cmd.extend([f"-maxrate:v:{out_idx}", f"{int(ideal*1.5)//1000}k"])
				cmd.extend([f"-bufsize:v:{out_idx}", f"{int(ideal*2)//1000}k"])

			# Core Color Profile: Pass-through source metadata to prevent Redshift/Colorshift
			# If metadata is missing, intelligent fallback based on resolution (HD+ -> BT.709)
			
			p_prim = primaries if primaries and primaries != "unknown" else None
			p_trc  = color_transfer if color_transfer and color_transfer != "unknown" else None
			p_spc  = s.get("color_space", "unknown")
			if p_spc == "unknown": p_spc = None

			# --- RED SHIFT FIX / SMART COLOR PIPELINE ---
			# We default to input tags, but override if repair is needed.
			
			vf_filters = []
			
			# Filter construction
			if needs_scale: vf_filters.append(f"scale={tgt_w}:{tgt_h}")
			
			# If Hidden HDR or Red Fix needed, use zscale for high-quality conversion
			if needs_red_fix or is_hidden_hdr:
				# Adaptive Transfer: Avoid "Neon" if saturation is actually low
				trc_in = "smpte2084"
				if is_pseudo_sdr: 
					trc_in = "bt709" 
					logs.append(f"   |SmartColor: Low Sat ({sat_stats[2]:.1f}) detected. Using Gamma transfer to prevent Neon.|")
				
				# Force Input override -> Target BT.709
				# We use zscale to convert FROM the likely-wrong input TO clean BT.709
				# Note: 'range=limited' for TV standard
				zscale = f"zscale=primariesin=bt2020:transferin={trc_in}:matrixin=bt2020nc:primaries=bt709:transfer=bt709:matrix=bt709:range=limited"
				vf_filters.append(zscale)
				
				# Set output tags to BT.709
				p_prim = "bt709"
				p_trc  = "bt709"
				p_spc  = "bt709"
			
			# Clean up any 'unknown' leftover
			elif not p_prim and not p_trc:
				if not is_hdr and (w >= 1280 or h >= 720):
					p_prim, p_trc, p_spc = "bt709", "bt709", "bt709"
					
			# Filters are accumulated in vf_filters and applied once at the end (Line 1341).
			# if vf_filters: cmd.extend([f"-filter:v:{out_idx}", ",".join(vf_filters)]) -- REMOVED DUPLICATE APPLY

			if p_prim: cmd.extend([f"-color_primaries:v:{out_idx}", p_prim])
			if p_trc:  cmd.extend([f"-color_trc:v:{out_idx}", p_trc])
			if p_spc:  cmd.extend([f"-colorspace:v:{out_idx}", p_spc])
			
			# Ensure metadata is written to container level too
			if p_prim: cmd.extend([f"-metadata:s:v:{out_idx}", f"color_primaries={p_prim}"])
			if p_trc:  cmd.extend([f"-metadata:s:v:{out_idx}", f"color_trc={p_trc}"])
			if p_spc:  cmd.extend([f"-metadata:s:v:{out_idx}", f"colorspace={p_spc}"])

			# Force 10-bit for HEVC (Best practice to prevent banding)
			is_hevc_enc = ("hevc" in c_enc or "265" in c_enc)

			# Prevent conflict: 'hevc_amf' is handled specifically above (Lines 1215+).
			if c_enc == "hevc_amf":
				pass # Already handled, skip generic fallbacks
			elif "amf" in c_enc:
				cmd.extend(["-quality", "quality", "-rc", "vbr_peak"])
				if is_hevc_enc:
					cmd.extend(["-profile:v", "main10"])
			elif "qsv" in c_enc:
				cmd.extend(["-preset", "slow", "-global_quality", "22"])
				if is_hevc_enc:
					cmd.extend(["-load_plugin", "hevc_hw"])
					cmd.extend(["-profile:v", "main10"])
			elif "nvenc" in c_enc:
				cmd.extend(["-preset", "p4", "-rc", "vbr", "-cq", "24"])
				if is_hevc_enc:
					cmd.extend(["-profile:v", "main10"])
			else:
				cmd.extend(["-preset", "medium"])
				# Only use CRF if NOT doing 2-pass with libx265
				if not (USE_TWO_PASS and "libx265" in c_enc):
					cmd.extend(["-crf", "22"])
				if is_hevc_enc:
					cmd.extend(["-profile:v", "main10"])

			# --- Filters were constructed above ---
			# vf = []
			# if needs_scale: vf.append(f"scale={tgt_w}:{tgt_h}")
			# if is_hevc_enc: ...
			# Re-integrating legacy filter logic if not partially handled above
			# Note: We handled scale and zscale above.
			# We still need format controls.
			
			# Add format filters to the EXISTING list if they aren't there
			if is_hevc_enc:
				if "libx265" in c_enc:
					vf_filters.append("format=yuv420p10le")
				else:
					vf_filters.append("format=p010le")
			elif "10" in pix:
				vf_filters.append("format=p010le")
			
			# Apply the accumulated filters
			if vf_filters: 
				# Filter deduplication might be good but let's trust flow 
				cmd.extend([f"-filter:v:{out_idx}", ",".join(vf_filters)])

			if w == 0: status = f"Re-encode {reason_tag} (Fix Unknown Res -> {c_enc} @ {hm_sz(ideal, 'bps')})"
			else: status = f"Re-encode {reason_tag} ({c_enc} @ {hm_sz(ideal, 'bps')})"

		# Chromecast / Apple Compatibility: Use 'hvc1' tag for HEVC in MP4
		if is_hevc and Utils.TAG_HEVC_AS_HVC1:
			cmd.extend([f"-tag:v:{out_idx}", "hvc1"])

		# Detailed Metadata Extraction
		field_order = s.get("field_order", "progressive")
		is_interlaced = (field_order != "progressive") and (field_order != "unknown")

		# is_hdr and is_10bit hoisted above

		r_frame_rate = s.get("r_frame_rate", "0/0")
		avg_frame_rate = s.get("avg_frame_rate", "0/0")
		is_vfr = (r_frame_rate != avg_frame_rate)

		# Enhanced Logging
		status_extras = []
		if is_hdr: status_extras.append("HDR")
		if is_interlaced: status_extras.append("Interlaced")
		if is_vfr: status_extras.append("VFR")

		extra_info = f"|{' '.join(status_extras)}" if status_extras else ""

		ar = _get_aspect_ratio(w, h)
		row = f"   |<V:{s.get('index','?'):2}>|{codec:^6}|{w}x{h}|{ar}|{fps:.2f} fps|{'10-bit' if is_10bit else '8-bit'}{extra_info}|{status}"
		logs.append(f"\033[91m{row}\033[0m")
		out_idx += 1

	if skip_all: logs.append("\033[91m  .Skip: Video streams are optimal.\033[0m")
	return cmd, skip_all, logs

def parse_audio(media: MediaFile) -> Tuple[List[str], bool, List[str]]:
	"""Generates FFmpeg arguments for audio streams with Default_lng prioritization.

	Args:
		media: The MediaFile object containing streams and context.

	Returns:
		Tuple of (command_args, skip_all, log_messages)
	"""
	logs, cmd = [], []
	skip_all = True
	out_idx = 0
	streams = media.streams
	
	# Priority Selection: Find the first stream matching Default_lng
	target_lang = (Utils.Default_lng or "eng").lower()
	found_default_idx = -1
	
	for i, s in enumerate([s for s in streams if s.get("codec_type") == "audio"]):
		slang = s.get('tags', {}).get('language', 'und').lower()
		if slang == target_lang:
			found_default_idx = i
			break
	
	# If no specific language match, default to first stream if multiple exist
	# 2. Check if the source already has the CORRECT default selection
	# Skip can only happen if: 
	# A) The intended default stream IS already marked default in the source
	# B) NO OTHER stream is marked default in the source
	source_audio_streams = [s for s in streams if s.get("codec_type") == "audio"]
	is_correctly_defaulted = True
	if found_default_idx != -1:
		for i, s in enumerate(source_audio_streams):
			is_def = bool(s.get("disposition", {}).get("default", 0))
			if i == found_default_idx:
				if not is_def: 
					is_correctly_defaulted = False
					break
			else:
				if is_def:
					is_correctly_defaulted = False
					break
	
	for s in streams:
		if s.get("codec_type") != "audio": continue
		idx = s['index']
		codec = s.get('codec_name', '')
		lang = s.get('tags', {}).get('language', 'und')
		ch = int(s.get('channels', 0))
		br = int(s.get('bit_rate', 0))

		cmd.extend(["-map", f"0:{idx}"])
		
		# Set Disposition (Priority)
		if out_idx == found_default_idx:
			cmd.extend([f"-disposition:a:{out_idx}", "default"])
		else:
			cmd.extend([f"-disposition:a:{out_idx}", "0"])
			
		action = ""
		if codec == 'aac' and ch <= 6:
			cmd.extend([f"-c:a:{out_idx}", "copy"])
			action = f"Copy ({'Default' if out_idx == found_default_idx else 'Secondary'})"
		else:
			skip_all = False
			cmd.extend([f"-c:a:{out_idx}", "aac"])
			if ch > 6:
				cmd.extend([f"-ac:a:{out_idx}", "6", f"-b:a:{out_idx}", "384k"])
				action = f"Re-encode (-> 5.1 384k, {'Default' if out_idx == found_default_idx else 'Secondary'})"
			else:
				cmd.extend([f"-b:a:{out_idx}", "192k"])
				action = f"Re-encode (-> Stereo 192k, {'Default' if out_idx == found_default_idx else 'Secondary'})"

		# sr = ... (moved up for log if needed)
		sr = int(s.get('sample_rate', 0))
		logs.append(f"\033[92m   |<A:{idx:2}>|{codec:^6}|{lang:<3}|Br:{hm_sz(br,'bps'):<10}|Ch:{ch}|SR:{sr}Hz| {action}\033[0m")
		out_idx += 1

	# 3. Precision: Check for missing languages requested via AI or Defaults
	existing_langs = {s.get('tags', {}).get('language', 'und').lower() for s in source_audio_streams}
	missing_ai_langs = [l for l in Utils.ADD_AI_AUD_LANGS if l.lower() not in existing_langs]
	
	if missing_ai_langs:
		skip_all = False
		logs.append(f"\033[93m   |Note: Missing AI Audio Languages: {', '.join(missing_ai_langs)}|\033[0m")

	if not is_correctly_defaulted:
		skip_all = False
		logs.append("\033[93m   |Note: Forcing Remux to correct Audio Language Priority|\033[0m")

	if skip_all: logs.append("\033[92m  .Skip: Audio streams are optimal.\033[0m")
	return cmd, skip_all, logs

def parse_subtl(media: MediaFile) -> Tuple[List[str], bool, List[str], List[str], str, str]:
	"""Generates FFmpeg arguments for subtitle streams with smart deduplication and Default_lng prioritization.

	Args:
		media: The MediaFile object containing streams and context.

	Returns:
		Tuple of (cmd, skip_all, logs, sidecar_cmd, side_lang, side_disp)
	"""
	cmd, logs = [], []
	skip_all = True
	out_idx = 0
	found_valid = False
	seen_hashes = set()  # To track identical bitstream content
	has_duplicates = False  # Track if we found any duplicates
	
	# Priority Selection: Find the first internal stream matching Default_lng
	target_lang = (Utils.Default_lng or "eng").lower()
	found_default_idx = -1
	sub_streams = [s for s in media.streams if s.get("codec_type") == "subtitle"]
	
	# We only count 'valid' subtitles that we actually keep
	valid_sub_idx = 0
	for s in sub_streams:
		codec = s.get("codec_name", "")
		if codec in ['mov_text', 'srt', 'ass', 'webvtt']:
			slang = s.get("tags", {}).get("language", "und").lower()
			if found_default_idx == -1 and slang == target_lang:
				found_default_idx = valid_sub_idx
			valid_sub_idx += 1
	
	# Note: We don't force a default here if no match, because sidecars might be better
	# If we have internal subs and no external ones found later, we might want to default the first internal
	# but for now let's be conservative: only force 'default' if it's the target language.

	# 2. Check if the source already has the CORRECT default selection
	is_correctly_defaulted = True
	if found_default_idx != -1:
		valid_internal_subs = [s for s in sub_streams if s.get("codec_name") in ['mov_text', 'srt', 'ass', 'webvtt']]
		for i, s in enumerate(valid_internal_subs):
			is_def = bool(s.get("disposition", {}).get("default", 0))
			if i == found_default_idx:
				if not is_def:
					is_correctly_defaulted = False
					break
			else:
				if is_def:
					is_correctly_defaulted = False
					break

	# 1. Internal
	current_valid_idx = 0
	for s in media.streams:
		if s.get("codec_type") == "subtitle":
			idx = s['index']
			codec = s.get("codec_name", "")
			lang = s.get("tags", {}).get("language", "und")
			title = s.get("tags", {}).get("title", "").strip()

			if codec in ['mov_text', 'srt', 'ass', 'webvtt']:
				# Check for bit-for-bit duplicate content
				sub_hash = _get_subtitle_hash(str(media.path), idx)
				if sub_hash:
					if sub_hash in seen_hashes:
						logs.append(f"\033[93m   |Skip: Duplicate Subtitle Content (Stream {idx})|")
						has_duplicates = True
						continue
					seen_hashes.add(sub_hash)

				cmd.extend(["-map", f"0:{idx}"])
				
				# Set Disposition
				if current_valid_idx == found_default_idx:
					cmd.extend([f"-disposition:s:{out_idx}", "default"])
				else:
					cmd.extend([f"-disposition:s:{out_idx}", "0"])

				if codec == 'mov_text':
					cmd.extend([f"-c:s:{out_idx}", "copy"])
					logs.append(f"\033[94m   |<S:{idx:2}>|{codec:^6}|{lang:<3}| Copy {'(Default)' if current_valid_idx == found_default_idx else ''}\033[0m")
				else:
					skip_all = False
					cmd.extend([f"-c:s:{out_idx}", "mov_text"])
					logs.append(f"\033[94m   |<S:{idx:2}>|{codec:^6}|{lang:<3}| Re-encode -> mov_text {'(Default)' if current_valid_idx == found_default_idx else ''}\033[0m")
				out_idx += 1
				current_valid_idx += 1
				found_valid = True

	# If we found duplicates/changed disposition, we need to process
	if has_duplicates or found_default_idx != -1:
		if not is_correctly_defaulted:
			skip_all = False
			logs.append("\033[93m   |Note: Forcing Remux to correct Subtitle Language Priority|")
		# If it's already correctly defaulted, we might still need to skip_all = False due to hashes
		# but if it was just about disposition, and that's correct, we don't force skip_all = False.
		elif has_duplicates:
			skip_all = False
	
	# 2. External
	sidecar_cmd = []
	side_lang = "eng"
	side_disp = "default"

	try:
		# If we already found an internal default, maybe don't make sidecar default?
		# Actually, sidecars are usually intentional matches, so we keep the logic in add_subtl_from_file
		res_cmd, res_skip, res_logs, res_lang, res_disp = add_subtl_from_file(str(media.path), sub_streams)
		logs.extend(res_logs)
		if res_cmd:
			sidecar_cmd = res_cmd
			side_lang = res_lang
			side_disp = res_disp
			# If sidecar is added as default, we might have two defaults (internal and sidecar). 
			# FFmpeg usually handles this, but players pick the first.
			skip_all = False
			found_valid = True
	except Exception: pass

	# 3. Precision: Check for missing languages (Defaults and AI)
	existing_langs = {s.get("tags", {}).get("language", "und").lower() for s in media.streams if s.get("codec_type") == "subtitle"}
	
	# Rule: Default language must be 'eng' (or target_lang)
	if target_lang not in existing_langs and Utils.ENSURE_ENG_SUB:
		# OPTIMIZATION: Only force processing if we actually found a sidecar to add!
		if sidecar_cmd:
			skip_all = False
			logs.append(f"\033[93m   |Action: Adding missing mandatory '{target_lang}' subtitle from sidecar.|\033[0m")
		else:
			logs.append(f"\033[93m   |Note: Missing mandatory '{target_lang}' subtitle (Not found in sidecars, skipping force-remux).|\033[0m")
	
	# Rule: Check AI translation requirements
	missing_ai_langs = [l for l in Utils.ADD_AI_SUB_LANGS if l.lower() not in existing_langs]
	if missing_ai_langs:
		# AI translation happens later/separately, so we might still skip if this is the only reason
		# but usually AI translation implies we NEED to process.
		# For now, let's keep it safe.
		pass

	if not found_valid:
		logs.append("\033[94m  .No compatible subtitles, check for external files.\033[0m")

	return cmd, skip_all, logs, sidecar_cmd, side_lang, side_disp

# =============================================================================
# 7. EXECUTION & PROGRESS
# =============================================================================

def _read_pipe1_progress(pipe, task_id, duration, src_fps):
	start_time = time.time()
	last_update = 0
	d = {}

	for line in iter(pipe.readline, ''):
		line = line.strip()
		if not line: continue

		if "=" in line:
			k, v = line.split("=", 1)
			d[k.strip()] = v.strip()

		if line == "progress=continue" or line == "progress=end":
			try:
				now = time.time()
				is_final = (line == "progress=end")
				
				# THROTTLE: Update terminal at most 5 times per second (every 0.2s)
				if (now - last_update) < 0.2 and not is_final:
					continue
				last_update = now

				# 1. Extract Current Time (Processed)
				us = 0
				for k in ["out_time_us", "out_time"]:
					v = str(d.get(k, "0")).strip()
					if not v or v in ("N/A", "00:00:00.000000"): continue
					try:
						if ":" in v:
							# Parse format "HH:MM:SS.mmmmmm"
							parts = v.replace(",", ".").split(":")
							if len(parts) == 3:
								h, m, s = parts
								us = int((float(h)*3600 + float(m)*60 + float(s)) * 1000000)
						else:
							# Parse raw microseconds (int)
							us = int(float(v))
						if us != 0: break # Found it
					except Exception: pass

				sec = us / 1_000_000

				# Fallback: If time-based progress is 0, try frame-based
				if sec == 0 and src_fps > 0 and duration > 0:
					current_frame = int(d.get("frame", 0))
					total_frames = int(duration * src_fps)
					if current_frame > 0 and total_frames > 0:
						sec = (current_frame / total_frames) * duration

				# Ensure we don't divide by zero
				safe_dur = max(duration, 1.0)
				pct = min(100.0, (sec / safe_dur) * 100) if sec > 0 else 0.0

				sz = int(d.get("total_size", 0))
				sz_str = f"{sz/1024/1024:.1f} MB"

				br = d.get("bitrate", "0").replace("kbits/s", "k")
				spd = d.get("speed", "0").replace("x", "")
				fps = d.get("frame", "0")

				el = now - start_time

				# 2. Metric Fallbacks (If FFmpeg reports N/A or 0.0)
				if spd in ("N/A", "0.00", "0") and el > 1.0 and sec > 0:
					spd = f"{sec / el:.2f}"
				if br in ("0k", "N/A", "0.0k") and sz > 0 and sec > 1.0:
					br = f"{(sz * 8) / sec / 1000:.1f}k"

				el = now - start_time
				eta = "--:--"
				# Show ETA if we have meaningful progress (>0.01%) AND at least 3 seconds elapsed
				if pct > 0.01 and el > 3:
					rem = (el / (pct/100)) - el
					eta = time.strftime("%H:%M:%S", time.gmtime(rem))

				# DIRECT PRINT (COMPACT: Shortened to <80 chars to prevent terminal wrap)
				# Indented with 3 spaces to align with headers/messages
				msg = f"\r   [T] {pct:>5.1f}%|{sz_str:>8}|{fps:>6}f|{br:>7}|{spd:>5}x|ETA:{eta}  "
				with Utils.print_lock:
					sys.stdout.write(msg)
					sys.stdout.flush()
			except Exception: pass

	sys.stdout.write("\n")
	return 0

# =============================================================================
# 6. ARTIFACT GENERATION (Restored & Robust)
# =============================================================================

def _faststart_remux(inp: Path, out: Path, task_id: str = "T1", duration: Optional[float] = None, add_hvc1_tag: bool = False) -> Tuple[bool, int]:
	"""
	Final remux: copy streams, ensure +faststart, and authoritatively set SKIP_KEY.
	Can also add the hvc1 video tag.
	"""
	if out.exists():
		try:		out.unlink(missing_ok=True) # Use missing_ok=True for cleaner deletion
		except Exception as e:			Utils.safe_print(f"   [{task_id}] WARNING: Could not delete existing output {out.name}: {e}")

	cmd2 = [FFMPEG, "-y", "-hide_banner",
					"-i", str(inp),
					"-map_metadata", "-1",	# clear; don't import source tags
					"-map", "0:v?",
					"-map", "0:a?",
					"-map", "0:s?",
					"-c", "copy",
				]

	# --- NEW: Add hvc1 tag if requested ---
	if add_hvc1_tag:	cmd2.extend(["-tag:v", "hvc1"])

	cmd2.extend([	"-movflags", "+faststart+use_metadata_tags", # Ensure faststart and modern tags
					"-metadata", f"comment={SKIP_KEY}",	str(out)	])

	# Use print_lock for thread safety
	log_action = "Stage-2 remux (+faststart,use_metadata_tags & SKIP_KEY)"
	if add_hvc1_tag:	log_action += " (+hvc1 tag)"
	Utils.safe_print(f"\n   [{task_id}] {log_action}")

	# Pass distinct task ID suffix (_FS)
	# Reuse _run_ffmpeg_live (assuming it exists in scope, or use subprocess directly if not)
	# NOTE: FFMpeg.py usually uses MediaFile.run logic or direct subprocess.
	# Since _run_ffmpeg_live might be from FFMpeg_o.py, we'll adapt to use _popen_managed if needed.
	# But wait, FFMpeg_o.py had _run_ffmpeg_live. FFMpeg.py DOES NOT seem to have it exposed at module level.
	# Check if _popen_managed is available. Yes.

	try:
		p = _popen_managed(cmd2, stdout=sp.PIPE, stderr=sp.PIPE)
		if duration and duration > 0:
			# Simple wait since we don't have the live-reader here easily without copy-paste
			# actually, let's use a simpler approach for artifacts: just wait.
			pass

		stdout, stderr = p.communicate(timeout=REMUX_TIMEOUT_S)
		rc2 = p.returncode
		PROC_MGR.unregister(p)

		if rc2 != 0:
			full_log = (stderr or b"").decode(errors="replace") if isinstance(stderr, bytes) else str(stderr)
			errlog_block(str(inp), f"ffmpeg faststart rc={rc2}", full_log)
			return False, rc2

		return True, 0

	except Exception as e:
		Utils.safe_print(f"[{task_id}] Remux Exception: {e}")
		return False, -1


def _artifact_run(cmd: List[str], task_id: str) -> bool:
	"""
	Lightweight FFmpeg runner for artifacts; no interactive HUD.
	Returns True on rc==0.
	"""
	if de_bug:	Utils.safe_print(f"[{task_id}] run: {' '.join(shlex.quote(c) for c in cmd)}")
	try:
		# STARTUPINFO to hide window on Windows
		startupinfo = None
		if IS_WIN:
			startupinfo = sp.STARTUPINFO()
			startupinfo.dwFlags |= sp.STARTF_USESHOWWINDOW

		proc = _popen_managed(cmd, stdout=sp.PIPE, stderr=sp.PIPE, startupinfo=startupinfo)
		try:		_, err = proc.communicate(timeout=max(120, REMUX_TIMEOUT_S // 4))
		finally:	PROC_MGR.unregister(proc)

		if proc.returncode != 0:
			with print_lock:
				print(f"[{task_id}] ffmpeg failed rc={proc.returncode}")
				if err:		print(err[-800:].decode(errors="ignore") if isinstance(err, bytes) else str(err)[-800:])
				return False
		return True
	except sp.TimeoutExpired:
		Utils.safe_print(f"[{task_id}] ffmpeg timeout.")
		return False
	except Exception as e:
		Utils.safe_print(f"[{task_id}] ffmpeg exception: {e}")
		return False


def _ff_atempo_chain(factor: float) -> str:
	"""
	Build a legal atempo chain for FFmpeg.
	Each 'atempo' filter must be between 0.5 and 2.0.
	"""
	if factor <= 0.0:	return "atempo=1.0"
	filters = []
	temp_factor = float(factor)
	import math

	while temp_factor > 2.0:
		filters.append("atempo=2.0")
		temp_factor /= 2.0
	while temp_factor < 0.5 and temp_factor > 0:
		filters.append("atempo=0.5")
		temp_factor /= 0.5

	if not math.isclose(temp_factor, 1.0):
		filters.append(f"atempo={temp_factor:.6f}")
	return ",".join(filters) if filters else "atempo=1.0"


def matrix_it(
	input_path: Path,
	output_image_path: Path,
	task_id_base: str,
	# --- Hints from output_info ---
	duration: Optional[float],
	width: Optional[int],
	height: Optional[int],
	# --- Config with Defaults ---
	columns: int = ADDITIONAL_MATRIX_COLS,
	rows: int = ADDITIONAL_MATRIX_ROWS,
	start_skip_percent: float = ADDITIONAL_MATRIX_SKIP_PCT_START,
	end_skip_percent: float = ADDITIONAL_MATRIX_SKIP_PCT_END,
	thumb_width: int = ADDITIONAL_MATRIX_WIDTH
) -> bool:
	"""
	Creates a thumbnail matrix image, skipping start/end percentages.
	Uses provided duration/dimensions.
	"""
	try:
		# --- Parameter and Input Info Validation ---
		if duration is None or duration <= 1.0 or width is None or width <= 0 or height is None or height <= 0:
			Utils.safe_print(f"   [{task_id_base}] ERROR: Insufficient info (duration/width/height) for matrix."); return False
		if columns < 1 or rows < 1 or not (0 <= start_skip_percent < 50) or not (0 <= end_skip_percent < 50):
			Utils.safe_print(f"   [{task_id_base}] ERROR: Invalid matrix parameters."); return False

		num_thumbs = columns * rows
		ext = output_image_path.suffix.lower();
		if ext not in [".png", ".jpg", ".jpeg"]: ext = ".png"

		# --- Calculations based on provided info ---
		start_time = duration * (start_skip_percent / 100.0)
		end_time = duration * (1 - end_skip_percent / 100.0)

		if start_time >= end_time:
			Utils.safe_print(f"   [{task_id_base}] ERROR: Start time % ({start_skip_percent}%) >= end time % ({100-end_skip_percent}%). Skipping matrix."); return False

		effective_duration = end_time - start_time
		if effective_duration <= 0:
			Utils.safe_print(f"   [{task_id_base}] ERROR: Zero or negative effective duration for matrix."); return False

		interval = effective_duration / num_thumbs if num_thumbs > 0 else 0
		thumb_height = int(round(thumb_width * height / width / 2) * 2) if width > 0 else int(round(thumb_width * 9/16 / 2) * 2)
		if thumb_height <= 0: thumb_height = int(round(thumb_width * 9/16 / 2) * 2)

		with tempfile.TemporaryDirectory(prefix="matrix_thumbs_") as temp_dir_str:
			temp_dir = Path(temp_dir_str); thumb_paths = []; extraction_ok = True
			for i in range(num_thumbs):
				time_pos = start_time + (i + 0.5) * interval
				time_pos = min(time_pos, duration - 0.01) # Ensure within bounds

				thumb_file = temp_dir / f"thumb_{i:03d}{ext}"
				thumb_paths.append(str(thumb_file))
				extract_cmd = [ FFMPEG, "-y", "-hide_banner", "-ss", str(time_pos),
								"-i", str(input_path), "-vframes", "1",
								"-vf", f"scale={thumb_width}:{thumb_height}:force_original_aspect_ratio=decrease,format=rgb24",
								"-an", str(thumb_file), ]

				if not _artifact_run(extract_cmd, task_id=f"{task_id_base}_T{i}"):
					Utils.safe_print(f"   [{task_id_base}] ERROR: Failed extracting thumb {i+1} at {time_pos:.2f}s.")
					extraction_ok = False; break

			if not extraction_ok: return False

			inputs_args = sum([["-i", p] for p in thumb_paths], [])

			concat_inputs = "".join(f"[{j}:v]" for j in range(num_thumbs))
			filter_complex = (
				f"{concat_inputs}concat=n={num_thumbs}:v=1:a=0[v]; "
				f"[v]tile={columns}x{rows}:padding=5:margin=5[out]"
			)
			tile_cmd =	[ FFMPEG, "-y", "-hide_banner" ] + inputs_args + \
						["-filter_complex", filter_complex, "-map", "[out]",
						"-frames:v", "1", str(output_image_path) ]

			if not _artifact_run(tile_cmd, task_id=f"{task_id_base}_TILE"):
				Utils.safe_print(f"   [{task_id_base}] ERROR: tiling thumbnails failed.")
				return False

		return True # Success

	except Exception as e:
		Utils.safe_print(f"   [{task_id_base}] ERROR in matrix_it: {e}")
		try: errlog_block(str(input_path), "matrix_it exception", f"{e}\n{traceback.format_exc()}")
		except Exception: pass
		return False


def speed_up( input_path: Path, factor: float, output_path: Path, task_id: str,
	has_audio: bool,
	duration: Optional[float] = None
) -> bool:
	"""Changes video and audio speed, with conditional downscale (no upscaling) and fast encode."""
	Utils.safe_print(f"   [{task_id}] Creating {factor:.2f}x speed version...")

	if factor <= 0:
		Utils.safe_print(f"   [{task_id}] ERROR: Speed factor must be positive."); return False

	TARGET_MAX_WIDTH = 960  # adjust to taste (960/1280/1920)

	# IMPORTANT: escape commas inside if() -> \,
	# Use positional args for scale: width:height:flags
	vf_chain = (
		f"scale=if(gt(iw\\,{TARGET_MAX_WIDTH})\\,{TARGET_MAX_WIDTH}\\,iw):-2:flags=bicubic,"
		"setsar=1,"
		f"setpts=PTS/{factor}"
	)

	cmd_base = [FFMPEG, "-y", "-hide_banner", "-i", str(input_path)]
	filter_args, map_args, audio_codec_args = [], [], []

	if has_audio:
		af = _ff_atempo_chain(factor)  # handles factors > 2.0 via chaining
		filter_args += ["-filter_complex", f"[0:v]{vf_chain}[v];[0:a]{af}[a]"]
		map_args    += ["-map", "[v]", "-map", "[a]"]
		audio_codec_args += ["-c:a", "aac", "-b:a", "160k", "-ar", "48000"]
	else:
		filter_args += ["-filter_complex", f"[0:v]{vf_chain}[v]"]
		map_args    += ["-map", "[v]", "-an"]

	crf_val = str(22 if ADDITIONAL_QUALITY_CRF is None else ADDITIONAL_QUALITY_CRF)
	video_codec_args = [
		"-c:v", "libx264",
		"-preset", "veryfast",
		"-crf", crf_val,
		"-pix_fmt", "yuv420p",
	]

	output_args = ["-movflags", "+faststart", "-f", "mp4", str(output_path)]
	cmd = cmd_base + filter_args + map_args + video_codec_args + audio_codec_args + output_args

	success = _artifact_run(cmd, task_id=task_id)

	if not success:
		errlog_block(str(input_path), f"speed_up failed [{task_id}]", " ".join(cmd))

	return success


def short_ver( input_path: Path, output_path: Path, task_id: str,
	duration: Optional[float],
	clip_duration: float        = ADDITIONAL_SHORT_DUR,
	start_skip_percent: float   = ADDITIONAL_SHORT_SKP_STRT
) -> bool:
	"""Creates a short clip by re-encoding, skipping a start percentage, with conditional downscale."""
	Utils.safe_print(f"   [{task_id}] Creating {clip_duration:.1f}s short version (re-encoding, skipping {start_skip_percent:.1f}%)...")

	if duration is None or duration <= 0:
		Utils.safe_print(f"   [{task_id}] ERROR: Cannot calculate start skip without total duration."); return False
	if clip_duration <= 0 or not (0 <= start_skip_percent < 100):
		Utils.safe_print(f"   [{task_id}] ERROR: Invalid clip duration or skip percentage."); return False

	start_time_sec = duration * (start_skip_percent / 100.0)
	actual_clip_duration = clip_duration
	if start_time_sec + clip_duration > duration:
		actual_clip_duration = duration - start_time_sec
		if actual_clip_duration <= 0.01:
			Utils.safe_print(f"   [{task_id}] ERROR: Start skip ({start_skip_percent}%) results in zero or negative duration clip."); return False

	TARGET_MAX_WIDTH = 1280

	# Escape commas in if()
	vf = f"scale=if(gt(iw\\,{TARGET_MAX_WIDTH})\\,{TARGET_MAX_WIDTH}\\,iw):-2:flags=bicubic,setsar=1"

	crf_val = str(22 if ADDITIONAL_QUALITY_CRF is None else ADDITIONAL_QUALITY_CRF)

	cmd = [
		FFMPEG, "-y", "-hide_banner",
		"-ss",   str(start_time_sec),
		"-i",    str(input_path),
		"-t",    str(actual_clip_duration),
		"-map",  "0:v:0?",
		"-map",  "0:a:0?",
		"-vf",   vf,
		"-c:v",  "libx264",
		"-preset","veryfast",
		"-crf",  crf_val,
		"-pix_fmt", "yuv420p",
		"-c:a",  "aac",
		"-b:a",  "160k",
		"-ar",   "48000",
		"-movflags", "+faststart",
		"-avoid_negative_ts", "make_zero",
		"-f", "mp4",
		str(output_path)
	]

	success = _artifact_run(cmd, task_id=task_id) # Use the lightweight runner
	if not success:
		errlog_block(str(input_path), f"short_ver (re-encode) failed [{task_id}]", " ".join(cmd))

	return success


def post_encode_artifacts(
	final_file_path: Path,
	output_info: Dict[str, Any],
	task_id_base: str,
	*,
	which: Optional[Iterable[str]] = None,   # e.g. ["matrix","speed","short"] ; None = follow toggles
	main_was_skipped: bool = False,          # tell us if main job was a true skip
	de_bug: bool = False,
) -> None:
	"""
	Best-effort artifact creation after successful encode/remux.
	"""

	# ---------- Guards ----------
	if not final_file_path or not final_file_path.exists():
		Utils.safe_print(f"   [{task_id_base}] Skipping artifacts: final file missing.")
		return
	if not output_info:
		Utils.safe_print(f"   [{task_id_base}] Skipping artifacts: missing output info.")
		return

	# Respect global master switch + skip policy
	if not Utils.ADD_ADDITIONAL:
		if not main_was_skipped:
			Utils.safe_print(f"   [{task_id_base}] Artifacts disabled (ADD_ADDITIONAL=False).")
		return
	if main_was_skipped and not Utils.FORCE_ARTIFACTS_ON_SKIP:
		Utils.safe_print(f"   [{task_id_base}] Main job skipped; artifacts disabled unless FORCE_ARTIFACTS_ON_SKIP=True.")
		return

	base_name = final_file_path.stem
	duration  = output_info.get("dur_out")
	width     = output_info.get("w_enc")
	height    = output_info.get("h_enc")
	has_audio = (output_info.get("ach_out") or 0) > 0

	# ---------- Small inner engine ----------
	def _make_artifact(final_path: Path, min_size: int, builder: Callable[[Path], bool], label: str) -> str:
		"""Run one artifact; return 'ok' | 'fail' | 'skip'."""
		# Fast idempotent skip
		try:
			if final_path.is_file() and final_path.stat().st_size >= min_size:
				Utils.safe_print(f"   [{task_id_base}] Skipping {label}: {final_path.name} already exists.")
				return "skip"
		except Exception:	pass  # treat as not present

		tmp = final_path.parent / f".__tmp_{final_path.name}"

		try:
			if tmp.exists():	tmp.unlink(missing_ok=True)
		except Exception:	pass

		ok = False
		try:	ok = bool(builder(tmp))
		except Exception as e:
			Utils.safe_print(f"   [{task_id_base}] ERROR creating {label}: {e}")
			try: errlog_block(str(final_file_path), f"{label} build exception", f"{e}\n{traceback.format_exc()}")
			except Exception: pass

		if ok:
			try:
				if tmp.is_file() and tmp.stat().st_size >= min_size:
					# Use retry_with_lock_info for the atomic replace
					retry_with_lock_info(
						f"finalize {final_path.name}",
						func	=tmp.replace,
						args	=(final_path,),
						de_bug	=de_bug
					)
					Utils.safe_print(f"   [{task_id_base}] {label} created: {final_path.name}")
					return "ok"
				else:
					Utils.safe_print(f"   [{task_id_base}] ERROR {label}: output too small or missing after build.")
			except Exception as e:
				Utils.safe_print(f"   [{task_id_base}] ERROR finalizing {label}: {e}")
				try: errlog_block(str(final_file_path), f"{label} finalize exception", f"{e}\n{traceback.format_exc()}")
				except Exception: pass

		# cleanup tmp
		try:
			if tmp.exists():	tmp.unlink(missing_ok=True)
		except Exception:		pass
		return "fail"

	def _build_matrix(tmp: Path) -> bool:
		return matrix_it(
			final_file_path,          # input_path
			tmp,                      # output_image_path (write to tmp for atomic replace)
			f"{task_id_base}_MATRIX", # task_id_base
			duration= float(duration or 0.0),
			width=    width,
			height=   height
		)

	def _build_speed(tmp: Path) -> bool:
		return bool(speed_up(
			final_file_path,
			Utils.ADDITIONAL_SPEED_FACTOR,
			tmp,
			f"{task_id_base}_SPEED",
			has_audio=has_audio,
			duration=duration
		))

	def _build_short(tmp: Path) -> bool:
		return bool(short_ver(
			final_file_path,
			tmp,
			f"{task_id_base}_SHORT",
			duration=duration
		))

	# Just add a new tuple for future artifacts.
	artifact_jobs: Dict[str, Tuple[bool, Path, int, Callable[[Path], bool], str]] = {
		# "key": (ENABLE_TOGGLE, final_path, min_size, build_function, label_for_logs)
		"matrix": ( Utils.ADD_ARTIFACT_MATRIX,
					final_file_path.with_name(f"{base_name}_matrix_{Utils.ADDITIONAL_MATRIX_COLS}x{Utils.ADDITIONAL_MATRIX_ROWS}.png"),
					100,
					_build_matrix,
					f"Create {Utils.ADDITIONAL_MATRIX_COLS}x{Utils.ADDITIONAL_MATRIX_ROWS} image matrix",
					),
		"speed": ( Utils.ADD_ARTIFACT_SPEED,
					final_file_path.with_name(f"{base_name}_fast_{Utils.ADDITIONAL_SPEED_FACTOR:.1f}x.mp4"),
					1024,
					_build_speed,
					f"Speed-up ({Utils.ADDITIONAL_SPEED_FACTOR:.1f}x)",
					),
		"short": ( Utils.ADD_ARTIFACT_SHORT,
					final_file_path.with_name(f"{base_name}_short_{int(Utils.ADDITIONAL_SHORT_DUR)}s.mp4"),
					1024,
					_build_short,
					f"Short version ({int(Utils.ADDITIONAL_SHORT_DUR)}s)",
				),
	}

	# Determine which to run: explicit list overrides toggles
	if which is None:
		# use toggles
		plan = [name for name, (enabled, *_rest) in artifact_jobs.items() if enabled]
	else:
		# normalize, keep only those we know about
		req = [w.strip().lower() for w in which]
		plan = [name for name in req if name in artifact_jobs]

	with Utils.print_lock:
		pretty = ", ".join(plan) if plan else "(none)"
		print(f"-> [{task_id_base}] Creating Additional Versions for {final_file_path.name} — requested: {pretty}")

	# ---------- Execute & summarize ----------
	results: Dict[str, str] = {}
	ok_like = 0

	if not plan: 	return # Don't print summary if nothing was planned

	for name in plan:
		enabled, path_out, min_size, builder, label = artifact_jobs[name]
		status = _make_artifact(path_out, min_size, builder, label)
		results[label] = status
		if status in ("ok", "skip"):  ok_like += 1

	with print_lock:
		parts = [f"{lbl.split(' ')[0]}={st}" for (lbl, st) in results.items()]
		print(f"-> [{task_id_base}] Finished Additional Versions — {ok_like}/{len(plan)} ok  |  " + "  ·  ".join(parts))
