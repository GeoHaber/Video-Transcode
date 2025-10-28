# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import re
import sys
import json
import time
import math
import shlex
import queue
import atexit
import signal
import tempfile
import platform
import threading
import traceback
import subprocess as sp

from typing import Any, Dict, List, Optional, Tuple, Literal
from pathlib import Path
from fractions import Fraction
from dataclasses import dataclass
from collections import defaultdict

# Import all constants, globals, and helpers from our foundation
from Utils import *

# -----------------------------------------------------------------------------
# Process management (Windows Job / POSIX groups)
# -----------------------------------------------------------------------------

if IS_WIN:
	import ctypes
	kernel32 = ctypes.WinDLL('kernel32', use_last_error=True)
	PROCESS_ALL_ACCESS = 0x1F0FFF

	class JOBOBJECT_BASIC_LIMIT_INFORMATION(ctypes.Structure):
		_fields_ = [
			("PerProcessUserTimeLimit", ctypes.c_longlong),
			("PerJobUserTimeLimit", ctypes.c_longlong),
			("LimitFlags", ctypes.c_uint32),
			("MinimumWorkingSetSize", ctypes.c_size_t),
			("MaximumWorkingSetSize", ctypes.c_size_t),
			("ActiveProcessLimit", ctypes.c_uint32),
			("Affinity", ctypes.c_size_t),
			("PriorityClass", ctypes.c_uint32),
			("SchedulingClass", ctypes.c_uint32),
		]

	class IO_COUNTERS(ctypes.Structure):
		_fields_ = [
			("ReadOperationCount", ctypes.c_ulonglong),
			("WriteOperationCount", ctypes.c_ulonglong),
			("OtherOperationCount", ctypes.c_ulonglong),
			("ReadTransferCount", ctypes.c_ulonglong),
			("WriteTransferCount", ctypes.c_ulonglong),
			("OtherTransferCount", ctypes.c_ulonglong),
			]

	class JOBOBJECT_EXTENDED_LIMIT_INFORMATION(ctypes.Structure):
		_fields_ = [
			("BasicLimitInformation", JOBOBJECT_BASIC_LIMIT_INFORMATION),
			("IoInfo", IO_COUNTERS),
			("ProcessMemoryLimit", ctypes.c_size_t),
			("JobMemoryLimit", ctypes.c_size_t),
			("PeakProcessMemoryUsed", ctypes.c_size_t),
			("PeakJobMemoryUsed", ctypes.c_size_t),
		]

	class _WinJob:
		JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x00002000
		JobObjectExtendedLimitInformation = 9

		def __init__(self) -> None:
			self.hJob = kernel32.CreateJobObjectW(None, None)
			if not self.hJob:
				raise OSError("CreateJobObjectW failed")

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
			if not self.hJob:
				return
			hProc = kernel32.OpenProcess(PROCESS_ALL_ACCESS, False, pid)
			if not hProc:
				return
			try:
				kernel32.AssignProcessToJobObject(self.hJob, hProc)
			finally:
				kernel32.CloseHandle(hProc)

		def close(self) -> None:
			if self.hJob:
					kernel32.CloseHandle(self.hJob)
					self.hJob = None
else:
	_WinJob = None  # type: ignore

class ChildProcessManager:
	def __init__(self) -> None:
		self._procs: List[sp.Popen] = []
		self._lock = threading.Lock()
		self._stopping = threading.Event()
		self._win_job = _WinJob() if IS_WIN else None
		self._install_signal_handlers()

	def _install_signal_handlers(self) -> None:
		def handler(signum: int, _frame: Any) -> None:
			if self._stopping.is_set():
				return
			self._stopping.set()
			with print_lock:
				print(f"\n\033[93m[signal]\033[0m Received {signum}. Terminating children...")
			self.terminate_all()

		signal.signal(signal.SIGINT, handler)
		signal.signal(signal.SIGTERM, handler)

	def _attach_job(self, proc: sp.Popen) -> None:
		if IS_WIN and self._win_job:
			try:
				self._win_job.add_pid(proc.pid)
			except Exception:
				pass

	def register(self, proc: sp.Popen) -> None:
		with self._lock:
			self._procs.append(proc)
		self._attach_job(proc)

	def unregister(self, proc: sp.Popen) -> None:
		with self._lock:
			try:
				self._procs.remove(proc)
			except ValueError:
				pass

	def terminate_all(self) -> None:
		with self._lock:
			procs = list(self._procs)

		if not procs:
			return

		if IS_WIN:
			for p in procs:
				try:
					p.send_signal(signal.CTRL_BREAK_EVENT)
				except Exception:
					pass
		else:
			for p in procs:
				try:
					os.killpg(os.getpgid(p.pid), signal.SIGTERM)
				except Exception:
					pass

				deadline = time.time() + 5.0
				for p in procs:
					try:
						p.wait(timeout=max(0, deadline - time.time()))
					except Exception:
						pass

				for p in procs:
					if p.poll() is None:
						try:
							if IS_WIN:
								p.kill()
							else:
								os.killpg(os.getpgid(p.pid), signal.SIGKILL)
						except Exception:
							pass

				for p in procs:
						try:
								p.wait(timeout=2)
						except Exception:
								pass

				with self._lock:
						self._procs.clear()

	def close(self) -> None:
		if IS_WIN and self._win_job:
			try:
				self._win_job.close()
			except Exception:
				pass

PROC_MGR = ChildProcessManager()
atexit.register(PROC_MGR.terminate_all)
atexit.register(PROC_MGR.close)

def _popen_managed(cmd: List[str], **kwargs: Any) -> sp.Popen:
		if IS_WIN:
				flags = kwargs.pop("creationflags", 0) | CREATE_NEW_PROCESS_GROUP
				kwargs["creationflags"] = flags
				kwargs.setdefault("text", True)
				kwargs.setdefault("encoding", "utf-8")
				kwargs.setdefault("errors", "replace")
		else:
				kwargs.setdefault("preexec_fn", os.setsid)
				kwargs.setdefault("text", True)
				kwargs.setdefault("encoding", "utf-8")
				kwargs.setdefault("errors", "replace")

		proc = sp.Popen(cmd, **kwargs)
		PROC_MGR.register(proc)
		return proc

# -----------------------------------------------------------------------------
# SRIK (per-run mailbox for planning data)
# -----------------------------------------------------------------------------

_SRIK_LOCK = threading.RLock()
_GLOBAL_SRIK: Dict[str, Dict[str, Any]] = {}

def _srik_key(path: str | Path) -> str:
		try:
				return str(Path(path).resolve())
		except Exception:
				return str(path)

def srik_update(path: str | Path,
								*,
								source: Optional[Dict[str, Any]] = None,
								plan: Optional[Dict[str, Any]] = None,
								output: Optional[Dict[str, Any]] = None) -> None:
		k = _srik_key(path)
		with _SRIK_LOCK:
				entry = _GLOBAL_SRIK.get(k, {})
				if source is not None:
						entry["source"] = source
				if plan is not None:
						entry["plan"] = plan
				if output is not None:
						entry["output"] = output
				_GLOBAL_SRIK[k] = entry

def srik_get(path: str | Path) -> Dict[str, Any]:
		k = _srik_key(path)
		with _SRIK_LOCK:
				return dict(_GLOBAL_SRIK.get(k, {}))

def srik_clear(path: str | Path) -> None:
		k = _srik_key(path)
		with _SRIK_LOCK:
				_GLOBAL_SRIK.pop(k, None)

# -----------------------------------------------------------------------------
# FFprobe
# -----------------------------------------------------------------------------
def ffprobe_run(input_file: str,
				execu: Optional[str] = None,
				de_bug: bool = False,
				check_corruption: bool = False) -> Tuple[Optional[Dict[str, Any]], bool, Optional[str]]:

	if execu is None:
		execu = FFPROBE

	metadata: Optional[Dict[str, Any]] = None
	is_corrupted = False
	error_msg: Optional[str] = None

	cmd = [
		execu, "-v", "error",
		"-analyzeduration", "5M", # Analyze up to 5MB of data
		"-probesize", "5M",       # Probe up to 5MB of data
		"-show_streams",
		"-show_format",
		"-of", "json",
		input_file
	]
	try:
		proc = _popen_managed(cmd, stdout=sp.PIPE, stderr=sp.PIPE)
		try:
			out, err = proc.communicate(timeout=PROBE_TIMEOUT_S)
		finally:
			PROC_MGR.unregister(proc)
		if proc.returncode != 0:
			error_msg = f"ffprobe failed with {proc.returncode}.\nStderr: {err.strip()}"
		elif out and out.strip().startswith("{"):
			metadata = json.loads(out)
			if err and err.strip():
				errlog_block(input_file, "ffprobe stderr (non-fatal)", err)
		else:
			error_msg = "ffprobe did not return valid JSON."
	except sp.TimeoutExpired:
		error_msg = f"ffprobe timed out after {PROBE_TIMEOUT_S} seconds."
	except Exception as e:
		error_msg = f"ffprobe exception: {e}"
	if not error_msg and check_corruption and os.path.isfile(FFMPEG):
		p2 = None # Initialize p2
		try:
			sniff = [FFMPEG, "-v", "error", "-xerror", "-i", input_file, "-t", "10", "-f", "null", "-"]
			p2 = _popen_managed(sniff, stdout=sp.DEVNULL, stderr=sp.PIPE)
			try:
				_, err2 = p2.communicate(timeout=30)
			finally:
				PROC_MGR.unregister(p2)
			if p2.returncode != 0:
				is_corrupted = True
		except sp.TimeoutExpired: # Catch specific timeout
			is_corrupted = True
			if p2: PROC_MGR.unregister(p2) # Ensure unregister on timeout
		except Exception:
			is_corrupted = True
			if p2: PROC_MGR.unregister(p2) # Ensure unregister on other errors
	if error_msg:
		errlog_block(input_file, "ffprobe error", error_msg)

	return metadata, is_corrupted, error_msg

# -----------------------------------------------------------------------------
# Planning helpers (video/audio/subs)
# -----------------------------------------------------------------------------

@dataclass
class VideoContext:
		input_file: str
		estimated_video_bitrate: int = 0

def _ideal_hevc_bps(w: int, h: int, fps: float) -> int:
		if w <= 0 or h <= 0:
				return 2_000_000
		return int(HEVC_BPP * w * h * (fps or 24.0))

def _parse_fps(avg_frame_rate: str) -> float:
		try:
				return float(Fraction(avg_frame_rate))
		except Exception:
				return 24.0

def compute_aspect_ratio(width: int, height: int) -> Tuple[str, float]:
		if width <= 0 or height <= 0:
				raise ValueError("Width and height must be positive")
		raw_ratio = width / height
		APPR_RATIO = {
				"21:9": 21 / 9,
				"16:9": 16 / 9,
				"3:2":  3 / 2,
				"4:3":  4 / 3,
				"1:1":  1.0,
		}
		best_ratio = min(APPR_RATIO, key=lambda k: abs(APPR_RATIO[k] - raw_ratio))
		return best_ratio, raw_ratio

def get_reencode_settings_based_on_source(codec: str,
																					w: int,
																					h: int,
																					br: int,
																					is_10b: bool,
																					fps: float
																					) -> Tuple[bool, List[str], str, Optional[str], int]:

		encod_mod: Literal["hw", "sw"] = "hw"
		needs_scaling = (w > 2600 or h > 1188)
		target_h = 1080 if needs_scaling else h
		if w > 0 and h > 0:
				target_w = int(round(target_h * (w / h)))
		else:
				target_w = int(round(target_h * 16 / 9))
		if target_w % 2:
				target_w += 1

		ideal_bps = _ideal_hevc_bps(target_w, target_h, fps)

		if br > 0 and br < (ideal_bps * EXTRA_SIZE_OK):
				with print_lock:
						print(f"   |Bit now:{hm_sz(br, 'bps')} Ideal:{hm_sz(ideal_bps,'bps')}| (clamping to source)")
				ideal_bps = min(br, ideal_bps)

		if ideal_bps > 0 and br > 0:
				try:
						ratio = br / max(1.0, float(ideal_bps))
						if ratio > (EXTRA_SIZE_OK + 0.05):
								msg = f"   |Expected Reduction: {ratio:.2f}x|{hm_sz(br,'bps')}/{hm_sz(ideal_bps,'bps')}|"
						else:
								msg = f"   |Similar: {ratio:.2f}x|{hm_sz(br,'bps')}/{hm_sz(ideal_bps,'bps')}|"
						with print_lock:
								print(msg)
				except Exception:
						pass

		is_hevc = (codec or "").lower() == "hevc"
		br_ok = (br <= int(ideal_bps * EXTRA_SIZE_OK)) if br > 0 else True

		reasons: List[str] = []
		if not is_hevc:
				reasons.append("Not HEVC")
		if needs_scaling:
				reasons.append("Scaling")
		if not br_ok:
				reasons.append("Bitrate High")

		must_recode = bool(reasons)
		use_target_10bit = ALWAYS_10BIT and HW_10BIT_ENCOD
		is_pix_fmt_ok = (use_target_10bit == is_10b)

		if not must_recode:
				if is_pix_fmt_ok:
						return False, [], "=> Copy (Fully Compliant)", None, ideal_bps
				return False, [], "=> Copy (Compliant, 8-bit OK)", None, ideal_bps

		if not is_pix_fmt_ok:
				reasons.append("Upgrading to 10-bit")

		pix_fmt = "p010le" if use_target_10bit else "yuv420p"
		profile = "main10" if use_target_10bit else "main"

		vf_filters: List[str] = []
		if needs_scaling:
				vf_filters.append(f"scale=-2:{target_h}:flags=spline")
		vf_filters.append(f"format={pix_fmt}")
		scaler = ",".join(vf_filters)

		if encod_mod == "sw":
				encoder = "libx265"
				preset_tag = "medium"
		else:
				encoder = "hevc_qsv"
				preset_tag = "slow"

		flags = [
				"-c:v", encoder,
				"-preset", preset_tag,
				"-b:v", f"{max(1, ideal_bps // 1000)}k",
				"-profile:v", profile
		]
		status = f"=> Re-encode ({encod_mod}) | {', '.join(reasons)}"
		return True, flags, status, scaler, ideal_bps

def parse_video(streams_in: List[Dict[str, Any]],
								context: VideoContext,
								de_bug: bool) -> Tuple[List[str], bool, List[str]]:

		ff_video: List[str] = []
		skip_all = True
		out_idx = 0
		logs: List[str] = []
		STREAM_SPECIFIC_OPTS = {"-c:v", "-b:v", "-profile:v"}

		for s in streams_in:
				if s.get("disposition", {}).get("attached_pic"):
					logs.append("\033[91m   .Skip: Attached picture stream.\033[0m")
					continue

				if s.get("codec_type") != "video":
					logs.append(f"\033[93m  ?!Warning: Skipping non-video stream index {s.get('index')}\033[0m")
					continue

				codec = s.get("codec_name", "")
				pix_fmt = s.get("pix_fmt", "")
				is_10bit = "10" in str(pix_fmt) or "p010" in str(pix_fmt)
				w = int(s.get("width", 0))
				h = int(s.get("height", 0))
				fps = _parse_fps(s.get("avg_frame_rate", ""))
				try:
					aspect_str, _ = compute_aspect_ratio(w, h)
				except Exception:
					aspect_str = "N/A"
				field_order = str(s.get("field_order", "progressive")).lower()
				is_interlaced = field_order != "progressive"
				color_transfer = str(s.get("color_transfer", "")).lower()
				is_hdr = "2084" in color_transfer or "hlg" in color_transfer
				avg_fps_str = s.get("avg_frame_rate", "0/0")
				r_fps_str = s.get("r_frame_rate", "0/0")
				is_vfr = avg_fps_str != r_fps_str and avg_fps_str != "0/0" and r_fps_str != "0/0"

				needs, flags, status, scaler, ideal = get_reencode_settings_based_on_source(
						codec, w, h, context.estimated_video_bitrate, is_10bit, fps
				)

				ff_video.extend(["-map", f"0:{s['index']}"])

				if not needs:
						ff_video.extend([f"-c:v:{out_idx}", "copy"])
						status = "=> Copy (Fully Compliant)"
						if is_interlaced:
								status += " \033[93m[WARN: Interlaced]\033[0m"
				else:
						skip_all = False
						j = 0
						while j < len(flags):
								opt = flags[j]
								if opt in STREAM_SPECIFIC_OPTS:
										opt_with_idx = f"{opt}:{out_idx}"
								else:
										opt_with_idx = opt

								nxt = flags[j + 1] if (j + 1 < len(flags) and not flags[j + 1].startswith("-")) else None
								if nxt is not None:
										ff_video.extend([opt_with_idx, nxt])
										j += 2
								else:
										ff_video.append(opt_with_idx)
										j += 1

						vf_chain: List[str] = []
						if is_interlaced:
								vf_chain.append("bwdif=mode=send_field:parity=auto:deint=all")
								status += " +Deinterlace"
						if scaler:
								vf_chain.append(scaler)
						if vf_chain:
								ff_video.extend([f"-filter:v:{out_idx}", ",".join(vf_chain)])

				log_msg = (
						f"\033[91m   |<V:{s['index']:2}>|{(codec or 'n/a'):^8}|{w}x{h}|{aspect_str}|"
						f"{fps:.2f} fps|{'10-bit' if is_10bit else '8-bit'}|"
						f"{'HDR' if is_hdr else 'SDR'}|"
						f"{'Interlaced' if is_interlaced else 'Progressive'}|"
						f"{'VFR' if is_vfr else 'CFR'}|"
						f"Bitrt: {hm_sz(context.estimated_video_bitrate, 'bps')} vs Ideal {hm_sz(ideal, 'bps')}| {status}\033[0m"
				)
				logs.append(log_msg)
				out_idx += 1

		if skip_all and out_idx > 0:
				logs.append("\033[91m  .Skip: Video streams are optimal.\033[0m")
		elif out_idx == 0:
				logs.append("\033[93m  ?!Warning: No valid video streams were mapped.\033[0m")

		return ff_video, skip_all, logs

def parse_audio(streams_in: List[Dict[str, Any]],
								context: VideoContext,
								de_bug: bool = False) -> Tuple[List[str], bool, List[str]]:

		if not streams_in:
				return [], True, []

		opts: List[str] = []
		out_idx = 0
		logs: List[str] = []
		any_recode_needed = False
		any_disposition_change_needed = False
		undetermined_lang_count = 0

		def score(s: Dict[str, Any]) -> int:
				tags = s.get("tags", {}) or {}
				sc = 100 if tags.get("language") == Default_lng else 0
				sc += 50 if s.get("disposition", {}).get("default") else 0
				sc += int(s.get("channels", 0)) * 10
				title = str(tags.get("title", "") or "").lower()
				if "commentary" in title:
						sc -= 1000
				return sc

		best_stream = max(streams_in, key=score) if streams_in else None

		for s in streams_in:
				idx = s["index"]
				codec = (s.get("codec_name") or "").lower()
				lang = (s.get("tags", {}) or {}).get("language", "und")
				ch = int(s.get("channels", 0))
				br = int(s.get("bit_rate", 0) or 0)
				disp = s.get("disposition", {})
				sr = int(s.get("sample_rate", 0))
				if lang == "und":
						undetermined_lang_count += 1

				is_compliant = (codec == "aac" and ch in {2, 6} and sr in {0, 48000})
				needs_recode = not is_compliant
				if needs_recode:
						any_recode_needed = True

				per_stream_opts: List[str] = ["-map", f"0:{idx}"]
				actions: List[str] = []

				if is_compliant:
						per_stream_opts.extend([
								f"-c:a:{out_idx}", "copy",
								f"-metadata:s:a:{out_idx}", f"language={lang}"
						])
						actions.append("Copy")
				else:
						reason: List[str] = []
						if codec != "aac":
								reason.append(f"{codec}->aac")
						if ch not in {2, 6}:
								reason.append(f"{ch}ch")
						if sr not in {0, 48000}:
								reason.append(f"{sr}Hz->48kHz")

						per_stream_opts.extend([
								f"-c:a:{out_idx}", "aac",
								f"-ar:a:{out_idx}", "48000",
								f"-metadata:s:a:{out_idx}", f"language={lang}"
						])

						if ch >= 6:
								bitrate = "384k"
								per_stream_opts.extend([f"-ac:a:{out_idx}", "6", f"-b:a:{out_idx}", bitrate])
								actions.append(f"Re-encode ({' '.join(reason)} -> 5.1 {bitrate})")
						else:
								bitrate = "192k"
								per_stream_opts.extend([f"-ac:a:{out_idx}", "2", f"-b:a:{out_idx}", bitrate])
								actions.append(f"Re-encode ({' '.join(reason)} -> Stereo {bitrate})")

				is_best = (s is best_stream)
				needs_disposition_change = (is_best != bool(disp.get("default")))
				if needs_disposition_change:
						any_disposition_change_needed = True

				if is_best:
						if needs_disposition_change:
								actions.append("Set Default")
						per_stream_opts.extend([f"-disposition:a:{out_idx}", "default"])
				else:
						if needs_disposition_change:
								actions.append("Clear Default")
						per_stream_opts.extend([f"-disposition:a:{out_idx}", "0"])

				opts.extend(per_stream_opts)
				logs.append(
						f"\033[92m   |<A:{idx:2}>|{codec:^8}|{lang:<3}|Br:{hm_sz(br,'bps'):<9}|Ch:{ch}|SR:{sr}Hz| {'|'.join(actions)}\033[0m"
				)
				out_idx += 1

		if undetermined_lang_count > 0:
				plural = "s have" if undetermined_lang_count > 1 else " has"
				logs.append(
						f"\033[93m ?!Warning: {undetermined_lang_count} audio stream{plural} undetermined language ('und').\033[0m"
				)

		can_skip_processing = (not any_recode_needed) and (not any_disposition_change_needed)
		if can_skip_processing:
				logs.append("\033[92m   .Skip: Audio streams are optimal (format and disposition).\033[0m")

		return opts, can_skip_processing, logs

_TEXT_SIDE_EXTS = {".srt", ".ass", ".ssa", ".vtt"}
_LANG_ALIASES = {
		"eng": {"en", "eng", "english"},
		"spa": {"es", "spa", "spanish", "esp", "es-419"},
		"fra": {"fr", "fre", "fra", "french"},
		"deu": {"de", "ger", "deu", "german"},
		"ita": {"it", "ita", "italian"},
		"por": {"pt", "por", "ptbr", "pt-br", "brazilian", "portuguese"},
		"rus": {"ru", "rus", "russian"},
		"jpn": {"ja", "jpn", "japanese"},
		"kor": {"ko", "kor", "korean"},
		"heb": {"he", "heb", "hebrew"},
		"hun": {"hu", "hun", "hungarian"},
		"ron": {"ro", "ron", "rum", "romanian"},
		"zho": {"zh", "chi", "zho", "chinese", "zh-cn", "zh-hans", "zh-hant"},
}

_ALIAS_TO_LANG3: Dict[str, str] = {}
for k, vals in _LANG_ALIASES.items():
		for v in vals:
				_ALIAS_TO_LANG3[v.lower()] = k

def _tokens_after_stem(sub_path: Path, video_stem: str) -> List[str]:
		remainder = sub_path.stem[len(video_stem):].lstrip(".-_ ")
		if not remainder:
				return []
		return [t for t in re.split(r"[.\-_ ]+", remainder) if t]

def _guess_lang3_from_filename(sub_path: Path, video_stem: str) -> Optional[str]:
		for tok in _tokens_after_stem(sub_path, video_stem):
				key = tok.lower()
				if key in _ALIAS_TO_LANG3:
						return _ALIAS_TO_LANG3[key]
		return None

_LATIN_SW = {
		"eng": {"the","and","you","that","with","this","have","not","for","are","was","your","it's","i'm","we're"},
		"spa": {"que","de","no","la","el","y","a","los","se","por","del","las","una","un","es","con"},
		"fra": {"le","la","les","de","des","du","un","une","et","que","pour","est","pas","qui"},
		"deu": {"und","der","die","das","nicht","ist","ein","eine","mit","ich","du","sie"},
		"ita": {"il","la","lo","gli","le","un","una","che","non","per","con","di","è"},
		"por": {"de","do","da","os","as","um","uma","que","não","por","com","para","é"},
		"ron": {"și","si","nu","sunt","este","un","o","în","pe","la","din","de"},
}
_SCRIPT_RANGES = {
		"rus": [(0x0400, 0x04FF)],
		"heb": [(0x0590, 0x05FF)],
		"kor": [(0xAC00, 0xD7AF)],
		"jpn": [(0x3040, 0x309F), (0x30A0, 0x30FF)],
		"zho": [(0x4E00, 0x9FFF)],
}

def _guess_lang3_from_text(sub_path: Path, max_bytes: int = 100_000) -> Optional[str]:
		try:
				raw = sub_path.read_bytes()[:max_bytes]
				try:
						text = raw.decode("utf-8", errors="ignore")
				except Exception:
						text = raw.decode("latin-1", errors="ignore")
		except Exception:
				return None

		for code, ranges in _SCRIPT_RANGES.items():
				for ch in text:
						if any(lo <= ord(ch) <= hi for (lo, hi) in ranges):
								return code

		words = re.findall(r"[A-Za-zÀ-ÿ']+", text.lower())
		if not words:
				return None

		best_code: Optional[str] = None
		best_score = 0.0
		for code, stopset in _LATIN_SW.items():
				hits = sum(1 for w in words if w in stopset)
				score = hits / max(1, len(words))
				if score > best_score:
						best_score = score
						best_code = code
		return best_code if best_score >= 0.0015 else None

def _score_sidecar(video_stem: str,
									path: Path,
									default_lng: str,
									keep_langs: set[str]) -> Tuple[int, Optional[str]]:
		score = {".srt": 30, ".ass": 20, ".vtt": 10, ".ssa": 5}.get(path.suffix.lower(), 0)
		tokens = {t.lower() for t in _tokens_after_stem(path, video_stem)}
		file_lang = _guess_lang3_from_filename(path, video_stem)
		if file_lang:
				if file_lang == default_lng:
						score += 1200
				elif file_lang in keep_langs:
						score += 400
		if "sdh" in tokens or "cc" in tokens:
				score += 25
		if "forced" in tokens:
				score -= 600
		text_lang = _guess_lang3_from_text(path)
		if text_lang:
				if text_lang == default_lng:
						score += 900
				elif text_lang in keep_langs:
						score += 250
		return score, (file_lang or text_lang)

def add_subtl_from_file(input_file: str) -> Tuple[List[str], bool, List[str]]:
		p = Path(input_file)
		stem = p.stem
		parent = p.parent
		default_lng = (globals().get("Default_lng", "eng") or "eng").lower()
		keep = set(globals().get("Keep_langua", [])) | {"ron"}
		keep = {k.lower() for k in keep} | {"eng"}
		candidates: List[Path] = []

		try:
				with os.scandir(parent) as it:
						for e in it:
								if not e.is_file():
										continue
								ep = Path(e.path)
								if ep.suffix.lower() not in _TEXT_SIDE_EXTS:
										continue
								st = ep.stem
								if st == stem or st.startswith(stem + "."):
										candidates.append(ep)
		except Exception:
				pass

		if not candidates:
				return [], True, []

		direct = [c for c in candidates if _guess_lang3_from_filename(c, stem) == default_lng]
		if direct:
				ext_pref = {".srt": 3, ".ass": 2, ".vtt": 1, ".ssa": 0}
				chosen = sorted(direct, key=lambda x: (-ext_pref.get(x.suffix.lower(), 0), x.name.lower()))[0]
				log_msg = f"\033[94m .Found {chosen.suffix} subtitle: {chosen.name}\033[0m"
				return [
						"-i", str(chosen),
						"-map", "1:0",
						"-c:s:0", "mov_text",
						"-metadata:s:s:0", f"language={default_lng}",
						"-disposition:s:0", "default"
				], False, [log_msg]

		scored = [(*_score_sidecar(stem, c, default_lng, keep), c) for c in candidates]
		scored.sort(
				key=lambda t: (
						t[0],
						{".srt": 3, ".ass": 2, ".vtt": 1, ".ssa": 0}.get(t[2].suffix.lower(), 0),
						t[2].name.lower()
				),
				reverse=True
		)
		best_score, best_lang, best_path = scored[0]
		lang3 = (best_lang or default_lng).lower()
		log_msg = f"\033[94m .Found {best_path.suffix} subtitle: {best_path.name}\033[0m"
		return [
				"-i", str(best_path),
				"-map", "1:0",
				"-c:s:0", "mov_text",
				"-metadata:s:s:0", f"language={lang3}",
				"-disposition:s:0", "default"
		], False, [log_msg]

def parse_subtl(sub_streams: List[Dict[str, Any]],
								context: VideoContext,
								de_bug: bool = False) -> Tuple[List[str], bool, List[str]]:

		logs: List[str] = []
		TEXT_CODECS = {"mov_text", "subrip", "srt", "ass", "ssa", "webvtt", "text"}
		input_file = context.input_file
		default_language = Default_lng

		text_streams = [s for s in sub_streams if str(s.get("codec_name", "")).lower() in TEXT_CODECS]

		if not text_streams:
				logs.append("\033[94m     .No embedded subtitles, checking for external files.\033[0m")
				return add_subtl_from_file(input_file)

		SCORE = {"IS_FORCED": -500, "IS_SDH": 10, "IS_DEFAULT": 100, "LANG_MATCH": 1000}

		def get_score(stream: Dict[str, Any]) -> int:
				tags = stream.get("tags", {})
				disp = stream.get("disposition", {})
				title = str(tags.get("title", "")).lower()
				score = 0
				lang = tags.get("language", "und")
				if lang == default_language:
						score += SCORE["LANG_MATCH"]
				if disp.get("default"):
						score += SCORE["IS_DEFAULT"]
				if "sdh" in title:
						score += SCORE["IS_SDH"]
				if disp.get("forced"):
						score += SCORE["IS_FORCED"]
				return score

		default_track = max(text_streams, key=get_score) if text_streams else None

		ffmpeg_commands: List[str] = []
		any_codec_change_needed = False
		any_disposition_change_needed = False
		undetermined_lang_count = 0

		for i, stream in enumerate(text_streams):
				in_index = stream["index"]
				codec = str(stream.get("codec_name", "")).lower()
				lang = stream.get("tags", {}).get("language", "und")
				is_new_default = (stream is default_track)
				was_original_default = bool(stream.get("disposition", {}).get("default"))
				if lang == "und":
						undetermined_lang_count += 1

				needs_codec_change = (codec != "mov_text")
				if needs_codec_change:
						any_codec_change_needed = True

				needs_disposition_change = (is_new_default != was_original_default)
				if needs_disposition_change:
						any_disposition_change_needed = True

				ffmpeg_commands.extend([
						"-map", f"0:{in_index}",
						f"-c:s:{i}", "mov_text" if needs_codec_change else "copy",
						f"-disposition:s:{i}", "default" if is_new_default else "0",
						f"-metadata:s:s:{i}", f"language={lang}",
				])

				actions: List[str] = []
				if needs_codec_change:
						actions.append("Re-encode")
				else:
						actions.append("Copy")

				if is_new_default and not was_original_default:
						actions.append("Set Default")
				elif not is_new_default and was_original_default:
						actions.append("Clear Default")
				elif is_new_default:
						actions.append("Keep Default")
				else:
						actions.append("Not Default")

				logs.append(
						f"\033[94m   |<S:{in_index:2}>|{codec:^8}|{lang:<3}| {'|'.join(actions)}\033[0m"
				)

		if undetermined_lang_count > 0:
				plural = "s have" if undetermined_lang_count > 1 else " has"
				logs.append(
						f"\033[93m ?!Warning: {undetermined_lang_count} mapped subtitle stream{plural} undetermined language ('und').\033[0m"
				)

		can_skip_processing = (not any_codec_change_needed) and (not any_disposition_change_needed)
		if can_skip_processing:
				logs.append("\033[94m  .Skip: Subtitles are already correct (format + disposition).\033[0m")

		return ffmpeg_commands, can_skip_processing, logs

def _extract_comment_tag(meta: Dict[str, Any]) -> str:
		candidates: List[str] = []
		fmt_tags = (meta.get("format", {}) or {}).get("tags", {}) or {}
		streams = meta.get("streams", []) or []
		vid0_tags = next(((s.get("tags", {}) or {}) for s in streams if s.get("codec_type") == "video"), {})
		for d in (fmt_tags, vid0_tags):
			for k, v in (d or {}).items():
				if (k or "").lower() in ("comment", "©cmt", "description", "desc"):
					val = str(v or "").strip()
					if val:
						candidates.append(val)
		return candidates[0] if candidates else ""

def parse_finfo(input_file: str,
				metadata: Dict[str, Any],
				de_bug: bool = False) -> Tuple[List[str], bool, List[str]]:

		if not metadata or "format" not in metadata:
			return [], True, [f"\033[93m !Error: Invalid metadata for '{input_file}'.\033[0m"]

		fmt: Dict[str, Any] = metadata.get("format", {})
		streams: List[Dict[str, Any]] = metadata.get("streams", []) or []
		s_by_type: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
		counts = {"video": 0, "audio": 0, "subtitle": 0, "data": 0}

		for s in streams:
			stype = (s.get("codec_type") or "?").lower()
			s_by_type[stype].append(s)
			if stype in counts:
				counts[stype] += 1

		v_count = counts["video"]
		a_count = counts["audio"]
		s_count = counts["subtitle"]

		missing = [k for k in ("audio", "video") if counts[k] == 0]
		if missing:
			kind = " and ".join(missing)
			noun = "streams" if len(missing) > 1 else "stream"
			verb = "were" if len(missing) > 1 else "was"
			warn = f"\033[96m !Warning: No {kind} {noun} {verb} found.\n Move: {input_file}\n To: {EXCEPT_DIR}\033[0m"
			print(warn)
			return [], True, [warn]

		tags = fmt.get("tags", {}) or {}
		raw_title = (tags.get("title") or "").strip()
		title = raw_title or Path(input_file).stem

		try:
			raw_comment = _extract_comment_tag(metadata)
		except Exception:
			raw_comment = (tags.get("comment") or "").strip()

		skip_f = (raw_comment == SKIP_KEY)

		try:
			srik_update(input_file, plan={"skip_key_found": bool(skip_f)})
		except Exception:
			pass

		log_messages: List[str] = []
		header = (
			f"   |=Title|{title}|\n"
			f"   |<FRMT>|Size: {hm_sz(int(fmt.get('size', 0)))}|"
			f"Bitrate: {hm_sz(int(fmt.get('bit_rate', 0)), 'bps')}|"
			f"Length: {hm_tm(float(fmt.get('duration', 0.0)))}|"
			f"Streams: V:{v_count} A:{a_count} S:{s_count}"
		)
		if skip_f:
			header += f"\n  .Skip: Format OK"
		else:
			header += f" |Comment: '{raw_comment or ''}' (Will add SKIP_KEY in remux)"
		log_messages.append(f"\033[96m{header}\033[0m")

		skip_flags: List[bool] = [skip_f]

		total_br = int(fmt.get("bit_rate", 0) or 0)
		audio_br = sum(int(s.get("bit_rate", 0) or 0) for s in s_by_type.get("audio", []))
		video_br = max(total_br - audio_br, 0)

		ctx = VideoContext(input_file=input_file, estimated_video_bitrate=video_br)

		plan_info: Dict[str, Any] = {
			"allow_growth_same_res_pct": ALLOW_GROWTH_SAME_RES_PCT,
			"audio_policy":              {"codec": "aac", "sr": 48000, "channels": [2, 6]},
			"expect_downscale":          False,
			"encoder":                   "copy",
			"ideal_bps":                 0,
			"skip_processing":           True,
			"skip_key_found":            bool(skip_f),
		}

		final_cmd: List[str] = []

		for s_type, func in [("video", parse_video), ("audio", parse_audio), ("subtitle", parse_subtl)]:
			cmd, skip, msgs = func(s_by_type.get(s_type, []), ctx, de_bug)
			final_cmd.extend(cmd)
			skip_flags.append(skip)
			log_messages.extend(msgs)

			if s_type == "video" and cmd:
				encoder_used = "copy"
				ideal_bps_found = 0
				try:
					for i, flag in enumerate(cmd):
						if flag.startswith("-c:v"):
							if i + 1 < len(cmd):
								encoder_used = cmd[i + 1]
								v_idx_str = '0'
								if ':' in flag and flag.split(':')[-1].isdigit():
									v_idx_str = flag.split(':')[-1]
								b_flag = f"-b:v:{v_idx_str}"
								b_flag_simple = "-b:v"
								if b_flag in cmd:
									b_idx = cmd.index(b_flag)
									if b_idx + 1 < len(cmd):
											ideal_bps_found = int(cmd[b_idx + 1].replace('k', '')) * 1000
								elif b_flag_simple in cmd:
									b_idx2 = cmd.index(b_flag_simple)
									if b_idx2 + 1 < len(cmd):
										ideal_bps_found = int(cmd[b_idx2 + 1].replace('k', '')) * 1000
								break
				except Exception:
					pass
				plan_info["encoder"] = encoder_used
				plan_info["ideal_bps"] = ideal_bps_found

		final_skip = all(skip_flags)
		needs_container_change = Path(input_file).suffix.lower() != ".mp4"
		if needs_container_change:
			final_skip = False
			suf = Path(input_file).suffix or "(none)"
			log_messages.append(f"\033[96m   .Container convert required: {suf} → .mp4 \033[0m")
		if final_skip:
			if not needs_container_change:
				log_messages.append("\033[96m  .Skip: File compliant, already processed, and has skip key.\033[0m")
			if FORCE_ARTIFACTS_ON_SKIP:
				try:
					has_audio = len(s_by_type.get("audio", [])) > 0
					plan_info.setdefault("artifact_from_source", True)
					plan_info.setdefault("has_audio_on_source", has_audio)
					srik_update(input_file, plan=plan_info)
				#	log_messages.append("\033[96m  .Artifacts-on-skip: Will create matrix/short/speed from source.\033[0m")
				except Exception:
					pass
		else:
			process_reasons: List[str] = []
			if not skip_f:
				process_reasons.append("Add skip key")
			if not skip_flags[1]:
				process_reasons.append("Video needs changes")
			if not skip_flags[2]:
				process_reasons.append("Audio needs changes")
			if not skip_flags[3]:
				process_reasons.append("Subtitles need changes")
			if needs_container_change:
				process_reasons.append("Container convert")
			if process_reasons:
				log_messages.append(f"\033[96m  .Processing Required ({', '.join(process_reasons)}) for: {Path(input_file).name}\033[0m")
			else:
				log_messages.append(f"\033[96m  .Processing Required for: {Path(input_file).name}\033[0m")
		try:
			v_streams = s_by_type.get("video", [])
			w_in = int((v_streams[0] or {}).get("width", 0)) if v_streams else 0
			h_in = int((v_streams[0] or {}).get("height", 0)) if v_streams else 0
			dur_in = float((fmt or {}).get("duration", 0.0) or 0.0)
			size_in = int((fmt or {}).get("size", 0) or 0)

			plan_info["expect_downscale"] = (w_in > 2600 or h_in > 1188)
			plan_info["skip_processing"] = final_skip

			source_info = {"dur_in": dur_in, "w_in": w_in, "h_in": h_in, "size_in": size_in}
			srik_update(input_file, source=source_info, plan=plan_info)
		except Exception as e:
			if de_bug:
				print(f"DEBUG: Failed to seed/update SRIK in parse_finfo: {e}")
			try:
				srik_update(input_file, plan={"skip_processing": final_skip, "skip_key_found": bool(skip_f)})
			except Exception:
				pass

		return ([], True, log_messages) if final_skip else (final_cmd, False, log_messages)

# -----------------------------------------------------------------------------
# Progress HUD helpers (keep minimal; artifacts will not use full HUD)
# -----------------------------------------------------------------------------

PROGRESS_REGEX = {
		"bitrate": re.compile(r"bitrate=\s*([0-9\.]+)\s*(kbits/s|bits/s)?", re.I),
		"frame": re.compile(r"frame=\s*([0-9]+)"),
		"speed": re.compile(r"speed=\s*([0-9\.]+)"),
		"size": re.compile(r"size=\s*([0-9]+)\s*(KiB|kB|B)?"),
		"time": re.compile(r"time=([0-9:.]+)"),
		"fps": re.compile(r"fps=\s*([0-9\.]+)"),
}

def progress_register(task_id: str, label: str = "") -> None:
	with progress_lock:
		progress_state[task_id] = {
			"label": label,
			"size_kb": 0.0,
			"frame": 0,
			"fps": 0.0,
			"speed": 0.0,
			"bitrate_kbps": 0.0,
			"time_sec": 0.0,
			"percent": 0.0,
			"eta": "--:--:--",
			"duration": 0.0
		}

def progress_remove(task_id: str) -> None:
	with progress_lock:
		progress_state.pop(task_id, None)

def progress_get_snapshot() -> Dict[str, Dict[str, float | str]]:
	with progress_lock:
		return {k: dict(v) for k, v in progress_state.items()}

def update_progress(line: str, task_id: str, debug: bool = False) -> None:
	line = line.strip()
	if not line or not (line.startswith(('frame=', 'size=', 'Lsize=')) or 'bitrate=' in line or 'speed=' in line):
		return

	try:
		matches = {k: rg.search(line) for k, rg in PROGRESS_REGEX.items()}
		vals: Dict[str, str] = {k: m.group(1) for k, m in matches.items() if m}
		if "time" not in vals or not vals["time"]:
						return
		h, m, s = map(float, vals["time"].split(":"))
		current_sec = h * 3600 + m * 60 + s
		if "speed" not in vals or not vals["speed"] or float(vals["speed"]) <= 0:
			return

		with progress_lock:
			st = progress_state.get(task_id)
			if not st:
				return
			duration = st.get("duration", 0.0)
			percent = 0.0
			if duration and duration > 0:
				percent = min(100.0, 100.0 * current_sec / duration)

			size_bytes = st.get("size_bytes", 0.0)
			if matches.get("size"):
				num = float(matches["size"].group(1))
				unit = (matches["size"].group(2) or "B").lower()
				size_bytes = num * 1024.0 if 'k' in unit else num

			bitrate_bps = st.get("bitrate_bps", 0.0)
			if matches.get("bitrate"):
				br_raw = float(matches["bitrate"].group(1))
				br_unit = (matches["bitrate"].group(2) or "").lower()
				bitrate_bps = br_raw * 1000.0 if "kbits/s" in br_unit else br_raw

			speed_val = float(vals["speed"])
			eta_str = "--:--:--"
			if duration > 0 and speed_val > 0 and current_sec > 0:
				remaining_sec_input = duration - current_sec
				if remaining_sec_input > 0:
					eta_sec_real = remaining_sec_input / speed_val
					eta_h = int(eta_sec_real // 3600)
					eta_m = int((eta_sec_real % 3600) // 60)
					eta_s = int(eta_sec_real % 60)
					eta_str = f"{eta_h:02d}:{eta_m:02d}:{eta_s:02d}"

			st.update({
				"frame":		int(vals.get("frame", 0)),
				"fps":			float(vals.get("fps", 0.0)),
				"speed":		speed_val,
				"bitrate_bps":	bitrate_bps,
				"size_bytes":	size_bytes,
				"percent":		percent,
				"time_sec":		current_sec,
				"eta":			eta_str
				})
	except Exception as e:
		if debug:
			with print_lock:
				print(f"\nWarning: Error parsing progress line: '{line}' - {e}", file=sys.stderr)

# -----------------------------------------------------------------------------
# Encoding Execution (2-stage pipeline)
# -----------------------------------------------------------------------------

# --- MODIFIED: Added 'duration' param ---
def _run_ffmpeg_live(cmd: List[str], task_id: str = "T1", timeout: int = STAGE_TIMEOUT_S, duration: Optional[float] = None) -> Tuple[int, str]:
	"""Runs FFmpeg, captures progress, handles timeouts, uses non-blocking stderr read (Refined)."""
	if de_bug:
		# Use shlex.quote for safer command string representation
		cmd_str = " ".join(shlex.quote(a) for a in cmd)
		with print_lock: print(f"\n--- [DEBUG] FFmpeg Command ({task_id}) ---\n{cmd_str}\n------------------------------\n")

	proc: Optional[sp.Popen] = None
	# Simplified type hint
	stderr_q: queue.Queue = queue.Queue()
	stderr_lines_all: List[str] = []
	reader_thread_handle: Optional[threading.Thread] = None
	# Flag to indicate if loop exited because process finished/reader EOF'd, not error/timeout
	clean_exit_signal = False

	def reader_thread(pipe: Any, q: queue.Queue, line_list: List[str]) -> None:
		"""Reads lines from FFmpeg's stderr pipe and puts them on the queue."""
		try:
			# Read line by line until the pipe closes ('')
			for line in iter(pipe.readline, ''):
				q.put(line)
				line_list.append(line) # Store for tail log
		except ValueError:
			# Ignore ValueError: I/O operation on closed file (can happen during shutdown)
			pass
		except Exception as e:
			# Log other reader errors if debugging
			if de_bug:
				with print_lock: print(f"DEBUG [{task_id}]: stderr reader thread exception: {e}")
		finally:
			try:
				pipe.close() # Ensure pipe is closed
			except Exception: pass
			q.put(None) # Signal EOF for the main thread
			if de_bug:
				with print_lock: print(f"DEBUG [{task_id}]: Reader thread finished.")
	try:
		# --- <<< ADDED: Get label, register task, and set duration >>> ---
		label = task_id # Default label
		try:
			# Try to find the "-i" argument and get the next item
			i_index = cmd.index("-i")
			if i_index + 1 < len(cmd):
				label = Path(cmd[i_index + 1]).name
		except Exception:
			pass # Stick with default label if "-i" not found

		progress_register(task_id, label=label)
		if duration and duration > 0:
			with progress_lock:
				if task_id in progress_state:
					progress_state[task_id]["duration"] = duration
		# --- <<< END ADDED SECTION >>> ---

		# --- Start Process ---
		proc = _popen_managed(cmd, stdout=sp.DEVNULL, stderr=sp.PIPE, bufsize=1)
		if de_bug:
			with print_lock: print(f"DEBUG [{task_id}]: FFmpeg process started (PID: {proc.pid}).")
		# --- Start Reader Thread ---
		reader_thread_handle = threading.Thread(target=reader_thread, args=(proc.stderr, stderr_q, stderr_lines_all), daemon=True)
		reader_thread_handle.start()
		if de_bug:
			with print_lock: print(f"DEBUG [{task_id}]: stderr reader thread started.")
		start_time = time.time()
		last_progress_print_time = 0.0
		# --- Main Loop ---
		while True:
			now = time.time()
			# --- Check Timeout ---
			if now - start_time > timeout:
				with print_lock: print(f"\n\033[93m[timeout]\033[0m FFmpeg ({task_id}) exceeded {timeout}s; terminating...")
				PROC_MGR.terminate_all() # Signal termination
				# Let finally block handle waiting/cleanup, just exit loop
				break
			# --- Check Process Status ---
			proc_status = proc.poll()
			if proc_status is not None:
				if de_bug:
					with print_lock: print(f"DEBUG [{task_id}]: Process ended independently with status {proc_status}.")
				# Process ended on its own, wait for reader EOF signal naturally
				clean_exit_signal = True # Mark potential clean exit
				# Loop continues until EOF is read from queue
			# --- Read from Queue (with timeout) ---
			try:
				line = stderr_q.get(timeout=0.2) # Wait up to 200ms for a line
				if line is None: # EOF signal from reader
					if de_bug:
						with print_lock: print(f"DEBUG [{task_id}]: Received EOF from reader.")
					clean_exit_signal = True # Mark clean exit via EOF
					break # Exit main loop normally
				# --- Process Line & Update Progress ---
				update_progress(line, task_id, de_bug)
				# Throttle display updates
				if now - last_progress_print_time >= 0.33:
					with progress_lock, print_lock:
						st = progress_state.get(task_id, {})
						size_bytes = st.get('size_bytes', 0); frame = st.get('frame', 0)
						fps = int(st.get('fps', 0.0)); bitrate_bps = st.get('bitrate_bps', 0)
						speed = st.get('speed', 0.0); percent = st.get('percent', 0.0)
						eta = st.get('eta', '--:--:--') # Get ETA from update_progress
						disp = (f"\r   [{task_id}]|Encode|"
								f"Size:{hm_sz(size_bytes):>8}|"
								f"Frames:{frame:>7}|"
								f"Fps:{fps:>4}|"
								f"Bitrate:{hm_sz(bitrate_bps,'bps'):>8}|"
								f"Speed:{speed:>4.1f}x|" # Wider field for speed
								f"{percent:>5.1f}%|" # Wider field for percent
								f"ETA:{eta}| ") # Add ETA to display
					sys.stderr.write(disp); sys.stderr.flush()
					last_progress_print_time = now
			except queue.Empty:
				# Queue was empty, just continue loop (check timeout/process status again)
				pass
			except Exception as q_err:
				# Catch unexpected errors during queue read/progress update
				with print_lock: print(f"\n   [{task_id}] ERROR in progress loop: {q_err}")
				clean_exit_signal = False # Not a clean exit
				break # Exit loop on error
	except Exception as e:
		# --- Catch errors during setup ---
		with print_lock: print(f"\n   [{task_id}] CRITICAL error in _run_ffmpeg_live setup: {e}\n{traceback.format_exc()}")
		if proc: PROC_MGR.unregister(proc) # Ensure unregister if proc exists
		try:
			errlog_block("<unknown-input>", f"ffmpeg exec exception [{task_id}]", " ".join(cmd) + f"\n\n{e}\n{traceback.format_exc()}")
		except Exception: pass
		clean_exit_signal = False # Not a clean exit
		# Fall through to finally for cleanup
	finally:
		# --- Cleanup ---
		final_rc: Optional[int] = None
		# Ensure process is handled
		if proc:
			# Check status again in case loop broke unexpectedly
			current_status = proc.poll()
			if current_status is None:
				# If loop didn't exit cleanly (timeout, error), ensure termination
				if not clean_exit_signal:
					with print_lock: print(f"   [{task_id}]: Ensuring FFmpeg process termination after loop exit...")
					PROC_MGR.terminate_all() # Use manager again to be sure
				# Wait for the process to finish
				try:
					proc.wait(timeout=5.0)
				except sp.TimeoutExpired:
					with print_lock: print(f"   [{task_id}] WARNING: FFmpeg did not terminate cleanly after wait.")
					try: proc.kill() # Last resort kill
					except Exception: pass
				except Exception as wait_err:
					with print_lock: print(f"   [{task_id}] WARNING: Error during final process wait: {wait_err}")
			final_rc = proc.poll() # Get final exit code
			PROC_MGR.unregister(proc) # Final unregister guaranteed
		# Ensure reader thread is joined
		if reader_thread_handle and reader_thread_handle.is_alive():
			if de_bug:
				with print_lock: print(f"DEBUG [{task_id}]: Waiting for reader thread to join in finally...")
			reader_thread_handle.join(timeout=2.0)
			if reader_thread_handle.is_alive():
				with print_lock: print(f"   [{task_id}] WARNING: Reader thread did not join cleanly.")
		# Drain queue one last time after thread join/process wait
		try:
			while True:
				line = stderr_q.get_nowait()
				if line is None: break # EOF marker
				# Could append to stderr_lines_all here if needed, but tail already captured in reader
		except queue.Empty:
			pass
		except Exception as drain_err:
			if de_bug:
				with print_lock: print(f"DEBUG [{task_id}]: Error draining queue in finally: {drain_err}")
		# Clear progress line from terminal
		with print_lock:
			sys.stderr.write("\r" + " " * 120 + "\r") # Wider clear
			sys.stderr.flush()
		# --- <<< ADDED: Remove task from progress state >>> ---
		progress_remove(task_id) # Clean up the progress state entry
		# --- <<< END ADDED SECTION >>> ---
		# Determine final return code and tail
		rc_to_return = final_rc if final_rc is not None else 1 # Default to error if unknown
		tail = "".join(stderr_lines_all[-60:]) # Get last 60 lines collected by reader
		if de_bug:
			with print_lock: print(f"DEBUG [{task_id}]: Exiting _run_ffmpeg_live finally block with rc={rc_to_return}.")
		return rc_to_return, tail

# --- MODIFIED: Added 'duration' param ---
def _faststart_remux(inp: Path, out: Path, task_id: str = "T1", duration: Optional[float] = None) -> Tuple[bool, int]:
	"""
	Final remux: copy streams, ensure +faststart, and authoritatively set SKIP_KEY.
	Clears container metadata, enables iTunes-style tags, and writes multiple aliases.
	(Refined with print_lock and distinct task ID).
	"""
	if out.exists():
		try:
			out.unlink(missing_ok=True) # Use missing_ok=True for cleaner deletion
		except Exception as e:
			# Use print_lock for thread safety
			with print_lock: print(f"   [{task_id}] WARNING: Could not delete existing output {out.name}: {e}")
			# Continue even if deletion failed, ffmpeg -y will handle it
	cmd2 = [FFMPEG, "-y", "-hide_banner",
			"-i", str(inp),
			"-map_metadata", "-1",    # clear; don't import source tags
			"-map", "0",               # copy all streams
			"-c", "copy",
			"-movflags", "+faststart+use_metadata_tags", # Ensure faststart and modern tags
			"-metadata", f"comment={SKIP_KEY}",
			# Write multiple aliases to maximize survivability
				#"-metadata", f"description={SKIP_KEY}",
				#"-metadata", f"©cmt={SKIP_KEY}",
			# also tag first video stream as backup
				# "-metadata:s:v:0", f"comment={SKIP_KEY}",
			str(out)
	]
	# Use print_lock for thread safety
	with print_lock: print(f"\n   [{task_id}] Stage-2 remux (+faststart,use_metadata_tags & SKIP_KEY)")
	# Pass distinct task ID suffix (_FS)
	# --- MODIFIED: Pass 'duration' to _run_ffmpeg_live ---
	rc2, tail2 = _run_ffmpeg_live(cmd2, task_id=f"{task_id}_FS", timeout=REMUX_TIMEOUT_S, duration=duration)
	# Log non-fatal errors even on success
	if rc2 == 0 and tail2 and any(k in tail2.lower() for k in ("error", "invalid", "no such file", "permission denied", "corrupt")):
		errlog_block(str(inp), "ffmpeg faststart stderr (non-fatal)", tail2)
	# Handle and log actual failures
	if rc2 != 0:
		# Use print_lock for thread safety
		with print_lock:
			print(f"[{task_id}] FFmpeg (stage-2 remux) failed with code {rc2}")
			print(tail2 or "(No stderr tail)") # Ensure something prints if tail2 is None
		errlog_block(str(inp), f"ffmpeg faststart rc={rc2} [{task_id}]", " ".join(cmd2) + "\n\n" + (tail2 or ""))

	return (rc2 == 0), rc2

# --- MODIFIED: Added 'duration' param ---
def run_encode_then_faststart(encode_cmd: List[str], src_path: Path, final_mp4_path: Path,
							task_id: str, allow_sanitize: bool = True, uniq: Optional[str] = None, duration: Optional[float] = None) -> bool:
	"""
	Runs a two-stage encode: Stage 1 processes streams to a temporary MP4,
	Stage 2 remuxes to the final path with +faststart and SKIP_KEY.
	Includes error handling and an optional sanitize step.
	"""
	# <<< FIX: Indentation added for the entire function body >>>
	token = uniq or "".join(random.choices(string.ascii_lowercase + string.digits, k=4))
	stage1_path = RUN_TMP / f"__stage1_{src_path.stem}_{token}.mp4"

	# --- Rebuild input list and filter original command ---
	external_inputs = [encode_cmd[i + 1] for i, x in enumerate(encode_cmd) if x == "-i"]

	copying_video = any(encode_cmd[xi].startswith("-c:v") and encode_cmd[xi + 1] == "copy"
						for xi in range(len(encode_cmd) - 1))

	# Build head arguments (ffmpeg executable, initial flags, main input)
	head = [FFMPEG, "-y", "-hide_banner"]
	if not copying_video:
		head.extend(["-err_detect", "ignore_err"])
	head.extend(["-i", str(src_path)])

	# Add external inputs to head arguments
	for ext_in in external_inputs:
		if not copying_video:
			head.extend(["-fflags", "+genpts", "-err_detect", "ignore_err"])
		head.extend(["-i", ext_in])

	# Build tail arguments by removing all '-i <input>' pairs from original command
	tail: List[str] = []
	i = 0
	while i < len(encode_cmd):
		if encode_cmd[i] == "-i":
			i += 2
		else:
			tail.append(encode_cmd[i])
			i += 1

	# Filter tail arguments: remove '-movflags' and '-metadata comment=...'
	filtered_tail: List[str] = []
	i = 0
	while i < len(tail):
		tok = tail[i]
		# Remove -movflags and its value
		if tok == "-movflags" and i + 1 < len(tail):
			i += 2
			continue
		# <<< FIX: Added back the crucial filter for -metadata comment= >>>
		# Remove -metadata comment=... and its value
		if tok == "-metadata" and i + 1 < len(tail):
			val = str(tail[i + 1])
			if val.lower().startswith("comment="):
				i += 2
				continue
		# Keep other arguments
		filtered_tail.append(tail[i])
		i += 1

	# Combine head and filtered tail for Stage 1 command
	cmd1 = head + filtered_tail

	# Add hvc1 tag if encoding HEVC and tag not already present
	if TAG_HEVC_AS_HVC1 and "-tag:v" not in cmd1:
		# Check if actually encoding HEVC before adding tag
		is_hevc_encode = False
		for j in range(len(cmd1) -1):
			if cmd1[j].startswith("-c:v") and "hevc" in cmd1[j+1].lower():
				is_hevc_encode = True
				break
		if is_hevc_encode:
			cmd1 += ["-tag:v", "hvc1"]

	# Add flags specific to stream copying
	if copying_video:
		cmd1 += ["-avoid_negative_ts", "make_zero", "-reset_timestamps", "1"]

	# Always use +faststart for Stage 1 output and specify output path
	cmd1 += ["-movflags", "+faststart", str(stage1_path)]

	# --- Stage 1 Execution ---
	with print_lock: print(f"   [{task_id}] Stage-1 encode -> MP4")
	# --- MODIFIED: Pass 'duration' ---
	rc1, tail1 = _run_ffmpeg_live(cmd1, task_id=task_id, timeout=STAGE_TIMEOUT_S, duration=duration)

	# Log non-fatal errors from stderr if encode succeeded nominally
	if rc1 == 0 and tail1 and any(k in tail1.lower() for k in ("error", "invalid", "no such file", "permission denied", "corrupt")):
		errlog_block(str(src_path), "ffmpeg stage-1 stderr (non-fatal)", tail1)

	# --- Handle Stage 1 Failure ---
	if rc1 != 0:
		with print_lock: print(f"[{task_id}] FFmpeg (stage-1) failed with code {rc1}"); print(tail1)
		errlog_block(str(src_path), f"ffmpeg stage-1 rc={rc1} [{task_id}]", " ".join(cmd1) + "\n\n" + (tail1 or ""))

		# --- Sanitize Attempt (if applicable) ---
		SUSPECT = ("non-monotonous", "invalid dts", "invalid nal", "max_analyze_duration", "malformed", "error reading", "assertion", "next_dts", "movenc.c")
		if allow_sanitize and any(p in (tail1 or "").lower() for p in SUSPECT):
			sanitize_src = RUN_TMP / f"__sanitize_{src_path.stem}_{token}.mkv"
			cmd_san = [FFMPEG, "-y", "-hide_banner", "-fflags", "+genpts", "-i", str(src_path), "-map", "0", "-map_metadata", "0", "-c", "copy", str(sanitize_src)]
			with print_lock: print(f"[{task_id}] Sanitize remux attempt -> {sanitize_src.name}")
			rcs, tailS = _run_ffmpeg_live(cmd_san, task_id=f"{task_id}_SAN", timeout=max(1200, STAGE_TIMEOUT_S // 8), duration=duration) # Pass duration here too
			if rcs == 0 and sanitize_src.exists() and sanitize_src.stat().st_size > 1024:
				with print_lock: print(f"[{task_id}] Sanitize remux OK. Retrying encode from sanitized source.")
				# Recursively call self with sanitized source, disallowing further sanitization
				# --- MODIFIED: Pass 'duration' to recursive call ---
				ok_retry = run_encode_then_faststart(encode_cmd, sanitize_src, final_mp4_path, task_id, allow_sanitize=False, duration=duration)
				# Clean up sanitize file regardless of retry outcome
				try: sanitize_src.unlink(missing_ok=True)
				except Exception: pass
				return ok_retry # Return result of the retry
			else:
				errlog_block(str(src_path), f"sanitize remux failed [{task_id}]", tailS or "(no stderr)")

		# --- Cleanup on Stage 1 Failure (if sanitize didn't run or failed) ---
		try: stage1_path.unlink(missing_ok=True)
		except Exception: pass
		return False # Stage 1 failed

	# --- Stage 2 Execution (Faststart Remux) ---
	# --- MODIFIED: Pass 'duration' ---
	ok2, _ = _faststart_remux(stage1_path, final_mp4_path, task_id, duration=duration) # Pass original task_id

	# --- Final Cleanup ---
	try:
		stage1_path.unlink(missing_ok=True)
	except Exception as e:
		# Log if cleanup fails, but don't fail the whole function
		if de_bug:
			with print_lock: print(f"DEBUG [{task_id}]: Failed to delete stage1 file {stage1_path}: {e}")

	return ok2

def ffmpeg_run(input_file: str,
							ff_com: List[str],
							duration: float,
							skip_it: bool,
							de_bug: bool = False,
							task_id: str = "") -> Optional[str]:

		if skip_it or not ff_com:
			return None

		src_path = Path(input_file)
		# --- REMOVED: progress_register is now handled by _run_ffmpeg_live ---
		# progress_register(task_id, label=src_path.name)

		safe_base = re.sub(r"[^a-zA-Z0-9_-]", "_", src_path.stem)
		uniq = "".join(random.choices(string.ascii_lowercase + string.digits, k=4))
		out_file = WORK_DIR / f"__{safe_base[:30]}_{uniq}{TMPF_EX}"

		# --- REMOVED: duration is now set by _run_ffmpeg_live ---
		# with progress_lock:
		# 	if task_id in progress_state:
		# 		progress_state[task_id]["duration"] = duration

		# --- MODIFIED: Pass 'duration' to run_encode_then_faststart ---
		ok = run_encode_then_faststart(ff_com, src_path, out_file, task_id, allow_sanitize=True, uniq=uniq, duration=duration)
		# --- REMOVED: progress_remove is now handled by _run_ffmpeg_live ---
		# progress_remove(task_id)

		if ok:
			return str(out_file)

		errlog_block(input_file, f"ffmpeg encode failed [{task_id}]", "Args: " + " ".join(ff_com))
		try:
			if out_file.exists():
				out_file.unlink()
		except OSError:
			pass
		return None

# -----------------------------------------------------------------------------
# Replacement + Validation + Artifacts
# -----------------------------------------------------------------------------

def _avg_bitrate(sz_bytes: Optional[int | float], dur_sec: Optional[float]) -> float:
	size_bits = float(sz_bytes or 0) * 8.0
	duration = float(dur_sec or 0.0)
	return size_bits / duration if duration > 0 else 0.0

def _artifact_run(cmd: List[str], task_id: str) -> bool:
	"""
	Lightweight FFmpeg runner for artifacts; no interactive HUD.
	Returns True on rc==0.
	"""
	if de_bug:
		with print_lock:
			print(f"[{task_id}] run: {' '.join(shlex.quote(c) for c in cmd)}")
	try:
		proc = _popen_managed(cmd, stdout=sp.PIPE, stderr=sp.PIPE)
		try:
			_, err = proc.communicate(timeout=max(120, REMUX_TIMEOUT_S // 4))
		finally:
			PROC_MGR.unregister(proc)
		if proc.returncode != 0:
			with print_lock:
				print(f"[{task_id}] ffmpeg failed rc={proc.returncode}")
				if err:
					print(err[-800:].decode(errors="ignore") if isinstance(err, bytes) else str(err)[-800:])
				return False
		return True
	except sp.TimeoutExpired:
		with print_lock:
			print(f"[{task_id}] ffmpeg timeout.")
		return False
	except Exception as e:
		with print_lock:
			print(f"[{task_id}] ffmpeg exception: {e}")
		return False

def _artifact_paths(base: Path, suffix: str) -> Path:
	parent = base.parent
	name = base.stem + suffix
	return parent / name

# --- Helper Function for Speed Up ---
def _ff_atempo_chain(factor: float) -> str:
	"""Builds a chain of atempo filters for factors outside [0.5, 100.0]."""
	if factor <= 0: return "atempo=1.0" # Invalid factor, return no-op
	filters = []
	temp_factor = factor
	# Handle factors > 100.0 by chaining *100 filters
	while temp_factor > 100.0:
		filters.append("atempo=100.0")
		temp_factor /= 100.0
	# Handle factors < 0.5 by chaining *0.5 filters
	while temp_factor < 0.5:
		filters.append("atempo=0.5")
		temp_factor /= 0.5
	# Add the remaining factor if it's not exactly 1.0 (within float tolerance)
	if not math.isclose(temp_factor, 1.0):
		filters.append(f"atempo={temp_factor:.6f}")

	# Return chain or no-op if factor was 1.0
	return ",".join(filters) if filters else "atempo=1.0"

def matrix_it( # Renamed back
	input_path: Path,
	output_image_path: Path,
	task_id_base: str, # <-- MODIFIED: Renamed param
	# --- Hints from output_info ---
	duration: Optional[float],
	width: Optional[int],
	height: Optional[int],
	# --- Config with Defaults ---
	columns: int = ADDITIONAL_MATRIX_COLS,
	rows: int = ADDITIONAL_MATRIX_ROWS,
	start_skip_percent: float = ADDITIONAL_MATRIX_SKIP_PCT_START, # Use percentage
	end_skip_percent: float = ADDITIONAL_MATRIX_SKIP_PCT_END,
	thumb_width: int = ADDITIONAL_MATRIX_WIDTH
) -> bool:
	"""
	Creates a thumbnail matrix image, skipping start/end percentages.
	Uses provided duration/dimensions.
	"""
	# --- Parameter and Input Info Validation ---
	with print_lock: print(f"   [{task_id_base}] Creating thumbnail matrix...")
	if duration is None or duration <= 1.0 or width is None or width <= 0 or height is None or height <= 0:
		with print_lock: print(f"   [{task_id_base}] ERROR: Insufficient info (duration/width/height) for matrix."); return False
	if columns < 1 or rows < 1 or not (0 <= start_skip_percent < 50) or not (0 <= end_skip_percent < 50):
		with print_lock: print(f"   [{task_id_base}] ERROR: Invalid matrix parameters."); return False

	num_thumbs = columns * rows
	ext = output_image_path.suffix.lower();
	if ext not in [".png", ".jpg", ".jpeg"]: ext = ".png"

	# --- Calculations based on provided info ---
	start_time = duration * (start_skip_percent / 100.0) # Use start percentage
	end_time = duration * (1 - end_skip_percent / 100.0)

	if start_time >= end_time:
		with print_lock: print(f"   [{task_id_base}] ERROR: Start time % ({start_skip_percent}%) >= end time % ({100-end_skip_percent}%). Skipping matrix."); return False

	effective_duration = end_time - start_time
	if effective_duration <= 0:
		with print_lock: print(f"   [{task_id_base}] ERROR: Zero or negative effective duration for matrix."); return False

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
			# --- MODIFIED: Pass task_id_base and a fixed duration for progress ---
			rc_thumb, tail_thumb = _run_ffmpeg_live(extract_cmd, task_id=f"{task_id_base}_T{i}", timeout=60, duration=5.0) # 5 sec duration
			if rc_thumb != 0:
				with print_lock: print(f"   [{task_id_base}] ERROR: Failed extracting thumb {i+1} at {time_pos:.2f}s.")
				errlog_block(str(input_path), f"matrix thumb failed [{task_id_base}_T{i}]", " ".join(extract_cmd) + f"\n\n{tail_thumb or ''}")
				extraction_ok = False; break
		if not extraction_ok: return False

		inputs_args = sum([["-i", p] for p in thumb_paths], [])

		# <<< FIX: Concat all image inputs *first*, then tile the result >>>
		concat_inputs = "".join(f"[{j}:v]" for j in range(num_thumbs)) # [0:v][1:v]...[11:v]
		filter_complex = (
			f"{concat_inputs}concat=n={num_thumbs}:v=1:a=0[v]; " # Concat N inputs into one stream [v]
			f"[v]tile={columns}x{rows}:padding=5:margin=5[out]"  # Tile the single stream [v]
		)
		# <<< END FIX >>>

		tile_cmd =	[ FFMPEG, "-y", "-hide_banner" ] + inputs_args + \
					["-filter_complex", filter_complex, "-map", "[out]",
					 "-frames:v", "1", str(output_image_path) ]

		# --- MODIFIED: Pass task_id_base and a fixed duration ---
		rc_tile, tail_tile = _run_ffmpeg_live(tile_cmd, task_id=f"{task_id_base}_TILE", timeout=300, duration=10.0) # 10 sec duration

		if rc_tile != 0:
			with print_lock: print(f"   [{task_id_base}] ERROR: Failed tiling thumbnails.")
			log_cmd = tile_cmd[:15] + ["..."] + tile_cmd[-5:] if len(tile_cmd) > 20 else tile_cmd
			errlog_block(str(input_path), f"matrix tile failed [{task_id_base}_TILE]", " ".join(log_cmd) + f"\n\n{tail_tile or ''}")
			return False

	return True

def speed_up( # Renamed back
	input_path: Path,
	factor: float,
	output_path: Path,
	task_id: str,
	# --- Hint from output_info ---
	has_audio: bool, # Required hint
	# --- MODIFIED: Added duration param ---
	duration: Optional[float] = None
) -> bool:
	"""Changes video and audio speed, using has_audio hint and high-quality encode."""
	with print_lock: print(f"   [{task_id}] Creating {factor:.2f}x speed version...")
	if factor <= 0:
		with print_lock: print(f"   [{task_id}] ERROR: Speed factor must be positive."); return False

	vf = f"setpts={1/factor:.6f}*PTS" # Video speed filter

	cmd_base = [ FFMPEG, "-y", "-hide_banner", "-i", str(input_path) ]
	filter_args = []
	map_args = []
	audio_codec_args = []

	if has_audio:
		af = _ff_atempo_chain(factor) # Build audio tempo filter chain
		filter_args.extend(["-filter_complex", f"[0:v]{vf}[v];[0:a]{af}[a]"])
		map_args.extend(["-map", "[v]", "-map", "[a]"]) # Map both filtered streams
		# Using AAC 160k as per original example, adjust if needed
		audio_codec_args.extend(["-c:a", "aac", "-b:a", "160k"])
	else:
		filter_args.extend(["-filter_complex", f"[0:v]{vf}[v]"])
		map_args.extend(["-map", "[v]", "-an"]) # Map video, disable audio

	video_codec_args = [
		"-c:v", "libx265",
		"-preset", ADDITIONAL_PRESET, # Use configured preset
		"-crf", str(ADDITIONAL_QUALITY_CRF), # Use configured CRF
		"-tag:v", "hvc1"
	]
	output_args = ["-movflags", "+faststart", str(output_path)]

	cmd = cmd_base + filter_args + map_args + video_codec_args + audio_codec_args + output_args

	# --- MODIFIED: Pass 'duration' ---
	rc, tail = _run_ffmpeg_live(cmd, task_id=task_id, timeout=STAGE_TIMEOUT_S, duration=duration)
	if rc != 0:
		errlog_block(str(input_path), f"speed_up failed [{task_id}]", " ".join(cmd) + f"\n\n{tail or ''}")

	success = (rc == 0 and output_path.exists() and output_path.stat().st_size > 100)
	if not success and rc == 0:
		errlog_block(str(input_path), f"speed_up post-check failed [{task_id}]", f"FFmpeg rc=0 but output missing/empty.\nCmd: {' '.join(cmd)}")
	return success

def short_ver( # Renamed back
	input_path: Path,
	output_path: Path,
	task_id: str,
	# --- Hints & Config ---
	# --- MODIFIED: Renamed param for clarity ---
	duration: Optional[float], # Hint for total video length
	clip_duration: float = ADDITIONAL_SHORT_DUR, # How long the clip should be
	start_skip_percent: float = ADDITIONAL_SHORT_SKP_STRT # % to skip first
) -> bool:
	"""Creates a short clip using stream copy, skipping a start percentage."""
	with print_lock: print(f"   [{task_id}] Creating {clip_duration:.1f}s short version (skipping {start_skip_percent:.1f}%)...")

	# --- MODIFIED: Use new param name 'duration' ---
	if duration is None or duration <= 0:
		with print_lock: print(f"   [{task_id}] ERROR: Cannot calculate start skip without total duration."); return False
	if clip_duration <= 0 or not (0 <= start_skip_percent < 100):
		with print_lock: print(f"   [{task_id}] ERROR: Invalid clip duration or skip percentage."); return False

	# Calculate start time based on percentage
	# --- MODIFIED: Use new param name 'duration' ---
	start_time_sec = duration * (start_skip_percent / 100.0)
	# Calculate end time
	end_time_sec = start_time_sec + clip_duration

	# Ensure end time doesn't exceed total duration
	# --- MODIFIED: Use new param name 'duration' ---
	if end_time_sec > duration:
		end_time_sec = duration
		# Prevent negative/zero duration if skip is too large or times coincide exactly
		if start_time_sec >= end_time_sec - 0.01: # Use small tolerance
			with print_lock: print(f"   [{task_id}] ERROR: Start skip ({start_skip_percent}%) results in zero or negative duration clip."); return False

	cmd = [ FFMPEG, "-y", "-hide_banner",
			"-i", str(input_path),         # Input first for accuracy
			"-ss", str(start_time_sec),    # Seek accurately after input
			"-to", str(end_time_sec),      # Cut accurately using end time
			"-map", "0",                   # Copy all streams
			"-c", "copy",                  # Stream copy
			"-movflags", "+faststart",
			"-avoid_negative_ts", "make_zero", # Handle potential timestamp issues
			str(output_path) ]

	# --- MODIFIED: Pass 'clip_duration' as the duration for this task ---
	rc, tail = _run_ffmpeg_live(cmd, task_id=task_id, timeout=REMUX_TIMEOUT_S, duration=clip_duration)
	success = False
	if rc != 0:
		harmless = ["Invalid data found when processing input", "Error while decoding stream"]
		if not any(e in (tail or "") for e in harmless):
			errlog_block(str(input_path), f"short_ver failed [{task_id}]", " ".join(cmd) + f"\n\n{tail or ''}")
		else:
			with print_lock: print(f"   [{task_id}] WARNING: Non-fatal error during short_ver (common).")
			# Check if output is valid despite error
			if output_path.exists() and output_path.stat().st_size > 1024: success = True
			else:
				with print_lock: print(f"   [{task_id}] ERROR: short_ver output missing/empty.")
	else:
		success = True # RC 0 means success

	# Final check even on success=True
	if success and (not output_path.exists() or output_path.stat().st_size <= 100):
		errlog_block(str(input_path), f"short_ver post-check failed [{task_id}]", f"FFmpeg rc=0 but output file missing or empty.\nCmd: {' '.join(cmd)}")
		success = False

	return success

# --- Artifact Orchestrator ---

def _post_encode_artifacts(
	final_file_path: Path,
	output_info: Dict[str, Any], # Info probed by clean_up
	task_id_base: str,
	de_bug: bool = False # Pass debug flag
) -> None:
	"""
	Generates matrix, speed up, and short versions using info from clean_up.
	Checks if artifacts already exist (as valid files) before attempting creation.
	Runs best-effort; failures here don't stop the main process.
	"""
	if not ADD_ADDITIONAL: return # Check master flag first

	if not final_file_path or not final_file_path.exists():
		with print_lock: print(f"   [{task_id_base}] Skipping artifacts: Final file missing.")
		return
	if not output_info:
		with print_lock: print(f"   [{task_id_base}] Skipping artifacts: Missing output info.")
		return

	with print_lock: print(f"--- [{task_id_base}] Creating Additional Versions for {final_file_path.name} ---")
	base_name = final_file_path.stem

	# --- Extract needed info ---
	duration	= output_info.get("dur_out")
	width		= output_info.get("w_enc")
	height		= output_info.get("h_enc")
	has_audio = output_info.get("ach_out", 0) > 0

	# --- Generate Matrix ---
	matrix_path = final_file_path.with_name(f"{base_name}_matrix.png")
	skip_matrix = False
	try:
		if matrix_path.is_file() and matrix_path.stat().st_size > 100:
			skip_matrix = True
			with print_lock: print(f"   [{task_id_base}] Skipping matrix creation: {matrix_path.name} already exists.")
	except FileNotFoundError: skip_matrix = False
	except OSError as e:
		with print_lock: print(f"   [{task_id_base}] Warning checking matrix existence ({matrix_path.name}): {e}. Attempting creation.")
		skip_matrix = False

	if not skip_matrix:
		try:
			# Use old name 'matrix_it'
			# --- MODIFIED: Pass 'task_id_base' ---
			matrix_ok = matrix_it(
				final_file_path, matrix_path, task_id_base=f"{task_id_base}_MATRIX",
				duration=duration, width=width, height=height
				# Uses constants for cols/rows/skips/width via defaults
			)
			if matrix_ok:
				with print_lock: print(f"   [{task_id_base}] Thumbnail matrix created: {matrix_path.name}")
		except Exception as e:
			with print_lock: print(f"   [{task_id_base}] ERROR creating matrix: {e}")
			try: errlog_block(str(final_file_path), "matrix artifact exception", f"{e}\n{traceback.format_exc()}")
			except Exception: pass

	# --- Generate Speed Up ---
	speed_path = final_file_path.with_name(f"{base_name}_fast_{ADDITIONAL_SPEED_FACTOR:.1f}x.mp4")
	skip_speed = False
	try:
		if speed_path.is_file() and speed_path.stat().st_size > 1024:
			skip_speed = True
			with print_lock: print(f"   [{task_id_base}] Skipping speed up: {speed_path.name} already exists.")
	except FileNotFoundError: skip_speed = False
	except OSError as e:
		with print_lock: print(f"   [{task_id_base}] Warning checking speed up existence ({speed_path.name}): {e}. Attempting creation.")
		skip_speed = False

	if not skip_speed:
		try:
			# Use old name 'speed_up'
			# --- MODIFIED: Pass 'duration' ---
			speed_ok = speed_up(
				final_file_path, ADDITIONAL_SPEED_FACTOR, speed_path, f"{task_id_base}_SPEED",
				has_audio=has_audio, duration=duration
			)
			if speed_ok:
				with print_lock: print(f"   [{task_id_base}] Speed up version created: {speed_path.name}")
		except Exception as e:
			with print_lock: print(f"   [{task_id_base}] ERROR creating speed up: {e}")
			try: errlog_block(str(final_file_path), "speed_up artifact exception", f"{e}\n{traceback.format_exc()}")
			except Exception: pass

	# --- Generate Short Version ---
	short_path = final_file_path.with_name(f"{base_name}_short_{int(ADDITIONAL_SHORT_DUR)}s.mp4")
	skip_short = False
	try:
		if short_path.is_file() and short_path.stat().st_size > 1024:
			skip_short = True
			with print_lock: print(f"   [{task_id_base}] Skipping short version: {short_path.name} already exists.")
	except FileNotFoundError: skip_short = False
	except OSError as e:
		with print_lock: print(f"   [{task_id_base}] Warning checking short version existence ({short_path.name}): {e}. Attempting creation.")
		skip_short = False

	if not skip_short:
		try:
			# Use old name 'short_ver'
			# --- MODIFIED: Pass 'duration' ---
			short_ok = short_ver(
				final_file_path, short_path, f"{task_id_base}_SHORT",
				duration=duration, # Pass total duration hint
				# Uses constants for clip duration & skip % via defaults
			)
			if short_ok:
				with print_lock: print(f"   [{task_id_base}] Short version created: {short_path.name}")
		except Exception as e:
			with print_lock: print(f"   [{task_id_base}] ERROR creating short version: {e}")
			try: errlog_block(str(final_file_path), "short_ver artifact exception", f"{e}\n{traceback.format_exc()}")
			except Exception: pass

	with print_lock: print(f"--- [{task_id_base}] Finished Additional Versions ---")

# --- MODIFIED: Added 'task_id' param with a default value ---
def clean_up(input_file: str, Outpt_fl: str, skip_it: bool = False, de_bug: bool = False, task_id: str = "CLEAN") -> int:
	"""
	Validates output against source and plan: core checks first, then size checks.
	Replaces original if all checks pass using retry logic. Returns savings or -1.
	"""
	if skip_it:
		try: srik_clear(input_file)
		except Exception: pass
		return -1

	in_path = Path(input_file)
	out_path = Path(Outpt_fl)
	in_size = 0
	out_size = 0
	savings = 0
	validation_passed = False
	reject_reason: Optional[str] = None
	source_info: Dict[str, Any] = {}
	plan_info: Dict[str, Any] = {}
	output_info: Dict[str, Any] = {}
	backup_made = False  # Initialize here
	backup: Optional[Path] = None # Initialize here
	final_path: Optional[Path] = None # Initialize here

	try: # --- Main Try Block Starts ---
		# --- 1. Basic Existence Checks ---
		if not out_path.exists():
			reject_reason = f"Output file missing: {Outpt_fl}"
			# No cleanup needed for output, just return error
			# SRIK clear happens in finally
			return -1
		if not in_path.exists():
			reject_reason = f"Input file missing during cleanup: {input_file}"
			# Output exists but input doesn't, clean output
			# Fall through to finally which handles cleanup
			return -1
		in_stat = in_path.stat()
		out_stat = out_path.stat()
		in_size = in_stat.st_size
		out_size = out_stat.st_size
		# --- 2. Fetch Plan Data & Probe Output ---
		data_fetch_error: Optional[str] = None
		try: # --- Inner Try for Data Fetching ---
			srik_data = srik_get(input_file)
			source_info = srik_data.get("source", {})
			plan_info = srik_data.get("plan", {})
			if not source_info or not plan_info:
				data_fetch_error = "Missing planning data (SRIK) for validation"
			if not data_fetch_error:
				out_meta, _, probe_err = ffprobe_run(str(out_path), check_corruption=False)
				if probe_err or not out_meta:
					data_fetch_error = f"Failed to re-probe output file: {probe_err or 'No metadata'}"
				else:
					out_fmt = out_meta.get("format", {})
					out_streams = out_meta.get("streams", [])
					out_v = next((s for s in out_streams if s.get("codec_type") == "video"), {})
					out_a = next((s for s in out_streams if s.get("codec_type") == "audio"), {})
					output_info = {
						"dur_out": float(out_fmt.get("duration", 0.0)),
						"size_out": out_size,
						"w_enc": int(out_v.get("width", 0)),
						"h_enc": int(out_v.get("height", 0)),
						"ach_out": int(out_a.get("channels", 0)),
						"asr_out": int(out_a.get("sample_rate", 0)),
						"acodec_out": str(out_a.get("codec_name", "")).lower(),
					}
		except Exception as e:
			data_fetch_error = f"Exception during validation prep: {e}"
		# --- End Inner Try for Data Fetching ---
		if data_fetch_error:
			reject_reason = data_fetch_error
		# --- 3. Core Validation Checks ---
		if not reject_reason:
			validation_details: List[str] = []
			# Duration Check
			dur_in = float(source_info.get("dur_in", 0.0))
			dur_out = float(output_info.get("dur_out", 0.0))
			if dur_in > 1.0:
				tol = max(DURATION_TOLERANCE_ABS, DURATION_TOLERANCE_PCT * dur_in)
				dt = abs(dur_in - dur_out)
				if dt > tol:
					validation_details.append(f"Duration mismatch (Diff:{dt:.1f}s > Tol:{tol:.1f}s)")
			# Resolution Check
			w_in = int(source_info.get("w_in", 0))
			h_in = int(source_info.get("h_in", 0))
			w_o = int(output_info.get("w_enc", 0))
			h_o = int(output_info.get("h_enc", 0))
			if w_o <= 0 or h_o <= 0:
				validation_details.append("Output video dimensions invalid (<=0)")
			elif (w_o > w_in or h_o > h_in) and (w_in > 0 and h_in > 0):
				if w_o > w_in * 1.01 or h_o > h_in * 1.01:
					validation_details.append(f"Output appears upscaled ({w_o}x{h_o} > {w_in}x{h_in})")
			elif bool(plan_info.get("expect_downscale")):
				target_h = 1080
				height_tolerance = target_h * 0.02
				if abs(h_o - target_h) > height_tolerance:
					validation_details.append(f"Expected downscale, output height wrong ({h_o} vs target {target_h})")
				elif w_in > 0 and h_in > 0:
					original_ar = w_in / h_in
					output_ar = w_o / h_o
					ar_tolerance = 0.05
					if abs(original_ar - output_ar) > ar_tolerance:
						validation_details.append(f"Aspect ratio changed (In:{original_ar:.2f}, Out:{output_ar:.2f})")
			# Audio Policy Check
			ap = plan_info.get("audio_policy", {"codec": "aac", "sr": 48000, "channels": [2, 6]})
			ac = output_info.get("acodec_out", "")
			ch = int(output_info.get("ach_out", 0))
			sr = int(output_info.get("asr_out", 0))
			if not (ac == ap["codec"] and ch in set(ap["channels"]) and (sr in (0, ap["sr"]))):
				validation_details.append(f"Audio policy fail (Got {ac}/{ch}ch/{sr}Hz)")
			if validation_details:
				reject_reason = "Core validation failed: " + "; ".join(validation_details)
		# --- 4. Size Validation Checks ---
		if not reject_reason:
			# "Too Small" Check
			min_allowed_size_ratio = MIN_SIZE_RATIO_DEFAULT
			calculated_dynamic_ratio = False
			try:
				dur_in2 = float(source_info.get("dur_in", 0.0)) # Use float
				target_bps = int(plan_info.get("ideal_bps", 0)) # Use int
				if dur_in2 > 0 and target_bps > 0 and in_size > 0:
					original_bps = _avg_bitrate(in_size, dur_in2)
					if original_bps > 0 and target_bps < original_bps * 0.7:
						expected_ratio = (target_bps / original_bps) * 0.7
						min_allowed_size_ratio = max(MIN_SIZE_RATIO_FLOOR, expected_ratio)
						calculated_dynamic_ratio = True
			except Exception:
				pass # Ignore errors, use default
			min_allowed_size = in_size * min_allowed_size_ratio
			if out_size < min_allowed_size:
				ratio_reason = f"target ratio ({min_allowed_size_ratio:.2f})" if calculated_dynamic_ratio else f"default ratio ({min_allowed_size_ratio:.2f})"
				reject_reason = (
					f"Output file too small ({hm_sz(out_size)} < {min_allowed_size_ratio*100:.0f}% "
					f"of input {hm_sz(in_size)}) - below minimum {hm_sz(min_allowed_size)} based on {ratio_reason}"
				)
			# "Too Large" Check (only if not already rejected)
			if not reject_reason:
				savings = in_size - out_size # Calculate savings
				file_grew = savings < 0
				if file_grew and AUTO_SIZE_GUARD and not FORCE_BIGGER:
					ratio = round((100 * savings / in_size), 1) if in_size > 0 else 0.0
					inflation_percent = -ratio
					inflation_bytes = -savings
					grew_too_much_pct = (INFLATE_MAX_BY is not None and inflation_percent > INFLATE_MAX_BY)
					grew_too_much_abs = (MAX_ABS_GROW_MB is not None and inflation_bytes > (MAX_ABS_GROW_MB * 1024 * 1024))
					if grew_too_much_pct or grew_too_much_abs:
						reject_reason = f"Output file grew too large (>{INFLATE_MAX_BY}% or >{MAX_ABS_GROW_MB}MB)"
		# --- 5. Determine if Validation Passed Overall ---
		if not reject_reason:
			validation_passed = True
		# --- 6. Proceed with Replacement IF Validation Passed ---
		if validation_passed:
			# Re-calculate savings just to be sure
			savings = in_size - out_size
			ratio = round((100 * savings / in_size), 1) if in_size > 0 else 0.0
			change_str = f"Saved {ratio:.1f}%" if savings >= 0 else f"Lost {-ratio:.1f}%"
			with print_lock: print(f"\n  .Size Was: {hm_sz(in_size)} Is: {hm_sz(out_size)} ({change_str})")
			if de_bug:
				with print_lock: print(f"DEBUG: Proceeding to replace; savings: {savings} bytes.")
			final_path = in_path.with_suffix(TMPF_EX) # Assign to final_path
			backup = in_path.with_suffix(f".{in_path.suffix.lstrip('.')}.orig_{RUN_TOKEN}")
			# backup_made needs to be defined before the inner try
			backup_made = False
			try: # --- Inner Try for Rename/Move/Artifacts ---
				# 1) Rename original -> backup (with retry)
				_retry_with_lock_info(
					action_desc=f"Renaming original to backup\n   {in_path} -> {backup}",
					func=in_path.rename,
					args=(backup,),
					path_to_inspect=str(in_path),
					de_bug=de_bug
				)
				backup_made = True # Mark success *after* call returns
				if de_bug:
					with print_lock: print(f"DEBUG: Renamed original to backup: {backup.name}")
				# 2) Move new output -> final (with retry)
				_retry_with_lock_info(
					action_desc=f"Moving new file to final location\n   {out_path} -> {final_path}",
					func=shutil.move,
					args=(str(out_path), str(final_path)),
					path_to_inspect=str(out_path), # Inspect the source before move
					de_bug=de_bug
				)
				if de_bug:
					with print_lock: print(f"DEBUG: Moved new file to final destination: {final_path.name}")
				# 3) Try fsync (best effort)
				try:
					dir_fd = os.open(str(final_path.parent), os.O_RDONLY | getattr(os, 'O_DIRECTORY', 0))
					os.fsync(dir_fd)
					os.close(dir_fd)
				except Exception as fs_err:
					if de_bug:
						with print_lock: print(f"DEBUG: fsync failed or skipped: {fs_err}")
				# 4) Delete backup (best effort)
				try:
					backup.unlink(missing_ok=True)
				except Exception as del_err:
					with print_lock: print(f" ?!Warning: Failed to delete backup file {backup.name}: {del_err}")
				# 5) Artifacts from the final, validated, swapped file
				if ADD_ADDITIONAL: # Check flag
					try:
						with print_lock: print("\n[post] Starting artifact generation from final file...")
						# --- MODIFIED: Pass the 'task_id' to _post_encode_artifacts ---
						_post_encode_artifacts(final_path, output_info, task_id_base=task_id, de_bug=de_bug) # Pass final_path
					except Exception as art_e:
						# Log artifact errors but don't fail the main cleanup
						try: errlog_block(str(final_path), "post artifacts exception", f"{art_e}\n{traceback.format_exc()}")
						except Exception: pass
			except Exception as replace_err: # Catch errors from _retry_with_lock_info or fsync/unlink
				reject_reason = f"File Replacement Failure: {replace_err}"
				with print_lock: print(f"ERROR during replacement: {replace_err}")
				# Attempt Rollback if backup was made
				if backup_made and backup.exists():
					try:
						if final_path and final_path.exists(): # Check if final_path was assigned
							final_path.unlink(missing_ok=True)
						# Restore original back into place (retry too)
						_retry_with_lock_info(
							action_desc=f"Restoring backup after failure\n   {backup} -> {in_path}",
							func=backup.rename,
							args=(in_path,),
							path_to_inspect=str(backup), # Inspect backup before rename
							de_bug=de_bug
						)
						with print_lock: print(f"  Successfully rolled back: Restored {in_path.name}")
					except Exception as restore_err:
						reject_reason += f"\nCRITICAL ROLLBACK FAILED for {in_path.name}: {restore_err}"
						try:
							errlog_block(input_file, "CRITICAL ROLLBACK FAILURE", reject_reason)
						except Exception:
							pass
			# --- End Inner Try for Rename/Move/Artifacts ---
	# --- Outer Exception Handling ---
	except FileNotFoundError as fnf_err:
		reject_reason = f"File missing during clean_up validation: {fnf_err}"
	except Exception as e:
		reject_reason = f"An unexpected error occurred in clean_up: {e}\n{traceback.format_exc()}"

	finally: # --- Finally Block Starts ---
		if reject_reason:
			with print_lock:
				print(f"\n--- 👎 VALIDATION FAILURE/ERROR for {input_file} 👎 ---")
				print(f"   Reason: {reject_reason}")
			# Delete output file if it exists and wasn't the source of FileNotFoundError
			# Need to check if 'e' exists locally if FileNotFoundError happened in outer try
			outer_exception = locals().get('e')
			is_fnf_outer = isinstance(outer_exception, FileNotFoundError)
			if out_path is not None and out_path.exists() and not is_fnf_outer:
				try:
					out_path.unlink()
					with print_lock: print(f"   Deleted rejected output file: {out_path.name}")
				except Exception as del_e:
					with print_lock: print(f"   ERROR: Failed to delete rejected output file {out_path.name}: {del_e}")
			try:
				errlog_block(input_file, "Cleanup Failure/Rejection", reject_reason)
			except Exception:
				pass # Avoid errors during error logging
			# Ensure backup is restored if necessary (final check)
			# Use backup_made flag set in the inner try block
			if backup_made and backup and backup.exists() and not in_path.exists():
				try:
					# Use retry logic for final restore attempt as well
					_retry_with_lock_info(
						action_desc=f"Restoring backup during final cleanup\n   {backup} -> {in_path}",
						func=backup.rename,
						args=(in_path,),
						path_to_inspect=str(backup),
						de_bug=de_bug,
						attempts=3, # Fewer attempts for cleanup rollback
						base_wait=1.0
					)
					with print_lock: print("   INFO: Restored backup during final cleanup.")
				except Exception as final_restore_err:
					with print_lock: print(f"   CRITICAL: Failed final restore {backup.name}: {final_restore_err}")
					try:
						errlog_block(input_file, "CRITICAL FINAL ROLLBACK FAILURE", f"Could not restore {backup.name}: {final_restore_err}")
					except Exception:
						pass # Avoid errors during final error logging

		# Always clear SRIK
		try:
			srik_clear(input_file)
		except Exception as srik_e: # Use different variable name
			if de_bug:
				with print_lock: print(f"DEBUG: Failed to clear SRIK: {srik_e}")

		# Final return based on state determined in the main try block
		if reject_reason or not validation_passed:
			return -1
		else:
			# If validation passed and no reject reason occurred, return savings
			return savings
	# --- Finally Block Ends ---
