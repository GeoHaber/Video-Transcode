# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import re
import sys
import json
import time
import shlex
import queue
import atexit
import signal
import tempfile
import threading
import traceback
import subprocess as sp
import math
import platform
import random
import string
import shutil
import charset_normalizer

from typing import Any, Dict, List, Optional, Tuple, Callable, Iterable, Union
from pathlib import Path
from fractions import Fraction
from dataclasses import dataclass, field
from collections import defaultdict

from Utils import *

IS_WIN = sys.platform.startswith("win")

# =============================================================================
# 1. HARDWARE & CONFIG
# =============================================================================

def detect_hardware_encoder() -> str:
	"""
	Detects the best available hardware encoder (NVIDIA, Intel QSV, AMD AMF)
	based on the system's hardware configuration.
	"""
	if IS_WIN:
		try:
			import wmi
			c = wmi.WMI()
			for gpu in c.Win32_VideoController():
				name = gpu.Name.lower()
				if "amd" in name or "radeon" in name: return "hevc_amf"
				if "intel" in name or "arc" in name: return "hevc_qsv"
				if "nvidia" in name: return "hevc_nvenc"
		except: pass
	else:
		try:
			lspci = sp.check_output("lspci", shell=True).decode().lower()
			if "amd" in lspci: return "hevc_amf"
			if "intel" in lspci: return "hevc_qsv"
		except: pass

	proc = platform.processor().lower()
	if "intel" in proc: return "hevc_qsv"
	if "amd" in proc: return "hevc_amf"
	return "libx265"

CURRENT_ENCODER = detect_hardware_encoder()
BIT_PER_PIX     = 0.05
FFMPEG          = "ffmpeg"
FFPROBE         = "ffprobe"
PROBE_TIMEOUT_S = 30
CORRUPTION_CHECK_TIMEOUT_S = 60

# =============================================================================
# 2. PROCESS MANAGEMENT
# =============================================================================

if IS_WIN:
	import ctypes
	kernel32 = ctypes.WinDLL('kernel32', use_last_error=True)
	PROCESS_ALL_ACCESS = 0x1F0FFF

class ChildProcessManager:
	"""
	Registers subprocesses and ensures they are terminated when the main
	Python script exits or receives a kill signal.
	"""
	def __init__(self):
		self._procs = []
		self._lock = threading.Lock()
		atexit.register(self.terminate_all)
		signal.signal(signal.SIGINT, lambda s,f: self.terminate_all())
		signal.signal(signal.SIGTERM, lambda s,f: self.terminate_all())

	def register(self, proc):
		"""Adds a process to the managed list."""
		with self._lock: self._procs.append(proc)

	def unregister(self, proc):
		"""Removes a process from the managed list (e.g., after clean exit)."""
		with self._lock:
			if proc in self._procs: self._procs.remove(proc)

	def terminate_all(self):
		"""Force kills all currently registered processes."""
		with self._lock:
			for p in self._procs:
				try: p.kill()
				except: pass
			self._procs.clear()

PROC_MGR = ChildProcessManager()

def _popen_managed(cmd: List[str], **kwargs) -> sp.Popen:
	"""
	Wrapper for subprocess.Popen that automatically registers the process
	with the ChildProcessManager for cleanup.
	"""
	if IS_WIN: kwargs.setdefault("creationflags", sp.CREATE_NEW_PROCESS_GROUP)
	else: kwargs.setdefault("preexec_fn", os.setsid)

	kwargs.setdefault("text", True)
	kwargs.setdefault("encoding", "utf-8")
	kwargs.setdefault("errors", "replace")

	proc = sp.Popen(cmd, **kwargs)
	PROC_MGR.register(proc)
	return proc

# =============================================================================
# 3. SRIK (PERSISTENCE)
# =============================================================================

_SRIK_LOCK = threading.RLock()
_GLOBAL_SRIK = {}

def srik_update(path: str, *, source=None, plan=None, output=None):
	"""Thread-safe update of the in-memory state dictionary for a file."""
	k = str(Path(path).resolve())
	with _SRIK_LOCK:
		entry = _GLOBAL_SRIK.get(k, {})
		if source: entry["source"] = source
		if plan: entry["plan"] = plan
		_GLOBAL_SRIK[k] = entry

def srik_get(path: str):
	"""Retrieves the state dictionary for a specific file path."""
	k = str(Path(path).resolve())
	with _SRIK_LOCK: return _GLOBAL_SRIK.get(k, {}).copy()

def srik_clear(path: str):
	"""Removes a file from the state dictionary."""
	k = str(Path(path).resolve())
	with _SRIK_LOCK: _GLOBAL_SRIK.pop(k, None)

# =============================================================================
# 4. METADATA & PROBING
# =============================================================================

@dataclass
class VideoMeta:
	"""Data structure holding essential video file metadata."""
	width: int = 0
	height: int = 0
	duration: float = 0.0
	bitrate: int = 0
	size: int = 0
	codec: str = ""
	streams: List[Dict] = field(default_factory=list)
	format_tags: Dict = field(default_factory=dict)

def ffprobe_run(input_file: str, execu=None, de_bug=False, check_corruption=False):
	"""
	Runs ffprobe to extract JSON metadata and optionally performs a quick
	decoding test to check for file corruption.
	"""
	cmd = [execu or FFPROBE, "-v", "error", "-show_streams", "-show_format", "-of", "json", input_file]
	meta_obj, corrupt, err_msg = None, False, None

	try:
		p = _popen_managed(cmd, stdout=sp.PIPE, stderr=sp.PIPE)
		out, err = p.communicate(timeout=PROBE_TIMEOUT_S)
		PROC_MGR.unregister(p)

		if p.returncode != 0:
			err_msg = f"FFprobe failed: {err}"
		elif out:
			try:
				data = json.loads(out)
				fmt = data.get("format", {})
				streams = data.get("streams", [])
				vid = next((s for s in streams if s.get('codec_type') == 'video'), {})

				br = int(fmt.get('bit_rate', 0) or 0)
				sz = int(fmt.get('size', 0) or 0)

				meta_obj = VideoMeta(
					width=int(vid.get('width', 0)),
					height=int(vid.get('height', 0)),
					duration=float(fmt.get('duration', 0)),
					bitrate=br,
					size=sz,
					codec=vid.get('codec_name', 'unknown'),
					streams=streams,
					format_tags=fmt.get("tags", {})
				)
			except Exception as e:
				err_msg = f"JSON Parse Error: {e}"
	except Exception as e:
		err_msg = str(e)

	if not err_msg and check_corruption:
		try:
			p2 = _popen_managed([FFMPEG, "-v", "error", "-xerror", "-i", input_file, "-t", "10", "-f", "null", "-"], stdout=sp.DEVNULL, stderr=sp.PIPE)
			_, c_err = p2.communicate(timeout=CORRUPTION_CHECK_TIMEOUT_S)
			PROC_MGR.unregister(p2)
			if p2.returncode != 0: corrupt = True
		except: corrupt = True

	return meta_obj, corrupt, err_msg

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
	"""Attempts to detect the character encoding of a text file."""
	try:
		data = file_path.read_bytes()[:102400]
		match = charset_normalizer.from_bytes(data).best()
		if match and match.encoding not in ('utf-8', 'ascii'): return str(match.encoding).lower()
	except: pass
	return None

def _tokens_after_stem(sub_path: Path, video_stem: str) -> List[str]:
	"""Extracts tokens (like 'eng', 'forced') from filename after the video name match."""
	remainder = sub_path.stem[len(video_stem):].lstrip(".-_ ")
	return [t for t in re.split(r"[.\-_ ]+", remainder) if t]

def _guess_lang3_from_filename(sub_path: Path, video_stem: str) -> Optional[str]:
	"""Guesses 3-letter language code based on filename tokens."""
	for tok in _tokens_after_stem(sub_path, video_stem):
		key = tok.lower()
		if key in _ALIAS_TO_LANG3: return _ALIAS_TO_LANG3[key]
	return None

def _score_sidecar(video_stem: str, path: Path, default_lng: str) -> int:
	"""Calculates a relevance score for a sidecar file to pick the best match."""
	score = {".srt": 30, ".ass": 20, ".vtt": 10}.get(path.suffix.lower(), 0)
	tokens = {t.lower() for t in _tokens_after_stem(path, video_stem)}
	lang = _guess_lang3_from_filename(path, video_stem)
	if lang == default_lng: score += 1200
	if "forced" in tokens: score -= 600
	if "sdh" in tokens or "cc" in tokens: score += 25
	return score

def add_subtl_from_file(input_file: str) -> Tuple[List[str], bool, List[str], str, str]:
	"""
	Scans for external subtitles, picks the best match, sanitizes it to UTF-8,
	and returns input arguments for FFmpeg.
	"""
	p = Path(input_file)
	stem = p.stem
	parent = p.parent
	candidates = []
	logs = []

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
			logs.append(f"\033[93m   .Info: Sanitized subtitle '{best_path.name}' to UTF-8 temp file.\033[0m")

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

@dataclass
class VideoContext:
	input_file: str
	estimated_video_bitrate: int = 0

def _ideal_hevc_bps(w, h, fps, source_br):
	"""Calculates the target HEVC bitrate based on resolution, FPS, and BIT_PER_PIX."""
	if w > 0 and h > 0:
		return max(250_000, int(w * h * fps * BIT_PER_PIX))
	if source_br > 0:
		return max(250_000, int(source_br * 0.65))
	return 1_000_000

def _get_aspect_ratio(w, h):
	"""Calculates a display-friendly aspect ratio string."""
	if not h: return "?"
	r = w/h
	if abs(r - 1.77) < 0.05: return "16:9"
	if abs(r - 1.33) < 0.05: return "4:3"
	return f"{w}:{h}"

def parse_video(streams, ctx):
	"""Generates FFmpeg args to scale, re-encode, or copy video streams."""
	logs, cmd = [], []
	skip_all = True
	out_idx = 0

	for s in streams:
		if s.get("codec_type") != "video" or s.get("disposition", {}).get("attached_pic"):
			continue

		w = int(s.get("width", 0))
		h = int(s.get("height", 0))
		fps_str = s.get("avg_frame_rate", "24/1")
		try: fps = float(Fraction(fps_str))
		except: fps = 24.0
		codec = s.get("codec_name", "")
		pix = s.get("pix_fmt", "")

		scale_trigger = (w > 2600 or h > 1188)
		tgt_h = 1080 if scale_trigger else h
		tgt_w = int(tgt_h * (w/h)) if h > 0 else w
		if tgt_w % 2: tgt_w += 1

		br = ctx.estimated_video_bitrate
		ideal = _ideal_hevc_bps(tgt_w, tgt_h, fps, br)

		pct = int(((br - ideal) / br) * 100) if br > 0 else 0
		change = "reduction" if pct > 0 else "increase"
		method = "(Pixel-Math)" if (w > 0 and h > 0) else "(Source-Ratio)"
		br_log = f"Source: {hm_sz(br, 'bps')} <=> Ideal: {hm_sz(ideal, 'bps')} {method} => App: {abs(pct)}% BitRate {change}"
		logs.append(f"   |{br_log}")

		is_hevc = (codec.lower() == 'hevc')
		is_bloated = (br > ideal * 1.5)
		needs_scale = (w != tgt_w or h != tgt_h) and (w > tgt_w)

		cmd.extend(["-map", f"0:{s['index']}"])

		status = ""
		if is_hevc and not needs_scale and not is_bloated and w > 0:
			cmd.extend([f"-c:v:{out_idx}", "copy"])
			logs.append(f"   |Skip: HEVC + bitrate below ideal|")
			status = "=> Copy (HEVC + Efficient Bitrate)|#COPY"
		else:
			skip_all = False
			c_enc = CURRENT_ENCODER
			cmd.extend([f"-c:v:{out_idx}", c_enc])
			cmd.extend([f"-b:v:{out_idx}", f"{ideal//1000}k"])
			cmd.extend([f"-maxrate:v:{out_idx}", f"{int(ideal*1.5)//1000}k"])
			cmd.extend([f"-bufsize:v:{out_idx}", f"{int(ideal*2)//1000}k"])

			if "amf" in c_enc: cmd.extend(["-quality", "quality", "-rc", "vbr_peak"])
			elif "qsv" in c_enc: cmd.extend(["-preset", "slow", "-global_quality", "22"])
			else: cmd.extend(["-preset", "medium", "-crf", "22"])

			vf = []
			if needs_scale: vf.append(f"scale={tgt_w}:{tgt_h}")
			if "10" in pix: vf.append("format=p010le")
			if vf: cmd.extend([f"-filter:v:{out_idx}", ",".join(vf)])

			if w == 0: status = f"Re-encode (Fix Unknown Res -> {c_enc} @ {hm_sz(ideal, 'bps')})"
			else: status = f"Re-encode ({c_enc} @ {hm_sz(ideal, 'bps')})"

		ar = _get_aspect_ratio(w, h)
		row = f"   |<V:{s.get('index','?'):2}>|{codec:^8}|{w}x{h}|{ar}|{fps:.2f} fps|{'10-bit' if '10' in pix else '8-bit'}|{status}"
		logs.append(f"\033[91m{row}\033[0m")
		out_idx += 1

	if skip_all: logs.append("\033[91m  .Skip: Video streams are optimal.\033[0m")
	return cmd, skip_all, logs

def parse_audio(streams, ctx):
	"""Generates FFmpeg args to normalize audio to AAC (stereo/5.1) or copy if valid."""
	cmd, logs = [], []
	skip_all = True
	out_idx = 0
	for s in streams:
		if s.get("codec_type") != "audio": continue
		idx = s['index']
		codec = s.get('codec_name', '')
		lang = s.get('tags', {}).get('language', 'und')
		ch = int(s.get('channels', 0))
		br = int(s.get('bit_rate', 0))

		cmd.extend(["-map", f"0:{idx}"])
		action = ""
		if codec == 'aac' and ch <= 6:
			cmd.extend([f"-c:a:{out_idx}", "copy"])
			action = "Copy"
		else:
			skip_all = False
			cmd.extend([f"-c:a:{out_idx}", "aac"])
			if ch > 6:
				cmd.extend([f"-ac:a:{out_idx}", "6", f"-b:a:{out_idx}", "384k"])
				action = "Re-encode (-> 5.1 384k)"
			else:
				cmd.extend([f"-b:a:{out_idx}", "192k"])
				action = "Re-encode (-> Stereo 192k)"

		logs.append(f"\033[92m   |<A:{idx:2}>|{codec:^6}|{lang:<3}|Br:{hm_sz(br,'bps'):<10}|Ch:{ch}| {action}\033[0m")
		out_idx += 1

	if skip_all: logs.append("\033[92m  .Skip: Audio streams are optimal.\033[0m")
	return cmd, skip_all, logs

def parse_subtl(streams, ctx):
	"""
	Identifies internal text streams and checks for external subtitles.
	Note: Returns sidecar info separately so it can be mapped LAST in the orchestrator.
	"""
	cmd, logs = [], []
	skip_all = True
	out_idx = 0
	found_valid = False

	# 1. Internal
	for s in streams:
		if s.get("codec_type") == "subtitle":
			idx = s['index']
			codec = s.get("codec_name", "")
			lang = s.get("tags", {}).get("language", "und")
			if codec in ['mov_text', 'srt', 'ass', 'webvtt']:
				cmd.extend(["-map", f"0:{idx}"])
				if codec == 'mov_text':
					cmd.extend([f"-c:s:{out_idx}", "copy"])
					logs.append(f"\033[94m   |<S:{idx:2}>|{codec:^6}|{lang:<3}| Copy\033[0m")
				else:
					skip_all = False
					cmd.extend([f"-c:s:{out_idx}", "mov_text"])
					logs.append(f"\033[94m   |<S:{idx:2}>|{codec:^6}|{lang:<3}| Re-encode -> mov_text\033[0m")
				out_idx += 1
				found_valid = True

	# 2. External
	sidecar_cmd = []
	side_lang = "eng"
	side_disp = "default"

	try:
		res_cmd, res_skip, res_logs, res_lang, res_disp = add_subtl_from_file(ctx.input_file)
		logs.extend(res_logs)
		if res_cmd:
			sidecar_cmd = res_cmd
			side_lang = res_lang
			side_disp = res_disp
			skip_all = False
			found_valid = True
	except: pass

	if not found_valid:
		logs.append("\033[94m  .No compatible subtitles, check for external files.\033[0m")

	return cmd, skip_all, logs, sidecar_cmd, side_lang, side_disp

def parse_finfo(input_file: str, metadata: Any, de_bug=False):
	"""
	Main orchestration function.
	1. Analyzes file metadata.
	2. Calls parse_video/audio/subtl.
	3. Assembles the final FFmpeg command, ensuring external subs are mapped LAST.
	"""
	fmt, streams, fmt_tags = {}, [], {}
	tot_br, dur, sz = 0, 0.0, 0

	if hasattr(metadata, 'streams'):
		streams, fmt_tags = metadata.streams, metadata.format_tags
		tot_br, dur, sz = metadata.bitrate, metadata.duration, metadata.size
	elif isinstance(metadata, dict):
		streams = metadata.get("streams", [])
		if "format" in metadata:
			fmt = metadata.get("format", {})
			fmt_tags = fmt.get("tags", {})
			tot_br = int(fmt.get('bit_rate', 0) or 0)
			dur = float(fmt.get('duration', 0) or 0)
			sz = int(fmt.get('size', 0) or 0)
		else:
			tot_br = int(metadata.get("bitrate", 0) or 0)
			dur = float(metadata.get("duration", 0) or 0)
			sz = int(metadata.get("size", 0) or 0)
			fmt_tags = metadata.get("format_tags", {})

	if not streams and sz == 0:
		return [], True, ["\033[93m !Error: Unreadable Metadata\033[0m"]

	vc = len([s for s in streams if s.get('codec_type') ==    'video'])
	ac = len([s for s in streams if s.get('codec_type') ==    'audio'])
	sc = len([s for s in streams if s.get('codec_type') == 'subtitle'])

	skip_found = (SKIP_KEY in str(fmt_tags.get("comment", "")))

	header = (	f"   |=Title|{Path(input_file).stem}|\n"
				f"   |<FRMT>|Size: {hm_sz(sz)}|Bitrate: {hm_sz(tot_br,'bps')}|"
				f"Length: {hm_tm(dur)}|Streams: V:{vc} A:{ac} S:{sc} |" )
	if skip_found: header 	+= "Comment: SKIP_KEY Found"
	else: header 			+= "Comment: Add SKIP_KEY"

	safe_print(f"\033[96m{header}\033[0m")

	aud_br = sum(int(s.get('bit_rate', 0) or 0) for s in streams if s.get('codec_type') == 'audio')
	vid_br = max(tot_br - aud_br, 0)

	ctx = VideoContext(input_file, vid_br)
	v_cmd, v_skip, v_logs = parse_video(streams, ctx)
	a_cmd, a_skip, a_logs = parse_audio(streams, ctx)
	s_cmd, s_skip, s_logs, side_in, side_lang, side_disp = parse_subtl(streams, ctx)

	all_logs = v_logs + a_logs + s_logs

	needs_cont = (Path(input_file).suffix.lower() != ".mp4")
	needs_key = not skip_found
	final_skip = (v_skip and a_skip and s_skip and not needs_cont and not needs_key)

	reasons = []
	if not v_skip: reasons.append("Video     needs changes")
	if not a_skip: reasons.append("Audios    need changes")
	if not s_skip: reasons.append("Subtitles need changes")
	if needs_cont: reasons.append("Container convert")
	if needs_key:  reasons.append("Add skip key")

	if final_skip: all_logs.append("\033[96m  .Skip: File compliant.\033[0m")
	else: all_logs.append(f"\033[96m  .Processing Required ({', '.join(reasons)}) for: {Path(input_file).name}\033[0m")

	cmd = [FFMPEG, "-y", "-hide_banner", "-i", input_file]

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

	if TAG_HEVC_AS_HVC1 and not v_skip: cmd.extend(["-tag:v", "hvc1"])
	cmd.extend(["-metadata", f"comment={SKIP_KEY}"])

	srik_update(input_file, source={"dur": dur}, plan={"cmd": cmd})

	return cmd, final_skip, all_logs

# =============================================================================
# 7. EXECUTION & PROGRESS
# =============================================================================

def _read_pipe1_progress(pipe, task_id, duration):
	"""Reads FFmpeg -progress pipe to display a live progress bar."""
	start_time = time.time()
	d = {}

	for line in iter(pipe.readline, ''):
		line = line.strip()
		if not line: continue

		if "=" in line:
			k, v = line.split("=", 1)
			d[k.strip()] = v.strip()

		if line == "progress=continue" or line == "progress=end":
			try:
				us = int(d.get("out_time_us", 0))
				if us == 0 and "out_time" in d:
					t_str = d["out_time"]
					if ":" in t_str:
						h, m, s = t_str.split(":")
						us = int((float(h)*3600 + float(m)*60 + float(s)) * 1000000)

				sec = us / 1_000_000
				pct = min(100.0, (sec / duration) * 100) if duration > 0 else 0

				sz = int(d.get("total_size", 0))
				sz_str = f"{sz/1024/1024:.1f} MB"

				br = d.get("bitrate", "0").replace("kbits/s", "k")
				spd = d.get("speed", "0").replace("x", "")
				fps = d.get("frame", "0")

				el = time.time() - start_time
				eta = "--:--"
				if pct > 0.1:
					rem = (el / (pct/100)) - el
					eta = time.strftime("%H:%M:%S", time.gmtime(rem))

				# DIRECT PRINT
				msg = f"\r   [{task_id}].Encode|Size:{sz_str:>9}|Frames:{fps:>7}|Bitrate:{br:>8}|Speed:{spd:>5}x| {pct:>5.1f}%|ETA:{eta}|   "
				sys.stdout.write(msg)
				sys.stdout.flush()
			except: pass

	sys.stdout.write("\n")
	PROC_MGR.unregister(pipe)
	return 0

def ffmpeg_run(input_file, cmd, duration, skip_it, de_bug, task_id):
	"""
	Executes the FFmpeg command in a subprocess.
	Captures stderr in a background thread to prevent deadlocks and display errors on failure.
	"""
	if skip_it or not cmd: return None
	temp = str(Path(input_file).with_suffix('.temp.mp4'))

	cmd_p = cmd[:]
	cmd_p = [x for x in cmd_p if x not in ("-stats", "-nostats")]
	cmd_p.extend(["-progress", "pipe:1", "-stats_period", "0.5"])
	cmd_p.extend(["-movflags", "+faststart", temp])

	safe_print(f"   [{task_id}] Stage-1 encode -> MP4")

	# 1. Change stderr to PIPE to capture errors
	p = _popen_managed(cmd_p, stdout=sp.PIPE, stderr=sp.PIPE, bufsize=1)

	# 2. Helper to capture stderr without blocking stdout
	stderr_log = []
	def _read_stderr(pipe, log_list):
		try:
			for line in iter(pipe.readline, ''):
				log_list.append(line)
		except: pass

	# 3. Start threads for both pipes
	t_out = threading.Thread(target=_read_pipe1_progress, args=(p.stdout, task_id, duration))
	t_err = threading.Thread(target=_read_stderr, args=(p.stderr, stderr_log))

	t_out.start()
	t_err.start()

	p.wait()
	t_out.join()
	t_err.join()

	PROC_MGR.unregister(p)

	# 4. Analyze Result
	if p.returncode == 0 and os.path.exists(temp):
		return temp
	else:
		# 5. Display the Cause of Failure
		safe_print(f"\033[91m   [Error] FFmpeg Failed (Code: {p.returncode})\033[0m")
		if stderr_log:
			safe_print("\033[93m   --- FFmpeg Error Log (Last 10 lines) ---\033[0m")
			# Print last 10 lines to avoid spamming, but enough to see the error
			for line in stderr_log[-10:]:
				sys.stdout.write(f"   > {line}")
			safe_print("\033[93m   ----------------------------------------\033[0m")
		return None

def clean_up(input_file, output_file, skip_it=False, de_bug=False, task_id=""):
	"""
	Verifies the output file size, backs up the original, and replaces it with the new encode.
	Optionally generates matrix images or speed-up versions.
	"""
	if skip_it or not output_file: return -1
	in_p = Path(input_file)
	out_p = Path(output_file)

	# FIX: Calculate sizes BEFORE moving
	in_size = in_p.stat().st_size
	out_size = out_p.stat().st_size

	if out_size < 1024:
		 safe_print("   [Error] Output too small.")
		 out_p.unlink()
		 return -1

	if ADD_ADDITIONAL:
		if ADD_ARTIFACT_MATRIX: matrix_it(out_p, in_p.with_name(f"{in_p.stem}_matrix.png"), task_id)
		if ADD_ARTIFACT_SPEED: speed_up(out_p, in_p.with_name(f"{in_p.stem}_fast_{ADDITIONAL_SPEED_FACTOR}x.mp4"), task_id)

	try:
		bk = in_p.with_suffix(".orig")
		in_p.rename(bk)

		# FIX: Handle extension change (MKV->MP4) without rename error
		if in_p.suffix.lower() != out_p.suffix.lower():
			# We already renamed MKV to .orig.
			# Now we just leave MP4 as MP4. No rename needed.
			pass
		else:
			out_p.rename(in_p)

		bk.unlink(missing_ok=True)

		saved = in_size - out_size
		pct = (saved / in_size) * 100 if in_size > 0 else 0
		label = "Saved" if saved >= 0 else "Lost"

		safe_print(f"\n   .Size Was: {hm_sz(in_size)} Is: {hm_sz(out_size)} {label} {pct:.1f}% !")
		return saved
	except Exception as e:
		safe_print(f"   [Error] Replace failed: {e}")
		# Try to rollback
		if bk.exists() and not in_p.exists():
			bk.rename(in_p)
		return -1

def matrix_it(inp, out, task_id):
	"""Generates a 4x4 contact sheet/matrix image of the video."""
	cmd = [FFMPEG, "-y", "-hide_banner", "-i", str(inp), "-vf", "select='not(mod(n,1000))',scale=320:-1,tile=4x4", "-frames:v", "1", "-q:v", "5", str(out)]
	_popen_managed(cmd, stdout=sp.DEVNULL, stderr=sp.DEVNULL).wait()

def speed_up(inp, out, task_id):
	"""Generates a sped-up version of the video (e.g. for previews)."""
	f = ADDITIONAL_SPEED_FACTOR
	cmd = [FFMPEG, "-y", "-hide_banner", "-i", str(inp), "-filter_complex", f"[0:v]setpts=PTS/{f}[v];[0:a]atempo={f}[a]", "-map", "[v]", "-map", "[a]", "-preset", "veryfast", str(out)]
	_popen_managed(cmd, stdout=sp.DEVNULL, stderr=sp.DEVNULL).wait()
	
