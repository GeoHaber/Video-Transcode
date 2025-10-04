# -*- coding: utf-8 -*-
from __future__ import annotations

# =============================
# MP4 flags helper (ensures +faststart)
# =============================
def _ensure_mp4_faststart_and_hvc1(args, container_ext=None):
	out = list(args)
	if "-movflags" not in out:
		out.extend(["-movflags", "+faststart"])
	return out

Rev = "FFMpeg.py Rev: 22.33 (Fixed ImportError, added FFmpeg timeout, unconditional spinner)"
print(Rev)

import os
import re
import sys
import json
import time
import stat
import random
import string
import shutil
import itertools
import threading
import traceback
import subprocess as SP

import os
import sys
sys.path.append(os.path.dirname(__file__) or ".")

from pathlib import Path
from collections import defaultdict
from typing import List, Dict, Tuple, Any, Optional

from My_utils import hm_sz, hm_time, copy_move

try:
	from colorama import just_fix_windows_console
	just_fix_windows_console()
except Exception:
	pass

print_lock = threading.Lock()
WORK_PARALLEL = False  # Single-file mode, so sequential printing

# =============================
# Transcode Configuration
# =============================
HEVC_BPP				= 0.052
BLOAT_COPY_TOLERANCE	= 1.15

AUDIO_51_FLOOR_BPS		= 160_000
AUDIO_51_CAP_BPS		= 384_000
AUDIO_51_DEFAULT_BPS	= 256_000
AUDIO_51_PRESERVE_MULT	= 1.00
AUDIO_STEREO_FLOOR_BPS	= 96_000
AUDIO_STEREO_CAP_BPS	= 256_000
AUDIO_STEREO_DEFAULT_BPS = 128_000
AUDIO_STEREO_PRESERVE 	= True
AUDIO_STEREO_PRESERVE_THRESH_BPS = 96_000
AUDIO_FALLBACK_EAC3_BPS = 448_000
EAC3_DIALNORM			= "-31"
NON_DEFAULT_DOWNMIX_TO_STEREO = True
DROP_COMMENTARY = False
USE_HE_AAC_AT_LOW_BPS = False
AUDIO_FORCE_AR = 48000

TAG_HEVC_AS_HVC1 = True
ALWAYS_FINALIZE_MP4_ON_SKIP = True
AUTO_SIZE_GUARD = True
MAX_SIZE_INFLATE_PCT = 35
MAX_ABS_GROW_MB = None
FORCE_BIGGER = False

File_extn = [ ".avi", ".h264", ".m4v", ".mkv", ".moov", ".mov", ".movhd", ".movie",
			".movx", ".mts", ".mp4", ".mpe", ".mpeg", ".mpg", ".mpv",
			".ts", ".vfw", ".vid", ".video", ".wmv", ".x264", ".xvid"
			]
Skip_key = "| <¯\\_(ツ)_/¯> |"
TMPF_EX = ".mp4"
Default_lng = "eng"
Keep_langua = ["eng", "fre", "ger", "heb", "hun", "ita", "jpn", "rum", "rus", "spa"]
ALWAYS_10BIT = True

FFMPEG  = shutil.which("ffmpeg")  or r"C:\Program Files\ffmpeg\bin\ffmpeg.exe"
FFPROBE = shutil.which("ffprobe") or r"C:\Program Files\ffmpeg\bin\ffprobe.exe"

if not os.path.isfile(FFMPEG):
	with print_lock:
		print(f"Warning: FFmpeg not found at '{FFMPEG}'.")
if not os.path.isfile(FFPROBE):
	with print_lock:
		print(f"Warning: FFprobe not found at '{FFPROBE}'.")

glb_vidolen: float = 0.0

# =============================
# Progress registry (per-task)
# =============================
progress_lock = threading.Lock()
progress_state = {}
progress_len = {}

def progress_register(task_id: str, label: str = "") -> None:
	with progress_lock:
		progress_state[task_id] = {
			'label': label, 'size_kb': 0.0, 'frame': 0, 'fps': 0.0, 'speed': 0.0,
			'bitrate_kbps': 0.0, 'time_sec': 0.0, 'percent': 0.0, 'eta': "--:--:--",
			'last_line': "", 'last_update': 0.0,
		}

def progress_set_duration(task_id: str, vid_len: float) -> None:
	with progress_lock:
		progress_len[task_id] = float(vid_len or 0.0)

def progress_remove(task_id: str) -> None:
	with progress_lock:
		progress_state.pop(task_id, None)
		progress_len.pop(task_id, None)

def progress_get_snapshot():
	with progress_lock:
		return {k: dict(v) for k, v in progress_state.items()}

def _parse_time_to_sec(time_str: str) -> float:
	try:
		parts = [float(p) for p in (time_str or "0").split(':')]
		while len(parts) < 3:
			parts.insert(0, 0.0)
		h, m, s = parts
		return h * 3600 + m * 60 + s
	except Exception:
		return 0.0

def _eta_and_percent(time_sec: float, speed_x: float, vid_len: float) -> Tuple[str, float]:
	if vid_len <= 0:
		return ("--:--:--", 0.0)
	rem = max(vid_len - time_sec, 0.0)
	eta_s = rem / speed_x if speed_x and speed_x > 0 else 0.0
	h, r = divmod(int(round(eta_s)), 3600)
	m, s = divmod(r, 60)
	pct = 100.0 * time_sec / vid_len
	return f"{h:02d}:{m:02d}:{s:02d}", pct

def progress_update(task_id: str, prog_line: str) -> bool:
	with progress_lock:
		if task_id not in progress_state:
			return False
		state = progress_state[task_id]
		state['last_line'] = prog_line
		state['last_update'] = time.time()
		m = re.search(r"frame=\s*(\d+)", prog_line)
		if m:
			state['frame'] = int(m.group(1))
		m = re.search(r"fps=\s*([\d.]+)", prog_line)
		if m:
			state['fps'] = float(m.group(1))
		m = re.search(r"size=\s*([\d.]+)kB", prog_line)
		if m:
			state['size_kb'] = float(m.group(1))
		m = re.search(r"bitrate=\s*([\d.]+)kbits/s", prog_line)
		if m:
			state['bitrate_kbps'] = float(m.group(1))
		m = re.search(r"speed=\s*([\d.]+)x", prog_line)
		if m:
			state['speed'] = float(m.group(1))
		m = re.search(r"time=\s*([:\d.]+)", prog_line)
		if m:
			time_str = m.group(1)
			state['time_sec'] = _parse_time_to_sec(time_str)
			vid_len = progress_len.get(task_id, 0.0)
			state['eta'], state['percent'] = _eta_and_percent(state['time_sec'], state['speed'], vid_len)
			disp_str = (
				f"\r[{task_id}]|FFmpeg|Size:{hm_sz(state['size_kb'] * 1024):>8}|"
				f"Frames:{state['frame']:>7}|Fps:{state['fps']:>5.1f}|"
				f"BitRate:{hm_sz(state['bitrate_kbps'] * 1000, 'bps'):>9}|Speed:{state['speed']:>6.2f}x|"
				f"ETA:{state['eta']:>8}|{state['percent']:>6.1f}%"
			)
			with print_lock:
				sys.stderr.write(disp_str)
				sys.stderr.flush()
		return True

# =============================
# Spinner for long-running tasks
# =============================
class Spinner(threading.Thread):
	def __init__(self, label: str = "Processing", indent: int = 0, interval: float = 0.25):
		super().__init__(daemon=True)
		self.label = label
		self.indent = indent
		self.interval = interval
		self._stop_event = threading.Event()
		self._chars = itertools.cycle(r"|/-\\")
		self._line = " " * self.indent + next(self._chars) + " " + self.label

	def run(self):
		while not self._stop_event.is_set():
			with print_lock:
				sys.stdout.write("\r" + self._line)
				sys.stdout.flush()
			self._line = " " * self.indent + next(self._chars) + " " + self.label
			time.sleep(self.interval)
		with print_lock:
			sys.stdout.write("\r" + " " * len(self._line) + "\r")
			sys.stdout.flush()

	def stop(self) -> None:
		self._stop_event.set()
		self.join()

	def print_spin(self, message: str):
		self.stop()
		with print_lock:
			print(message)
		self.start()

# =============================
# FFprobe with corruption check
# =============================
def ffprobe_run(input_file: str, execu: str = FFPROBE, de_bug: bool = False, check_corruption: bool = False) -> Tuple[Dict[str, Any], bool]:
	if not input_file:
		raise FileNotFoundError("No input file provided")
	if not execu or not os.path.isfile(execu):
		raise FileNotFoundError(f"FFprobe not found at '{execu}'.")
	metadata: Dict[str, Any] = {}
	is_corrupted = False
	cmd = [execu, "-v", "error", "-show_streams", "-show_format", "-of", "json", "-read_intervals", "0%+1", input_file]
	if de_bug:
		with print_lock:
			print(f"FFprobe metadata cmd: {' '.join(cmd)}")
	try:
		r = SP.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=300, check=False)
		out = (r.stdout or "").strip()
		if out.startswith("{"):
			metadata = json.loads(out)
		else:
			if de_bug and r.stderr:
				with print_lock:
					last = r.stderr.strip().splitlines()[-1]
					print("ffprobe stderr:", last)
	except SP.TimeoutExpired:
		if de_bug:
			with print_lock:
				print(f"FFprobe timed out on: {input_file}")
	except json.JSONDecodeError as e:
		if de_bug:
			with print_lock:
				print(f"FFprobe JSON decode error on: {input_file}: {e}")
	except Exception as e:
		if de_bug:
			with print_lock:
				print(f"FFprobe error for {input_file}: {e}")
				print(traceback.format_exc())
	if check_corruption and os.path.isfile(FFMPEG):
		try:
			rr = SP.run([FFMPEG, "-v", "error", "-xerror", "-err_detect", "explode", "-threads", "1",
						 "-i", input_file, "-t", "10", "-f", "null", "-"],
						capture_output=True, text=True, encoding="utf-8", errors="replace",
						timeout=30, check=False)
			if rr.returncode != 0:
				is_corrupted = True
		except Exception:
			is_corrupted = True
	return metadata, is_corrupted

# =============================
# Video re-encoding settings
# =============================
def get_reencode_settings_based_on_source(
	codec_name: str,
	width: int,
	height: int,
	bitrate_bps: Optional[int],
	is_10bit: bool,
	fps: Optional[float] = None,
	allow_hw: bool = True,
	de_bug: bool = False
) -> Tuple[bool, List[str], str, Optional[str], int, Optional[str]]:
	w = int(width or 0)
	h = int(height or 0)
	fr = float(fps or 24.0)
	if w <= 0 or h <= 0 or fr <= 0:
		w, h, fr = 1920, 1080, 24.0
	needs_scaling = h > 1188
	target_height = 1080 if needs_scaling else h
	target_width = int(round(target_height * (w / h))) if w > 0 and h > 0 else int(round(target_height * 16 / 9))
	if target_width % 2:
		target_width += 1
	ideal_bps = int(HEVC_BPP * target_width * target_height * fr)
	target_bps = min(ideal_bps, int(bitrate_bps)) if bitrate_bps and bitrate_bps > 0 else ideal_bps
	if target_height >= 720:
		target_bps = max(target_bps, 1_000_000)
	is_hevc = (codec_name or "").lower() == "hevc"
	is_1080_or_less = h <= 1188
	br_ok = not bitrate_bps or bitrate_bps <= int(ideal_bps * BLOAT_COPY_TOLERANCE)
	if de_bug:
		with print_lock:
			print(f"Debug: get_reencode_settings: codec={codec_name}, width={w}, height={h}, fps={fr}, "
				  f"bitrate_bps={bitrate_bps}, ideal_bps={ideal_bps}, is_hevc={is_hevc}, is_1080_or_less={is_1080_or_less}")
	comparison_symbol = ">"
	if bitrate_bps is not None:
		if bitrate_bps == int(ideal_bps * BLOAT_COPY_TOLERANCE):
			comparison_symbol = "="
		elif bitrate_bps < int(ideal_bps * BLOAT_COPY_TOLERANCE):
			comparison_symbol = "<"
	if is_hevc and is_1080_or_less and br_ok:
		status_text = f"=> Copy (HEVC compliant, bitrate {hm_sz(bitrate_bps, 'bps')} {comparison_symbol} {hm_sz(ideal_bps * BLOAT_COPY_TOLERANCE, 'bps')})"
		return False, [], status_text, None, target_bps, None
	reasons = [r for r, c in [("Not HEVC", not is_hevc), ("Scaling", needs_scaling), ("Bitrate High", not br_ok)] if c]
	maxrate_bps = int(target_bps * 1.2)
	bufsize_bps = int(target_bps * 3.0)
	pix_fmt = "p010le" if ALWAYS_10BIT else "yuv420p"
	profile = "main10" if pix_fmt == "p010le" else "main"
	scaler = None
	if needs_scaling:
		scaler = f"scale=-2:{target_height}:flags=spline+accurate_rnd+full_chroma_int,format={pix_fmt}"
	encoder_flags: List[str] = []
	mode = "HW" if allow_hw else "SW"
	if allow_hw:
		encoder_flags.extend([
			"hevc_qsv", "-preset", "medium", "-rc_mode", "la_vbr",
			"-b:v", f"{max(target_bps, 1) // 1000}k",
			"-maxrate", f"{max(maxrate_bps, 1) // 1000}k",
			"-bufsize", f"{max(bufsize_bps, 1) // 1000}k",
			"-look_ahead", "1", "-g", "240", "-bf", "3",
			"-profile:v", profile
		])
	else:
		vbv_kbps = max(maxrate_bps, 1) // 1000
		vbv_buf_kbps = max(bufsize_bps, 1) // 1000
		encoder_flags.extend([
			"libx265", "-preset", "medium", "-crf", "23",
			"-x265-params",
			f"profile={profile}:aq-mode=3:qcomp=0.70:deblock=-1,-1:repeat-headers=1:vbv-maxrate={vbv_kbps}:vbv-bufsize={vbv_buf_kbps}"
		])
	status_text = (
		f"=> Re-encode ({mode})" +
		(" |Scaling" if needs_scaling else "") +
		(f"|Bitrate {hm_sz(bitrate_bps, 'bps')} {comparison_symbol} {hm_sz(ideal_bps * BLOAT_COPY_TOLERANCE, 'bps')}" if bitrate_bps else "")
	)
	return True, encoder_flags, status_text, scaler, target_bps, pix_fmt

# =============================
# FFmpeg command builder
# =============================
def parse_finfo(input_file: str, metadata: Dict[str, Any], de_bug: bool = False) -> Tuple[List[str], bool]:
	format_info = metadata.get("format", {})
	file_ext = os.path.splitext(input_file)[1].lower()
	size = int(format_info.get("size", 0))
	duration = float(format_info.get("duration", 0.0))
	bitrate = int(format_info.get("bit_rate", 0))
	title = format_info.get("tags", {}).get("title", Path(input_file).stem)
	raw_comment = format_info.get("tags", {}).get("comment", "").strip()
	f_skip = (raw_comment == Skip_key)
	log = f"\033[96m |=Title|{title}|\n"
	if f_skip:
		log += f"\033[96m |=Comment|{Skip_key}|\033[0m\n"
	log += f" |<FRMT>|Size: {hm_sz(size)}|Bitrate: {hm_sz(bitrate, 'bps')}|Length: {hm_time(duration)}|"
	streams = metadata.get("streams", [])
	streams_by_type = defaultdict(list)
	for s in streams:
		streams_by_type[s.get("codec_type", "?")].append(s)
	sc = {k: len(v) for k, v in streams_by_type.items()}
	log += f"Streams: V:{sc.get('video', 0)} A:{sc.get('audio', 0)} S:{sc.get('subtitle', 0)} D:{sc.get('data', 0)}"
	if f_skip:
		log += "|K+N Update|\033[0m\n"
		log += "\033[96m .Note: Has skip key, re-evaluating compliance.\033[0m\n"
	if 'audio' not in streams_by_type:
		log += "\033[93m !Warning: No audio stream found in this file.\033[0m\n"
	total_file_br = (size * 8) / duration if duration > 0 else 0
	total_audio_br = sum(int(s.get("bit_rate", 0)) for s in streams_by_type.get("audio", []))
	estimated_video_bitrate = int((total_file_br - total_audio_br) * 0.98) if total_file_br > total_audio_br > 0 else int(total_file_br * 0.80)

	# Video analysis
	ff_video = []
	skip_all_video = True
	v_summary = []
	for idx, stream in enumerate(streams_by_type.get("video", [])):
		codec = stream.get("codec_name", "").lower()
		pix_fmt = stream.get("pix_fmt", "")
		handler = stream.get("tags", {}).get("handler_name", "")
		if codec in ("mjpeg", "png"):
			log += f"\033[91m |<V:{idx:2}>|{codec:^8}| Ignored\033[0m\n"
			continue
		width, height = int(stream.get("width", 0)), int(stream.get("height", 0))
		is_10bit = "10" in pix_fmt or "p010" in pix_fmt
		fps = 24.0
		try:
			if "/" in (rate := stream.get("avg_frame_rate", "")):
				fps = eval(rate)
		except:
			pass
		needs_re, flags, status, scaler, target_bps, out_pix_fmt = get_reencode_settings_based_on_source(
			codec, width, height, estimated_video_bitrate, is_10bit, fps, allow_hw=True, de_bug=de_bug
		)
		ff_video.extend(["-map", f"0:v:{idx}"])
		handler_needs_fix = handler.lower() != "videohandler"
		tag_needs_fix = stream.get("codec_tag_string", "").lower() != "hvc1" and TAG_HEVC_AS_HVC1
		if not needs_re and not handler_needs_fix and not tag_needs_fix:
			ff_video.extend([f"-c:v:{idx}", "copy"])
		else:
			skip_all_video = False
			if needs_re:
				ff_video.extend([f"-c:v:{idx}"] + flags)
				if scaler:
					ff_video.extend([f"-vf:v:{idx}", scaler])
					v_summary.append("Scaling")
			else:
				ff_video.extend([f"-c:v:{idx}", "copy"])
			if handler_needs_fix:
				ff_video.extend([f"-metadata:s:v:{idx}", "handler_name=VideoHandler"])
				status += " |Fix Handler"
				v_summary.append("Fix Handler")
			if tag_needs_fix:
				ff_video.extend([f"-tag:v:{idx}", "hvc1"])
				status += " |Fix Tag"
				v_summary.append("Fix Tag")
		log += f"\033[91m |<V:{idx:2}>| {codec:^8} |{width}x{height}|{fps:.2f}fps|{'10-bit' if is_10bit else '8-bit'}| {status}\033[0m\n"
	if skip_all_video:
		log += "\033[91m .Skip: Video streams are optimal.\033[0m\n"

	# Audio analysis
	ffmpeg_audio_options = []
	a_skip = True
	output_audio_idx = 0
	a_summary = []
	best_candidate, best_score = None, float('-inf')
	for audio_strm in streams_by_type.get("audio", []):
		score = 0
		tags = audio_strm.get("tags", {})
		lang = tags.get("language", "und")
		title = tags.get("title", "").lower()
		disposition = audio_strm.get("disposition", {})
		if lang == Default_lng:
			score += 100
		if disposition.get("default", 0):
			score += 50
		score += int(audio_strm.get("channels", 0)) * 10
		if "commentary" in title and DROP_COMMENTARY:
			continue
		if score > best_score:
			best_score = score
			best_candidate = audio_strm
	disposition_needs_change = any(
		(s['index'] == best_candidate['index'] and not s.get("disposition", {}).get("default", 0)) or
		(s['index'] != best_candidate['index'] and s.get("disposition", {}).get("default", 0))
		for s in streams_by_type.get("audio", []) if best_candidate
	)
	for audio_strm in streams_by_type.get("audio", []):
		input_idx = audio_strm['index']
		codec_name = audio_strm.get("codec_name", "u").lower()
		lang = audio_strm.get("tags", {}).get("language", "und")
		channels = int(audio_strm.get("channels", 0))
		bit_rate = int(audio_strm.get("bit_rate", 0))
		disposition = audio_strm.get("disposition", {})
		is_compliant = codec_name in ["aac", "eac3"] and channels <= 6
		handler_is_correct = audio_strm.get("tags", {}).get("handler_name") == "SoundHandler"
		if not is_compliant or not handler_is_correct or disposition_needs_change:
			a_skip = False
		is_best = (best_candidate and input_idx == best_candidate['index'])
		stream_opts = ["-map", f"0:a:{output_audio_idx}"]
		log_action = []
		if is_compliant:
			stream_opts.extend([f"-c:a:{output_audio_idx}", "copy"])
			log_action.append("Copy (Compliant)")
		else:
			if channels >= 6 and NON_DEFAULT_DOWNMIX_TO_STEREO:
				stream_opts.extend([f"-c:a:{output_audio_idx}", "aac", f"-q:a:{output_audio_idx}", "2", f"-ac:a:{output_audio_idx}", "2"])
				log_action.append("Re-encode to AAC Stereo")
				a_summary.append("Re-encode audio to AAC")
			else:
				stream_opts.extend([f"-c:a:{output_audio_idx}", "eac3", f"-b:a:{output_audio_idx}", "640k"])
				log_action.append("Re-encode to E-AC3 640k")
				a_summary.append("Re-encode audio to E-AC3")
		if is_best:
			stream_opts.extend([f"-disposition:a:{output_audio_idx}", "default"])
			if not disposition.get("default", 0):
				log_action.append("Set Default")
				a_summary.append("Set Default audio")
		else:
			stream_opts.extend([f"-disposition:a:{output_audio_idx}", "0"])
			if disposition.get("default", 0):
				log_action.append("Clear Default")
				a_summary.append("Clear Default audio")
		if not handler_is_correct:
			stream_opts.extend([f"-metadata:s:a:{output_audio_idx}", "handler_name=SoundHandler"])
			log_action.append("Fix Handler")
			a_summary.append("Fix Handler")
		log += f"\033[92m |<A:{input_idx:2}>| {codec_name:^8} |{lang:<3}|Br:{hm_sz(bit_rate, 'bps'):<9}|Ch:{channels}| {'|'.join(log_action)}\033[0m\n"
		ffmpeg_audio_options.extend(stream_opts)
		output_audio_idx += 1
	if a_skip and not disposition_needs_change:
		log += "\033[92m .Skip: Audio streams are optimal.\033[0m\n"

	# Subtitle analysis
	ff_subttl = []
	external_inputs = []
	s_needs = False
	s_summary = []
	TEXT_BASED_CODECS = ("subrip", "ass", "mov_text")
	best_streams = {lang: {'score': -1, 'stream': None} for lang in Keep_langua}
	text_streams_to_keep = []
	embedded_langs = set()
	for stream in streams_by_type.get("subtitle", []):
		codec = stream.get("codec_name", "").lower()
		if codec == "hdmv_pgs_subtitle":
			log += f"\033[94m |<S:{stream['index']:2}>|{codec:^8}| Ignored (PGS subtitle not supported in .mp4)\033[0m\n"
			continue
		lang = stream.get("tags", {}).get("language", "und")
		if lang in Keep_langua and codec in TEXT_BASED_CODECS:
			score = 100
			tags = stream.get("tags", {})
			disposition = stream.get("disposition", {})
			title = tags.get("title", "").lower()
			if disposition.get("forced", 0) or "forced" in title:
				score -= 1000
			if "sdh" in title:
				score += 100
			if disposition.get("default", 0):
				score += 50
			score += int(stream.get("bit_rate", 0))
			score += int(stream.get("nb_frames", 0) or stream.get("nb_read_packets", 0))
			if score > best_streams[lang]['score']:
				best_streams[lang] = {'score': score, 'stream': stream}
				text_streams_to_keep.append(stream)
				embedded_langs.add(lang)
	if text_streams_to_keep:
		best_sub = next((s for s in text_streams_to_keep if s.get("tags", {}).get("language") == Default_lng), text_streams_to_keep[0])
		needs_disposition_change = any(
			(s['index'] == best_sub['index'] and not s.get("disposition", {}).get("default", 0)) or
			(s['index'] != best_sub['index'] and s.get("disposition", {}).get("default", 0))
			for s in text_streams_to_keep
		)
		all_codecs_compliant = all(s.get('codec_name') == 'mov_text' for s in text_streams_to_keep) if file_ext == ".mp4" else all(s.get('codec_name') in TEXT_BASED_CODECS for s in text_streams_to_keep)
		all_handlers_correct = all(s.get("tags", {}).get("handler_name") == "SubtitleHandler" for s in text_streams_to_keep)
		for i, stream in enumerate(text_streams_to_keep):
			is_best = (stream['index'] == best_sub['index'])
			handler_is_correct = stream.get("tags", {}).get("handler_name") == "SubtitleHandler"
			codec_is_compliant = stream.get('codec_name') == 'mov_text' if file_ext == ".mp4" else stream.get('codec_name') in TEXT_BASED_CODECS
			stream_opts = ["-map", f"0:s:{i}?"]
			log_status = []
			if codec_is_compliant:
				stream_opts.extend([f"-c:s:{i}", "copy"])
				log_status.append("Copy")
			else:
				stream_opts.extend([f"-c:s:{i}", "mov_text" if file_ext == ".mp4" else "subrip"])
				log_status.append("Re-encode")
				s_summary.append("Re-encode subtitle")
				s_needs = True
			if is_best:
				stream_opts.extend([f"-disposition:s:{i}", "default"])
				if not stream.get("disposition", {}).get("default", 0):
					log_status.append("Set Default")
					s_summary.append("Set Default subtitle")
					s_needs = True
			else:
				stream_opts.extend([f"-disposition:s:{i}", "0"])
				if stream.get("disposition", {}).get("default", 0):
					log_status.append("Clear Default")
					s_summary.append("Clear Default subtitle")
					s_needs = True
			if not handler_is_correct:
				stream_opts.extend([f"-metadata:s:s:{i}", "handler_name=SubtitleHandler"])
				log_status.append("Fix Handler")
				s_summary.append("Fix Handler")
				s_needs = True
			ff_subttl.extend(stream_opts)
			lang = stream.get('tags', {}).get('language', 'und')
			score = best_streams.get(lang, {}).get('score', 0)
			log += f"\033[94m |<S:{stream['index']:2}>| {stream.get('codec_name'):^8} |{lang:3}|Score:{score:<5} {'|'.join(log_status)}\033[0m\n"
		if all_codecs_compliant and not needs_disposition_change and all_handlers_correct:
			log += "\033[94m .Skip: Embedded subtitle streams are compliant.\033[0m\n"
	num_embedded = len(text_streams_to_keep)
	base_name, file_ext = os.path.splitext(input_file)
	for ext in [".srt", ".ass"]:
		test_file = base_name + ext
		if os.path.isfile(test_file):
			lang = Default_lng
			for l in Keep_langua:
				if f".{l}." in test_file.lower() or test_file.lower().endswith(f".{l}{ext}"):
					lang = l
					break
			if lang not in embedded_langs:
				log += f"\033[94m .Found {ext} external subtitle: {os.path.basename(test_file)} (lang: {lang})\033[0m\n"
				external_inputs.extend(["-i", test_file])
				stream_opts = [
					"-map", f"{1 + len(external_inputs)//2}:0",
					f"-c:s:{num_embedded + len(external_inputs)//2}", "mov_text" if file_ext == ".mp4" else "subrip",
					f"-metadata:s:s:{num_embedded + len(external_inputs)//2}", f"language={lang}",
					f"-disposition:s:{num_embedded + len(external_inputs)//2}", "default" if lang == Default_lng and not embedded_langs else "0"
				]
				ff_subttl.extend(stream_opts)
				s_summary.append(f"Add {lang} subtitle")
				s_needs = True
	if not text_streams_to_keep and not external_inputs:
		log += "\033[94m .No embedded subtitles found, checking for external file.\033[0m\n"
		if not external_inputs:
			log += "\033[94m .Skip: No compatible subtitles found.\033[0m\n"
	ff_data = []
	d_needs = False
	d_summary = []
	for idx, stream in enumerate(streams_by_type.get("data", []) + streams_by_type.get("attachment", [])):
		if stream.get("tags", {}).get("handler_name") == "SubtitleHandler":
			log += "\033[94m .Data streams compliant.\033[0m\n"
			break
		ff_data.extend(["-map", f"-0:d:{idx}"])
		d_needs = True
		log += f"\033[94m |<D:{idx:2}>| {stream.get('codec_name'):^8} | Removed\033[0m\n"
		d_summary.append("Remove data streams")
	if not d_needs:
		log += "\033[94m .Skip: No data streams to remove.\033[0m\n"
	all_summary = v_summary + a_summary + s_summary + d_summary
	skip_it = False
	if f_skip and not all_summary and file_ext == ".mp4":
		log += "\033[96m .Skip: File is compliant and already processed.\033[0m\n"
		skip_it = True
	else:
		if file_ext != ".mp4":
			all_summary.append("Convert to .mp4")
		log += f"\033[96m .Process: {Path(input_file).name}\033[0m\n"
	cmd = [FFMPEG, "-y"] + external_inputs + ["-i", input_file] + ff_video + ffmpeg_audio_options + ff_subttl + ff_data + ["-metadata", f"comment={Skip_key}", "-f", "mp4"]
	print(log)
	return cmd, skip_it

# =============================
# FFmpeg runner with progress and HW-to-SW fallback
# =============================
def ffmpeg_run(input_file: str, ff_com: List[str], skip_it: bool = False, execu: str = FFMPEG, de_bug: bool = False, task_id: str = "") -> Optional[str]:
	if skip_it or not ff_com:
		return None
	ff_com = _ensure_mp4_faststart_and_hvc1(ff_com, ff_com[-1].rsplit('.', 1)[-1] if '.' in ff_com[-1] else None)
	base_name, _ = os.path.splitext(os.path.basename(input_file))
	out_file = os.path.join(os.path.dirname(input_file), base_name + TMPF_EX)
	ff_com[-1] = out_file
	if de_bug:
		with print_lock:
			print(f"FFmpeg command: {' '.join(ff_com)}")
	try:
		progress_register(task_id, label=base_name)
		metadata, _ = ffprobe_run(input_file, FFPROBE, de_bug)
		video_duration = float(metadata.get("format", {}).get("duration", 0.0) or 0.0)
		progress_set_duration(task_id, video_duration)
		spinner = Spinner(label=f"FFmpeg Processing {base_name}")
		try:
			spinner.start()
		except Exception:
			pass
		process = SP.Popen(ff_com, stdout=SP.PIPE, stderr=SP.STDOUT, text=True, encoding="utf-8", errors="replace")
		err_lines = []
		for line in process.stdout:
			err_lines.append(line)
			progress_update(task_id, line)
		process.communicate(timeout=3600)  # 1-hour timeout
		try:

			spinner.stop()

		except Exception:

			pass
		progress_remove(task_id)
		if process.returncode != 0:
			err = '\n'.join(err_lines[-5:])
			with print_lock:
				print(f"\033[91mFFmpeg failed for {input_file} (return code {process.returncode}):\n{err}\033[0m")
			if "qsv" in err.lower() and any(e in err.lower() for e in ["no device available", "init failed"]):
				with print_lock:
					print(f"\033[93mHW encoding failed for {input_file}. Retrying with SW encoding.\033[0m")
				metadata, _ = ffprobe_run(input_file, FFPROBE, de_bug)
				ff_com_sw, _ = parse_finfo(input_file, metadata, de_bug)
				ff_com_sw = [cmd.replace("hevc_qsv", "libx265") for cmd in ff_com_sw]
				ff_com_sw[-1] = out_file
				if de_bug:
					with print_lock:
						print(f"FFmpeg retry command: {' '.join(ff_com_sw)}")
				spinner = Spinner(label=f"FFmpeg SW Retry {base_name}")
				spinner.start()
				progress_register(task_id, label=base_name)
				progress_set_duration(task_id, video_duration)
				process_sw = SP.Popen(ff_com_sw, stdout=SP.PIPE, stderr=SP.STDOUT, text=True, encoding="utf-8", errors="replace")
				err_lines_sw = []
				for line in process_sw.stdout:
					err_lines_sw.append(line)
					progress_update(task_id, line)
				process_sw.communicate(timeout=3600)
				try:
					spinner.stop()
				except Exception:
					pass
				progress_remove(task_id)
				if process_sw.returncode != 0:
					err_sw = '\n'.join(err_lines_sw[-5:])
					with print_lock:
						print(f"\033[91mFFmpeg SW retry failed for {input_file}:\n{err_sw}\033[0m")
					if os.path.exists(out_file):
						os.remove(out_file)
					return None
				return out_file
			if os.path.exists(out_file):
				os.remove(out_file)
			return None
		return out_file
	except SP.TimeoutExpired:
		if 'spinner' in locals():
			try:
				spinner.stop()
			except Exception:
				pass
		progress_remove(task_id)
		with print_lock:
			print(f"\033[91mFFmpeg timed out for {input_file}\033[0m")
		if os.path.exists(out_file):
			os.remove(out_file)
		return None
	except Exception as e:
		if 'spinner' in locals():
			try:
				spinner.stop()
			except Exception:
				pass
		progress_remove(task_id)
		with print_lock:
			print(f"\033[91mFFmpeg run error for {input_file}: {e}\033[0m")
			if de_bug:
				print(traceback.format_exc())
		if os.path.exists(out_file):
			os.remove(out_file)
		return None

# =============================
# Clean up after transcoding
# =============================
def clean_up(input_file: str, output_file: str, de_bug: bool = False) -> int:
	if not os.path.exists(input_file):
		print(f"Input file '{input_file}' does not exist.")
		return -1
	if not os.path.exists(output_file):
		print(f"Output file '{output_file}' does not exist.")
		return -1
	input_file_size = os.path.getsize(input_file)
	output_file_size = os.path.getsize(output_file)
	if output_file_size <= 100:
		print(f"Output file '{output_file}' is too small. Deleting it and keeping original.")
		os.remove(output_file)
		return 0
	size_diff = output_file_size - input_file_size
	ratio = round(100 * (size_diff / input_file_size), 2) if input_file_size > 0 else float('inf') if output_file_size > 0 else 0
	extra = "+Bigger" if ratio > 0 else ("=Same" if size_diff == 0 else "-Smaller")
	msj_ = f"   .Size Was: {hm_sz(input_file_size)} Is: {hm_sz(output_file_size)} {extra}: {hm_sz(abs(size_diff))} {ratio}% "
	if ratio > MAX_SIZE_INFLATE_PCT and AUTO_SIZE_GUARD and not FORCE_BIGGER:
		print(f"\n  WARNING: New file over {ratio}% larger than the original.\n{msj_}")
		if input("   Proceed to replace the original file? (y/n): ").lower() != 'y':
			print("   Operation skipped. Keeping original and deleting new file.")
			os.remove(output_file)
			return 0
	print(msj_.ljust(80))
	random_chars = ''.join(random.sample(string.ascii_letters + string.digits, 4))
	temp_file = input_file + random_chars + "_Delete_.old"
	try:
		os.rename(input_file, temp_file)
		shutil.move(output_file, input_file)
		if de_bug:
			if input(f" Are you sure you want to delete {temp_file}? (y/n): ").lower() != "y":
				print("   Not Deleted.")
				shutil.move(temp_file, input_file)
				return 0
			print(f"   File {temp_file} Deleted.")
		os.remove(temp_file)
	except PermissionError:
		p = Path(input_file)
		tagged_name = f"{p.stem}_HEVC.mp4"
		tagged_path = str(p.with_name(tagged_name))
		print(f"\n  WARNING: Original file '{p.name}' is locked or in use.")
		print(f" - Leaving original untouched and saving new file as '{tagged_name}'.")
		shutil.move(output_file, tagged_path)
		return size_diff
	except Exception as e:
		print(f" An unexpected error occurred in clean_up: {e}")
		if de_bug:
			print(traceback.format_exc())
		if os.path.exists(temp_file):
			try:
				shutil.move(temp_file, input_file)
			except Exception as e_restore:
				print(f" CRITICAL: Failed to restore original file from '{temp_file}': {e_restore}")
		return -1
	return size_diff

# =============================
# Auxiliary functions
# =============================
def short_ver(input_file: str, start: str = "00:00:00", duration: str = "00:05:00", execu: str = FFMPEG, de_bug: bool = False) -> Optional[str]:
	base_name, _ = os.path.splitext(os.path.basename(input_file))
	out_file = base_name + "_short.mp4"
	cmd = [execu, "-ss", start, "-i", input_file, "-t", duration, "-c:v", "copy", "-c:a", "copy", "-y", out_file]
	return out_file if run_ffm(cmd, de_bug) else None

def video_diff(file1: str, file2: str, de_bug: bool = False) -> Optional[str]:
	base_name, _ = os.path.splitext(os.path.basename(file1))
	out_file = base_name + "_diff.mp4"
	cmd = [FFMPEG, "-i", file1, "-i", file2, "-filter_complex", "blend=all_mode=difference", "-c:v", "libx265", "-preset", "faster", "-c:a", "copy", "-y", out_file]
	return out_file if run_ffm(cmd, de_bug) else None

def speed_up(input_file: str, factor: float = 4.0, de_bug: bool = False) -> Optional[str]:
	base_name, _ = os.path.splitext(os.path.basename(input_file))
	out_file = base_name + f"_speed{factor:g}x.mp4"
	remaining = factor
	atempos = []
	while remaining > 2.0:
		atempos.append("atempo=2.0")
		remaining /= 2.0
	atempos.append(f"atempo={remaining:.6g}")
	atempo_filter = ",".join(atempos)
	vf_expr = f"setpts=PTS/{factor:.6g}"
	cmd = [FFMPEG, "-i", input_file, "-filter_complex", f"[0:v]{vf_expr}[v];[0:a]{atempo_filter}[a]", "-map", "[v]", "-map", "[a]", "-y", out_file]
	return out_file if run_ffm(cmd, de_bug) else None

def matrix_it(input_file: str, columns: int = 3, rows: int = 3, ext: str = ".png", de_bug: bool = False) -> bool:
	base_name, _ = os.path.splitext(os.path.basename(input_file))
	out_file = base_name + "_matrix" + ext
	select_expr = "select='not(mod(n,300))'"
	tile_expr = f"tile={columns}x{rows}:padding=5:margin=5"
	vf_filter = f"[0:v]{select_expr},{tile_expr}"
	cmd = [FFMPEG, "-i", input_file, "-frames:v", "1", "-vf", vf_filter, "-vsync", "vfr", "-y", out_file]
	return run_ffm(cmd, de_bug)

# =============================
# FFmpeg low-level runner
# =============================
def run_ffm(cmd: List[str], de_bug: bool = False) -> bool:
	if de_bug:
		with print_lock:
			cmd_str = ' '.join((('"' + arg + '"') if (' ' in arg) else arg) for arg in cmd)
			print(f"Running FFmpeg: {cmd_str}")
	try:
		process = SP.Popen(cmd, stdout=SP.PIPE, stderr=SP.STDOUT, text=True, encoding="utf-8", errors="replace")
		err_lines = []
		for line in process.stdout:
			err_lines.append(line)
		process.communicate(timeout=3600)
		if process.returncode != 0:
			if de_bug:
				with print_lock:
					print(f"FFmpeg failed with return code {process.returncode}:\n{''.join(err_lines[-5:])}")
			return False
		return True
	except SP.TimeoutExpired:
		with print_lock:
			print(f"FFmpeg timed out: {' '.join(cmd)}")
		return False
	except Exception as e:
		if de_bug:
			with print_lock:
				print(f"FFmpeg run error: {e}")
				print(traceback.format_exc())
		return False

# =============================
# Main entry point
# =============================
if __name__ == "__main__":
	de_bug = False
	if len(sys.argv) > 1:
		input_file = sys.argv[1]
		if len(sys.argv) > 2:
			de_bug = "--debug" in sys.argv
		if not os.path.isfile(input_file):
			with print_lock:
				print(f"Error: File not found at '{input_file}'")
			sys.exit(1)
		try:
			metadata, is_corrupted = ffprobe_run(input_file, FFPROBE, de_bug, check_corruption=True)
			if is_corrupted:
				with print_lock:
					print(f"\033[93mError: File '{input_file}' is corrupted (invalid data detected). Copying to {MOVE_TO_EXCEPT}.\033[0m")
				copy_move(input_file, MOVE_TO_EXCEPT, move=False)
				sys.exit(1)
			ff_run_cmd, skip_it = parse_finfo(input_file, metadata, de_bug)
			if not skip_it:
				out_file = ffmpeg_run(input_file, ff_run_cmd, skip_it, FFMPEG, de_bug, "T1")
				if out_file:
					size_diff = clean_up(input_file, out_file, de_bug)
					with print_lock:
						print(f"\nSuccess! Output file: {out_file}")
						if size_diff >= 0:
							print(f"Saved: {hm_sz(size_diff)}")
						else:
							print("No space saved.")
				else:
					with print_lock:
						print(f"FFmpeg failed for: {input_file}, copying to {MOVE_TO_EXCEPT}")
					copy_move(input_file, MOVE_TO_EXCEPT, move=False)
			else:
				with print_lock:
					print("\nNothing to do. Skipping.")
		except Exception as e:
			with print_lock:
				print(f"An error occurred: {e}")
				if de_bug:
					print(traceback.format_exc())
			sys.exit(1)
	else:
		with print_lock:
			print("Usage: python FFMpeg.py <path_to_video_file> [--debug]")
		sys.exit(1)
