# -*- coding: utf-8 -*-
import os
import re
import sys
import json
import time
import datetime as TM
import subprocess as SP
import stat
import random
import string
import shutil

from collections import defaultdict
from dataclasses import dataclass
from typing	import List, Dict, Tuple, Any, Optional

# External references from your code
from Yaml		import *
from My_Utils	import *

# Paths to FFmpeg/FFprobe executables
ffmpg_bin = r"C:\Program Files\ffmpeg\bin"
ffmpeg = os.path.join(ffmpg_bin, "ffmpeg.exe")
ffprob = os.path.join(ffmpg_bin, "ffprobe.exe")

try:
	if not os.path.isfile(ffmpeg):
		raise FileNotFoundError(f"FFmpeg not found at '{ffmpeg}'.")
	if not os.path.isfile(ffprob):
		raise FileNotFoundError(f"FFprobe not found at '{ffprob}'.")
except FileNotFoundError as e:
	print(f"Error: {e}")

glb_vidolen = 0

###############################################################################
#                               FFPROBE
###############################################################################
@perf_monitor
def ffprobe_run(
	input_file: str,
	execu: str = ffprob,
	de_bug: bool = False
) -> dict:
	"""
	Runs ffprobe to extract media information from a file and returns JSON data.
	Raises exceptions on errors or if no valid JSON is returned.
	"""
	if not input_file:
		raise FileNotFoundError("No input_file provided.")
	cmd = [
		execu, "-i", input_file,
		"-hide_banner",
		"-analyzeduration", "100000000",
		"-probesize", "50000000",
		"-v", "fatal",
		"-show_programs", "-show_format", "-show_streams",
		"-show_error", "-show_data", "-show_private_data",
		"-of", "json"
	]
	try:
		out = SP.run(cmd, stdout=SP.PIPE, stderr=SP.PIPE, check=True)
		return json.loads(out.stdout.decode("utf-8"))
	except (FileNotFoundError, SP.CalledProcessError, SP.TimeoutExpired, json.JSONDecodeError) as e:
		raise Exception(f"ffprobe_run error: {e} in file: {input_file}") from e

###############################################################################
#                             RUN FFMPEG
###############################################################################
regex_dict = {
	"bitrate": re.compile(r"bitrate=\s*([0-9\.]+)"),
	"frame":   re.compile(r"frame=\s*([0-9]+)"),
	"speed":   re.compile(r"speed=\s*([0-9\.]+)"),
	"size":    re.compile(r"size=\s*([0-9]+)"),
	"time":    re.compile(r"time=([0-9:.]+)"),
	"fps":     re.compile(r"fps=\s*([0-9]+)")
}

def extract_progress_data(line_to: str) -> dict:
	regx_val = {}
	for key, rx in regex_dict.items():
		match = rx.search(line_to)
		if match: regx_val[key] = match.group(1)
	return regx_val

def calculate_eta(regx_val: dict, sp_float: float) -> Tuple[str, float]:
	global glb_vidolen
	if glb_vidolen <= 0: return "--:--:--", 0.0
	try:
		time_str = regx_val.get("time", "00:00:00.00")
		t_parts = list(map(float, time_str.strip().split(':')))
		while len(t_parts) < 3: t_parts.insert(0, 0.0)
		h, m, s_float = t_parts
		a_sec = h * 3600 + m * 60 + s_float
		dif = max(glb_vidolen - a_sec, 0.0)
		eta_seconds = dif / sp_float if sp_float > 0 else 9999.0
		hours, remainder = divmod(round(eta_seconds), 3600)
		minutes, seconds = divmod(remainder, 60)
		eta_str = f"{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}"
		return eta_str, a_sec
	except Exception: return "--:--:--", 0.0
##==============-------------------  End   -------------------==============##

def show_progrs(line_to: str, sy: str, de_bug: bool = False) -> bool:
	line_to = line_to.strip()
	if not line_to or de_bug:
		if de_bug: print(line_to)
		return True
	if "cpb:" in line_to and "N/A" in line_to: return True
	if all(x in line_to for x in ["fps=", "speed=", "size="]):
		regx_val = extract_progress_data(line_to)
		try:
			fp_int, sp_float = int(float(regx_val.get("fps", 0))), float(regx_val.get("speed", 0))
			eta_str, a_sec = calculate_eta(regx_val, sp_float)
			size_val = hm_sz(int(regx_val.get('size', '0')) * 1024, 'bytes')
			bitrate_val = hm_sz(float(regx_val.get('bitrate', '0')) * 1000)
			percent = 100 * a_sec / glb_vidolen if glb_vidolen > 0 else 0
			disp_str = (f"    | {sy} |Size: {size_val:>10}|Frames: {int(regx_val.get('frame', 0)):>7}|Fps: {fp_int:>4}"
						f"|BitRate: {bitrate_val:>10}|Speed: {sp_float:>5.2f}x|ETA: {eta_str:>9}|{percent:5.1f}% |")
			sys.stderr.write('\r' + disp_str + " " * 5); sys.stderr.flush()
		except Exception as e: print(f"show_progrs exception: {e} in line: {line_to}")
	elif any(x in line_to for x in ["muxing overhead:", "global headers:"]):
		sys.stderr.write('\r' + ' ' * 120 + '\r'); sys.stderr.flush()
		print(f"   .Done: {line_to[31:]} |  ")
		return False
	return True
##==============-------------------  End   -------------------==============##

@perf_monitor
def run_ffm(args: List[str], de_bug: bool = False) -> bool:
	with SP.Popen(args, stdout=SP.PIPE, stderr=SP.STDOUT, text=True, encoding="utf-8", errors="replace") as process:
		for line in process.stdout:
			if not show_progrs(line, "|/-o+\\"[int(time.time()*2) % 5], de_bug): break
		process.communicate()
	if process.returncode != 0: print (f"\nFFmpeg failed with return code {process.returncode}")
	return process.returncode == 0
##==============-------------------  End   -------------------==============##

@perf_monitor
def ffmpeg_run(input_file: str, ff_com: List[str], skip_it: bool, execu: str = ffmpeg, de_bug: bool = False) -> Optional[str]:
	if not input_file or skip_it: return None
	file_name, _ = os.path.splitext(os.path.basename(input_file))
	# --- CHANGE IS HERE ---
	# The next two lines are reverted to the original logic
	out_file = "_" + stmpd_rad_str(7, file_name[0:25])
	out_file = os.path.normpath(re.sub(r"[^\w\s_-]+", "", out_file).strip().replace(" ", "_") + TmpF_Ex)

	ff_head = [execu, "-thread_queue_size", "24", "-i", input_file, "-hide_banner"]

	# The output file in this list is now just the filename `out_file`, not the full path
	ff_tail = ["-metadata", f"title={file_name}", "-metadata", f"comment={Skip_key}", "-metadata", "author=Encoded by GeoHab", "-movflags", "+faststart", "-fflags", "+genpts", "-y", out_file]

	full_cmd = ff_head + ff_com + ff_tail
	if de_bug:
		print("\n--- [DEBUG] FFmpeg Command Sent ---")
		print(' '.join(f'"{arg}"' if ' ' in arg else arg for arg in full_cmd))
		print("-----------------------------------\n")

	return out_file if run_ffm(full_cmd, de_bug=de_bug) else None
##==============-------------------  End   -------------------==============##

###############################################################################
#                              STREAM PARSERS
###############################################################################
class VideoContext:
	def __init__(self, input_file: str, **kwargs):
		self.input_file = input_file
		self.vid_width = kwargs.get("vid_width", 0)
		self.vid_height = kwargs.get("vid_height", 0)
		self.estimated_video_bitrate = kwargs.get("estimated_video_bitrate", 0)
		self.duration = kwargs.get("duration", 0.0)
		self.file_size = kwargs.get("file_size", 0)

def get_reencode_settings_based_on_source(
	codec_name: str, width: int, height: int, bitrate_bps: Optional[int], is_10bit: bool, allow_hw: bool = True
) -> Tuple[bool, List[str], str, Optional[str], int]:

	# 1. --- Scaling Logic ---
	# Simplified the condition for clarity.
	needs_scaling = False
	target_height = height
	if width > 1920 or height > 1080:
		needs_scaling = True
		target_height = 1080

	# 2. --- Corrected Bitrate & Quality Logic ---
	# This is the ideal bitrate for the *target resolution*. This value is now constant.
	ideal_bitrate_for_res = 3456 * target_height

	target_bitrate = ideal_bitrate_for_res

	needs_reencode = False
	reasons = []
	is_source_hevc = codec_name.lower() == "hevc"

	if not is_source_hevc:
		needs_reencode = True
		reasons.append("HEVC")
		if bitrate_bps:
			# If the source is H.264, we aim for HEVC's efficiency (approx. 65% of the size).
			# We will target this more efficient bitrate, but not exceed the ideal for the resolution.
			efficient_bitrate = int(bitrate_bps * 0.65)
			target_bitrate = min(efficient_bitrate, ideal_bitrate_for_res)
			reasons.append("Better Codec")

	if bitrate_bps and bitrate_bps > int(ideal_bitrate_for_res * 1.2):
		needs_reencode = True
		if "High Bitrate" not in reasons:
			reasons.append("High Bitrate")

	# 3. --- VBV (Compatibility) & CRF (Quality) Calculations ---
	# VBV ensures the bitrate doesn't spike too high for device compatibility.
	# It's now based on our calculated `target_bitrate`.
	vbv_maxrate = int(target_bitrate * 1.1) # Allow 10% headroom for spikes
	vbv_bufsize = int(vbv_maxrate * 3)   # A 3x buffer is safe and allows for quality fluctuations

	# Calculate a CRF value based on how the source bitrate compares to the ideal.
	# This value will be used for BOTH SW and HW encoding to ensure consistent quality.
	ratio = (bitrate_bps / ideal_bitrate_for_res) if bitrate_bps else 1.0
	crf = 22
	if ratio   > 2.0: crf = 28
	elif ratio > 1.5: crf = 26
	elif ratio > 1.2: crf = 24

    # This logic remains: it improves quality when converting from an 8-bit source.
	if not is_10bit: crf -= 2

	# 4. --- Unified Encoder Settings ---
	scaler = f"scale=-1:{target_height}" if needs_scaling else None
	force_sw = needs_scaling or not allow_hw

	if not force_sw:
		# HW ENCODING (Intel QSV)
		# We now use ICQ (Intelligent Constant Quality) mode, which is QSV's equivalent of CRF.
		# This makes the HW output quality consistent with the SW output.
		encoder_flags = ["hevc_qsv", "-preset", "slow", "-look_ahead", "1",
						 "-b:v",     f"{target_bitrate // 1000}k",
						 "-maxrate", f"{vbv_maxrate // 1000}k",
						 "-bufsize", f"{vbv_bufsize // 1000}k",
						 "-pix_fmt", "p010le"] # Always output 10-bit
	else:
		# SW ENCODING (libx265)
		# This path remains CRF-based but is now cleaner.
		x265_params = [f"preset=slow:tune=grain:crf={crf}",
					   f"vbv-maxrate={vbv_maxrate // 1000}",
					   f"vbv-bufsize={vbv_bufsize // 1000}"]
		encoder_flags = ["libx265", "-x265-params", ":".join(x265_params),
						 "-pix_fmt", "yuv420p10le"] # Always output 10-bit

	status_text = f"=> Re-encode ({'SW' if force_sw else 'HW'})"
	if reasons: status_text += f" [{', '.join(reasons)}]"
	if needs_scaling: status_text += " |Scaling"

	return needs_reencode, encoder_flags, status_text, scaler, ideal_bitrate_for_res

###==============-------------------  End   -------------------==============###

def parse_video(streams_in: List[Dict[str, Any]], context: VideoContext, de_bug: bool = False) -> Tuple[List[str], bool]:
	ff_video = []; skip_all = True
	for idx, stream in enumerate(streams_in):
		codec, pix_fmt, handler = stream.get("codec_name", ""), stream.get("pix_fmt", ""), stream.get("tags", {}).get("handler_name", "")
		if codec in ("mjpeg", "png"):
			print(f"\033[91m    |<V:{idx:2}>|{codec:^8}| Ignored\033[0m"); continue
		width, height = int(stream.get("width", 0)), int(stream.get("height", 0))
		is_10bit = "10" in pix_fmt or "p010" in pix_fmt
		bitrate_bps = context.estimated_video_bitrate

		# Get the initial assessment from the helper function
		needs_re, flags, status, scaler, ideal = get_reencode_settings_based_on_source(codec, width, height, bitrate_bps, is_10bit)

		ff_video.extend(["-map", f"0:{stream['index']}"])
		handler_needs_fix = handler.lower() != "videohandler"

		if not needs_re and not handler_needs_fix:
			ff_video.extend([f"-c:v:{idx}", "copy"])
			status = "=> Copy (Compliant)"  # Overwrite status with the correct message
		else:
			# An action is required, so the file will not be skipped.
			skip_all = False

			# Determine the base action and status (re-encode or copy).
			if needs_re:
				ff_video.extend([f"-c:v:{idx}"] + flags)
				if scaler: ff_video.extend([f"-filter:v:{idx}", scaler])
				# The 'status' from the helper function is already correct for a re-encode.
			else:
				# If not re-encoding, the action must be a copy.
				ff_video.extend([f"-c:v:{idx}", "copy"])
				status = "=> Copy" # Set a clean, accurate base status.

			# Separately, check if the handler needs to be fixed and update the command and status.
			if handler_needs_fix:
				ff_video.extend([f"-metadata:s:v:{idx}", "handler_name=VideoHandler"])
				# Append the handler fix reason to the status message.
				status += " |Fix Handler"

		msj = f"|<V:{idx:2}>|{codec:^8}|{width}x{height}|{'10-bit' if is_10bit else '8-bit'}| Bitrate: {hm_sz(bitrate_bps)} vs Ideal: {hm_sz(ideal)}| {status}"
		print(f"\033[91m    {msj} \033[0m")

	if skip_all: print("    .Skip: Video streams are already optimal.")
	return ff_video, skip_all
##==============-------------------  End   -------------------==============##

def parse_audio(streams_in: List[Dict[str, Any]], context: VideoContext, de_bug: bool = False) -> Tuple[List[str], bool]:
	if not streams_in: return [], True
	ffmpeg_audio_options, a_skip, output_audio_idx = [], True, 0
	best_candidate, best_score = None, float('-inf')
	for audio_strm in streams_in:
		score = 0; tags = audio_strm.get("tags", {}); lang = tags.get("language", "und"); title = tags.get("title", "").lower(); disposition = audio_strm.get("disposition", {})
		if lang == Default_lng: score += 100
		if disposition.get("default", 0): score += 50
		score += int(audio_strm.get("channels", 0)) * 10
		if "commentary" in title: score -= 1000
		if score > best_score: best_score = score; best_candidate = audio_strm
	disposition_needs_change = any((s['index'] == best_candidate['index'] and not s.get("disposition", {}).get("default", 0)) or (s['index'] != best_candidate['index'] and s.get("disposition", {}).get("default", 0)) for s in streams_in if best_candidate)
	for audio_strm in streams_in:
		input_idx = audio_strm['index']; codec_name = audio_strm.get("codec_name", "u").lower(); lang = audio_strm.get("tags", {}).get("language", "und"); channels = int(audio_strm.get("channels", 0)); bit_rate = int(audio_strm.get("bit_rate", 0)); disposition = audio_strm.get("disposition", {})
		is_compliant = codec_name in ["eac3", "aac"] and channels <= 6
		handler_is_correct = audio_strm.get("tags", {}).get("handler_name") == "SoundHandler"
		if not is_compliant or not handler_is_correct: a_skip = False
		is_best = (best_candidate and input_idx == best_candidate['index']); stream_opts = ["-map", f"0:{input_idx}"]; log_action = ""
		if is_compliant: stream_opts.extend([f"-c:a:{output_audio_idx}", "copy"]); log_action = "=> Copy (Compliant)"
		else:
			if channels >= 6: stream_opts.extend([f"-c:a:{output_audio_idx}", "eac3", f"-b:a:{output_audio_idx}", "640k"]); log_action = "=> Re-encode to E-AC3 640k"
			else: stream_opts.extend([f"-c:a:{output_audio_idx}", "aac", f"-q:a:{output_audio_idx}", "2", f"-ac:a:{output_audio_idx}", "2"]); log_action = "=> Re-encode to AAC Stereo"
		if is_best:
			stream_opts.extend([f"-disposition:a:{output_audio_idx}", "default"])
			if not disposition.get("default", 0): log_action += " | Set Default"
		else:
			stream_opts.extend([f"-disposition:a:{output_audio_idx}", "0"])
			if disposition.get("default", 0): log_action += " | Clear Default"
		if not handler_is_correct: stream_opts.extend([f"-metadata:s:a:{output_audio_idx}", "handler_name=SoundHandler"]); log_action += " |Fix Handler"
		msg = f"|<A:{audio_strm['index']:2}>|{audio_strm.get('codec_name', 'u'):^8}|{lang:<3}|Br:{hm_sz(int( audio_strm.get('bit_rate', 0))):<9}|Ch:{int(audio_strm.get('channels', 0))}| {log_action}"
		if is_best and not disposition.get("default", 0): msg += "|Selected as Default"
		print(f"\033[92m    {msg}\033[0m")
		ffmpeg_audio_options.extend(stream_opts)
		output_audio_idx += 1
	final_skip = a_skip and not disposition_needs_change
	if final_skip: print("    .Skip: Audio streams are already optimal.")
	return ffmpeg_audio_options, final_skip
##==============-------------------  End   -------------------==============##

def add_subtl_from_file(input_file: str) -> Tuple[List[str], bool]:
	base_name, _ = os.path.splitext(input_file)
	for ext in [".srt", ".ass"]:
		test_file = base_name + ext
		if os.path.isfile(test_file):
			print(f"    .Found {ext} external subtitle: {os.path.basename(test_file)} FIX ffmpeg comand order ")
		#	return ["-i", test_file, "-map", "1:0", "-c:s:0", "mov_text", "-metadata:s:s:0", f"language={Default_lng}", "-disposition:s:0", "default"], False
		else :
			print(f"    .No external {ext} subtitle file found.")
	return [], True
##==============-------------------  End   -------------------==============##

def parse_subtl(sub_streams: List[Dict[str, Any]], context: VideoContext, de_bug: bool = False) -> Tuple[List[str], bool]:
	# If there are no embedded subtitle streams, check for external files.
	if not sub_streams:
		print("    .No embedded subtitles found, checking for external file.")
		return add_subtl_from_file(context.input_file)

	TEXT_BASED_CODECS = ("subrip", "ass", "mov_text")
	# 1. --- Scoring and Best Stream Selection (from your original logic) ---
	best_streams = {lang: {'score': -1, 'stream': None} for lang in Keep_langua}

	def _score_subtitle(stream: Dict[str, Any]) -> int:
		"""Calculates a score for a subtitle stream to determine its quality."""
		score = 100
		tags, disposition = stream.get("tags", {}), stream.get("disposition", {})
		title = tags.get("title", "").lower()
		if disposition.get("forced", 0) or "forced" in title: score -= 1000 # Heavily penalize 'forced' subs
		if "sdh" in title: score += 100 # Prefer SDH
		if disposition.get("default", 0): score += 50
		# Use bitrate and frame count as a proxy for quality/completeness
		score += int(stream.get("bit_rate", 0))
		score += int(stream.get("nb_frames", 0) or stream.get("nb_read_packets", 0))
		return score

	# Find the best subtitle stream for each desired language
	for stream in sub_streams:
		lang = stream.get("tags", {}).get("language", "und")
		if lang in Keep_langua and stream.get("codec_name") in TEXT_BASED_CODECS:
			score = _score_subtitle(stream)
			if score > best_streams[lang]['score']:
				best_streams[lang] = {'score': score, 'stream': stream}

	# Collect the winning streams
	text_streams_to_keep = [data['stream'] for data in best_streams.values() if data['stream']]

	if not text_streams_to_keep:
		print("    .Skip: No compatible text-based subtitles found.")
		return [], True

	# 2. --- Compliance and Disposition Check (a more robust version) ---
	best_sub_candidate = next((s for s in text_streams_to_keep if s.get("tags", {}).get("language") == Default_lng), text_streams_to_keep[0])

	# Check if the 'default' flag needs to be moved
	needs_disposition_change = any(
		(s['index'] == best_sub_candidate['index'] and not s.get("disposition", {}).get("default", 0)) or
		(s['index'] != best_sub_candidate['index'] and s.get("disposition", {}).get("default", 0))
		for s in text_streams_to_keep
	)
	# Check if all codecs are 'mov_text'
	all_codecs_compliant = all(s.get('codec_name') == 'mov_text' for s in text_streams_to_keep)
	# Check if all handlers are correct
	all_handlers_correct = all(s.get("tags", {}).get("handler_name") == "SubtitleHandler" for s in text_streams_to_keep)

	# The full compliance check: only skip if EVERYTHING is perfect.
	if all_codecs_compliant and not needs_disposition_change and all_handlers_correct:
		print("    .Skip: Subtitle streams are already fully compliant.")
		return [], True

	ff_subttl = []
	for i, stream in enumerate(text_streams_to_keep):
		is_best = (stream['index'] == best_sub_candidate['index'])
		handler_is_correct = stream.get("tags", {}).get("handler_name") == "SubtitleHandler"
		codec_is_compliant = stream.get('codec_name') == 'mov_text'

		stream_opts = ["-map", f"0:{stream['index']}"]
		log_status = []

		# Decide whether to copy or convert the codec
		if codec_is_compliant and not needs_disposition_change:
			stream_opts.extend([f"-c:s:{i}", "copy"])
			log_status.append("Copy")
		else:
			stream_opts.extend([f"-c:s:{i}", "mov_text"])
			log_status.append("Re-encode")

		# Set the 'default' flag
		if is_best:
			stream_opts.extend([f"-disposition:s:{i}", "default"])
			if not stream.get("disposition", {}).get("default", 0):
				log_status.append("Set Default")
		else:
			stream_opts.extend([f"-disposition:s:{i}", "0"])
			if stream.get("disposition", {}).get("default", 0):
				log_status.append("Clear Default")

		# Set the handler name
		if not handler_is_correct:
			stream_opts.extend([f"-metadata:s:s:{i}", "handler_name=SubtitleHandler"])
			log_status.append("Fix Handler")

		ff_subttl.extend(stream_opts)

		# Detailed print status
		lang = stream.get('tags', {}).get('language', 'und')
		score = best_streams.get(lang, {}).get('score', 0)
		print(f"\033[94m    |<S:{stream['index']:2}>|{stream.get('codec_name'):^8}|{lang:3}|Score:{score:<5}{'|'.join(log_status)}\033[0m")

	return ff_subttl, False

##==============-------------------  End   -------------------==============##

def parse_extrd(streams_in: List[Dict[str, Any]], context: VideoContext, de_bug: bool = False) -> Tuple[List[str], bool]:
	ff_data, skip_all = [], True
	for idx, stream in enumerate(streams_in):
		if stream.get("tags", {}).get("handler_name") == "SubtitleHandler": return [], True
		ff_data.extend(["-map", f"-0:d:{idx}"])
	return ff_data, skip_all
##==============-------------------  End   -------------------==============##

@perf_monitor
def parse_finfo(input_file: str, metadata: Dict[str, Any], de_bug: bool = False) -> Tuple[List[str], bool]:
	_format, _streams = metadata.get("format", {}), metadata.get("streams", [])
	if not _format or not _streams: raise ValueError("Invalid metadata")

	size, duration = int(_format.get("size", 0)), float(_format.get("duration", 0.0))
	bitrate, title = int(_format.get("bit_rate", 0)),   _format.get("tags", {}).get("title", "No_title")
	f_comment = _format.get("tags", {}).get("comment", "No_Comment")
	global glb_vidolen; glb_vidolen = duration

	streams_by_type = defaultdict(list)
	for s in _streams: streams_by_type[s.get("codec_type", "?")].append(s)

	sc = {k: len(v) for k, v in streams_by_type.items()}
	msj  = f"    |=Title|{title}|\n"
	msj += f"    |<FRMT>|Size: {hm_sz(size)}|Bitrate: {hm_sz(bitrate)}|Length: {hm_time(duration)}|Streams: V:{sc.get('video', 0)} A:{sc.get('audio', 0)} S:{sc.get('subtitle', 0)}"
	if de_bug: msj += f"\n  ! Debug Mode ! Sk: {Skip_key}"
	print (f"\033[96m{msj}\033[0m")

	# Check for the absence of an audio stream and print a warning.
	if 'audio' not in streams_by_type:
		print("\033[93m    !Warning: No audio stream found in this file.\033[0m")

	f_skip = f_comment == Skip_key
	if f_skip: print("    .Skip Format")

	primary_video = streams_by_type.get("video", [{}])[0]
	context = VideoContext(
		input_file	=input_file,
		file_size	=size,
		duration	=duration,
		vid_width	=primary_video.get("width", 0),
		vid_height	=primary_video.get("height", 0)
	)

	total_file_br = (size * 8) / duration if size > 0 and duration > 0 else 0
	total_audio_br = sum(int(s.get("bit_rate", "0")) for s in streams_by_type.get("audio", []) if s.get("bit_rate", "0").isdigit())
	if total_file_br > total_audio_br > 0: context.estimated_video_bitrate = int((total_file_br - total_audio_br) * 0.98)
	elif total_file_br > 0: context.estimated_video_bitrate = int(total_file_br * 0.80)

	final_cmd, final_skip = [], f_skip

	stream_map = [
		("V_cmd", "video",		parse_video),
		("A_cmd", "audio",		parse_audio),
		("S_cmd", "subtitle",	parse_subtl),
		("D_cmd", "data", 		parse_extrd),
	]

	for label, stream_type, func in stream_map:
		streams		= streams_by_type.get(stream_type, [])
		cmd, skip	= func(streams, context, de_bug)
		final_cmd.extend(cmd); final_skip = final_skip and skip
		if de_bug and cmd: print(f"    {label}= {cmd}")

	print("    .Skip: All." if final_skip else "    .Process: File.")
	return final_cmd, final_skip
##==============-------------------  End   -------------------==============##

@perf_monitor
def clean_up(input_file: str, output_file: str, skip_it: bool = False, de_bug: bool = False) -> int:
	if skip_it: return 0
	temp_file = ""
	try:
		if not os.path.exists(input_file):
			print(f"Input file '{input_file}' does not exist."); return -1
		input_file_size = os.path.getsize(input_file); os.chmod(input_file, stat.S_IWRITE)
		if not os.path.exists(output_file):
			print(f"Output file '{output_file}' does not exist."); return -1
		output_file_size = os.path.getsize(output_file); os.chmod(output_file, stat.S_IWRITE)
		if output_file_size <= 100:
			print(f"Output file '{output_file}' is too small. Deleting it and keeping original.")
			os.remove(output_file); return 0
		size_diff = output_file_size - input_file_size
		ratio = round(100 * (size_diff / input_file_size), 2) if input_file_size > 0 else float('inf') if output_file_size > 0 else 0
		extra = "+Bigger" if ratio > 0 else ("=Same" if size_diff == 0 else "-Lost")
		msj_ = f"   .Size Was: {hm_sz(input_file_size)} Is: {hm_sz(output_file_size)} {extra}: {hm_sz(abs(size_diff))} {ratio}% "
		if ratio > 5 :
			print(f"\n WARNING: New file over {ratio}% larger than the original.\n{msj_}")
			if input("Proceed to replace the original file? (y/n): ").lower() != 'y':
				print("Operation skipped. Keeping original and deleting new file.")
				os.remove(output_file); return 0
		final_output_file = input_file if input_file.endswith('.mp4') else input_file.rsplit('.', 1)[0] + '.mp4'
		random_chars = ''.join(random.sample(string.ascii_letters + string.digits, 4))
		temp_file = input_file + random_chars + "_Delete_.old"
		print(msj_.ljust(80))
		os.rename(input_file, temp_file)
		shutil.move(output_file, final_output_file)
		if de_bug:
			if input(f" Are you sure you want to delete {temp_file}? (y/n): ").lower() != "y":
				print("   Not Deleted."); shutil.move(temp_file, input_file); return 0
			print(f"   File {temp_file} Deleted.")
		os.remove(temp_file)
		return size_diff
	except Exception as e:
		print(f" An error occurred: {e}")
		if temp_file and os.path.exists(temp_file): shutil.move(temp_file, input_file)
	return -1
##==============-------------------  End   -------------------==============##

@perf_monitor
def matrix_it(input_file: str, execu: str = ffmpeg, ext: str = ".png", columns: int = 3, rows: int = 3) -> bool:
	base_name, _ = os.path.splitext(os.path.basename(input_file))
	out_file = base_name + "_matrix" + ext
	select_expr = "select='not(mod(n,300))'"
	tile_expr = f"tile={columns}x{rows}:padding=5:margin=5"
	vf_filter   = f"[0:v]{select_expr},{tile_expr}"
	cmd = [execu, "-i", input_file, "-frames:v", "1", "-vf", vf_filter, "-vsync", "vfr", "-y", out_file]
	return run_ffm(cmd)

@perf_monitor
def speed_up(input_file: str, factor: float = 4.0, de_bug: bool = False) -> Optional[str]:
	base_name, _ = os.path.splitext(os.path.basename(input_file))
	out_file = base_name + f"_speed{int(factor)}x.mp4"
	atempo_filter = f"atempo={factor}" if factor <= 2.0 else "atempo=2.0,atempo=2.0"
	vf_expr = f"setpts=PTS/{factor}"
	cmd = [ffmpeg, "-i", input_file, "-filter_complex", f"[0:v]{vf_expr}[v];[0:a]{atempo_filter}[a]", "-map", "[v]", "-map", "[a]", "-y", out_file]
	return out_file if run_ffm(cmd, de_bug=de_bug) else None

@perf_monitor
def video_diff(file1: str, file2: str, de_bug: bool = False) -> Optional[str]:
	base_name, _ = os.path.splitext(os.path.basename(file1))
	out_file = base_name + "_diff.mp4"
	cmd = [ffmpeg, "-i", file1, "-i", file2, "-filter_complex", "blend=all_mode=difference", "-c:v", "libx265", "-preset", "faster", "-c:a", "copy", "-y", out_file]
	return out_file if run_ffm(cmd, de_bug=de_bug) else None

@perf_monitor
def short_ver(input_file: str, start: str = "00:00:00", duration: str = "00:05:00", execu: str = ffmpeg, de_bug: bool = False) -> Optional[str]:
	base_name, _ = os.path.splitext(os.path.basename(input_file))
	out_file = base_name + "_short.mp4"
	cmd = [execu, "-ss", start, "-i", input_file, "-t", duration, "-c:v", "copy", "-c:a", "copy", "-y", out_file]
	return out_file if run_ffm(cmd, de_bug=de_bug) else None

def main():
	if len(sys.argv) < 2:
		print("Usage: python FFMpeg.py <path_to_video_file>")
		return
	input_file = sys.argv[1]
	if not os.path.isfile(input_file):
		print(f"Error: File not found at '{input_file}'")
		return
	try:
		metadata = ffprobe_run(input_file)
		ff_run_cmd, skip_it = parse_finfo(input_file, metadata, de_bug=False)
		if not skip_it:
			out_file = ffmpeg_run(input_file, ff_run_cmd, skip_it)
			if out_file: print(f"\nSuccess! Output file: {out_file}")
			else: print("\nffmpeg run failed or was aborted.")
	except Exception as e:
		print(f"\nAn error occurred: {e}")

if __name__ == "__main__":
	main()
