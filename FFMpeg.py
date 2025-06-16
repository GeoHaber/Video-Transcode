# -*- coding: utf-8 -*-
import os
import re
import sys
import json
import time
import datetime as TM
import subprocess as SP

from collections import defaultdict
from dataclasses import dataclass
from typing	import List, Dict, Tuple, Any, Optional

# External references from your code
from Yaml		import * # if needed
from My_Utils	import * # e.g. hm_sz, hm_time, divd_strn, stmpd_rad_str, perf_monitor


# Paths to FFmpeg/FFprobe executables (update if needed)
ffmpg_bin = r"C:\Program Files\ffmpeg\bin"
ffmpeg = os.path.join(ffmpg_bin, "ffmpeg.exe")
ffprob = os.path.join(ffmpg_bin, "ffprobe.exe")

try:
	if not os.path.isfile(ffmpeg):
		raise FileNotFoundError(f"FFmpeg not found at '{ffmpeg}'.")
	if not os.path.isfile(ffprob):
		raise FileNotFoundError(f"FFprobe not found at '{ffprob}'.")

	result = SP.run([ffmpeg, "-version"], stdout=SP.PIPE, stderr=SP.PIPE)
	if result.returncode == 0:
		version_info = result.stdout.decode("utf-8")
		match = re.search(r"ffmpeg version (\d+\.\d+\.\d+)", version_info)
		if match:
			ffmpeg_version = match.group(1)
		else:
			print(f"Warning: Could not extract ffmpeg version.")
	else:
		print(f"Error running ffmpeg -version:\n{result.stderr.decode('utf-8')}")

except FileNotFoundError as e:
	print(f"Error: {e}")
except Exception as e:
	print(f"An unexpected error occurred: {e}")

###############################################################################
#                          DATA CLASSES & CONTEXT
###############################################################################
@dataclass
class VideoContext:
	"""A data class to hold shared information about the video file."""
	duration: float = 0.0
	file_size: int = 0
	estimated_video_bitrate: int = 0
	estimation_method: str = "(default)"
	vid_width: int = 0
	vid_height: int = 0

glb_vidolen = 0 # Referenced by your show_progrs

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
	"bitrate": re.compile(r"bitrate=\s*([0-9\.]+)"), "frame":   re.compile(r"frame=\s*([0-9]+)"),
	"speed":   re.compile(r"speed=\s*([0-9\.]+)"), "size":    re.compile(r"size=\s*([0-9]+)"),
	"time":    re.compile(r"time=([0-9:.]+)"), "fps":     re.compile(r"fps=\s*([0-9]+)")
}

def extract_progress_data(line_to: str) -> dict:
	regx_val = {}
	for key, rx in regex_dict.items():
		match = rx.search(line_to)
		if match:
			regx_val[key] = match.group(1)
	return regx_val

def calculate_eta(regx_val: dict, sp_float: float) -> Tuple[str, float]:
	global glb_vidolen
	if glb_vidolen <= 0:
		return "--:--:--", 0.0
	try:
		time_str = regx_val.get("time", "00:00:00.00")
		t_parts = list(map(float, time_str.strip().split(':')))
		while len(t_parts) < 3:
			t_parts.insert(0, 0.0)
		h, m, s_float = t_parts
		a_sec = h * 3600 + m * 60 + s_float

		dif = max(glb_vidolen - a_sec, 0.0)
		eta_seconds = dif / sp_float if sp_float > 0 else 9999.0
		hours, remainder = divmod(round(eta_seconds), 3600)
		minutes, seconds = divmod(remainder, 60)
		eta_str = f"{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}"
		return eta_str, a_sec
	except Exception:
		return "--:--:--", 0.0


def show_progrs(line_to: str, sy: str, de_bug: bool = False) -> bool:
	line_to = line_to.strip()
	if not line_to or de_bug:
		if de_bug:
			print(line_to)
		return True

	if "cpb:" in line_to and "N/A" in line_to:
		return True

	if all(x in line_to for x in ["fps=", "speed=", "size="]):
		regx_val = extract_progress_data(line_to)
		try:
			fp_int = int(float(regx_val.get("fps", 0)))
			sp_float = float(regx_val.get("speed", 0))
			eta_str, a_sec = calculate_eta(regx_val, sp_float)

			size_val = hm_sz(int(regx_val.get('size', '0')) * 1024, 'bytes')
			bitrate_val = hm_sz(float(regx_val.get('bitrate', '0')) * 1000)
			percent = 100 * a_sec / glb_vidolen if glb_vidolen else 0

			disp_str = (
				f"    | {sy} "
				f"|Size: {size_val:>10}"
				f"|Frames: {int(regx_val.get('frame', 0)):>7}"
				f"|Fps: {fp_int:>4}"
				f"|BitRate: {bitrate_val:>10}"
				f"|Speed: {sp_float:>5.2f}x"
				f"|ETA: {eta_str:>9}"
				f"|{percent:5.1f}% |    "
			)
			sys.stderr.write('\r' + disp_str)
			sys.stderr.flush()
		except Exception as e:
			print(f"show_progrs exception: {e} in line: {line_to}")

	elif any(x in line_to for x in ["muxing overhead:", "global headers:"]):
		print(f"\n   .Done: {line_to[31:]} |  ")
		return False

	return True

@perf_monitor
def run_ffm(args: List[str], de_bug: bool = False) -> bool:
	with SP.Popen(args, stdout=SP.PIPE, stderr=SP.STDOUT, text=True, encoding="utf-8", errors="replace") as process:
		for line in process.stdout:
			if show_progrs(line, "|/-o+\\"[int(time.time()*2) % 5], de_bug) == -1:
				break
		process.communicate()
	if process.returncode != 0:
		print (f"\nFFmpeg failed with return code {process.returncode}")
	return process.returncode == 0

@perf_monitor
def ffmpeg_run(input_file: str, ff_com: List[str], skip_it: bool, execu: str = ffmpeg, de_bug: bool = False) -> Optional[str]:
	if not input_file or skip_it: return None
	file_name, _ = os.path.splitext(os.path.basename(input_file))
	out_file = "_" + stmpd_rad_str(7, file_name[0:25])
	out_file = os.path.normpath(re.sub(r"[^\w\s_-]+", "", out_file).strip().replace(" ", "_") + TmpF_Ex)
	ff_head = [execu, "-thread_queue_size", "24", "-i", input_file, "-hide_banner"]
	ff_tail = ["-metadata", f"title={file_name}", "-metadata", f"comment={Skip_key}", "-metadata", "author=Encoded by GeoHab", "-movflags", "+faststart", "-fflags", "+genpts", "-y", out_file]

	full_cmd = ff_head + ff_com + ff_tail
	if de_bug:
		print("\n--- [DEBUG] FFmpeg Command Sent ---")
		print(' '.join(f'"{arg}"' if ' ' in arg else arg for arg in full_cmd))
		print("-----------------------------------\n")

	return out_file if run_ffm(full_cmd, de_bug=de_bug) else None

###############################################################################
#                              STREAM PARSERS
###############################################################################
# PATCH: Smaller Output File Size Tuning + CLI Mode Support

@perf_monitor
# PATCH: Improve HW Encoding File Size Control for hevc_qsv

@perf_monitor
def get_optimal_reencode_options(
	is_10bit: bool,
	height: int,
	use_hw_accel: bool = False,
	color_primaries: str = None,
	color_trc: str = None,
	color_space: str = None,
	mode: str = "balanced",
	de_bug: bool = False
) -> List[str]:
	"""
	Returns FFmpeg encoding options with improved bitrate controls.
	"""
	MODES = {
		"smaller": {
			2000: (26, 27, 8000, 16000),
			900:  (25, 26, 3000, 8000),
			600:  (24, 25, 1800, 3600),
			0:    (23, 24, 1200, 2400),
		},
		"faster": {
			2000: (21, 22, 20000, 40000),
			900:  (20, 21,  8000, 20000),
			600:  (19, 20,  5000, 10000),
			0:    (18, 19,  3000,  6000),
		},
		"balanced": {
			2000: (23, 24, 10000, 20000),
			900:  (22, 23,  4500, 11000),
			600:  (21, 22,  3000,  6000),
			0:    (20, 21,  2000,  4000),
		},
	}

	# Sort height thresholds descending and get appropriate settings
	params = next((v for k, v in sorted(MODES[mode].items(), reverse=True) if height > k), MODES[mode][0])
	crf, icq, vbv_maxrate, vbv_bufsize = params

	if use_hw_accel:
		if de_bug:
			print(f"      | HW {mode.upper()}: ICQ={icq}, VBV={vbv_maxrate}k")
		return [
			"hevc_qsv", "-preset", "slow", "-rc", "vbr",
			"-b:v", f"{vbv_maxrate}k", "-maxrate", f"{vbv_maxrate}k",
			"-bufsize", f"{vbv_bufsize}k", "-look_ahead", "1",
			"-pix_fmt", "p010le" if is_10bit else "nv12",
		]
	else:
		if de_bug:
			print(f"      | SW {mode.upper()}: CRF={crf}, VBV={vbv_maxrate}k")
		x265_params = [
			f"preset=slow:tune=grain:crf={crf}",
			"psy-rd=1.0:psy-rdoq=1.0", "aq-mode=2", "no-sao=1",
			f"vbv-maxrate={vbv_maxrate}:vbv-bufsize={vbv_bufsize}"
		]
		if color_primaries: x265_params.append(f"colorprim={color_primaries}")
		if color_trc:       x265_params.append(f"transfer={color_trc}")
		if color_space:     x265_params.append(f"colormatrix={color_space}")
		return [
			"libx265",
			"-x265-params", ":".join(x265_params),
			"-pix_fmt", "yuv420p10le" if is_10bit else "yuv420p"
		]

@perf_monitor
def parse_video(video_streams: List[Dict[str, Any]], context: VideoContext, de_bug: bool = False) -> Tuple[List[str], bool]:
	"""Parses video streams, handling re-encoding, copying, and metadata."""
	USE_HW_ACCEL = True # Set to True for Intel QSV hardware encoding

	ff_video_args, skip_all = [], True
	if not video_streams:
		print("     No primary video stream found. Skipping video processing.")
		return [], True

	output_v_idx = 0
	for stream in video_streams:
		codec_name = stream.get("codec_name", "XXX")
		if codec_name in ['mjpeg', 'png']:
			print(f"     |<V:{stream['index']}>|{codec_name:^8}| Ignoring Attachment/Cover Art.")
			continue

		stream_bitrate_str = stream.get("bit_rate")
		if stream_bitrate_str and stream_bitrate_str.isdigit():
			this_bitrate = int(stream_bitrate_str)
			bitrate_source_msg = "(from stream)"
		else:
			this_bitrate = context.estimated_video_bitrate
			bitrate_source_msg = context.estimation_method

		pix_fmt = stream.get("pix_fmt", "")
		max_vid_btrt = 3400000
		bit_depth_str = "8-bit"
		if pix_fmt.endswith("10le"):
			bit_depth_str = "10-bit"
			max_vid_btrt = int(max_vid_btrt * 1.25)
		# Final target bitrate for this stream
		max_vid_btrt = min(this_bitrate, max_vid_btrt)

		needs_scaling = context.vid_width > 1920 or context.vid_height > 1080
		already_friendly = codec_name.lower() == "hevc" and this_bitrate <= max_vid_btrt and not needs_scaling

		handler_needs_change = stream.get("tags", {}).get("handler_name") != "VideoHandler (Gemini Optimized)"
		if not already_friendly or handler_needs_change:
			skip_all = False

		ff_stream_args = ["-map", f"0:{stream['index']}", f"-c:v:{output_v_idx}"]
		extra_msg = ""

		if already_friendly:
			ff_stream_args.extend(["copy", "-bsf:v", "hevc_mp4toannexb"])
			extra_msg = "=> Copy (HEVC, Bitrate/Res OK)"
		else:
			encoder_opts = get_optimal_reencode_options(
				is_10bit=pix_fmt.endswith("10le"), height=context.vid_height, use_hw_accel=USE_HW_ACCEL,
				color_primaries=stream.get("color_primaries"), color_trc=stream.get("color_trc"),
				color_space=stream.get("color_space"), de_bug=de_bug
			)
			ff_stream_args.extend(encoder_opts)
			extra_msg = f"=> Re-encode ({'HW' if USE_HW_ACCEL else 'SW'})"
			if needs_scaling:
				if USE_HW_ACCEL:
					ff_stream_args.extend(["-vf", "vpp_qsv=w=1920:h=1080"])
					extra_msg += " | HW Scaling (vpp_qsv)"
				else:
					ff_stream_args.extend(["-vf", "scale=1920:1080:force_original_aspect_ratio=decrease:flags=lanczos,unsharp=5:5:0.5:5:5:0.2"])
					extra_msg += " | SW Scaling"

		if handler_needs_change:
			 ff_stream_args.extend([f"-metadata:s:v:{output_v_idx}", "handler_name=VideoHandler (Gemini Optimized)"])
			 extra_msg += "|Set Handler"

		msg = f"|<V:{stream['index']:2}>|{codec_name:^8}|{context.vid_width}x{context.vid_height}|{bit_depth_str}|Bitrate: {hm_sz(this_bitrate)} {bitrate_source_msg}|{extra_msg}|"
		print(f"\033[91m    {msg}\033[0m")
		ff_video_args.extend(ff_stream_args)
		output_v_idx += 1

	if skip_all: print("    .Skip: Video streams are already optimal.")
	return ff_video_args, skip_all

@perf_monitor
def parse_audio(audio_streams: List[Dict[str, Any]], context: VideoContext, de_bug: bool = False) -> Tuple[List[str], bool]:
	if not audio_streams:
		return [], True

	best_candidate = None
	original_default = next((s for s in audio_streams if s.get("disposition", {}).get("default", 0)), None)

	# Apply scoring to all audio streams
	scored_streams = []
	for s in audio_streams:
		score = 0
		if s.get("tags", {}).get("language") == Default_lng:
			score += 50
		score += int(s.get("channels", 0)) * 10
		title = s.get("tags", {}).get("title", "").lower()
		if "commentary" in title:
			score -= 100
		scored_streams.append((score, s))

	if scored_streams:
		scored_streams.sort(key=lambda x: x[0], reverse=True)
		best_candidate = scored_streams[0][1]
	elif original_default:
		best_candidate = original_default
	elif audio_streams:
		best_candidate = audio_streams[0]

	ffmpeg_audio_options, all_skippable, output_audio_idx = [], True, 0
	disposition_needs_change = any(
		(s['index'] == best_candidate['index'] and not s.get("disposition", {}).get("default", 0)) or
		(s['index'] != best_candidate['index'] and s.get("disposition", {}).get("default", 0))
		for s in audio_streams if best_candidate
	)

	for audio_strm in audio_streams:
		is_compliant = (audio_strm.get("codec_name", "u").lower() in ['eac3', 'aac']) and int(audio_strm.get("channels", 0)) <= 6
		handler_is_correct = audio_strm.get("tags", {}).get("handler_name") == "SoundHandler"

		if not is_compliant or not handler_is_correct:
			all_skippable = False

		is_best = (best_candidate and audio_strm['index'] == best_candidate['index'])

		stream_opts = ["-map", f"0:{audio_strm['index']}"]
		log_action = ""

		if is_compliant:
			stream_opts.extend([f"-c:a:{output_audio_idx}", "copy"])
			log_action = "=> Copy (Compliant)"
		else:
			if int(audio_strm.get("channels", 0)) >= 6:
				stream_opts.extend([f"-c:a:{output_audio_idx}", "eac3", f"-b:a:{output_audio_idx}", "640k"])
				log_action = "=> Re-encode to E-AC3 640k"
			else:
				stream_opts.extend([f"-c:a:{output_audio_idx}", "aac", f"-q:a:{output_audio_idx}", "2", f"-ac:a:{output_audio_idx}", "2"])
				log_action = "=> Re-encode to AAC (VBR q:2) [Stereo Mixdown]"

		if is_best:
			stream_opts.extend([f"-disposition:a:{output_audio_idx}", "default"])
			if not audio_strm.get("disposition", {}).get("default", 0):
				log_action += "|Set Default"
		else:
			stream_opts.extend([f"-disposition:a:{output_audio_idx}", "0"])
			if audio_strm.get("disposition", {}).get("default", 0):
				log_action += "|Clear Default"

		if not handler_is_correct:
			stream_opts.extend([f"-metadata:s:a:{output_audio_idx}", "handler_name=SoundHandler"])
			log_action += "|Set Handler"

		if is_best and handler_is_correct:
			log_action += "|Selected as Default"

		ffmpeg_audio_options.extend(stream_opts)
		msg = f"|<A:{audio_strm['index']:2}>|{audio_strm.get('codec_name', 'u'):^8}|{audio_strm.get('tags', {}).get('language', 'und'):<3}|Br:{int(audio_strm.get('bit_rate', 0)):>7}|Ch:{int(audio_strm.get('channels', 0))}| {log_action}"
		print(f"\033[92m    {msg}\033[0m")
		output_audio_idx += 1

	final_skip = all_skippable and not disposition_needs_change
	if final_skip:
		print("    .Skip: Audio streams are already optimal.")
	return ffmpeg_audio_options, final_skip

def _score_subtitle(stream: Dict[str, Any]) -> int:
	score = 100
	tags, disposition = stream.get("tags", {}), stream.get("disposition", {})
	title = tags.get("title", "").lower()
	if disposition.get("forced", 0) == 1 or "forced" in title:
		score -= 1000
	if "sdh" in title:
		score += 500
	if disposition.get("default", 0) == 1:
		score += 200
	score += int(stream.get("bit_rate", 0))
	score += int(stream.get("nb_frames", 0) or stream.get("nb_read_packets", 0))
	return score

def add_subtl_from_file(input_file: str, de_bug: bool = False) -> Tuple[List[str], bool]:
	base_name, _ = os.path.splitext(input_file)
	subtitle_exts = [".srt", ".ass"]
	found_file = None

	for ext in subtitle_exts:
		test_file = base_name + ext
		if os.path.isfile(test_file):
			found_file = test_file
			break

	if not found_file:
		print("    .No external subtitle file found.")
		return [], True

	print(f"    .Using external subtitle: {os.path.basename(found_file)}")
	return ["-sub_charenc", "UTF-8", "-i", found_file, "-c:s", "mov_text", "-map", "1:0", "-metadata:s:s:0", "handler_name=SubtitleHandler"], False

@perf_monitor
def parse_subtl(sub_streams: List[Dict[str, Any]], context: VideoContext, de_bug: bool = False) -> Tuple[List[str], bool]:
	if not sub_streams:
		return add_subtl_from_file("", de_bug)

	TEXT_BASED_CODECS = ("subrip", "ass", "mov_text")
	best_streams = {lang: {'score': -1, 'stream': None} for lang in Keep_langua}
	stream_scores = {}

	for stream in sub_streams:
		lang, codec = stream.get("tags", {}).get("language", "und"), stream.get("codec_name", "unknown")
		if lang in Keep_langua:
			score = _score_subtitle(stream)
			stream_scores[stream['index']] = score
			if lang not in best_streams or score > best_streams[lang]['score']:
				best_streams[lang] = {'score': score, 'stream': stream}

	streams_to_process = [data['stream'] for data in best_streams.values() if data['stream']]
	text_streams_to_keep = []

	for stream in streams_to_process:
		lang, codec = stream.get("tags", {}).get("language", "und"), stream.get("codec_name", "unknown")
		score = stream_scores.get(stream['index'], '?')
		if codec in TEXT_BASED_CODECS:
			text_streams_to_keep.append(stream)
			print(f"\033[94m    |<S:{stream['index']:2}>|{codec:^8}|{lang:3}| Score:{score:5} | => Keep (Text-based)\033[0m")
		else:
			print(f"\033[90m    |<S:{stream['index']:2}>|{codec:^8}|{lang:3}| Score:{score:5} | => Remove (Image-based, not supported in MP4)\033[0m")

	if not text_streams_to_keep:
		print("    .Skip: No compatible text-based subtitles found.")
		return [], True

	best_sub_candidate = best_streams.get(Default_lng, {}).get('stream') if Default_lng in best_streams else None
	needs_disposition_change = any(
		(s['index'] == best_sub_candidate['index'] and not s.get("disposition", {}).get("default", 0)) or
		(s['index'] != best_sub_candidate['index'] and s.get("disposition", {}).get("default", 0))
		for s in text_streams_to_keep if best_sub_candidate
	)
	all_compliant = all(s.get('codec_name') == 'mov_text' for s in text_streams_to_keep)
	handler_is_correct = all(s.get("tags", {}).get("handler_name") == "SubtitleHandler" for s in text_streams_to_keep)

	if all_compliant and not needs_disposition_change and handler_is_correct:
		print("    .Skip: Subtitle section is already compliant.")
		return [], True

	ff_subttl = []
	for i, stream in enumerate(text_streams_to_keep):
		is_best = (best_sub_candidate and stream['index'] == best_sub_candidate['index'])
		stream_opts = ["-map", f"0:{stream['index']}"]

		if stream.get('codec_name') == 'mov_text' and not needs_disposition_change:
			stream_opts.extend([f"-c:s:{i}", "copy"])
		else:
			stream_opts.extend([f"-c:s:{i}", "mov_text"])

		if is_best:
			stream_opts.extend([f"-disposition:s:{i}", "default"])
		else:
			stream_opts.extend([f"-disposition:s:{i}", "0"])

		if stream.get("tags", {}).get("handler_name") != "SubtitleHandler":
			stream_opts.extend([f"-metadata:s:s:{i}", "handler_name=SubtitleHandler"])

		ff_subttl.extend(stream_opts)

	print("    .Process: Re-encoding/setting default subtitles.")
	return ff_subttl, False


@perf_monitor
def parse_extrd(data_streams: List[Dict[str, Any]], context: VideoContext, de_bug: bool = False) -> Tuple[List[str], bool]:
	if not data_streams: return [], True
	ff_data = [cmd for d in data_streams for cmd in ("-map", f"-0:d:{d['index']}")]
	print(f"\033[90m    | Removing {len(data_streams)} data stream(s).\033[0m")
	return ff_data, False

###############################################################################
#                        MAIN PARSING FUNCTION
###############################################################################
@perf_monitor
def parse_finfo(input_file: str, metadata: Dict[str, Any], de_bug: bool = False) -> Tuple[List[str], bool]:
	_format, _streams = metadata.get("format", {}), metadata.get("streams", [])
	if not _format or not _streams:
		raise ValueError("Invalid metadata")

	size =		int(_format.get("size", 0))
	duration =	float(_format.get("duration", 0.0))
	global glb_vidolen
	glb_vidolen = duration
	bitrate = int(_format.get("bit_rate", 0))

	title =		_format.get("tags", {}).get("title", "No_title")
	f_comment =	_format.get("tags", {}).get("comment", "No_Comment")

	streams_by_type = defaultdict(list)
	for s in _streams: streams_by_type[s.get("codec_type", "?")].append(s)

	sc = {k: len(v) for k, v in streams_by_type.items()}
	print(f"\033[96m    |=Title|{title}|\n    |<FRMT>|Size: {hm_sz(size, 'bytes')}|Bitrate: {hm_sz(bitrate)}|Length: {hm_time(duration)}|Streams: V:{sc.get('video',0)} A:{sc.get('audio',0)} S:{sc.get('subtitle',0)}\033[0m")

	video_streams = streams_by_type.get("video", [])
	primary_video = video_streams[0] if video_streams else {}
	context = VideoContext(duration=duration, file_size=size, vid_width=primary_video.get("width", 0), vid_height=primary_video.get("height", 0))

	total_file_br = (size * 8) / duration if size > 0 and duration > 0 else 0
	total_audio_br = sum(int(s.get("bit_rate", "0")) for s in streams_by_type.get("audio", []) if s.get("bit_rate", "0").isdigit())

	if total_file_br > 0 and total_audio_br > 0:
		context.estimated_video_bitrate = int((total_file_br - total_audio_br) * 0.98)
		context.estimation_method = "(file size - audio br)"
	elif total_file_br > 0:
		context.estimated_video_bitrate = int(total_file_br * 0.80)
		context.estimation_method = "(80% of file size)"
	else:
		context.estimated_video_bitrate = 2000000

	v_cmd, v_skip = parse_video(video_streams, context, de_bug)
	a_cmd, a_skip = parse_audio(streams_by_type.get("audio", []), context, de_bug)
	s_cmd, s_skip = parse_subtl(streams_by_type.get("subtitle", []), context, de_bug)
	d_cmd, d_skip = parse_extrd(streams_by_type.get("data", []), context, de_bug)

	already_processed = f_comment == Skip_key
	final_skip = already_processed and v_skip and a_skip and s_skip and d_cmd
	if final_skip:	print("    .Skip: File already processed and all components meet current rules.")
	else:			print("    .Process: File requires processing.")
	return v_cmd + a_cmd + s_cmd + d_cmd, final_skip

###############################################################################
#                           OTHER UTILITIES
###############################################################################
@perf_monitor
def matrix_it(input_file: str, execu: str = ffmpeg, ext: str = ".png", columns: int = 3, rows: int = 3) -> bool:
	base_name, _ = os.path.splitext(os.path.basename(input_file))
	out_file = base_name + "_matrix" + ext
	select_expr = "select='not(mod(n,300))'"
	tile_expr   = f"tile={columns}x{rows}:padding=5:margin=5"
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

# --- Main entry point for Trans_code.py ---
# This ensures that calling `parse_finfo` from another script works as expected.
# The main() function below is for direct execution of this file.
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
