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
from Yaml		import *      # if needed
from My_Utils	import *  # e.g. hm_sz, hm_time, divd_strn, stmpd_rad_str, perf_monitor


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
		# Use regular expression to extract the desired version part
		match = re.search(r"ffmpeg version (\d+\.\d+\.\d+)", version_info)
		if match:
			ffmpeg_version = match.group(1)
#			print(f"Ffmpeg version: {ffmpeg_version} ")
		else:
			print(f"Warning: Could not extract the desired ffmpeg version from output:\n{version_info}")
	else:
		print(f"Error running ffmpeg -version:\n{result.stderr.decode('utf-8')}")

except FileNotFoundError as e:
	print(f"Error: {e}")
except Exception as e:
	print(f"An unexpected error occurred: {e}")

# Some global or shared constants (rather than global variables)
TmpF_Ex = "_out.mp4"

###############################################################################
#                          DATA CLASSES & CONTEXT
###############################################################################
@dataclass
class VideoContext:
	"""Holds metadata about the video file being processed."""
	vid_width: int = 0
	vid_height: int = 0
	vid_length: float = 0.0  # total duration in seconds as float
	vid_bitrate: int = 0
	total_frames: int = 0

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
		"-v", "fatal",   # quiet, panic, fatal, error, warning, info, verbose, debug, trace
		"-show_programs",
		"-show_format",
		"-show_streams",
		"-show_error",
		"-show_data",
		"-show_private_data",
		"-of", "json"
	]

	try:
		out = SP.run(cmd, stdout=SP.PIPE, stderr=SP.PIPE, check=True)
		jsn_out = json.loads(out.stdout.decode("utf-8"))

		if not jsn_out or "error" in jsn_out:
			err_msg = jsn_out.get("error") if jsn_out else f"{input_file} no JSON output"
			print ("ffprobe error: %s", err_msg)
			raise ValueError(f"ffprobe error: {err_msg}")
		return jsn_out

	except (FileNotFoundError, SP.CalledProcessError, SP.TimeoutExpired, json.JSONDecodeError) as e:
		raise Exception(f"ffprobe_run error: {e} in file: {input_file}") from e

###############################################################################
#                             RUN FFMPEG
###############################################################################
@perf_monitor
def run_ffm(args: List[str], de_bug: bool = False) -> bool:
	"""
	Run an ffmpeg command. If `de_bug` is True, print full output; otherwise, show a spinner.
	Returns True if command succeeds, False otherwise.
	"""
	if de_bug: print(f"run_ffm: {' '.join(args)}")
	if not args:
		print ("run_ffm: No arguments provided.")
		return False

	try:
		if de_bug:
			# Debug mode: Capture stdout/stderr and print them
			print(f"Running ffmpeg in debug mode.\n {' '.join(args)}")
			process = SP.run(args, stdout=SP.PIPE, stderr=SP.STDOUT)
			print(f"Stdout:\n{process.stdout.decode('utf-8', errors='replace')}")
			if process.returncode != 0:
				print (f"FFmpeg failed with return code {process.returncode}")
				return False
			time.sleep(1)
			return True
		else:
			spin_char = "|/-o+\\"
			spinner_counter = 0
			with SP.Popen (    args,
							stdout=SP.PIPE,
							stderr=SP.STDOUT,
							text=True,             # same as universal_newlines=True in modern Python
							encoding="utf-8",   # or "cp437", "cp65001",
							errors="replace"
						) as process:
				for line in process.stdout:
					# show_progrs is your custom function that parses ffmpeg progress
					if show_progrs(line, spin_char[spinner_counter % len(spin_char)], de_bug=False):
						spinner_counter += 1
#                    print ( SP.PIPE )
				_, _ = process.communicate()

			if process.returncode != 0:
				print (f"FFmpeg failed with return code {process.returncode}")
				return False
		return True

	except Exception as e:
		print (f"run_ffm exception: {e}")
		return False

@perf_monitor
def ffmpeg_run(
	input_file: str,
	ff_com: List[str],
	skip_it: bool,
	execu: str = "ffmpeg",
	de_bug: bool = False,
	max_retries: int = 2,
	retry_delay: int = 2
) -> Optional[str]:
	"""
	Builds and runs an FFmpeg command with optional retries. Returns the output file
	name on success or None on skip/failure.
	"""
	if not input_file or skip_it:
#        print("Skipping ffmpeg_run: skip_it=%s, input_file=%s", skip_it, input_file)
		return None

#    print ("ffmpeg_run start: %s", input_file)

	file_name, _ = os.path.splitext(os.path.basename(input_file))
	out_file = os.path.normpath("_" + stmpd_rad_str(7, file_name[0:25]))
	out_file = re.sub(r"[^\w\s_-]+", "", out_file).strip().replace(" ", "_") + TmpF_Ex

	# Attempt to get ffmpeg version
	try:
		ffmpeg_vers = SP.check_output([execu, "-version"]).decode("utf-8").splitlines()[0].split()[2]
	except SP.CalledProcessError as e:
		print (f"Error getting ffmpeg version: {e}")
		return None

	# Head + user command + tail
	ff_head = [
		execu, "-thread_queue_size", "24",
		"-i", input_file,
		"-hide_banner"
	]
	ff_tail = [
		"-metadata",    f"title={file_name}",
		"-metadata",    f"comment={Skip_key}",
		"-metadata",    f"encoder=ffmpeg {ffmpeg_vers}",
		"-metadata",    "copyright=2025",
		"-metadata",    "author=Encoded by GeoHab",
		"-movflags",    "+faststart",         # Place moov atom at the beginning for fast start
		"-fflags",      "+fastseek",          # Enable fast seeking
		"-fflags",      "+genpts",            # Generate presentation timestamps
		"-keyint_min",  "30",                 # Minimum interval between keyframes
		"-g",           "60",                 # Set the GOP (Group of Pictures) size
		"-y",           out_file
	]

	for attempt in range(1, max_retries + 1):
		try:
			if attempt > 1:
				time.sleep(retry_delay)
				print (f"Retry attempt {attempt}")
				# In debug mode for the second attempt
				ff_head = [execu, "-report", "-loglevel", "verbose", "-i", input_file, "-hide_banner"]

			full_cmd = ff_head + ff_com + ff_tail
			if run_ffm(full_cmd, de_bug=de_bug):
				if de_bug: print (f"Successfully created file: {out_file}")
				return out_file

		except Exception as e:
			print (f"ffmpeg_run attempt {attempt} failed: {e}")

		if attempt == max_retries:
			msg = f"Failed after {attempt} attempts."
			print (msg)
			raise Exception(msg)

	return None

###############################################################################
#                              PARSE VIDEO & FRIENDS
###############################################################################
@perf_monitor
def get_encoder_options(
	codec_name: str,
	is_10bit: bool,
	bit_rate: int,
	use_hw_accel: bool = False
	) -> List[str]:
	"""
	Returns FFmpeg arguments for encoding with x265 or QSV, optimized for high quality.

	:param codec_name: The name of the codec to use.
	:param is_10bit: Boolean indicating if the video is 10-bit.
	:param bit_rate: The bit rate of the input video in bits per second.
	:param use_hw_accel: Boolean to determine if hardware acceleration (QSV) should be used.
	:return: List of FFmpeg arguments.
	"""
	# Define the quality preset. For high quality, we'll target 'high'.
	# CRF 20 is a great balance of quality and file size. CRF 18 is near-lossless.
	target_quality = "as_is"

	# Each preset contains (bitrate in kb/s, quality level or CRF value)
	quality_presets = {
		"low":    (bit_rate // 4096, 25),  # Lower quality
		"medium": (bit_rate // 2048, 23),  # Good quality
		"as_is":  (bit_rate // 1600, 22),  # Default quality, slight bitrate reduction
		"high":   (bit_rate // 1024, 20),  # Excellent quality
		"higher": (bit_rate // 512,  18)   # Visually lossless
	}

	# Unpack bitrate and quality for the chosen preset
	target_bitrate, quality_level = quality_presets[target_quality]

	# Convert bitrates to string format with 'k' for kilobits
	bitrate_str, maxrate_str, bufsize_str = [f"{x}k" for x in (target_bitrate, target_bitrate * 1.5, target_bitrate * 2.5)]

	if use_hw_accel:
		# Return configuration for hardware acceleration (QSV) using ICQ for quality.
		# ICQ (Intelligent Constant Quality) is the QSV equivalent of CRF.
		return [
			"hevc_qsv", "-load_plugin", "hevc_hw",
			"-init_hw_device", "qsv=qsv:MFX_IMPL_hw_any",
			"-filter_hw_device", "qsv",
			"-pix_fmt", "p010le" if is_10bit else "nv12",
			"-rc", "icq",  # Use Intelligent Constant Quality rate control
			"-q", str(quality_level),  # Set the ICQ quality level (lower is better)
			"-b:v", bitrate_str, "-maxrate", maxrate_str, "-bufsize", bufsize_str, # Bitrate hints
			"-look_ahead", "1", "-look_ahead_depth", "40",  # Optimal look-ahead depth
			"-preset", "slow"  # 'slow' preset for best QSV quality
		]

	# x265 parameters for high-quality software encoding
	# Enhanced for better detail retention and psychovisual quality
	x265_params = (
		"open-gop=0:keyint=60:min-keyint=30:scenecut=40:"
		"bframes=8:b-adapt=2:"
		"psy-rd=2.0:psy-rdoq=2.0:"  # Increase psychovisual strength for detail
		"aq-mode=3:aq-strength=1.0:" # Auto-variance AQ is excellent
		"deblock=-1,-1:" # Less aggressive deblocking to retain detail
		"nr-inter=400:nr-intra=100:" # Light denoising for better compression
		"me=umh:subme=7"
	)
	# Return configuration for software encoding using libx265
	return [
		"libx265", "-x265-params", x265_params,
		"-pix_fmt", "yuv420p10le" if is_10bit else "yuv420p",
		"-crf", str(quality_level),  # Use CRF for constant quality
		"-b:v", bitrate_str, # Provide target bitrate
		"-maxrate", maxrate_str, "-bufsize", bufsize_str, # Set ceiling and buffer
		"-preset", "slow"  # 'slow' is a great balance of speed and quality
	]
@perf_monitor
def parse_video(
	streams_in: List[Dict[str, Any]],
	context: VideoContext,
	de_bug: bool = False,
	skip_it: bool = False
) -> Tuple[List[str], bool]:

	global glb_vidolen
	glb_vidolen = context.vid_length

	if de_bug: print (f"parse_video start, skip_it={skip_it}")
	ff_video = []
	skip_all = True

	for idx, stream in enumerate(streams_in):
		if "codec_name" not in stream:
			raise Exception(f"No codec_name in video stream {stream}")

		codec_name   = stream.get("codec_name", "XXX")
		pix_fmt      = stream.get("pix_fmt", "")
		handler_name = stream.get("tags", {}).get("handler_name", "Unknown")
		frm_rate     = divd_strn(stream.get("r_frame_rate", "25"))
		this_bitrate = int(stream.get("bit_rate", context.vid_bitrate * 0.8))

		if codec_name.lower() in ['mjpeg', 'png']:
			print (f"    |<V:{idx:2}>|{codec_name:^8}| Ignore !!" )
			skip_all = False
			continue

		# Compute total frames
		context.total_frames = round(frm_rate * context.vid_length)

		# Decide maximum allowed bitrate
		max_vid_btrt = 3900000
		bit_depth_str = "8-bit"
		if pix_fmt.endswith("10le"):
			bit_depth_str = "10-bit"
			max_vid_btrt = int(max_vid_btrt * 1.25)

		# Final target bitrate for this stream
		btrt = min(this_bitrate, max_vid_btrt)

		# Decide if we need to scale
		output_resolutions = [
			(7600, 4300, "8K"),
			(3800, 2100, "4K"),
			(2100, 1920, "2K"),
			(1280, 720,  "HD")
		]
		output_label = "SD"
		for w, h, label in output_resolutions:
			if context.vid_width >= w or context.vid_height >= h:
				output_label = label
				break
		needs_scaling = (output_label in ["2K", "4K", "8K"])

		# The combined condition:
		already_streaming_friendly = (
			codec_name.lower() == "hevc"
			and this_bitrate <= max_vid_btrt
			and not needs_scaling
		)

		# Build ffmpeg args
		ff_vid = ["-map", f"0:v:{idx}", f"-c:v:{idx}"]
		extra_msg = ""

		if already_streaming_friendly:
			# Just copy
			ff_vid.append("copy")
			extra_msg = f"=> Copy (x265 < {hm_sz(max_vid_btrt)}, stream OK)"
		else:
			# Re-encode with streaming-friendly logic
			encoder_opts = get_encoder_options(
				codec_name,
				pix_fmt.endswith("10le"),
				int(btrt),
				True  # use_hw_accel = True
			)
			ff_vid.extend(encoder_opts)
			skip_all = False
			extra_msg = f" Re-encode: {hm_sz(btrt)}"

		if needs_scaling:
			use_hw_accel = False # Set to True to use QSV scaling
			max_width  = 1920
			max_height = 1080

			# Use -1 to maintain aspect ratio automatically during scaling
			# flags=lanczos uses a high-quality scaling algorithm
			scale_chain = f"scale=-1:{max_height}:flags=lanczos"

			# A subtle sharpening filter to compensate for softness from scaling
			sharp_chain = "unsharp=5:5:0.5:5:5:0.2"

			extra_msg += f"| Scaling: {context.vid_width}x{context.vid_height} -> to {max_height}p"

			if use_hw_accel:
				# For QSV, use the vpp_qsv filter for hardware-accelerated scaling
				qsv_scale_chain = f"vpp_qsv=w=-1:h={max_height}"
				ff_vid.extend(["-vf", qsv_scale_chain])
				extra_msg += " (HW)"
			else:
				# For software, combine scaling and sharpening
				full_filter_chain = f"{scale_chain},{sharp_chain}"
				ff_vid.extend(["-vf", full_filter_chain])
				extra_msg += " (SW)"

			skip_all = False

		# Update handler name if needed
		desired_handler = "VideoHandler x265"
		if handler_name != desired_handler:
			ff_vid.extend([
				f"-metadata:s:v:{idx}",
				f"handler_name={desired_handler}"
			])
			skip_all = False
			extra_msg += f"|Handler => {desired_handler}"

		# Print log
		msg = (
			f"|<V:{idx:2}>|{codec_name:^8}|{context.vid_width}x{context.vid_height}|"
			f"{bit_depth_str}|Bitrate: {hm_sz(this_bitrate,'bits')}|Frames: {hm_sz(context.total_frames,'frames')}|{extra_msg}|"
		)
		print(f"\033[91m    {msg}\033[0m")

		ff_video += ff_vid

		if de_bug: print(f"Stream {idx} => {ff_vid}")

	if skip_all :
		if de_bug: print (f"Skipping video because skip_all={skip_all} and skip_it={skip_it}")
		print("   .Skip: Video")

	return ff_video, skip_all

###############################################################################
#                            PARSE AUDIO
###############################################################################
## This function REPLACES the parse_audio in YOUR FFMpeg.py
# It assumes globals like TmpF_Ex, Max_a_btr, Keep_langua, Default_lng, max_bitrate
# and functions like hm_sz, perf_monitor are available in FFMpeg.py's scope.
@perf_monitor
def parse_audio(
	streams: List[Dict[str, Any]],
	de_bug: bool = False
) -> Tuple[List[str], bool]:
	"""
	Parses and processes audio streams, with improved logic to correctly handle
	single, compliant audio streams to prevent re-encoding loops.
	"""
	ffmpeg_audio_options: List[str] = []
	all_skippable = True

	# --- Stage 1: Analyze Streams ---
	extracted_data = []
	best_english_stream_candidate = None
	original_default_stream_info = None

	for idx, audio_strm in enumerate(streams):
		s_data = {
			"input_idx": idx,
			"codec_name": audio_strm.get("codec_name", "unknown").lower(),
			"channels": int(audio_strm.get("channels", 0)),
			"bitrate": int(audio_strm.get("bit_rate", 0)),
			"language": audio_strm.get("tags", {}).get("language", "und"),
			"original_deflt": bool(audio_strm.get("disposition", {}).get("default", 0)),
			"will_be_output_default": False
		}
		extracted_data.append(s_data)
		if s_data["original_deflt"]:
			original_default_stream_info = s_data
		if s_data["language"] == Default_lng:
			if best_english_stream_candidate is None or s_data["channels"] > best_english_stream_candidate.get("channels", 0):
				best_english_stream_candidate = s_data

	# --- Stage 2: Determine Default Stream ---
	stream_to_make_default = best_english_stream_candidate or \
							(extracted_data[0] if len(extracted_data) == 1 else None) or \
							original_default_stream_info or \
							(extracted_data[0] if extracted_data else None)

	if stream_to_make_default:
		stream_to_make_default["will_be_output_default"] = True
		# Only consider changing the default flag a modification if there are MULTIPLE streams.
		# If there's only one stream, it will be default anyway, so we don't need to process the file for this.
		if len(extracted_data) > 1 and not stream_to_make_default["original_deflt"]:
			all_skippable = False

	# --- Stage 3: Generate FFmpeg Commands ---
	output_audio_idx = 0
	for data in extracted_data:
		input_idx = data["input_idx"]
		codec = data["codec_name"]
		channels = data["channels"]
		lang = data.get("language", "und")
		is_original_default = data["original_deflt"]

		is_compliant = (
			(codec == 'eac3' and channels == 6) or
			(codec == 'aac' and channels <= 6)
		)

		stream_opts = ["-map", f"0:a:{input_idx}"]
		log_action = ""

		if is_compliant:
			stream_opts.extend([f"-c:a:{output_audio_idx}", "copy"])
			log_action = "=> Copy (Compliant)"
		else:
			all_skippable = False # Re-encoding is always a modification
			if channels >= 6:
				stream_opts.extend([f"-c:a:{output_audio_idx}", "eac3", f"-b:a:{output_audio_idx}", "640k"])
				log_action = "=> Re-encode to E-AC3 640k"
			else:
				stream_opts.extend([f"-c:a:{output_audio_idx}", "aac", f"-q:a:{output_audio_idx}", "2"])
				log_action = "=> Re-encode to AAC (VBR q:2)"
				if channels > 2:
					stream_opts.extend([f"-ac:a:{output_audio_idx}", "2"])
					log_action += " [Stereo Mixdown]"

		log_disposition = ""
		if data["will_be_output_default"]:
			stream_opts.extend([f"-disposition:a:{output_audio_idx}", "default"])
			log_disposition = "|Is new default"
		else:
			stream_opts.extend([f"-disposition:a:{output_audio_idx}", "0"])
			if is_original_default:
				log_disposition = "|Was default (now cleared)"

		ffmpeg_audio_options.extend(stream_opts)

		bitrate = data["bitrate"]
		msg = (f"|<A:{input_idx:2}>|{codec:^8}|{lang:<3}|Br:{bitrate:>7}|"
			   f"Ch:{channels}|Dis:{int(is_original_default)}| {log_action}{log_disposition}")
		print(f"\033[92m    {msg}\033[0m")
		output_audio_idx += 1

	if all_skippable:
		print("    .Skip: Audio")
		return [], True

	return ffmpeg_audio_options, False

###############################################################################
#                           PARSE SUBTITLES
###############################################################################
# These two functions REPLACE _score_english_sub and parse_subtl in your FFMpeg.py
# They assume globals like Keep_langua and Default_lng are available.

def _score_subtitle(stream: Dict[str, Any]) -> int:
    """
    Calculates a "quality score" for a subtitle stream based on a set of heuristics.
    This version is more robust to handle cases where metadata is missing.
    """
    score = 100 # Base score for just existing as a valid candidate
    tags = stream.get("tags", {})
    title = tags.get("title", "").lower()
    disposition = stream.get("disposition", {})

    # Penalize "Forced" subtitles, as they are usually not the main track.
    if disposition.get("forced", 0) == 1 or "forced" in title:
        score -= 1000

    # Reward SDH (Subtitles for the Deaf and Hard of Hearing) for completeness.
    if "sdh" in title or "hearing impaired" in title:
        score += 500

    # Give a bonus to the stream that was originally the default.
    if disposition.get("default", 0) == 1:
        score += 200

    # Use bitrate and frame count as the strongest indicators of content amount.
    # ffprobe sometimes returns 0 for these in certain containers, but if it exists, it's valuable.
    score += int(stream.get("bit_rate", 0))
    score += int(stream.get("nb_frames", 0) or stream.get("nb_read_packets", 0))

    return score


@perf_monitor
def parse_subtl(
	streams_in: List[Dict[str, Any]],
	de_bug: bool = False
) -> Tuple[List[str], bool]:
	"""
    Parses, scores, and processes subtitle streams. It finds the single best
    stream for each desired language and uses a stable command structure,
    while also correctly determining if the section can be skipped.
	"""

	# --- Pass 1: Find the single best stream for each desired language ---
	best_streams_by_lang = {}
	for stream in streams_in:
		if stream.get("codec_type") != "subtitle":
			continue

		lang = stream.get("tags", {}).get("language", "und")
		codec = stream.get("codec_name", "unknown")

		if lang in Keep_langua and codec in ("subrip", "ass", "mov_text"):
			score = _score_subtitle(stream)
			if lang not in best_streams_by_lang or score > best_streams_by_lang[lang]['score']:
				best_streams_by_lang[lang] = {'score': score, 'stream': stream}

	streams_to_keep = sorted([data['stream'] for data in best_streams_by_lang.values()], key=lambda s: s['index'])
	indices_to_keep = {s['index'] for s in streams_to_keep}

	# --- Pass 2: Determine if any changes are needed (SKIP LOGIC) ---
	all_skippable = True

	# Count the number of original streams that are text-based and in our keep languages
	original_candidate_count = sum(1 for s in streams_in if s.get("codec_type") == "subtitle" and s.get("tags", {}).get("language", "und") in Keep_langua and s.get("codec_name") in ("subrip", "ass", "mov_text"))

	# If we are reducing the number of streams (e.g., from 2 english to 1), we can't skip.
	if len(streams_to_keep) != original_candidate_count:
		all_skippable = False
	else:
		# If the stream count is the same, check if their properties are already perfect.
		best_eng_stream = best_streams_by_lang.get(Default_lng, {}).get('stream')
		for stream in streams_to_keep:
			is_default = stream.get('disposition', {}).get('default', 0)

			# All kept streams must already be 'mov_text'.
			if stream.get('codec_name') != 'mov_text':
				all_skippable = False; break

			# The best English track must be default, and no others should be.
			if best_eng_stream and stream['index'] == best_eng_stream['index']:
				if not is_default: all_skippable = False; break
			elif is_default:
				all_skippable = False; break

	# --- Logging ---
	for stream in streams_in:
		if stream.get("codec_type") == "subtitle":
			lang = stream.get("tags", {}).get("language", "und")
			codec = stream.get("codec_name", "unknown")
			if stream['index'] in indices_to_keep:
				data = next((d for d in best_streams_by_lang.values() if d['stream']['index'] == stream['index']), None)
				if data:
					extra_msg = "|Set as Best Default" if best_streams_by_lang.get(Default_lng, {}).get('stream') == stream else ""
					msg = f"|<S:{stream['index']:2}>|{codec:^8}|{lang:3}| => Keep BEST for lang (Score: {int(data['score'])}){extra_msg}"
					print(f"\033[94m    {msg}\033[0m")
			else:
				msg = f"|<S:{stream['index']:2}>|{codec:^8}|{lang:3}| => Remove"
				print(f"\033[90m    {msg}\033[0m")

	if all_skippable:
		print("    .Skip: Subtitle section is already compliant.")
		return [], True

	# --- Pass 3: If not skippable, generate commands using YOUR WORKING structure ---
	ff_subttl = []

	# 1. Map all the streams we decided to keep.
	for stream in streams_to_keep:
		ff_subttl.extend(["-map", f"0:{stream['index']}"])

	# 2. Set dispositions for the MAPPED streams by their new output index.
	output_sub_idx = 0
	for stream in streams_to_keep:
		if best_streams_by_lang.get(Default_lng, {}).get('stream') == stream:
			ff_subttl.extend([f"-disposition:s:{output_sub_idx}", "default"])
		else:
			ff_subttl.extend([f"-disposition:s:{output_sub_idx}", "0"])
		output_sub_idx += 1

	# 3. Add a SINGLE, GLOBAL codec command at the end. THIS PREVENTS THE CRASH.
	if streams_to_keep:
		ff_subttl.extend(["-c:s", "mov_text"])

	return ff_subttl, False

###############################################################################
#                           PARSE EXTRA DATA
###############################################################################
@perf_monitor
def parse_extrd(
	streams_in: List[Dict[str, Any]],
	de_bug: bool = False
) -> Tuple[List[str], bool]:
	"""
	Parse data streams. Usually we discard them, or at least we map them carefully.
	"""
	if not streams_in: return [], True
	"""Parses and removes all data streams."""
	ff_data =[]
	for idx, d in enumerate(streams_in):
		codec_name		= d.get('codec_name','?')
		tags			= d.get("tags", {})
		handler_name	= tags.get("handler_name", "?")
		msj = f"\033[90m    |<D:{idx:2}>|{codec_name:^8}|"
		if handler_name == 'SubtitleHandler':
#			print("   .Skip: Extradata")
			msj += f" |Keep Subtitle |"
			print ( msj + "\033[0m")
			return [], True
		print ( msj + "\033[0m")

		ff_dd = ["-map", f"-0:d:{idx}"]
		ff_data += ff_dd
	return ff_data, True


###############################################################################
#                        PARSE FORMAT / MAIN PARSER
###############################################################################
@perf_monitor
def parse_frmat(
	input_file: str,
	metadata: Dict[str, Any],
	de_bug: bool
) -> Tuple[List[str], bool]:
	"""
	Parses the top-level 'format' and 'streams' from ffprobe data,
	then delegates to parse_video, parse_audio, parse_subtl, etc.
	Returns combined FFmpeg args and skip flag.
	"""
	_format  = metadata.get("format", {})
	_streams = metadata.get("streams", [])
	if not _format:
		raise ValueError(f"'format' not found in metadata:\n{json.dumps(metadata, indent=2)}")
	if not _streams:
		raise ValueError(f"'streams' not found in metadata:\n{json.dumps(metadata, indent=2)}")

	filename =       _format.get("filename", "No_file_name")
	size =           int(float(_format.get("size", 0)))
	duration =       int(float(_format.get("duration", 0.0)))
	bitrate =        int(_format.get("bit_rate", 0))
	nb_streams =     int(_format.get("nb_streams", 0))
	nb_programs =    int(_format.get("nb_programs", 0))

	tags =           _format.get("tags",{})
	f_comment =     tags.get("comment",	"No_Comment")
	title =         tags.get("title",	"No_title")

	# We build a context object for video
	context = VideoContext(
		vid_width=0,    # Will set later from parse_video
		vid_height=0,
		vid_length=duration,
		vid_bitrate=bitrate,
		total_frames=0
	)

	# Build stream groups
	streams_by_type = defaultdict(list)
	for s in _streams:
		codec_type = s.get("codec_type", "?")
		streams_by_type[codec_type].append(s)

	video_streams = streams_by_type["video"]
	audio_streams = streams_by_type["audio"]
	subtl_streams = streams_by_type["subtitle"]
	datax_streams = streams_by_type["data"]

	# Also build a stream_counts dict for your console print
	stream_counts = {
		"Strms": nb_streams, "V": len(video_streams), "A": len(audio_streams),
		"S": len(subtl_streams), "D": len(datax_streams),"Prog": nb_programs,
	}

	# Summaries for console
	fmrt_msj = (
		f"    |=Title|{title}|\n"
		f"    |<FRMT>|Size: {hm_sz(size)}|Bitrate: {hm_sz(bitrate)}|Length: {hm_time(duration)}|"
	)
	fmrt_msj += ''.join([f"{count}:{key}|" for key, count in stream_counts.items() if count != 0])
	print(f"\033[96m{fmrt_msj}\033[0m")

	# --- STEP 1: PERFORM FULL ANALYSIS OF ALL COMPONENTS ---
	ff_com = []

	v_skip = True
	if video_streams:
		first_vid = video_streams[0]
		context.vid_width	= first_vid.get("width", 2)
		context.vid_height	= first_vid.get("height", 1)
		v_cmd, v_skip = parse_video(video_streams, context, de_bug, False)
		ff_com.extend(v_cmd)
	else:
		print (f"No Video in: {input_file}")
		time.sleep(2)
		return [], True

	a_skip = True
	if audio_streams:
		a_cmd, a_skip = parse_audio(audio_streams, de_bug)
		ff_com.extend(a_cmd)
	else:
		print (f"No Audio in: {input_file}")
		time.sleep(2)
		return [], True

	s_skip = True
	if subtl_streams:
		s_cmd, s_skip = parse_subtl(subtl_streams, de_bug)
		ff_com.extend(s_cmd)
	else:
		s_cmd, s_skip = add_subtl_from_file(input_file, de_bug)
		ff_com.extend(s_cmd)

	d_skip = True
	if datax_streams:
		d_cmd, d_skip = parse_extrd(datax_streams, de_bug)
		ff_com.extend(d_cmd)

	# --- STEP 2: MAKE THE FINAL, INTELLIGENT SKIP DECISION ---

	# First, check if all components meet the current rules.
	all_components_ok = v_skip and a_skip and s_skip and d_skip

	# Now, check if the file was previously processed.
	already_processed = (f_comment == Skip_key)

	# The final decision: Skip only if the file was already processed AND all components are still OK.
	final_skip = already_processed and all_components_ok

	# Provide clear feedback on the decision
	if final_skip:
		print("   .Skip: File was already processed and all components meet current rules.")
	elif already_processed and not all_components_ok:
		print("   .Process: File was processed before, but some components no longer meet current rules.")
	elif not already_processed:
		print("   .Process: New file, processing required.")

	return ff_com, final_skip

###############################################################################
#                        ADD SUBTITLE FROM FILE
###############################################################################
@perf_monitor
def add_subtl_from_file(input_file: str, de_bug: bool) -> Tuple[List[str], bool]:
	"""
	Searches for a subtitle file in the same directory as the input file.
	Returns a (list_of_subtitle_args, skip_flag).
	"""
	print(f"    No Subtitle in: {input_file}")
	largest_file = None
	extensions = [".srt", ".ass", ".sub"]
	directory = os.path.dirname(input_file)
	for file in os.listdir(directory):
		if any(file.endswith(ext) for ext in extensions):
			filepath = os.path.join(directory, file)
			if not largest_file or os.path.getsize(filepath) > os.path.getsize(largest_file):
				largest_file = filepath

	if largest_file:
		if de_bug:
			print (f"Found external subtitle: {largest_file}")
		# If you want to auto-add:
		# ff_sub = ["-i", largest_file, "-map", "1:0", "-c:s", "mov_text"]
		# return ff_sub, False
		# For now, just skip:
		return [], True
	else:
		if de_bug:
			print ("No external subtitle found.")
	return [], True

###############################################################################
#                           FILE INFO PARSER
###############################################################################
@perf_monitor
def parse_finfo(input_file: str, metadata: Dict[str, Any], de_bug: bool = False) -> Tuple[Any, bool]:
	"""
	High-level entry point that uses parse_frmat to get the combined ffmpeg command & skip flag.
	"""
	if not input_file or not metadata:
		print("parse_finfo: missing file or metadata")
		return [], True

	ff_run_cmd, skip_it = parse_frmat(input_file, metadata, de_bug)

	return ff_run_cmd, skip_it

###############################################################################
#                          ADDITIONAL UTILITIES
###############################################################################
# Precompiled regex for ffmpeg progress
regex_dict = {
	"bitrate": re.compile(r"bitrate=\s*([0-9\.]+)"),
	"frame":   re.compile(r"frame=\s*([0-9]+)"),
	"speed":   re.compile(r"speed=\s*([0-9\.]+)"),
	"size":    re.compile(r"size=\s*([0-9]+)"),
	"time":    re.compile(r"time=([0-9:.]+)"),  # Updated
	"fps":     re.compile(r"fps=\s*([0-9]+)")
}

def extract_progress_data(line_to: str) -> dict:
	"""Extracts relevant progress data using regular expressions."""
	regx_val = {}
	try:
		for key, rx in regex_dict.items():
			match = rx.search(line_to)
			if match:
				regx_val[key] = match.group(1)
	except Exception as e:
		print(f"Error extracting progress data: {e}")
	return regx_val

def calculate_eta(regx_val: dict, sp_float: float) -> str:
	"""
	Calculates ETA from 'time' and 'speed'.
	Depends on global 'glb_vidolen' (total video length in seconds).
	"""
	try:
		time_str = regx_val.get("time", "00:00:00.00")
		parts = time_str.split(".")
		hh, mm, ss = map(int, parts[0].split(":"))
		fractional = float("0." + parts[1]) if len(parts) > 1 else 0.0
		a_sec = hh * 3600 + mm * 60 + ss + fractional
#		glb_vidolen = 0
		# 'glb_vidolen' is assumed to be defined globally
		dif = max(glb_vidolen - a_sec, 0.0)
		eta = dif / sp_float if sp_float > 0 else 9999.0
		eta = round(eta)
		mints, secs = divmod(eta, 60)
		hours, mints = divmod(mints, 60)
		return f"{hours:02d}:{mints:02d}:{secs:02d}"
	except Exception as e:
		print(f"Error calculating ETA: {e}")
		return "00:00:00"

def show_progrs(line_to: str, sy: str, de_bug: bool = False) -> bool:
	"""
	Parses ffmpeg progress lines to display a spinner plus stats (fps, speed, etc.).
	:param line_to: A single line of ffmpeg progress.
	:param sy:      Spinner or status symbol to show.
	:param de_bug:  If True, prints raw lines (debug mode).
	:return:
	  True  -> Keep reading lines,
	  False -> Stop if "N/A" found,
	  -1    -> Done if "muxing overhead" or "global headers" found.
	"""
	# 1) Debug: print the raw line and continue
	if de_bug :
		print(line_to)
		return True

	# 2) If the line has "N/A", we stop
	if "N/A" in line_to:
		line_to = line_to.rstrip('\r\n')  # Remove all trailing '\r' and '\n'
		disp_str =f"    | {sy} | {line_to}               \r"
		sys.stderr.write(disp_str)
		sys.stderr.flush()
#		print (disp_str, end='')
		return False

	# 3) If the line has all of: ["fps=", "speed=", "size="]
	elif all(x in line_to for x in ["fps=", "speed=", "size="]):
		regx_val = extract_progress_data(line_to)
		try:
			fp_int = int(regx_val.get("fps", 0))
			sp_float = float(regx_val.get("speed", 0))
			eta_str = calculate_eta(regx_val, sp_float)

			disp_str = (
				f"    | {sy} "
				f"|Size: {hm_sz(regx_val.get('size', '0')):>10}"
				f"|Frames: {int(regx_val.get('frame', 0)):>7}"
				f"|Fps: {fp_int:>4}"
				f"|BitRate: {hm_sz(regx_val.get('bitrate', '0')):>6}"
				f"|Speed: {sp_float:>5}"
				f"|ETA: {eta_str:>9}|                   \r"
			)
			sys.stderr.write(disp_str)
			sys.stderr.flush()
	#		print (disp_str, end='')

		except Exception as e:
			print(f"show_progrs exception: {e} in line: {line_to}")

	# 4) Else if the line has all of: ["time=", "speed=", "size="] (but no "fps=")
	elif all(x in line_to for x in ["time=", "speed=", "size="]):
		regx_val = extract_progress_data(line_to)
		try:
			sp_float = float(regx_val.get("speed", 0))
			eta_str = calculate_eta(regx_val, sp_float)

			disp_str = (
				f"    | {sy} "
				f"|Size: {hm_sz(regx_val.get('size', '0')):>10}"
				f"|BitRate: {hm_sz(regx_val.get('bitrate', '0')):>6}"
				f"|ETA: {eta_str:>9}|                   \r"
			)
			sys.stderr.write(disp_str)
			sys.stderr.flush()
#			print (disp_str, end='')

		except Exception as e:
			print(f"show_progrs exception: {e} in line: {line_to}")

	# 5) Else if any of ["muxing overhead:", "global headers:"] -> done
	elif any(x in line_to for x in ["muxing overhead:", "global headers:"]):
#		Done: [out#0/mp4 @ 0000026f47d68e80] video:603557KiB audio:181972KiB subtitle:82KiB other streams:0KiB global headers:14KiB muxing overhead: 0.504575%
		line_to = line_to.rstrip('\r\n')  # Remove all trailing '\r' and '\n'
		print(f"   .Done: {line_to[31:]} |  ")
		return -1

	# 6) Otherwise, return True to continue reading lines
	return True


###############################################################################
#                          EXAMPLE EXTRA FUNCTIONS
###############################################################################
@perf_monitor
def matrix_it(
	input_file: str,
	execu: str = ffmpeg,
	ext: str = ".png",
	columns: int = 3,
	rows: int = 3
) -> bool:
	"""
	Create a 'columns x rows' collage from frames in the input video. Default is 3x3.
	We use FFmpeg's 'select' filter to pick frames and 'tile' filter to arrange them.
	The result is a single image (e.g., .png).
	"""
	if de_bug : print(f"matrix_it: creating a {columns}x{rows} collage for {input_file}")
	# e.g. glb_totfrms, vid_width
	base_name, _ = os.path.splitext(os.path.basename(input_file))
	out_file = base_name + "_matrix" + ext
	# We'll keep it close to what you had, but ideally these come from a context object.
	select_expr = "select='not(mod(n,300))'"
	tile_expr   = f"tile={columns}x{rows}:padding=5:margin=5"
	vf_filter   = f"[0:v]{select_expr},{tile_expr}"
	cmd = [
		execu,
		"-i", input_file,
		"-frames:v", "1",           # Only produce one output image
		"-vf", vf_filter,
		"-vsync", "vfr",
		"-y", out_file
	]
	if run_ffm(cmd):
		if de_bug : print(f"Matrix collage created: {out_file}")
		return True
	else:
		print("Failed to create matrix collage.")
		return False

@perf_monitor
def speed_up(input_file: str, factor: float = 4.0, de_bug: bool = False) -> Optional[str]:
	"""
	Create a 'factor'x speed version of the input video using FFmpeg filters.
	E.g., factor=4.0 => 4x speed (25% the original duration).
	"""
	if de_bug : print(f"speed_up: input={input_file} factor={factor:.2f}")
	# Generate output file name
	base_name, _ = os.path.splitext(os.path.basename(input_file))
	out_file = base_name + f"_speed{int(factor)}x.mp4"
	if factor <= 2.0:
		atempo_filter = f"atempo={factor}"
	elif factor <= 4.0:
		atempo_part1 = 2.0
		atempo_part2 = factor / 2.0
		atempo_filter = f"atempo={atempo_part1},atempo={atempo_part2}"
	else:
		atempo_filter = "atempo=2.0,atempo=2.0"  # 4x
	vf_expr = f"setpts=PTS/{factor}"
	af_expr = atempo_filter
	cmd = [
		ffmpeg, "-i", input_file,
		"-filter_complex", f"[0:v]{vf_expr}[v];[0:a]{af_expr}[a]",
		"-map", "[v]",
		"-map", "[a]",
		"-y", out_file
	]
	if run_ffm(cmd, de_bug=de_bug):
		if de_bug : print(f"Speeded-up file created: {out_file}")
		return out_file
	else:
		print("Failed to speed-up file.")
		return None

@perf_monitor
def video_diff(file1: str, file2: str, de_bug: bool = False) -> Optional[str]:
	"""
	Visually compare two videos by showing the pixel difference (blend=all_mode=difference).
	Produces an output diff.mp4 or something similar.
	"""
	if de_bug : print(f"video_diff: comparing {file1} vs {file2}")
	base_name, _ = os.path.splitext(os.path.basename(file1))
	out_file = base_name + "_diff.mp4"
	cmd = [
		ffmpeg,
		"-i", file1,
		"-i", file2,
		"-filter_complex", "blend=all_mode=difference",
		"-c:v", "libx265",
		"-preset", "faster",
		"-c:a", "copy",
		"-y", out_file
	]
	if run_ffm(cmd, de_bug=de_bug):
		if de_bug : print(f"Difference file created: {out_file}")
		return out_file
	else:
		print("Failed to create difference file.")
		return None

@perf_monitor
def short_ver(
	input_file: str,
	start: str = "00:00:00",
	duration: str = "00:05:00",
	execu: str = ffmpeg,
	de_bug: bool = False
) -> Optional[str]:
	"""
	Make a short version of the input file, starting at 'start' timecode,
	for 'duration'. Example: short_ver('in.mp4', start="00:00:30", duration="00:01:00")
	"""
	if de_bug : print(f"short_ver: input={input_file} start={start} duration={duration}")
	base_name, _ = os.path.splitext(os.path.basename(input_file))
	out_file = base_name + "_short.mp4"
	cmd = [
		execu,
		"-ss", start,          # jump to start
		"-i", input_file,
		"-t", duration,        # record for 'duration' seconds (or h:mm:ss)
		"-c:v", "copy",
		"-c:a", "copy",
		"-y", out_file
	]
	if run_ffm(cmd, de_bug=de_bug):
		if de_bug : print(f"Short version created: {out_file}")
		return out_file
	else:
		print("Failed to create short version.")
		return None

###############################################################################
#                                 USAGE EXAMPLE
###############################################################################
def main():
	"""
	Demonstration entry point (not mandatory).
	You can adapt to your actual usage.
	"""
	input_file = r"test_video.mp4"
	metadata = ffprobe_run(input_file, de_bug=False)

	# 1) Parse & gather FFmpeg commands
	ff_run_cmd, skip_it = parse_finfo(input_file, metadata, de_bug=False)

	# 2) If skip_it, do nothing
	if skip_it:
		if de_bug : print(f"Skipping entire file: {input_file}")
		return

	# 3) Otherwise, run ffmpeg
	out_file = ffmpeg_run(input_file, ff_run_cmd, skip_it, execu=ffmpeg, de_bug=False)
	if out_file:
		if de_bug : print(f"Output file: {out_file}")
	else:
		print("No output created.")

if __name__ == "__main__":
	print ("Testing")
#    main()
