# -*- coding: utf-8 -*-
import os
import re
import sys
import json
import time
import logging
import subprocess as SP
import datetime as TM

from collections import defaultdict
from dataclasses import dataclass
from typing import List, Dict, Tuple, Any, Optional

# External references from your code
from My_Utils import *  # e.g. hm_sz, hm_time, divd_strn, stmpd_rad_str, perf_monitor
from Yaml import *      # if needed

# Set up logging (simple example)
logging.basicConfig(
	level=logging.WARNING,
	format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
	datefmt="%H:%M:%S"
)
logger = logging.getLogger(__name__)

# Paths to FFmpeg/FFprobe executables (update if needed)
ffmpg_bin = r"C:\Program Files\ffmpeg\bin"
ffmpeg = os.path.join(ffmpg_bin, "ffmpeg.exe")
ffprob = os.path.join(ffmpg_bin, "ffprobe.exe")

if not os.path.exists(ffmpeg) or not os.path.exists(ffprob):
	raise OSError(f"FFmpeg or FFprobe not found in {ffmpg_bin}.")

logger.info(
	"Ffmpeg version: %s",
	SP.run([ffmpeg, "-version"], stdout=SP.PIPE)
	  .stdout.decode("utf-8")[15:20]
)

# Some global or shared constants (rather than global variables)
Skip_key = "AlreadyEncoded"
TmpF_Ex = "_out.mp4"
File_extn = (".mp4", ".mkv", ".avi", ".mov", ".flv")  # whichever you allow
Keep_langua = ("eng", "fre", "spa")  # example of keep-languages for subtitles

###############################################################################
#                          DATA CLASSES & CONTEXT
###############################################################################
@dataclass
class VideoContext:
	"""Holds metadata about the video file being processed."""
	vid_width: int = 0
	vid_height: int = 0
	vid_length: int = 0    # total duration in seconds
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
			logger.error("ffprobe error: %s", err_msg)
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
	logger.debug("run_ffm: %s", " ".join(args))

	if de_bug:
		print("run_ffm: %s", " ".join(args))

	if not args:
		logger.error("run_ffm: No arguments provided.")
		return False

	try:
		if de_bug:
			# Debug mode: Capture stdout/stderr and print them
			print ("Running ffmpeg in debug mode.\n%s", " ".join(args))
			process = SP.run(args, stdout=SP.PIPE, stderr=SP.STDOUT)
			print ("Stdout:\n%s", process.stdout.decode("utf-8", errors="replace"))
			if process.returncode != 0:
				print ("FFmpeg failed with return code %d", process.returncode)
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
				print ("FFmpeg failed with return code %d", process.returncode)
				return False
		return True

	except Exception as e:
		logger.exception("run_ffm exception: %s", e)
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

#    logger.info("ffmpeg_run start: %s", input_file)

	file_name, _ = os.path.splitext(os.path.basename(input_file))
	out_file = os.path.normpath("_" + stmpd_rad_str(7, file_name[0:25]))
	out_file = re.sub(r"[^\w\s_-]+", "", out_file).strip().replace(" ", "_") + TmpF_Ex

	# Attempt to get ffmpeg version
	try:
		ffmpeg_vers = SP.check_output([execu, "-version"]).decode("utf-8").splitlines()[0].split()[2]
	except SP.CalledProcessError as e:
		logger.error("Error getting ffmpeg version: %s", e)
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
		"-metadata",    "copyright=2024",
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
				logger.warning("Retry attempt %d...", attempt)
				# In debug mode for the second attempt
				ff_head = [execu, "-report", "-loglevel", "verbose", "-i", input_file, "-hide_banner"]

			full_cmd = ff_head + ff_com + ff_tail
			if run_ffm(full_cmd, de_bug=de_bug):
				logger.info("Successfully created file: %s", out_file)
				return out_file

		except Exception as e:
			logger.error("ffmpeg_run attempt %d failed: %s", attempt, e)

		if attempt == max_retries:
			msg = f"Failed after {attempt} attempts."
			logger.error(msg)
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
	Returns a list of FFmpeg arguments to encode with x265 or QSV for streaming-friendly output.
	"""
	# Keep your existing 'target_quality' = 'as_is'
	target_quality = "as_is"

	quality_presets = {
		"low": {
			"bitrate": (bit_rate // (1024 * 3)),
			"quality": 26
		},
		"medium": {
			"bitrate": (bit_rate // (1024 * 1.5)),
			"quality": 24
		},
		"as_is": {
			"bitrate": (bit_rate // 1024),
			"quality": 21
		},
		"high": {
			"bitrate": (bit_rate // (1024 * 0.75)),
			"quality": 20
		},
		"higher": {
			"bitrate": (bit_rate // (1024 * 0.5)),
			"quality": 18
		}
	}

	preset = quality_presets[target_quality]
	base_target_bitrate = int(preset["bitrate"])
	global_quality = preset["quality"]

	# If 10-bit source, bump up the bitrate slightly
	if is_10bit:
		base_target_bitrate = int(base_target_bitrate * 1.25)

	# Convert to 'k'
	target_bitrate = f"{base_target_bitrate}k"
	max_bitrate_int = int(base_target_bitrate * 1.5)
	max_bitrate = f"{max_bitrate_int}k"
	bufsize_int = max_bitrate_int * 2
	bufsize = f"{bufsize_int}k"

	# Hardware QSV path
	if use_hw_accel:
		hw_pix_fmt = "p010le" if is_10bit else "nv12"
		return [
			"hevc_qsv",
			"-load_plugin", "hevc_hw",
			"-init_hw_device", "qsv=qsv:MFX_IMPL_hw_any",
			"-filter_hw_device", "qsv",
			"-pix_fmt", hw_pix_fmt,
			"-b:v", target_bitrate,
			"-maxrate", max_bitrate,
			"-bufsize", bufsize,
			"-look_ahead", "1",
			"-look_ahead_depth", "90",
			"-global_quality", str(round(global_quality)),
			"-rc:v", "vbr_la",
			"-preset", "slow"
		]

	# Software x265 path
	sw_pix_fmt = "yuv420p10le" if is_10bit else "yuv420p"
	x265_params_str = (
		"open-gop=0:"
		"keyint=60:"
		"min-keyint=30:"
		"scenecut=40:"
		"bframes=3:"
		"b-adapt=2:"
		"psy-rd=1:"
		"aq-mode=3:"
		"aq-strength=0.8:"
		"deblock=1,1:"
		"me=umh:"
		"subme=7"
	)

	return [
		"libx265",
		"-x265-params", x265_params_str,
		"-pix_fmt", sw_pix_fmt,
		"-crf", str(round(global_quality)),
		"-b:v", target_bitrate,
		"-maxrate", max_bitrate,
		"-bufsize", bufsize,
		"-preset", "slow"
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

	logger.debug("parse_video start, skip_it=%s", skip_it)
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

		if codec_name.lower() == 'mjpeg':
			print (f"    |<V:{idx:2}>| > Skip {codec_name:6} |" )
#            ff_video.extend (['-map', '0:' + str(idx) + '?'])
			skip_all = False
			continue

		# Compute total frames
		context.total_frames = round(frm_rate * context.vid_length)

		# Decide maximum allowed bitrate
		max_vid_btrt = 3620000
		bit_depth_str = "8-bit"
		if pix_fmt.endswith("10le"):
			bit_depth_str = "10-bit"
			max_vid_btrt = int(max_vid_btrt * 1.25)

		# Final target bitrate for this stream
		btrt = min(this_bitrate * 1.1, max_vid_btrt)

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
		#  - Must be HEVC
		#  - Must have bitrate <= max
		#  - Must not need scaling
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

		# If scaling is needed, we must re-encode
		if needs_scaling:
			new_w = 1920
			new_h = round((1920 / context.vid_width) * context.vid_height / 2) * 2
			ff_vid.extend([
				"-vf", f"scale={new_w}:{new_h}",
				"-pix_fmt", "yuv420p10le",
				"-crf", "21",
				"-b:v", f"{max_vid_btrt}",
				"-preset", "slow"
			])
			skip_all = False
			extra_msg += f"| Scale {context.vid_width}x{context.vid_height} -> {new_w}x{new_h}"

		# Update handler name if needed
		desired_handler = "VideoHandler x265"
		if handler_name != desired_handler:
			ff_vid.extend([
				f"-metadata:s:v:{idx}",
				f"handler_name={desired_handler}"
			])
			skip_all = False
			extra_msg += f"| Handler => {desired_handler}"

		# Print log
		msg = (
			f"|<V:{idx:2}>|{codec_name:^8}|{context.vid_width}x{context.vid_height}|"
			f"{bit_depth_str}|Bitrate: {hm_sz(this_bitrate)}|Frames: {context.total_frames}|{extra_msg}|"
		)
		print(f"\033[91m    {msg}\033[0m")

		ff_video += ff_vid

		if de_bug:
			logger.debug("Stream %s => %s", idx, ff_vid)

	if skip_all :
		logger.info("Skipping video because skip_all=%s and skip_it=%s", skip_all, skip_it)
		print("   .Skip: Video")

	return ff_video, skip_all

###############################################################################
#                            PARSE AUDIO
###############################################################################
@perf_monitor
def parse_audio(
	streams: List[Dict[str, Any]],
	de_bug: bool = False
) -> Tuple[List[str], bool]:
	"""
	Parse and process audio streams, prioritizing the best English stream
	based on channels and bitrate.
	"""
	ffmpeg_audio_options: List[str] = []
	all_skippable = True

	best_stream = None
	prev_default_idx = None
	extracted_data = []

	# Analyze streams
	for idx, audio_stream in enumerate(streams):
		codec_name = audio_stream.get("codec_name", "unknown")
		channels = int(audio_stream.get("channels", -1))
		bitrate = int(audio_stream.get("bit_rate", 0))
		language = audio_stream.get("tags", {}).get("language", "und")
		disposition = audio_stream.get("disposition", {})
		dispo_default = int(disposition.get("default", 0))
		handler_name = audio_stream.get("tags", {}).get("handler_name", "Unknown")
		sample_rate = audio_stream.get("sample_rate", "N/A")

		extracted_data.append({
			"codec_name": codec_name,
			"channels": channels,
			"bitrate": bitrate,
			"language": language,
			"dispo_default": dispo_default,
			"handler_name": handler_name,
			"sample_rate": sample_rate
		})
		if dispo_default:
			prev_default_idx = idx

		# Pick best English by channels then bitrate
		if language == "eng":
			if (best_stream is None
				or channels > best_stream["channels"]
				or (channels == best_stream["channels"] and bitrate > best_stream["bitrate"])):
				best_stream = extracted_data[-1]

	# Assign disposition & generate ffmpeg commands
	for idx, data in enumerate(extracted_data):
		copy_codec = (
			data["codec_name"] in ("aac", "vorbis", "mp3", "opus")
			and data["channels"] <= 8
		)
		stream_opts = ["-map", f"0:a:{idx}"]

		if copy_codec:
			stream_opts.extend([f"-c:a:{idx}", "copy"])
		else:
			# re-encode to e.g. libvorbis
			stream_opts.extend([f"-c:a:{idx}", "libvorbis", "-q:a", "8"])

		# Update metadata if needed
		if data["handler_name"] != "SoundHandler":
			stream_opts.extend([
				f"-metadata:s:a:{idx}",
				"handler_name=SoundHandler"
			])

		# Disposition
		if data == best_stream:
			stream_opts.extend([f"-disposition:a:{idx}", "default"])
			if prev_default_idx is None or best_stream != extracted_data[prev_default_idx]:
				all_skippable = False
		else:
			stream_opts.extend([f"-disposition:a:{idx}", "none"])

		ffmpeg_audio_options.extend(stream_opts)

		msg = (
			f"|<A:{idx:2}>|{data['codec_name']:^8}|"
			f"Br:{data['bitrate']:>7}|{data['language']}|"
			f"Frq:{data['sample_rate']:>6}|Ch:{data['channels']}|"
			f"Dis:{data['dispo_default']}|Handler:{data['handler_name']}"
		)
		# Additional notes
		if data["dispo_default"]:
			msg += "|Was default"
		if data == best_stream:
			msg += "|Is new default"
		print(f"\033[92m    {msg}\033[0m")

	# Decide skip
	skip_audio = (
		len(streams) == 1
		or (best_stream and prev_default_idx is not None and best_stream == extracted_data[prev_default_idx])
	)
	if skip_audio:
		all_skippable = True
		logger.info("Skipping Audio")
		print("   .Skip: Audio")

	if de_bug:
		logger.debug("Audio opts: %s", ffmpeg_audio_options)

	return ffmpeg_audio_options, all_skippable

###############################################################################
#                           PARSE SUBTITLES
###############################################################################
# Example: Some global or previously defined variable
#Keep_langua = ["eng", "spa", "fre"]  # Adjust to your needs

def _score_english_sub(codec_name: str, disposition: Dict[str, int], handler_name: str) -> int:
    """
    Assign a 'score' to an English subtitle. Higher means more preferred.
    You can customize these rules as you see fit.
    """
    score = 0
    # Prefer mov_text over subrip
    if codec_name == "mov_text":
        score += 3
    elif codec_name == "subrip":
        score += 2

    # Bonus points if it's forced or default in the source
    if disposition.get("forced", 0) == 1:
        score += 2
    if disposition.get("default", 0) == 1:
        score += 1

    # Bonus if the handler_name is mov_text
    if handler_name.lower() == "mov_text":
        score += 1

    return score


def parse_subtl(
    streams_in: List[Dict[str, Any]],
    de_bug: bool = False
) -> Tuple[List[str], bool]:
    """
    Parse and extract data from subtitle streams, removing some, keeping others.
    Enhanced so that the "best" English subtitle is forced to be default.
    """

    ff_subttl = []
    all_skippable = True

    # First, find the "best" English subtitle (highest score).
    best_eng_idx = -1
    best_eng_score = float("-inf")

    for idx, this_sub in enumerate(streams_in):
        # Extract fields we'll need for scoring
        codec_name =	this_sub.get("codec_name", "unknown?")
        disposition =	this_sub.get("disposition", {"forced": 0, "default": 0})
        tags =			this_sub.get("tags", {})
        handler_name =	tags.get("handler_name", "Unknown")
        language =		tags.get("language", "und")

        # Only compute a score if it's subrip/mov_text AND English
        if codec_name in ("subrip", "mov_text") and language == "eng":
            s = _score_english_sub(codec_name, disposition, handler_name)
            if s > best_eng_score:
                best_eng_score = s
                best_eng_idx = idx

    #
    # Now do the main pass: keep/remove subtitles and set dispositions.
    #
    for idx, this_sub in enumerate(streams_in):
        ff_sub = []
        extra = ""
        metadata_changed = False

        codec_name =	this_sub.get("codec_name", "unknown?")
        codec_type =	this_sub.get("codec_type", "unknown?")
        disposition =	this_sub.get("disposition", {"forced": 0, "default": 0})
        tags =			this_sub.get("tags", {})
        handler_name =	tags.get("handler_name", "Unknown")
        language =		tags.get("language", "und")

        if codec_name in ("hdmv_pgs_subtitle", "dvd_subtitle", "ass", "unknown?"):
            # Remove these
            ff_sub = ["-map", f"-0:s:{idx}"]
            extra += f" Delete: {codec_name} {language} |"
            all_skippable = False

        elif codec_name in ("subrip", "mov_text"):
            # Keep subrip/mov_text
            ff_sub = ["-map", f"0:s:{idx}"]

            # Is this English?
            if language == "eng":
                extra += f"Keep: {codec_name} {language}|"

                # If this stream is the best English one, set default
                if idx == best_eng_idx and best_eng_idx != -1:
                    extra += "Set to Default|"
                    ff_sub.extend([
                        f"-c:s:{idx}", "mov_text",
                        f"-metadata:s:s:{idx}", f"language={language}",
                        f"-disposition:s:s:{idx}", "default"
                    ])
                else:
                    # It's English but not the best
                    extra += "Not Default|"
                    ff_sub.extend([
                        f"-c:s:{idx}", "mov_text",
                        f"-metadata:s:s:{idx}", f"language={language}",
                        f"-disposition:s:s:{idx}", "0"
                    ])

            # If it's a non-English language we want to keep
            elif language in Keep_langua:
                extra += f"Keep: {codec_name} {language}"
                ff_sub.extend([
                    f"-c:s:{idx}", "mov_text",
                    f"-metadata:s:s:{idx}", f"language={language}",
                    f"-disposition:s:s:{idx}", "0"
                ])

            else:
                # Remove any other languages not in Keep_langua
                ff_sub = ["-map", f"-0:s:{idx}"]
                extra += f" Delete: {codec_name} {language} X"

            # Handler name fix
            if handler_name != "mov_text":
                extra += f" handler_name: {handler_name} -> mov_text"
                ff_sub.extend([
                    f"-metadata:s:s:{idx}",
                    "handler_name=mov_text"
                ])
                metadata_changed = True

        # Otherwise, remove unrecognized formats (if any)
        else:
            ff_sub = ["-map", f"-0:s:{idx}"]
            extra += f" Delete: {codec_name} {language} X"
            all_skippable = False

        # Build the debug message in your original style
        msg = (
            f"|<S:{idx:2}>|{codec_name[:8]:^8}|{codec_type[:8]}|"
            f"{language:3}|Disp: default={disposition.get('default',0)}, forced={disposition.get('forced',0)}|"
            f"{extra}"
        )
        print(f"\033[94m    {msg}\033[0m")

        ff_subttl += ff_sub
        if metadata_changed:
            all_skippable = False

    # Force mov_text for all kept subtitles (global setting)
    ff_subttl.extend(["-c:s", "mov_text"])

    # If we ended up removing everything, all_skippable might be True
    if all_skippable:
        logger.info("Skipping Subtitles")
        print("   .Skip: Subtitle")

    return ff_subttl, all_skippable

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
	ff_data = []
	skip_all = True
	for idx, data_stream in enumerate(streams_in):
		codec_name = data_stream.get("codec_name", "")
		codec_long_name = data_stream.get("codec_long_name", "")
		codec_type = data_stream.get("codec_type", "")
		tags = data_stream.get("tags", {})
		handler_name = tags.get("handler_name", "Unknown")

		msj = f"    |<D:{idx:2}>|{codec_name:^8}| {codec_long_name:<9}| {codec_type:^11} | {handler_name}"
		if handler_name == 'SubtitleHandler':
			msj += "|Keep Subtitle|"
			print(msj)
			print ("   .Skip: Data")
			return [], True

		# Potential logic: keep or remove
		ff_dd = ["-map", f"-0:d:{idx}"]
		ff_data += ff_dd
		# If you do something special with data, put it here

	return ff_data, skip_all

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
	size =          int(float(_format.get("size", 0)))
	duration =      int(float(_format.get("duration", 0.0)))
	bitrate =       int(_format.get("bit_rate", 0))
	nb_streams =    int(_format.get("nb_streams", 0))
	nb_programs =   int(_format.get("nb_programs", 0))

	tags =           _format.get("tags", {})
	f_comment =     tags.get("comment", "No_comment")
	title =         tags.get("title", "No_title")

	# We build a context object for video
	context = VideoContext(
		vid_width=0,    # Will set later from parse_video
		vid_height=0,
		vid_length=duration,
		vid_bitrate=bitrate,
		total_frames=0
	)

	# For logging
	summary_msg = (
		f"Title={title} | "
		f"Size={hm_sz(size)} | "
		f"Bitrate={hm_sz(bitrate)} | "
		f"Length={hm_time(duration)} | "
		f"Streams={nb_streams} | "
		f"Programs={nb_programs}"
	)
	logger.info("parse_frmat: %s", summary_msg)

	# Build stream groups
	from collections import defaultdict
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
		"V": len(video_streams),
		"A": len(audio_streams),
		"S": len(subtl_streams),
		"D": len(datax_streams),
		"Prog": nb_programs,
		"Strms": nb_streams
	}

	# Summaries for console
	summary_msg = (
		f"    |=Title|{title}|\n"
		f"    |>FRMT<|Size: {hm_sz(size)}|Bitrate: {hm_sz(bitrate)}|Length: {hm_time(duration)}|"
	)
	summary_msg += ''.join([f"{key}:{count}|" for key, count in stream_counts.items() if count != 0])
	print(f"\033[96m{summary_msg}\033[0m")

	# Skip logic
	fnam, ext = os.path.splitext(os.path.basename(input_file))
	good_fname = (fnam == title and ext == ".mp4")
	skip_it = (good_fname and f_comment == Skip_key)

	if skip_it:
		print("   .Skip: Format")

	# Parse video
	ff_com = []
	v_skip = True
	if video_streams:
		# We set context.vid_width / context.vid_height from the first video stream
		first_vid = video_streams[0]
		context.vid_width = first_vid.get("width", 2)
		context.vid_height = first_vid.get("height", 1)

		v_cmd, v_skip = parse_video(video_streams, context, de_bug, skip_it)
		ff_com.extend(v_cmd)
	else:
		logger.warning("No Video in: %s", input_file)
		time.sleep(2)
		return [], True

	# Parse audio
	a_skip = True
	if audio_streams:
		a_cmd, a_skip = parse_audio(audio_streams, de_bug)
		ff_com.extend(a_cmd)
	else:
		logger.warning("No Audio in: %s", input_file)
		time.sleep(2)
		return [], True

	# Parse subtitles
	s_skip = True
	if subtl_streams:
		s_cmd, s_skip = parse_subtl(subtl_streams, de_bug)
		ff_com.extend(s_cmd)
	else:
		# Try external subtitles?
		s_cmd, s_skip = add_subtl_from_file(input_file, de_bug)
		ff_com.extend(s_cmd)

	# Parse data
	d_skip = True
	if datax_streams:
		d_cmd, d_skip = parse_extrd(datax_streams, de_bug)
		ff_com.extend(d_cmd)
		# we might do skip logic, etc.

	# Combine skip logic
	final_skip = skip_it and v_skip and a_skip and s_skip and d_skip
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
	directory = os.path.dirname(input_file)
	largest_file = None
	extensions = [".srt", ".ass", ".sub"]

	for file in os.listdir(directory):
		if any(file.endswith(ext) for ext in extensions):
			filepath = os.path.join(directory, file)
			if not largest_file or os.path.getsize(filepath) > os.path.getsize(largest_file):
				largest_file = filepath

	if largest_file:
		if de_bug:
			logger.debug("Found external subtitle: %s", largest_file)
		# If you want to auto-add:
		# ff_sub = ["-i", largest_file, "-map", "1:0", "-c:s", "mov_text"]
		# return ff_sub, False
		# For now, just skip:
		return [], True
	else:
		if de_bug:
			logger.debug("No external subtitle found.")
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
		logger.error("parse_finfo: missing file or metadata")
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
	"time":    re.compile(r"time=\S([0-9:]+)"),
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
		hh, mm, ss = 0, 0, 0
		if "time" in regx_val:
			hh, mm, ss = map(int, regx_val["time"].split(":"))
		a_sec = hh * 3600 + mm * 60 + ss

		# 'glb_vidolen' is assumed to be defined globally
		dif = abs(glb_vidolen - a_sec)
		eta = round(dif / sp_float) if sp_float else 9999

		mints, secs = divmod(int(eta), 60)
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
		line_to =f"    | {sy} | {line_to}               \r"
		print (line_to, end='')
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
			print (disp_str, end='')

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
			print (disp_str, end='')

		except Exception as e:
			print(f"show_progrs exception: {e} in line: {line_to}")

	# 5) Else if any of ["muxing overhead:", "global headers:"] -> done
	elif any(x in line_to for x in ["muxing overhead:", "global headers:"]):
#		Done: [out#0/mp4 @ 0000026f47d68e80] video:603557KiB audio:181972KiB subtitle:82KiB other streams:0KiB global headers:14KiB muxing overhead: 0.504575%
		line_to = line_to.rstrip('\r\n')  # Remove all trailing '\r' and '\n'
		print(f"    | Done: {line_to[31:]} |  ")
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
	logger.info("matrix_it: creating a %dx%d collage for %s", columns, rows, input_file)
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
		logger.info("Matrix collage created: %s", out_file)
		return True
	else:
		logger.error("Failed to create matrix collage.")
		return False

@perf_monitor
def speed_up(input_file: str, factor: float = 4.0, de_bug: bool = False) -> Optional[str]:
	"""
	Create a 'factor'x speed version of the input video using FFmpeg filters.
	E.g., factor=4.0 => 4x speed (25% the original duration).
	"""
	logger.info("speed_up: input=%s factor=%.2f", input_file, factor)
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
		logger.info("Speeded-up file created: %s", out_file)
		return out_file
	else:
		logger.error("Failed to speed-up file.")
		return None

@perf_monitor
def video_diff(file1: str, file2: str, de_bug: bool = False) -> Optional[str]:
	"""
	Visually compare two videos by showing the pixel difference (blend=all_mode=difference).
	Produces an output diff.mp4 or something similar.
	"""
	logger.info("video_diff: comparing %s vs %s", file1, file2)
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
		logger.info("Difference file created: %s", out_file)
		return out_file
	else:
		logger.error("Failed to create difference file.")
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
	logger.info("short_ver: input=%s start=%s duration=%s", input_file, start, duration)
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
		logger.info("Short version created: %s", out_file)
		return out_file
	else:
		logger.error("Failed to create short version.")
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
		logger.info("Skipping entire file: %s", input_file)
		return

	# 3) Otherwise, run ffmpeg
	out_file = ffmpeg_run(input_file, ff_run_cmd, skip_it, execu=ffmpeg, de_bug=False)
	if out_file:
		logger.info("Output file: %s", out_file)
	else:
		logger.warning("No output created.")

if __name__ == "__main__":
	print ("Testing")
#    main()
