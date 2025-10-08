# -*- coding: utf-8 -*-
import os
import re
import sys
import json
import time
import stat
import queue  # Ensure this is imported
import random
import string
import shutil
import threading
import datetime as TM
import subprocess as SP

from My_Utils import *
from pathlib import Path
from collections import defaultdict
from dataclasses import dataclass
from typing import List, Dict, Tuple, Any, Optional


p = Path(globals().get("__file__", sys.argv[0]));
print(f"{p.name} — {TM.datetime.fromtimestamp(p.stat().st_ctime):%Y-%m-%d %H:%M:%S}")

Excepto = r"C:\_temp"

File_extn = [
	".h264", ".m4v", ".mkv", ".moov", ".mov", ".movhd", ".movie",
	".movx", ".mts", ".mp4", ".mpe", ".mpeg", ".mpg", ".mpv",
	".ts", ".vfw", ".vid", ".video", ".x264", ".xvid"
]
# External references from your code
Skip_key     = "| <¯\\_(ツ)_/¯> |"
TmpF_Ex      = ".mp4"
Default_lng = "eng"

Keep_langua = ["eng", "fre", "ger", "heb", "hun", "ita", "jpn", "rum", "rus", "spa"]
Tmp_exte = ".mp4"  # Fixed typo from : to =

HEVC_BPP	= 0.053
DEFAULT_FPS	= 24.0  # Fallback if FPS cannot be extracted

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

def _has_encoder(name: str) -> bool:
	try:
		p = SP.run([ffmpeg, "-hide_banner", "-encoders"],
				   capture_output=True, text=True, encoding="utf-8", errors="replace")
		s = (p.stdout or "") + (p.stderr or "")
		return name in s
	except Exception:
		return False

HAS_NVENC = _has_encoder("hevc_nvenc")
HAS_QSV   = _has_encoder("hevc_qsv")

glb_vidolen = 0

###############################################################################
#                               FFPROBE
###############################################################################

def ffprobe_run(
	input_file: str,
	execu: str = ffprob,
	de_bug: bool = False
) -> dict:
	"""
	Runs ffprobe to get media info and optionally checks for corruption with ffmpeg.

	Args:
		input_file: The path to the video file.
	#	check_corruption: If True, performs a quick ffmpeg decode check.
	#	timeout: The timeout in seconds for the subprocess calls.

	Returns:
		A dictionary containing the media information.

	Raises:
		FileNotFoundError: If the input file does not exist.
		ValueError: If ffprobe returns invalid JSON.
		RuntimeError: If ffprobe or ffmpeg fails, containing a detailed error message.
		subprocess.TimeoutExpired: If a command times out.
	"""
	if not os.path.isfile(input_file):
		raise FileNotFoundError(f"Input file not found: {input_file}")

	check_corruption = False
	# --- 1. Get Media Information using ffprobe ---
	ffprobe_cmd = [
		ffprob,
		"-v", "error",
		"-show_streams",
		"-show_format",
		"-of", "json",
		"-analyzeduration", "400000000",
		"-probesize", "200000000",
		"-i", input_file,
	]

	try:
		process = SP.run(
			ffprobe_cmd,
			capture_output=True,
			text=True,
			encoding="utf-8",
			errors="replace",
			timeout=300,
			check=True  # Automatically raises CalledProcessError on non-zero exit codes
		)
		metadata = json.loads(process.stdout)

	except SP.CalledProcessError as e:
		# Create the detailed error message from the first implementation
		error_message = (
			f"ffprobe failed with exit code {e.returncode}.\n"
			f"Stderr: {e.stderr.strip()}\n"
			f"Stdout: {e.stdout.strip()}"
		)
		error_log = os.path.splitext(os.path.basename(input_file))[0] + "_error.log"
		with open(error_log, "a") as f:
			f.write(f"{error_message}\n")
		raise RuntimeError(error_message) from e

	except json.JSONDecodeError as e:
		raise ValueError("ffprobe did not return valid JSON.") from e

	# --- 2. Optional Corruption Check using ffmpeg ---
	if check_corruption:
		ffmpeg_cmd = [
			ffmpeg,
			"-v", "error",   # Report only errors
			"-xerror",       # Exit immediately on error
			"-i", input_file,
			"-t", "10",      # Decode for 10 seconds
			"-f", "null",    # Don't write an output file
			"-",
		]
		try:
			SP.run(
				ffmpeg_cmd,
				capture_output=True,
				timeout=100,
				check=True # This will raise an exception if ffmpeg finds an error
			)
		except SP.CalledProcessError as e:
			# Create a detailed error for the corruption check
			error_message = (
				f"Corruption check failed with exit code {e.returncode}.\n"
				f"ffmpeg stderr: {e.stderr.strip()}"
			)
			raise RuntimeError(error_message) from e

	return metadata
###############################################################################
#                             RUN FFMPEG
###############################################################################
# Precompile regex patterns globally for efficiency
regex_dict = {
		"bitrate": re.compile(r"bitrate=\s*([0-9\.]+)"),
		"frame": re.compile(r"frame=\s*([0-9]+)"),
		"speed": re.compile(r"speed=\s*([0-9\.]+)"),
		"size": re.compile(r"size=\s*([0-9]+)"),
		"time": re.compile(r"time=([0-9:.]+)"),
		"fps": re.compile(r"fps=\s*([0-9\.]+)")  # Allows decimals; no change needed
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
	except Exception:
		return "--:--:--", 0.0
##==============-------------------  End   -------------------==============##

def show_progrs(line_to: str, sy: str, de_bug: bool = False) -> bool:
    line = (line_to or "").strip()
    if not line:
        return True

    # Print raw when debugging
    if de_bug:
        print(line)

    # Only parse progress lines
    if "fps=" in line and "speed=" in line and "size=" in line:
        vals = extract_progress_data(line)

        def _f(k, d=0.0):
            try:
                v = vals.get(k, d)
                if isinstance(v, str) and v.strip().lower() in ("n/a", "nan", "inf", "-inf", ""):
                    return d
                return float(v)
            except Exception:
                return d

        def _i(k, d=0):
            try:
                return int(vals.get(k, d))
            except Exception:
                return d

        try:
            fps   = _f("fps", 0.0)
            frame = _i("frame", 0)

            # size comes in KiB from ffmpeg progress most of the time; be tolerant
            try:
                size_kib = float(vals.get("size", 0))
            except Exception:
                size_kib = 0.0
            size_val = hm_sz(int(size_kib * 1024), 'b')

            # bitrate may be 'N/A'
            br_txt = str(vals.get("bitrate", "0")).strip()
            try:
                br_bps = float(br_txt) * 1000.0
                br_val = hm_sz(br_bps, 'bps')
            except Exception:
                br_val = br_txt  # keep 'N/A'

            # speed may be 'n/a'
            sp_txt = str(vals.get("speed", "n/a")).strip().lower()
            try:
                sp_num = float(sp_txt)
                sp_disp = f"{sp_num:>6.2f}x"
            except Exception:
                sp_disp = f"{sp_txt:>6}"

            eta_str, a_sec = calculate_eta(vals, sp_num if 'sp_num' in locals() else 0.0)
            pct = (100.0 * a_sec / glb_vidolen) if glb_vidolen > 0 else 0.0

            disp = (f"    | {sy} |Size: {size_val:>7}|Frames: {frame:>7}|Fps: {fps:>6.1f}"
                    f"|BitRate: {br_val:>9}|Speed: {sp_disp}|ETA: {eta_str:>9}|{pct:5.1f}% |")
            sys.stderr.write('\r' + disp + "    "); sys.stderr.flush()

        except Exception as e:
            with open("ffmpeg_progress_errors.log", "a", encoding="utf-8", errors="replace") as f:
                f.write(f"show_progrs exception: {e} in line: {line}\n")

    return True

##==============-------------------  End   -------------------==============##

def run_ffm(args: List[str], de_bug: bool = False) -> bool:
	if de_bug:
		print("\n--- [DEBUG] FFmpeg Command Sent ---")
		print(args)
		print("-----------------------------------\n")

	process = SP.Popen(args, stdout=SP.PIPE, stderr=SP.PIPE, text=True, encoding="utf-8", errors="replace")

	stdout_queue = queue.Queue()
	stderr_queue = queue.Queue()

	def read_stdout():
		while True:
			try:
				line = process.stdout.readline()
				if not line:
					break
				stdout_queue.put(line)
			except (ValueError, EOFError):
				break  # Handle closed file

	def read_stderr():
		while True:
			try:
				line = process.stderr.readline()
				if not line:
					break
				stderr_queue.put(line)
			except (ValueError, EOFError):
				break  # Handle closed file

	stdout_thread = threading.Thread(target=read_stdout, daemon=True)
	stderr_thread = threading.Thread(target=read_stderr, daemon=True)
	stdout_thread.start()
	stderr_thread.start()

	# Process progress from stderr (FFmpeg progress is in stderr)
	while process.poll() is None:
		try:
			line = stderr_queue.get(timeout=0.1)  # Non-blocking from stderr
			if not show_progrs(line, "|/-o+\\"[int(time.time()*2) % 5], de_bug):
				break
		except queue.Empty:
			pass

		# Optional: Handle stdout if needed (rare for FFmpeg)
		try:
			out_line = stdout_queue.get_nowait()
			# Process if necessary, e.g., print(out_line)
		except queue.Empty:
			pass

	process.communicate()
	stdout_thread.join(timeout=5)
	stderr_thread.join(timeout=5)

	stderr_lines = []
	while not stderr_queue.empty():
		stderr_lines.append(stderr_queue.get())

	if process.returncode != 0:
		print(f"\nFFmpeg failed with return code {process.returncode}")
		if stderr_lines:
			print("Last errors:\n" + "".join(stderr_lines[-5:]))

	return process.returncode == 0
##==============-------------------  End   -------------------==============##

def run_ffmpeg(cmd: List[str], de_bug: bool = False) -> Tuple[int, str, str]:
	"""Run ffmpeg and return (returncode, stdout, stderr)."""
	if de_bug:
		print("\n--- [DEBUG] run_ffmpeg ---")
		print(" ".join(f'"{a}"' if " " in a else a for a in cmd))
		print("--------------------------\n")
	cp = SP.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")
	return cp.returncode, cp.stdout, cp.stderr

def ffmpeg_run(
	input_file: str,
	ff_com: List[str],
	skip_it: bool,
	execu: str = "ffmpeg",
	de_bug: bool = False
) -> Optional[str]:
	"""
	Runs an FFmpeg command and creates the output file in the script's local directory.

	Returns the full path to the output file on success, otherwise None.
	"""
	if not input_file or skip_it:
		return None

	# --- 1. Use pathlib for Modern and Robust Path Handling ---
	input_path = Path(input_file)
	script_dir = Path(__file__).resolve().parent # This gets the script's directory

	# --- 2. Create a Safe and Unique Output Filename ---
	# Sanitize the original filename to use as a base
	safe_base_name = re.sub(r"[^\w\-\. ]", "", input_path.stem[:33]).strip()	# Generate a unique temporary filename using your helper function
	out_filename = f"_{safe_base_name}_{stmpd_rad_str(4, '')}_{TmpF_Ex}"
	# --- 3. Define Output Path in the Script's Directory ---
	out_path = script_dir / out_filename

	# --- 4. Construct the FFmpeg Command ---
	# Using f-strings and a more readable list construction
	full_cmd = [
		execu,
		"-hide_banner",
		"-thread_queue_size", "24",
		"-i", str(input_path),
		*ff_com, # Unpack the main command components
		"-metadata", f"title={input_path.stem}",
		"-metadata", f"comment={Skip_key}",
		"-metadata", "author=Encoded by GeoHab",
		"-movflags", "+faststart",
		"-fflags", "+genpts",
		"-y", # Overwrite output file if it exists
		str(out_path)
	]

	# --- 5. Execute the Command ---
	if run_ffm(full_cmd, de_bug=de_bug):
		return str(out_path)
	else:
		# Optional: Clean up failed output file
		if out_path.exists():
			out_path.unlink()
		return None
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
	codec_name: str, width: int, height: int, bitrate_bps: Optional[int], is_10bit: bool, fps: float, allow_hw: bool = True
) -> Tuple[bool, List[str], str, Optional[str], int]:
	"""
	Analyzes video properties and determines the optimal FFmpeg settings for re-encoding to HEVC.

	Returns a tuple containing:
	- needs_reencode (bool): True if the video should be re-encoded.
	- encoder_flags (List[str]): The FFmpeg command flags.
	- status_text (str): A human-readable summary of the decision.
	- scaler (Optional[str]): The FFmpeg scale filter, if needed.
	- target_bitrate (int): The calculated ideal bitrate for the video.
	"""
	# --- 1. Define the Ideal State (Resolution and Bitrate) ---
	needs_scaling =  height > 1088
	target_height = 1080 if needs_scaling else height

	# Calculate target width while maintaining aspect ratio, ensuring it's an even number
	target_width = int(width * (target_height / height)) if height > 0 else width
	if target_width % 2 != 0:
		target_width -= 1

	# The ideal bitrate is calculated using the BPP formula based on the *target* resolution.
	ideal_bitrate = int(HEVC_BPP * target_width * target_height * fps) if width > 0 and height > 0 and fps > 0 else 2_000_000

	# --- 2. Compare Source to Ideal to Decide on Action ---
	is_source_hevc = codec_name.lower() == "hevc"
	is_bitrate_ok = bitrate_bps is None or bitrate_bps <= int(ideal_bitrate * 1.20) # Allow 20% tolerance

	# The "copy" condition: if all criteria are met, no re-encode is needed.
	if is_source_hevc and not needs_scaling and is_bitrate_ok:
		return False, ["-c", "copy"], "=> Copy (Compliant HEVC)", None, ideal_bitrate

	# If we are here, a re-encode is necessary. Now, determine the reasons and target bitrate.
	reasons = []
	target_bitrate = ideal_bitrate

	if not is_source_hevc:
		reasons.append("Codec (Not HEVC)")
		# For non-HEVC sources, target an efficient bitrate, but don't exceed the ideal.
		if bitrate_bps:
			efficient_bitrate = int(bitrate_bps * 0.65) # HEVC is ~35% more efficient than H.264
			target_bitrate = min(efficient_bitrate, ideal_bitrate)
	elif not is_bitrate_ok:
		reasons.append("High Bitrate")
		# Target the ideal bitrate since the source is bloated
		target_bitrate = ideal_bitrate

	# --- 3. Calculate Quality (CRF) and Compatibility (VBV) Settings ---
	vbv_maxrate = int(target_bitrate * 1.2) # VBV maxrate with 20% headroom
	vbv_bufsize = int(vbv_maxrate * 3)   # A 3x buffer is safe and allows for quality fluctuations

	# Calculate a CRF value based on how the source bitrate compares to the ideal.
	# This value will be used for BOTH SW and HW encoding to ensure consistent quality.
	ratio = (bitrate_bps / ideal_bitrate) if bitrate_bps else 1.0
	crf = 22
	if ratio   > 2.0: crf = 28
	elif ratio > 1.5: crf = 26
	elif ratio > 1.2: crf = 24

	# This logic remains: it improves quality when converting from an 8-bit source.
	if not is_10bit: crf -= 2

	# 4. --- Unified Encoder Settings ---
	scaler	= f"scale=-1:{target_height}" if needs_scaling else None
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

	return True, encoder_flags, status_text, scaler, ideal_bitrate

###==============-------------------  End   -------------------==============###
def parse_video(streams: List[Dict[str, Any]], ctx, de_bug: bool = False) -> Tuple[List[str], bool]:
    """
    Simple, robust video decision:
    - If HEVC and not low-res and bitrate reasonable -> copy
    - Else re-encode with libx265 (software)
    - Low-res (<1280x720) is upscaled to true 1920x1080 via fit+pad
    - Explicitly maps the chosen input video stream
    Returns (cmd_parts, skip_flag)
    """
    if not streams:
        print("\033[93m    !No video streams.\033[0m")
        return [], True

    # first non-attached video
    v = next((s for s in streams
              if s.get("codec_type") == "video" and not s.get("disposition", {}).get("attached_pic", 0)), None)
    if v is None:
        print("\033[93m    !No playable video stream.\033[0m")
        return [], True

    def _i(x, d=0):
        try: return int(x)
        except Exception: return d
    from fractions import Fraction

    codec  = (v.get("codec_name") or "").lower()
    w      = _i(v.get("width"))
    h      = _i(v.get("height"))
    fps_s  = (v.get("avg_frame_rate") or v.get("r_frame_rate") or "0/1")
    try:    fps = float(Fraction(fps_s)) if "/" in fps_s else float(fps_s)
    except: fps = 0.0

    br_bps = _i(v.get("bit_rate")) or getattr(ctx, "estimated_video_bitrate", 0) or 0
    ideal  = int(max(1, (max(w,1) * max(h,1) * max(fps, 24.0)) / 3.5))  # same heuristic you used

    need_upscale = (h < 720) or (w < 1280)

    # IMPORTANT: map this video stream (since audio/subs may add their own -map)
    cmd: List[str] = ["-map", f"0:{v.get('index', 0)}"]

    # Copy only when it's already good enough
    allow_copy = (codec == "hevc") and (not need_upscale) and (0 < br_bps <= int(ideal * 1.10))
    if allow_copy:
        print(f"\033[92m    |<V: 0>|  {codec:<6}|{w}x{h}|8-bit| Fps:{int(fps)}| "
              f"Bitrate: {hm_sz(br_bps,'bps')} vs Ideal: {hm_sz(ideal,'bps')}| => Copy (Compliant)\033[0m")
        cmd += ["-c:v:0", "copy", "-disposition:v:0", "default"]
        return cmd, True

    # Re-encode (software, always works)
    reason = "Codec/bitrate not optimal → Re-encode"
    vf = None
    if need_upscale:
        # fit to 1920x1080 preserving AR, then pad to exact 1920x1080
        vf = ("scale=1920:1080:flags=lanczos:force_original_aspect_ratio=decrease,"
              "pad=1920:1080:(ow-iw)/2:(oh-ih)/2")
        reason = "Low-res source → Upscale to 1920x1080 (fit+pad)"

    if vf:
        cmd += ["-vf", vf]

    # Simple, portable encoder settings
    cmd += ["-c:v:0", "libx265", "-crf", "23", "-preset", "medium",
            "-pix_fmt", "yuv420p", "-disposition:v:0", "default"]

    print(f"\033[91m    |<V: 0>|  {codec:<6}|{w}x{h}|8-bit| Fps:{int(fps)}| "
          f"Bitrate: {hm_sz(br_bps,'bps')} vs Ideal: {hm_sz(ideal,'bps')}| => Re-encode (SW) |{reason}\033[0m")
    return cmd, False


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
		msg = f"|<A:{audio_strm['index']:2}>|{audio_strm.get('codec_name', 'u'):^8}|{lang:<3}|Br:{hm_sz(int(audio_strm.get('bit_rate', 0))):<9}|Ch:{int(audio_strm.get('channels', 0))}| {log_action}"
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
			print(f"    .Found {ext} external subtitle: {os.path.basename(test_file)}")
			return ["-i", test_file, "-map", "1:0", "-c:s:0", "mov_text", "-metadata:s:s:0", f"language={Default_lng}", "-disposition:s:0", "default"], False
		else :
			print(f"    .No external {ext} subtitle file found.")
	return [], True
##==============-------------------  End   -------------------==============##

def parse_subtl(sub_streams: List[Dict[str, Any]], context: VideoContext, de_bug: bool = False) -> Tuple[List[str], bool]:
	if not sub_streams:
		print("    .No embedded subtitles found, checking for external file.")
		return add_subtl_from_file(context.input_file)

	TEXT_BASED_CODECS = ("subrip", "ass", "mov_text")
	best_streams = {lang: {'score': -1, 'stream': None} for lang in Keep_langua}

	def _score_subtitle(stream: Dict[str, Any]) -> int:
		score = 100
		tags, disposition = stream.get("tags", {}), stream.get("disposition", {})
		title = tags.get("title", "").lower()
		if disposition.get("forced", 0) or "forced" in title: score -= 1000
		if "sdh" in title: score += 100
		if disposition.get("default", 0): score += 50
		score += int(stream.get("bit_rate", 0))
		score += int(stream.get("nb_frames", 0) or stream.get("nb_read_packets", 0))
		return score

	for stream in sub_streams:
		lang = stream.get("tags", {}).get("language", "und")
		if lang in Keep_langua and stream.get("codec_name") in TEXT_BASED_CODECS:
			score = _score_subtitle(stream)
			if score > best_streams[lang]['score']:
				best_streams[lang] = {'score': score, 'stream': stream}

	text_streams_to_keep = [data['stream'] for data in best_streams.values() if data['stream']]

	if not text_streams_to_keep:
		print("    .Skip: No compatible text-based subtitles found.")
		return [], True

	best_sub_candidate = next((s for s in text_streams_to_keep if s.get("tags", {}).get("language") == Default_lng), text_streams_to_keep[0])

	needs_disposition_change = any(
		(s['index'] == best_sub_candidate['index'] and not s.get("disposition", {}).get("default", 0)) or
		(s['index'] != best_sub_candidate['index'] and s.get("disposition", {}).get("default", 0))
		for s in text_streams_to_keep
	)
	all_codecs_compliant = all(s.get('codec_name') == 'mov_text' for s in text_streams_to_keep)
	all_handlers_correct = all(s.get("tags", {}).get("handler_name") == "SubtitleHandler" for s in text_streams_to_keep)

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

		if codec_is_compliant and not needs_disposition_change:
			stream_opts.extend([f"-c:s:{i}", "copy"])
			log_status.append("Copy")
		else:
			stream_opts.extend([f"-c:s:{i}", "mov_text"])
			log_status.append("Re-encode")

		if is_best:
			stream_opts.extend([f"-disposition:s:{i}", "default"])
			if not stream.get("disposition", {}).get("default", 0):
				log_status.append("Set Default")
		else:
			stream_opts.extend([f"-disposition:s:{i}", "0"])
			if stream.get("disposition", {}).get("default", 0):
				log_status.append("Clear Default")

		if not handler_is_correct:
			stream_opts.extend([f"-metadata:s:s:{i}", "handler_name=SubtitleHandler"])
			log_status.append("Fix Handler")

		ff_subttl.extend(stream_opts)

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

def parse_finfo(input_file: str, metadata: Dict[str, Any], de_bug: bool = False) -> Tuple[List[str], bool]:
    """
    Prints status/log lines internally and returns only:
      (final_cmd: List[str], final_skip: bool)

    - Stream-count banner (V/A/S)
    - Robust 'no valid audio' check (channels>0 and sample_rate>0)
    - Sets glb_vidolen so show_progrs can compute %/ETA
    - Builds VideoContext with estimated_video_bitrate
    - Calls parse_video / parse_audio / parse_subtl / parse_extrd, accepting (cmd,skip) or (cmd,skip,messages)
    """
    def _i(v, d=0):
        try:
            return int(v)
        except Exception:
            return d

    def _f(v, d=0.0):
        try:
            return float(v)
        except Exception:
            return d

    # ---- validate metadata ----
    if not metadata or "format" not in metadata:
        print(f"\033[93m !Error: Invalid metadata for '{input_file}'.\033[0m")
        return [], True

    fmt     = metadata["format"]
    streams = metadata.get("streams") or []

    # ---- header fields ----
    size     = _i(fmt.get("size"))
    duration = _f(fmt.get("duration"))
    bitrate  = _i(fmt.get("bit_rate"))
    tags     = (fmt.get("tags", {}) or {})
    title    = tags.get("title") or Path(input_file).stem
    f_comment = (tags.get("comment") or "")
    f_comment = f_comment.strip() if isinstance(f_comment, str) else ""
    f_skip   = (f_comment == Skip_key)

    # make ETA work in show_progrs
    global glb_vidolen
    glb_vidolen = duration if duration > 0 else 0

    # ---- bucket streams & counts ----
    s_by_type: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for s in streams:
        s_by_type[s.get("codec_type", "?")].append(s)

    sc_v = len(s_by_type.get("video", []))
    sc_a = len(s_by_type.get("audio", []))
    sc_s = len(s_by_type.get("subtitle", []))

    # ---- banner ----
    print(f"\033[96m    |=Title|{title}|\033[0m")
    print(
        f"\033[96m    |<FRMT>|Size: {hm_sz(size)}|Bitrate: {hm_sz(bitrate,'bps')}|"
        f"Length: {hm_time(duration)}|Streams: V:{sc_v} A:{sc_a} S:{sc_s}"
        f"{'|K+N Update|' if not f_skip else ''}\033[0m"
    )
    if f_skip:
        print("\033[96m    .Note: Has skip key, re-evaluating compliance.\033[0m")

    # ---- robust 'no valid audio' check ----
    has_valid_audio = any(
        (s.get("codec_type") == "audio")
        and _i(s.get("channels")) > 0
        and _i(s.get("sample_rate")) > 0
        for s in streams
    )
    if not has_valid_audio:
        print(f"\033[93m    !Warning: No valid audio stream found in this file. Moving to {Excepto}\033[0m")
        try:
            # be tolerant of different signatures of copy_move
            try:
                copy_move(input_file, Excepto, True)  # (src, dst, move)
            except TypeError:
                try:
                    copy_move(input_file, Excepto, True, False)  # (src, dst, move, overwrite)
                except TypeError:
                    copy_move(input_file, Excepto, move=True)    # kwargs
        except Exception as e:
            print(f"\033[93m    !Move failed: {e}\033[0m")
        return [], True

    # ---- build context ----
    primary_video = s_by_type.get("video", [{}])[0] if sc_v else {}
    ctx = VideoContext(
        input_file=input_file,
        file_size=size,
        duration=duration,
        vid_width=primary_video.get("width", 0) or 0,
        vid_height=primary_video.get("height", 0) or 0
    )

    # estimated video bitrate (fall back if needed)
    total_file_br  = (size * 8) / duration if size > 0 and duration > 0 else 0
    total_audio_br = sum(_i(s.get("bit_rate")) for s in s_by_type.get("audio", []) if _i(s.get("bit_rate")) > 0)
    if total_file_br > total_audio_br > 0:
        ctx.estimated_video_bitrate = int((total_file_br - total_audio_br) * 0.98)
    elif total_file_br > 0:
        ctx.estimated_video_bitrate = int(total_file_br * 0.80)

    # ---- aggregate commands from parsers ----
    final_cmd: List[str] = []
    all_skips: List[bool] = [f_skip]

    for label, s_type, func in [
        ("V_cmd", "video",    parse_video),
        ("A_cmd", "audio",    parse_audio),
        ("S_cmd", "subtitle", parse_subtl),
        ("D_cmd", "data",     parse_extrd),
    ]:
        try:
            res = func(s_by_type.get(s_type, []), ctx, de_bug)
        except Exception as e:
            print(f"\033[93m    !Error in {func.__name__}: {e}\033[0m")
            continue

        # accept (cmd, skip) or (cmd, skip, messages)
        if not isinstance(res, tuple) or len(res) < 2:
            print(f"\033[93m    !Error: {func.__name__} returned unexpected value: {type(res).__name__}\033[0m")
            continue

        cmd, skip = res[0], res[1]
        # if a 3rd element is present and is a list of log lines, print them
        if len(res) >= 3 and isinstance(res[2], list):
            for ln in res[2]:
                try:
                    print(ln)
                except Exception:
                    pass

        if de_bug and cmd:
            print(f"    {label}= {cmd}")

        final_cmd.extend(cmd)
        all_skips.append(bool(skip))

    final_skip = all(all_skips)
    print(
        "\033[96m    .Skip: File compliant, already processed.\033[0m"
        if final_skip else f"\033[96m    .2 Pass Process: {Path(input_file).name}\033[0m"
    )

    # return EXACTLY two values to match Trans_code.py
    return final_cmd, final_skip

##==============-------------------  End   -------------------==============##


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

		# Quality check
		is_better = check_quality_better(input_file, output_file)
		if not is_better:
			print("Quality check failed: Keeping original file.")
			time.sleep(5)  # Wait 5 seconds
#			os.remove(output_file)
#			return 0

		size_diff = output_file_size - input_file_size
		ratio = round(100 * (size_diff / input_file_size), 2) if input_file_size > 0 else float('inf') if output_file_size > 0 else 0
		extra = "+Bigger" if ratio > 0 else ("=Same" if size_diff == 0 else "-Lost")
		msj_ = f"   .Size Was: {hm_sz(input_file_size)} Is: {hm_sz(output_file_size)} {extra}: {hm_sz(abs(size_diff))} {ratio}% "
		if ratio > 35 :
			print(f"\n  WARNING: New file over {ratio}% larger than the original.\n{msj_}")
			if input("   Proceed to replace the original file? (y/n): ").lower() != 'y':
				print("   Operation skipped. Keeping original and deleting new file.")
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
		error_log = os.path.splitext(os.path.basename(input_file))[0] + "_error.log"
		with open(error_log, "a") as f:
			f.write(f"clean_up error: {e}\n")
		print(f" An error occurred: {e}")
		if temp_file and os.path.exists(temp_file): shutil.move(temp_file, input_file)
	return -1
##==============-------------------  End   -------------------==============##
def check_quality_better(original: str, processed: str, *, frames: int = 120, de_bug: bool = False) -> bool:
	"""
	Fast quality check using SSIM (Y) + PSNR.
	- Processed (distorted) is upscaled to Original (reference) with scale2ref.
	- Sample at 2 fps for speed, cap frames.
	- Map exactly ONE filtered output to null muxer to avoid DTS warnings.
	"""
	import re

	# [0] = processed (distorted), [1] = original (reference)
	vf = (
		# reset clocks
		"[0:v]setpts=PTS-STARTPTS[dx0];"
		"[1:v]setpts=PTS-STARTPTS[rx0];"
		# match sizes by upscaling processed -> original
		"[dx0][rx0]scale2ref=flags=lanczos[dist][ref];"
		# sample and normalize format (keeps timestamps monotonic)
		"[dist]fps=2,format=yuv420p[dx];"
		"[ref]fps=2,format=yuv420p[rx];"
		# metrics (each produces a video output) – NAME THEM
		"[dx][rx]ssim=stats_mode=0[vssim];"
		"[dx][rx]psnr=stats_mode=0[vpsnr]"
	)

	cmd = [
		ffmpeg,
		"-hide_banner", "-nostats",
		"-fflags", "+genpts",
		"-i", processed, "-i", original,
		"-an", "-sn", "-dn",                # video only
		"-filter_complex", vf,
		# MAP EXACTLY ONE FILTER OUTPUT so null muxer sees a single stream
		"-map", "[vpsnr]",
		"-vsync", "0",
		"-frames:v", str(frames),
		"-map_metadata", "-1", "-map_chapters", "-1",
		"-f", "null", "-"
	]
	if de_bug:
		print("[quality] cmd:", " ".join(f'"{a}"' if " " in a else a for a in cmd))

	rc, _out, err = run_ffmpeg(cmd, de_bug=de_bug)

	# Parse metrics from stderr
	ssimY   = re.findall(r"SSIM\s+Y:([0-9.]+)", err or "")
	ssimAll = re.findall(r"SSIM.*All:([0-9.]+)", err or "")
	psnra   = re.findall(r"PSNR.*average:([0-9.]+)", err or "")

	ssim = float(ssimY[-1]) if ssimY else (float(ssimAll[-1]) if ssimAll else 0.0)
	try:
		psnr = float(psnra[-1]) if psnra else 0.0
	except Exception:
		psnr = 0.0

	if de_bug:
		print(f"[quality] rc={rc}  SSIM_Y={ssim:.4f}  PSNR={psnr:.2f}")

	# Tweak thresholds to taste
	return rc == 0 and (ssim >= 0.92) and (psnr >= 32.0)

def ai_upscale(input_file: str, output_file: str, target_height: int = 1080, de_bug: bool = False) -> bool:
	"""
	Uses Real-ESRGAN to upscale the video to 1080p using AI.
	Requires 'realesrgan' package installed (pip install realesrgan).
	Extracts frames, upscales, reassembles with FFmpeg.
	"""
	try:
		import tempfile
		from PIL import Image
		from realesrgan import RealESRGANer
		from realesrgan.archs.srvgg_arch import SRVGGNetCompact

		# Calculate scale factor
		metadata = ffprobe_run(input_file)
		video_stream = [s for s in metadata['streams'] if s['codec_type'] == 'video'][0]
		height = video_stream['height']
		scale = target_height / height if height < target_height else 1.0

		if scale <= 1.0:
			print("    .No AI upscale needed.")
			shutil.copy(input_file, output_file)
			return True

		# Create temp dir for frames
		with tempfile.TemporaryDirectory() as tmp_dir:
			frames_dir = os.path.join(tmp_dir, 'frames')
			upscale_dir = os.path.join(tmp_dir, 'upscaled')
			os.makedirs(frames_dir)
			os.makedirs(upscale_dir)

			# Extract frames with FFmpeg (preserve 10-bit as 16-bit PNG)
			extract_cmd = [ffmpeg, '-i', input_file, '-pix_fmt', 'rgb48be', os.path.join(frames_dir, 'frame%06d.png')]
			SP.run(extract_cmd, check=True)

			# Load Real-ESRGAN model (assume model downloaded)
			model = SRVGGNetCompact(num_in_ch=3, num_out_ch=3, num_feat=64, num_conv=32, upscale=4, act_type='prelu')
			netscale = 4
			model_path = 'RealESRGAN_x4plus.pth'  # Assume downloaded
			upsampler = RealESRGANer(
				scale=netscale,
				model_path=model_path,
				model=model,
				tile=0,
				tile_pad=10,
				pre_pad=0,
				half=not de_bug,  # FP16 if not debug
			)

			# Upscale each frame
			for frame_file in sorted(os.listdir(frames_dir)):
				if frame_file.endswith('.png'):
					img_path = os.path.join(frames_dir, frame_file)
					img = Image.open(img_path).convert('RGB')
					img_array = np.array(img)
					output, _ = upsampler.enhance(img_array, outscale=scale)
					output_img = Image.fromarray(output)
					output_img.save(os.path.join(upscale_dir, frame_file))

			# Reassemble video with FFmpeg, copy audio/subtitles, use 10-bit
			assemble_cmd = [
				ffmpeg, '-framerate', str(video_stream['r_frame_rate']), '-i', os.path.join(upscale_dir, 'frame%06d.png'),
				'-i', input_file, '-map', '0:v', '-map', '1:a?', '-map', '1:s?', '-c:v', 'libx265', '-pix_fmt', 'yuv420p10le',
				'-crf', '22', '-c:a', 'copy', '-c:s', 'copy', '-y', output_file
			]
			SP.run(assemble_cmd, check=True)

		print(f"    .AI upscale completed to {output_file}")
		return True
	except Exception as e:
		error_log = os.path.splitext(os.path.basename(input_file))[0] + "_error.log"
		with open(error_log, "a") as f:
			f.write(f"ai_upscale error: {e}\n")
		print(f"AI upscale failed: {e}. Falling back to no upscale.")
		return False

def matrix_it(input_file: str, execu: str = ffmpeg, ext: str = ".png", columns: int = 3, rows: int = 3) -> bool:
	base_name, _ = os.path.splitext(os.path.basename(input_file))
	out_file = base_name + "_matrix" + ext
	select_expr = "select='not(mod(n,300))'"
	tile_expr = f"tile={columns}x{rows}:padding=5:margin=5"
	vf_filter   = f"[0:v]{select_expr},{tile_expr}"
	cmd = [execu, "-i", input_file, "-frames:v", "1", "-vf", vf_filter, "-vsync", "vfr", "-y", out_file]
	return run_ffm(cmd)

def speed_up(input_file: str, factor: float = 4.0, de_bug: bool = False) -> Optional[str]:
	base_name, _ = os.path.splitext(os.path.basename(input_file))
	out_file = base_name + f"_speed{int(factor)}x.mp4"
	atempo_filter = f"atempo={factor}" if factor <= 2.0 else "atempo=2.0,atempo=2.0"
	vf_expr = f"setpts=PTS/{factor}"
	cmd = [ffmpeg, "-i", input_file, "-filter_complex", f"[0:v]{vf_expr}[v];[0:a]{atempo_filter}[a]", "-map", "[v]", "-map", "[a]", "-y", out_file]
	return out_file if run_ffm(cmd, de_bug=de_bug) else None

def video_diff(file1: str, file2: str, de_bug: bool = False) -> Optional[str]:
	base_name, _ = os.path.splitext(os.path.basename(file1))
	out_file = base_name + "_diff.mp4"
	cmd = [ffmpeg, "-i", file1, "-i", file2, "-filter_complex", "blend=all_mode=difference", "-c:v", "libx265", "-preset", "faster", "-c:a", "copy", "-y", out_file]
	return out_file if run_ffm(cmd, de_bug=de_bug) else None

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
		primary_video = [s for s in metadata['streams'] if s['codec_type'] == 'video'][0]
		height = primary_video['height']
		temp_upscale = None
		if height < 720:
			print("    .Performing AI upscale to 1080p...")
			temp_upscale = os.path.join(os.path.dirname(input_file), "temp_upscaled.mp4")
			if ai_upscale(input_file, temp_upscale):
				input_file = temp_upscale

		ff_run_cmd, skip_it = parse_finfo(input_file, metadata, de_bug=False)
		if not skip_it:
			out_file = ffmpeg_run(input_file, ff_run_cmd, skip_it)
			if out_file:
				size_diff = clean_up(sys.argv[1], out_file, skip_it, de_bug)  # Use original input for clean_up
				print(f"\nSuccess! Output file: {out_file} Size diff: {size_diff}")
			else:
				print("\nffmpeg run failed or was aborted.")
		if temp_upscale and os.path.exists(temp_upscale):
			os.remove(temp_upscale)
	except Exception as e:
		error_log = os.path.splitext(os.path.basename(input_file))[0] + "_error.log"
		with open(error_log, "a") as f:
			f.write(f"main error: {e}\n")
		print(f"\nAn error occurred: {e}")

if __name__ == "__main__":
	main()
