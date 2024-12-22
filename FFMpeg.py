# -*- coding: utf-8 -*-
import os
import re
import sys
import json

import datetime  as TM
import subprocess as SP

from collections import defaultdict
from typing import List, Dict, Tuple, Any, Optional

from My_Utils import *
from Yaml import *

debug = False

ffmpg_bin = 'C:\\Program Files\\ffmpeg\\bin'

ffmpeg = os.path.join(ffmpg_bin, "ffmpeg.exe" )
ffprob = os.path.join(ffmpg_bin, "ffprobe.exe")
# ffplay = os.path.join(ffmpg_bin, "ffplay.exe")

if not os.path.exists(ffmpeg) or not os.path.exists(ffprob) :
	input(f"{ffmpeg}\nPath Does not Exist:")
	raise OSError

#SP.run( [ffmpeg, '-version'] )
ffmpeg_vers = f"Ffmpeg version: {SP.run(['ffmpeg', '-version'], stdout=SP.PIPE).stdout.decode('utf-8')[15:20]} (:)"
print( ffmpeg_vers )

##==============-------------------   End   -------------------==============##

@perf_monitor
def ffprobe_run(input_file: str, execu=ffprob, de_bug=False) -> dict:
	"""
	Runs ffprobe to extract media information from a file and returns the parsed JSON data.

	Args:
		input_file (str): Path to the input media file.
		execu (str, optional): Path to the ffprobe executable (defaults to 'ffprob').
		de_bug (bool, optional): Enables debug verbosity for ffprobe (if supported). Defaults to False.

	Returns:
		dict: Parsed JSON data containing the media information extracted by ffprobe.

	Raises:
		FileNotFoundError: If the input file is not found.
		ValueError: If ffprobe reports an error or the JSON output cannot be decoded.
	"""
	msj = sys._getframe().f_code.co_name
#    print(f"  +{msj} Start: {TM.datetime.now():%T}")
	if not input_file :
		raise FileNotFoundError(f"{msj} No input_file provided.")

	cmd = [execu, '-i', input_file,
					'-hide_banner',
					'-analyzeduration', '100000000',
					'-probesize',        '50000000',
					'-v', 'fatal',      # XXX quiet, panic, fatal, error, warning, info, verbose, de_bug, trace
					'-show_programs',
					'-show_format',
					'-show_streams',
					'-show_error',
					'-show_data',
					'-show_private_data',
					'-of','json'        # XXX default, csv, xml, flat, ini
					]
	try:
		out = SP.run(cmd, stdout=SP.PIPE, check=True)
		jsn_ou = json.loads(out.stdout.decode('utf-8'))

		if not jsn_ou or 'error' in jsn_ou:
			error_msg = jsn_ou.get('error') if jsn_ou else f"{input_file} no JSON output)"
			print (f"ffprobe error: {error_msg}")
			raise ValueError(f"ffprobe error: {error_msg}")
		return jsn_ou

	except (FileNotFoundError, SP.CalledProcessError, SP.TimeoutExpired, json.JSONDecodeError) as e:
		msj += f" {msj} Error {e} getting metadata from file:\n{input_file}"
		raise Exception(msj) from e

##>>============-------------------<  End  >------------------==============<<##

# XXX:  Returns encoded file: out_file
@perf_monitor
def ffmpeg_run(input_file: str, ff_com: list, skip_it: bool, execu: str = "ffmpeg", de_bug: bool = False, max_retries: int = 2, retry_delay: int = 2) -> str:
	"""
	Create command line, run ffmpeg with retries, and avoid redundant command building.
	Args:
	  input_file: Path to the input file.
	  ff_com: List of ffmpeg command arguments.
	  skip_it: Flag to skip processing the file.
	  execu: Path to ffmpeg executable (default: "ffmpeg").
	  de_bug: Enable debug printing (default: False).
	  max_retries: Maximum number of retries (default: 2).
	  retry_delay: Delay in seconds between retries (default: 3).
	  Returns:
	  Path to the output file on success, None on skip or failure.
	"""
	msj = sys._getframe().f_code.co_name

	if not input_file or skip_it:
		if de_bug: print(f"{msj}\nSkip: {skip_it}\nFile: {input_file}")
		return None

	print(f"  +{msj} Start: {TM.datetime.now():%T}")

	file_name, _ = os.path.splitext(os.path.basename(input_file))
	out_file     = os.path.normpath('_' + stmpd_rad_str(7, file_name[0:25]))
	out_file     = re.sub(r'[^\w\s_-]+', '', out_file).strip().replace(' ', '_') + TmpF_Ex

	try:
		ffmpeg_vers = SP.check_output([execu, "-version"]).decode("utf-8").splitlines()[0].split()[2]
	except SP.CalledProcessError as e:
		print(f"Error getting ffmpeg version: {e}")
		return ''

	ff_head = [execu, "-thread_queue_size", "24", "-i", input_file, "-hide_banner"]

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
		"-y",           out_file,
	]

	for attempt in range(1, max_retries + 1):
		try:
			if attempt > 1:
				time.sleep(retry_delay)
				print(f"Attempt: {attempt}")
				de_bug = True
				ff_head = [execu, "-report", "-loglevel", "verbose", "-i", input_file, "-hide_banner"]
			todo = ff_head + ff_com + ff_tail

			if run_ffm(todo, de_bug = de_bug):
				return out_file
		except Exception as e:
			print(f"Attempt: {attempt} Failed: {e}")

		if attempt == max_retries:
			print(f"Failed after {attempt} attempts.")
			raise Exception(f"Failed after {attempt} attempts.")

	return None
##>>============-------------------<  End  >------------------==============<<##
@perf_monitor
def run_ffm(args, de_bug=False):
	""" Run ffmpeg command """
	msj = sys._getframe().f_code.co_name

	if not args:
		print(f"{msj} Exit no args = {args}")
		return False
#    de_bug = True
	try :
		if de_bug:
			print(f"\n{msj} debug mode\n{' '.join(args)}\n")

			runit = SP.run(
				args,
#				universal_newlines=True,  # Enable line-by-line text output
#				encoding=console_encoding,
				stderr=SP.STDOUT,
				stdout=SP.PIPE,
			)
			print("\nStd: ", runit.stdout)
			print("\nErr: ", runit.stderr)
			msj += f" Done\n"
			print(msj)
			if runit.returncode != 0:
				print(f"{msj} Command failed with return code {runit.returncode}")
				return False
			time.sleep(2)
			return True

		else:
			with SP.Popen(
				args,
				universal_newlines=True,
#				encoding=console_encoding,
				stderr=SP.STDOUT,
				stdout=SP.PIPE,
			) as process:

				spin_char = "|/-o+\\"
				spinner_counter = 0
				for line in process.stdout:
					if show_progrs(line, spin_char[spinner_counter % len(spin_char)], de_bug=False) :
						spinner_counter += 1

				out, err = process.communicate()
				if err or out:
					msj = f"\nError:\nL:{line}\nE:{err}\nO:{out}"
					print(msj)
					time.sleep(3)

				process.stdout.close()

			if process.returncode != 0:
				msj = f"ffrun error:\n Cmd:{args}\n Stdout: {out}\n"
				print(f"Process return code: {msj}")
				return False

	except Exception as e:
		print(f"{msj} Exception: {e}\n{args}\n")
		return False

	return True
##==============-------------------   End   -------------------==============##

@perf_monitor
def clean_metadata(data):
	"""
	Recursively clean metadata by ensuring all strings are UTF-8 encoded.
	If any encoding issues occur, replace problematic characters.
	"""
	if isinstance(data, dict):
		return {key: clean_metadata(value) for key, value in data.items()}
	elif isinstance(data, list):
		return [clean_metadata(element) for element in data]
	elif isinstance(data, str):
		try:
			return data.encode('utf-8').decode('utf-8')
		except UnicodeDecodeError:
			return data.encode('utf-8', errors='replace').decode('utf-8', errors='replace')
	else:
		return data
@perf_monitor
def parse_frmat(input_file: str, mta_dta: Dict[str, any], de_bug: bool) -> Tuple[List[str], List[str], List[str], List[str], bool]:
	''' Parse and extract data from file format '''
	msj = sys._getframe().f_code.co_name

	global glb_vidolen  # make it global so we can reuse for fmpeg check ...
	global glb_bitrate

	_format = mta_dta.get('format', {})
	_streams = mta_dta.get('streams', [])
	if not _format :
		msj += f" 'format' keyword not in\n{json.dumps(mta_dta, indent=2)}\n"
		print ( msj )
		raise ValueError(msj)
	if not _streams:
		msj += f" 'streams' keyword not in\n{json.dumps(mta_dta, indent=2)}\n"
		print(msj)
		raise ValueError(msj)
	if de_bug:
		print(f"F: {json.dumps(_format, indent=2)}\n ")

	size        =           _format.get('size',        0)
	filename    =           _format.get('filename',    'No_file_name')

	glb_vidolen = int(float(_format.get('duration',    0.0)) )
	glb_bitrate = int(      _format.get('bit_rate',    0) )
	nb_streams  = int(      _format.get('nb_streams',  0) )
	nb_programs = int(      _format.get('nb_programs', 0) )

	tags        =            _format.get('tags', {})
	f_comment   =              tags.get('comment',     'No_comment')
	title       =              tags.get('title',       'No_title')

	# Check if `_streams` exists after cleaning

	# Initialize a dictionary to group streams by codec_type
	streams_by_type = defaultdict(list)

	for i, stream in enumerate(_streams, start=1):
		codec_type = stream.get('codec_type','?')
		streams_by_type[codec_type].append(stream)
	if i != nb_streams:
		print(f" i: {i} != nb_streams: {nb_streams}")
		input ("WTF")


	# Collect all unknown or unrecognized codec_types
	known_types = {'video', 'audio', 'subtitle', 'data'}

	unknown_streams = {k: v for k, v in streams_by_type.items() if k not in known_types}
	if unknown_streams:
		for codec_type, stream in unknown_streams.items():
			msj += f" ! Unrecognized codec_type: {codec_type} in stream {stream}"
			print(msj)

	# Extract the lists of streams for each codec_type
	video_streams = streams_by_type['video']
	audio_streams = streams_by_type['audio']
	subtl_streams = streams_by_type['subtitle']
	datax_streams = streams_by_type['data']

	# Create summary message
	stream_counts = {
		'Prgrm': nb_programs,
		'Strms': nb_streams,
		'V': len(video_streams),
		'A': len(audio_streams),
		'S': len(subtl_streams),
		'D': len(datax_streams)
	}

	# XXX: Check for skip condition # XXX:
	fnam, ext = os.path.splitext(os.path.basename(input_file))
	good_fname = fnam == title and ext == '.mp4'
#	if not good_fname:
#		print(f"  Ext:{ext}\t Fname:{fnam}\n  Title:{title}")

	summary_msg = f"    |=Title|{title}|\n    |>FRMT<|Size: {hm_sz(size)}|Bitrate: {hm_sz(glb_bitrate)}|Length: {hm_time(glb_vidolen)}|"
	summary_msg += ''.join([f" {key}: {count}|" for key, count in stream_counts.items() if count != 0])
	print(f"\033[96m{summary_msg}\033[0m")

	f_skip = good_fname and f_comment == Skip_key
	if f_skip:
		print("   .Skip: Format")

#	de_bug = True
	# Call parse functions for streams if they exist
	ff_com = []
	if video_streams:
		ff_video, v_skip = parse_video(video_streams, de_bug, f_skip )
		ff_com.extend(ff_video)
		skip_it = f_skip and v_skip
		if de_bug:
			print(f"\nVideo ffmpg = {ff_com}\nSkip={skip_it}, Fskip={f_skip}, Vskip={v_skip}\n")
	else :
		msj += f"\nNo Video in: {input_file}\n"
		print(msj)
		time.sleep(2)
		return [], True

	if audio_streams:
		ff_audio, a_skip = parse_audio(audio_streams, de_bug)
		ff_com.extend(ff_audio)
		skip_it = skip_it and a_skip
		if de_bug:
			print(f"\nAudio ffmpg = {ff_com}\nSkip={skip_it}, Askip={a_skip}\n")
	else :
		msj += f"\nNo Audio in: {input_file}\n"
		print(msj)
		time.sleep(2)
		return [], True

	if subtl_streams:
		ff_subtl, s_skip = parse_subtl(subtl_streams, de_bug)
		ff_com.extend(ff_subtl)
		skip_it = skip_it and s_skip
		if de_bug:
			print(f"\nSubtit ffmpg = {ff_com}\nSkip={skip_it}, Sskip={s_skip}\n")
	else :
		ff_subtl, s_skip  = add_subtl_from_file(input_file, de_bug=True)
		ff_com.extend(ff_subtl)
# XXX:         skip_it = skip_it and s_skip #after fixing the add_subtitle ...
		skip_it = skip_it and True

	if datax_streams:
		ff_datat, d_skip = parse_extrd(datax_streams, de_bug)
		d_skip = True    # XXX: Avoid reencode
		skip_it = skip_it and d_skip
		if de_bug:
			print(f"\nData ffmpg = {ff_com}\nSkip={skip_it}, Dskip={d_skip}\n")

	if de_bug:
		input(f"\n{msj}\n Ready to do it?")

	return ff_com, skip_it

##>>============-------------------<  End  >------------------==============<<##
def add_subtl_from_file(input_file: str, de_bug: bool) -> tuple[list, bool]:
# XXX: Needs work it breacks the ffmpeg syntax  combined with audio and video perhaps stand alone ?
	"""
	Searches for a subtitle file in the same directory as the input file
	and prompts for confirmation before adding it.

	Args:
		input_file (str): Path to the input file.
		de_bug (bool): Flag to enable debug messages.

	Returns:
		tuple: A tuple containing subtitle arguments (as a list) and a flag
			indicating whether to skip subtitle addition (True) or not (False).
	"""
	msj = f"    | No Subtitle |"

	# Get the directory of the input file
	directory = os.path.dirname(input_file)

	largest_file = None
	extensions = ['.srt', '.ass', '.sub']

	for file in os.listdir(directory):
		if any(file.endswith(ext) for ext in extensions):
			filepath = os.path.join(directory, file)
			# Directly compare file sizes here, updating largest_file if the current file is larger
			if not largest_file or os.path.getsize(filepath) > os.path.getsize(largest_file):
				largest_file = filepath

	if largest_file:
		if de_bug:
			msj += f"    | Ext subtitle file:\n    {largest_file}"
			print(msj)
		'''
		if input("Add subtitles from this file (y/N)? ").lower() == "y":
			# Return arguments for subtitles and indicate success (False)
			ff_sub = ['-i', f"{largest_file}", '-map', '1:0', '-c:s', 'mov_text']
			return ff_sub, False
		else:
			print(f"    | Ext subtitle file ignored:\n    {largest_file}")
			# User declined, indicate skipping subtitles (True)
			return [], True
		'''
	else:
		if de_bug:
			print("    |No External Sub File |")
	return [], True
##>>============-------------------<  End  >------------------==============<<##

def get_encoder_options(codec_name, is_10bit, bit_rate, use_hw_accel=False):
	msj = sys._getframe().f_code.co_name

	# Quality presets
	target_quality='as_is'
	quality_presets = {
		'low':		{'bitrate': (bit_rate // (1024 * 3   )), 'quality': 26},
		'medium':	{'bitrate': (bit_rate // (1024 * 1.5 )), 'quality': 24},
		'as_is':	{'bitrate': (bit_rate // (1024       )), 'quality': 21},
		'high':		{'bitrate': (bit_rate // (1024 * 0.75)), 'quality': 20},
		'higher':	{'bitrate': (bit_rate // (1024 * 0.5 )), 'quality': 18},
	}

	preset = quality_presets[target_quality]

	base_target_bitrate	= int(preset['bitrate'])
	global_quality		= preset['quality']

	# Adjust for 10-bit content
	if is_10bit:
		target_bitrate = str(int(base_target_bitrate * 1.25)) + 'k'
	else:
		target_bitrate = str(base_target_bitrate) + 'k'

	# Calculate max_bitrate and bufsize
	max_bitrate	= str(int(int(target_bitrate.rstrip('k')) * 1.5)) + 'k'
	bufsize		= str(int(int(max_bitrate.rstrip('k')) * 2)) + 'k'

	if use_hw_accel:
#		print(f"    {msj} HW accelerated")
		hw_pix_fmt = "p010le" if is_10bit else "nv12"
		return [
			'hevc_qsv',
			'-load_plugin',			'hevc_hw',
			'-init_hw_device',		'qsv=qsv:MFX_IMPL_hw_any',
			'-filter_hw_device', 	'qsv',
			'-pix_fmt',				hw_pix_fmt,
			'-b:v',					target_bitrate,
			'-maxrate',				max_bitrate,
			'-bufsize',				bufsize,
			'-look_ahead',			'1',
			'-look_ahead_depth',	'90',
			'-global_quality',		str(round(global_quality)),
			'-rc:v',				'vbr_la',  # Use variable bitrate with lookahead
			'-preset',				'slow',
		]
	else:
#		print(f"    {msj} SW")
		sw_pix_fmt = "yuv420p10le" if is_10bit else "yuv420p"
		return [
			'libx265',
			'-x265-params',		'bframes=8:psy-rd=1:aq-mode=3:aq-strength=0.8:deblock=1,1',
			'-pix_fmt',			sw_pix_fmt,
			'-crf',				str(round(global_quality)),
			'-b:v',				target_bitrate,
			'-maxrate',			max_bitrate,
			'-bufsize',			bufsize,
			'-preset',			'slow',
		]
##>>============-------------------<  End  >------------------==============<<##

@perf_monitor
def parse_video(strm_in, de_bug=False, skip_it=False):
	''' Parse and extract data from video streams '''
	msj = sys._getframe().f_code.co_name
	if de_bug:
		print(f"    +{msj} Start: {TM.datetime.now():%T}")

	global glb_vidolen  # make it global so we can reuse for fmpeg check ...
	global vid_width    # NOTE used by matrix_it
	global glb_totfrms

	use_hw_accel = True
	skip_all = True  # Track if all streams can be skipped

	ff_video = []

	for indx, this_vid in enumerate(strm_in):
		ff_vid = []
		extra = ''

		# Ensure codec information is present
		if 'codec_name' not in this_vid:
			msj += f"   No codec_name |<V:{this_vid}\n"
			print(msj)
			raise Exception(msj)

		# Extract necessary video information
		codec_name	= this_vid.get('codec_name', 'XXX')
		pix_fmt		= this_vid.get('pix_fmt', '')
		vid_width	= this_vid.get('width', 2)
		vid_heigh	= this_vid.get('height', 1)
		tags		= this_vid.get('tags', {})
		handler_name = tags.get('handler_name', 'Unknown')
		frm_rate	= divd_strn(this_vid.get('r_frame_rate', '25'))
		_vi_btrt	= int(this_vid.get('bit_rate', glb_bitrate * 0.8))

		encoder_options = "No Encoder"
		if 'bit_rate' not in this_vid:
			extra = 'Bit Rate Estimate'

		# Skip unsupported formats
		if codec_name.lower() in ['mjpeg', 'png', 'XXX']:
			print(f"    |<V:{indx:2}>|{codec_name:^8}| Remove it |")
			ff_vid = ['-map', f'-0:v:{indx}']
			continue

		# Calculate total frames
		glb_totfrms = round(frm_rate * glb_vidolen)

		# Determine maximum bitrate
		max_vid_btrt = 3600000
		msj = " 8-bit"
		if pix_fmt.endswith("10le"):
			msj = "10-bit"
			max_vid_btrt = int(max_vid_btrt * 1.25)

		btrt = min(_vi_btrt * 1.1, max_vid_btrt)

		# Calculate aspect ratio
		original_ratio = vid_width / vid_heigh
		standard_ratios = {
			"4:3": 4 / 3,
			"16:9": 16 / 9,
			"3:2": 3 / 2,
			"1:1": 1,
		}
		aspct_r = min(standard_ratios, key=lambda k: abs(standard_ratios[k] - original_ratio))

		# Map the input video stream
		ff_vid = ['-map', f'0:v:{indx}', f'-c:v:{indx}']

		# Decide whether to copy or re-encode the stream
		if codec_name == 'hevc' and _vi_btrt <= (max_vid_btrt * 1.1 ):
			ff_vid.append('copy')
			extra += ' => Copy'
		else:
			encoder_options = get_encoder_options(codec_name, pix_fmt.endswith("10le"), btrt, use_hw_accel)
			ff_vid.extend(encoder_options)
			extra += f'| Re-encode bitrate: {hm_sz(btrt)}'
			skip_all = False

		# Determine output resolution
		output_resolutions = [
			(7600, 4300, '8K'),
			(3800, 2100, '4K'),
			(2100, 1920, '2K'),
			(1280, 720, 'HD')
		]
		output = "SD"
		for w, h, label in output_resolutions:
			if vid_width >= w or vid_heigh >= h:
				output = label
				break

		# Apply scaling if resolution exceeds HD
		if output in ["2K", "4K", "8K"]:
			nw, nh = 1920, round((1920 / vid_width) * vid_heigh / 2) * 2  # Ensure even height
			ff_vid.extend([
				'-vf', f'scale={nw}:{nh}', '-pix_fmt', 'yuv420p10le',
				'-crf', '21', '-b:v', f'{max_vid_btrt}', '-preset', 'slow'
			])
			extra += f'|Scale {vid_width}x{vid_heigh} to {nw}x{nh}'
			skip_all = False

		# Update handler name if needed
		desired_handler_name = "VideoHandler x265"
		if handler_name != desired_handler_name:
			ff_vid.extend([f"-metadata:s:v:{indx}", f"handler_name={desired_handler_name}"])
			extra += f'| Change to: {desired_handler_name}'
			skip_all = False

		# Print detailed log message
		message = (
			f"    |<V:{indx:2}>|{codec_name:^8}|{vid_width:<4}x{vid_heigh:<4}|{aspct_r}|{msj}|"
			f"Bitrate: {hm_sz(_vi_btrt):>6}|FPS: {frm_rate:>4}|"
			f"Frames: {hm_sz(glb_totfrms, 'F'):>8}|{extra}|"
		)
		print(f"\033[91m{message}\033[0m")

		ff_video += ff_vid

		if de_bug:
			print(f"\nStream Data: {json.dumps(this_vid, indent=2)}\n"
				  f"Encoder Options: {encoder_options}\n"
				  f"Command: {ff_vid}\nSkip: {skip_all}")

	# Handle skipping logic
	if skip_all and skip_it:
		print("   .Skip: Video")

	return ff_video, skip_all


##>>============-------------------<  End  >------------------==============<<##
# XXX: Audio
@perf_monitor
def parse_audio(streams, de_bug=False):
	"""Parse and process audio streams, prioritizing the best English stream based on channels and bitrate."""

	ffmpeg_audio_options = []
	all_skippable = True

	best_stream = None
	previous_default_index = None

	# Single loop to identify the best stream

		# Track the previous default stream index

		# Determine the best English stream if available
	extracted_data = []

	# Second loop to generate ffmpeg options and set dispositions
	for indx, audio_stream in enumerate(streams):
		codec_name = audio_stream.get('codec_name', 'unknown')
		channels = int(audio_stream.get('channels', -1))
		bitrate = int(audio_stream.get('bit_rate', 0))
		language = audio_stream.get('tags', {}).get('language', 'und')
		disposition = audio_stream.get('disposition', {})
		dispo_default = int(disposition.get('default', 0))
		handler_name = audio_stream.get('tags', {}).get('handler_name', 'Unknown')
		sample_rate = audio_stream.get('sample_rate', 'N/A')

		extracted_data.append({
			'codec_name': codec_name,
			'channels': channels,
			'bitrate': bitrate,
			'language': language,
			'dispo_default': dispo_default,
			'handler_name': handler_name,
			'sample_rate': sample_rate
		})
		if dispo_default:
			previous_default_index = indx
		if language == 'eng':
			if (best_stream is None or
					channels > best_stream['channels'] or
					(channels == best_stream['channels'] and bitrate > best_stream['bitrate'])):
				best_stream = extracted_data[-1]
	for indx, data in enumerate(extracted_data):
		# Generate ffmpeg options for the current stream
		copy_codec = data['codec_name'] in ('aac', 'vorbis', 'mp3', 'opus') and data['channels'] <= 8
		stream_options = ['-map', f'0:a:{indx}']

		if copy_codec:
			stream_options.extend([f'-c:a:{indx}', 'copy'])
		else:
			stream_options.extend([f'-c:a:{indx}', 'libvorbis', '-q:a', '8'])

		# Metadata handling
		if data['handler_name'] != "SoundHandler":
			stream_options.extend([f"-metadata:s:a:{indx}", "handler_name=SoundHandler"])


		# Set the disposition for each stream, ensuring only the best stream is set to default
		if data == best_stream:
			stream_options.extend([f'-disposition:a:{indx}', 'default'])
			if previous_default_index is None or best_stream != extracted_data[previous_default_index]:
				all_skippable = False
		else:
			stream_options.extend([f'-disposition:a:{indx}', 'none'])

		ffmpeg_audio_options.extend(stream_options)
		# Print audio stream details after setting disposition
		message = (f"    |<A:{indx:2}>|{data['codec_name']:^8}|Br:{data['bitrate']:>9}|{data['language']}|Frq:{data['sample_rate']:>8}|"
				   f"Ch:{data['channels']}|Dis:{data['dispo_default']}|Handler:{data['handler_name']}")
		if data['dispo_default']:
			message += " |Was default"
		if data == best_stream:
			message += " |Is new default"
		print(f'\033[92m{message}\033[0m')

	# Ensure -disposition commands are correctly assigned

	# Set skip_audio flag:
	# - If there's only one audio stream
	# - Or if there are multiple streams, but the best stream is the same as the previous default
	skip_audio = len(streams) == 1 or (best_stream and previous_default_index is not None and best_stream == extracted_data[previous_default_index])

	if skip_audio:
		all_skippable = True
		print("   .Skip: Audio")
	else:
		all_skippable = False
#		print("   .Do not skip: Audio - Default stream changed.")
#		print(f"   =A: {ffmpeg_audio_options}")

	# Debug output of the ffmpeg options
	if de_bug:  # Ensure debug output is printed
		print(f"   =A: {ffmpeg_audio_options}")

	return ffmpeg_audio_options, all_skippable


##>>============-------------------<  End  >------------------==============<<##


# XXX: Subtitle
@perf_monitor
def parse_subtl(streams_in, de_bug=False):
	"""Parse and extract data from subtitle streams."""

	all_skippable = True  # Initialize variable to keep track of whether all subtitle streams can be skipped
	ff_subttl = []
	default_eng_set = False  # Track if default English subtitle has been set

	for indx, this_sub in enumerate(streams_in):
		ff_sub = []
		extra = ''
		metadata_changed = False  # New flag to track metadata changes only

		codec_name   = this_sub.get('codec_name', 'unknown?')
		codec_type   = this_sub.get('codec_type', 'unknown?')
		disposition  = this_sub.get('disposition', {'forced': 0, 'default': 0})

		tags         = this_sub.get('tags', {})
		handler_name = tags.get('handler_name', 'Unknown')
		language     = tags.get('language', 'und')

		if codec_name in ('hdmv_pgs_subtitle', 'dvd_subtitle', 'ass', 'unknown?'):
			ff_sub = [f'-map', f'-0:s:{indx}']
			extra += f" Delete: {codec_name} {language} |"
			all_skippable = False  # Metadata change means this stream isn't fully skippable

		elif codec_name in ('subrip', 'mov_text'):
			ff_sub = [f'-map', f'0:s:{indx}']
			if language == 'eng':
				extra += f"Keep: {codec_name} {language}|"
				if not default_eng_set:
					default_eng_set = True  # Set the first English subtitle as default
					extra += f"Set to Default|"
					ff_sub.extend([f'-c:s:{indx}', 'mov_text', f'-metadata:s:s:{indx}', f'language={language}', f'-disposition:s:s:{indx}', 'default'])
				else:
					extra += f"Not Default|"
			elif language in Keep_langua:
				extra += f"Keep: {codec_name} {language}"
			else:
				ff_sub = [f'-map', f'-0:s:{indx}']
				extra += f" Delete: {codec_name} {language} X"
			if handler_name != "mov_text":  # Use the correct subtitle handler for MP4
				extra += f" handler_name: {handler_name} Change to: mov_text"
				ff_sub.extend([f"-metadata:s:s:{indx}", "handler_name=mov_text"])
				metadata_changed = True

		# Print message for this subtitle stream
		message = f"    |<S:{indx:2}>|{codec_name[:8]:^8}|{codec_type[:8]}|{language:3}|Disp: default={disposition['default']}, forced={disposition['forced']}|{extra}"
		print(f"\033[94m{message}\033[0m")

		ff_subttl += ff_sub  # Only add if the list is not empty
		if not metadata_changed:  # Only mark as skippable if no content or metadata changes occurred
			all_skippable &= True
		else:
			all_skippable = False  # Metadata change means this stream isn't fully skippable

	ff_subttl.extend([f'-c:s', 'mov_text'])

	if de_bug:
		print(f"S:= {ff_subttl}")

	if all_skippable:
		print("   .Skip: Subtitle")

	return ff_subttl, all_skippable

##>>============-------------------<  End  >------------------==============<<##

# XXX: Extra Data
@perf_monitor
def parse_extrd(streams_in, de_bug=False):
	"""Parse and extract data from data streams."""

	ff_data = []
	for indx, this_dat in enumerate(streams_in):
#        if de_bug:    print(f"    +{msj} Start: {TM.datetime.now():%T}\n {json.dumps(this_dat, indent=2)}")
		ff_dd = []

		index           = this_dat.get('index', -1)
		codec_name      = this_dat.get('codec_name', '')
		codec_lng_nam   = this_dat.get('codec_long_name', '')
		codec_type      = this_dat.get('codec_type', '')

		tags            = this_dat.get('tags', {})
		handler_name	= tags.get('handler_name', 'Unknown')

		msj = f"    |<D:{index:2}>|{codec_name:^8}| {codec_lng_nam:<9}| {codec_type:^11} | {handler_name}"
		if handler_name == 'SubtitleHandler':
			msj += "|Keep Subtitle|"
			print(msj)
			print ("   .Skip: Data")
			return [], True

		ff_dd = ['-map', f'-0:d:{indx}' ]
		print(msj)

		ff_data += ff_dd

	if de_bug:
		print(f"D:= {ff_data}")

	return ff_data, True

##>>============-------------------<  End  >------------------==============<<##

@perf_monitor
def parse_finfo(input_file: str, mta_dta: Dict[str, any], de_bug: bool= False ) -> Tuple[bool, bool]:
	''' Decide what based on streams info '''
	msj = sys._getframe().f_code.co_name
	print(f"  +{msj} Start: {TM.datetime.now():%T}")
	str_t = time.perf_counter()

	if not input_file or not mta_dta :
		print (f"\n{msj}\nF:{input_file}\nM:{mta_dta} Exit:")
		return False , True
	try :
		ff_run_cmnd, skip_it = parse_frmat(input_file, mta_dta, de_bug)
#        breakpoint()  # Set a breakpoint here
	except ValueError as e:
		if e.args[0] == "Skip It":
			print(f"Go on: {e}")
			return False , True
	end_t = time.perf_counter()
	if de_bug: print(f'  -End: {TM.datetime.now():%T}\tTotal: {hm_time(end_t - str_t)}')

	return ff_run_cmnd, skip_it
##>>============-------------------<  End  >------------------==============<<##

# Precompile regular expressions
regex_dict = {
		"bitrate":	re.compile(r"bitrate=\s*([0-9\.]+)"),
		"frame":	re.compile(r"frame=\s*([0-9]+)"),
		"speed":	re.compile(r"speed=\s*([0-9\.]+)"),
		"size":		re.compile(r"size=\s*([0-9]+)"),
		"time":		re.compile(r"time=\S([0-9:]+)"),
		"fps":		re.compile(r"fps=\s*([0-9]+)"),
		}

def show_progrs(line_to, sy, de_bug=False):
	msj = sys._getframe().f_code.co_name
	_P = ""

	if "N/A" in line_to:
		return False

	elif all(substr in line_to for substr in ["fps=", "speed=", "size="]):
		regx_val = {}
		try:
			# Use compiled regular expressions
			for key, regex in regex_dict.items():
				regx_val[key] = regex.search(line_to).group(1)

			fp = regx_val["fps"]
			fp_int = int(fp)

			if fp_int >= 1:
				sp = regx_val["speed"]
				sp_float = float(sp)
				time_parts = regx_val["time"].split(":")
				a_sec = int(time_parts[0]) * 3600 + int(time_parts[1]) * 60 + int(time_parts[2])
				dif = abs(glb_vidolen - a_sec)
				eta = round(dif / sp_float)
				mints, secs = divmod(int(eta), 60)
				hours, mints = divmod(mints, 60)
				_eta = f"{hours:02d}:{mints:02d}:{secs:02d}"
				_P = f"    | {sy} |Size: {hm_sz(regx_val['size']):>7}|Frames: {int(regx_val['frame']):>6}|Fps: {fp_int:>3}|BitRate: {hm_sz(regx_val['bitrate']):>6}|Speed: {sp_float:>5}|ETA: {_eta:>8}|    \r"
				if de_bug:
					print(f"\n {line_to}\n | {sy} |Size: {hm_sz(regx_val['size']):>7}|Frames: {int(regx_val['frame']):>6}|Fps: {fp_int:>3}|BitRate: {hm_sz(regx_val['bitrate']):>6}|Speed: {sp_float:>5}|ETA: {_eta:>8}|")

		except Exception as e:
			msj = f"    {msj} Exeption: {e} in {line_to}"
			print(msj)

	elif any(substr in line_to for substr in ["muxing overhead:", "global headers:"]):
		match = re.search(r"\[out#0\/mp4 @ \w+\]\s*(.*)", line_to)
		if match:
			print(f"\n    |<+>|Done: {match.group(1)}")
		else:
			print(f"\n    |<+>|Done: {line_to}")
		return -1

	sys.stderr.write(_P)
	sys.stderr.flush()
	return True

##>>============-------------------<  End  >------------------==============<<##

@perf_monitor
def matrix_it(input_file, execu=ffmpeg, ext ='.png'):
	''' Create a 3x3 matrix colage '''
	str_t = time.perf_counter()
	msj = sys._getframe().f_code.co_name
	print(f"  +{msj} Start: {TM.datetime.now():%T}")

	global glb_totfrms , vid_width

# XXX Create Matrix Colage:
	file_name, _ext = os.path.splitext(input_file)
	if _ext not in File_extn: # XXX: Very unlikely !!
		msj = f"Input: {input_file} Not video file."
		raise Exception (msj)

	if os.path.isfile(file_name + ext):
		print( f"   | PNG Exists ¯\\_(%)_/¯ Skip")
		end_t = time.perf_counter()
		print(f'  -End: {TM.datetime.now():%T}\tTotal: {hm_time(end_t - str_t)}')
		return False

	else:
		file_name += ext
		width = str(vid_width)
		# We have 9 tiles plus a bit more
		slice = str(round( glb_totfrms / 9 + 1 ))

		skip0 = '00:01:13'
#        zzzle =  "[0:v]select=not(mod(n\," + slice + ")), scale=" + width + ":-1:, tile=3x3:nb_frames=9:padding=3:margin=3"
		zzzle = f"[0:v]select=not(mod(n\\, {slice})), scale={width}:-1:, tile=3x3:nb_frames=9:padding=3:margin=3 "

		if glb_totfrms > 6000:
			todo = (execu, '-ss', skip0, '-vsync', 'vfr', '-i', input_file, '-frames', '1', '-vf', zzzle, '-y', file_name)
		else:
			todo = (execu,               '-vsync', 'vfr', '-i', input_file, '-frames', '1', '-vf', zzzle, '-y', file_name)
		# XXX:
		if run_ffm(todo):
			msj = f"\r    |3x3| Matrix Created {ext}"
			print(msj)
			end_t = time.perf_counter()
			print(f'  -End: {TM.datetime.now():%T}\tTotal: {hm_time(end_t - str_t)}')
			return os.path.getsize(file_name)
		else:
			msj = f"   = Failed to Created .PNG >"
			raise Exception(msj)

	end_t = time.perf_counter()
	print(f'  -End: {TM.datetime.now():%T}\tTotal: {hm_time(end_t - str_t)}')
##>>============-------------------<  End  >------------------==============<<##


@perf_monitor
def speed_up ( input_file, *other) :
	''' Create a 4x sped up version '''
	str_t = time.perf_counter()
	msj = sys._getframe().f_code.co_name
	print(f"  +{msj} Start: {TM.datetime.now():%T}")
	spdup = 3
# https://trac.ffmpeg.org/wiki/How%20to%20speed%20up%20/%20slow%20down%20a%20video
	file_name, _ = os.path.splitext( os.path.basename(input_file) )
	out_f = '_Speed_ '+file_name +stmpd_rad_str(3) +TmpF_Ex

	todo = (ffmpeg, '-i', input_file,'-filter:v', "setpts=PTS/ 3 , '-af', asetrate={aud_smplrt} 3  , aresample={aud_smplrt}", '-y', out_f )

#    todo = (ffmpeg, '-i', input_file, '-filter_complex', "[0:v]setpts=PTS/3[v];[0:a]atempo=3[a]", '-map', "[v]", '-map', "[a]", '-y', out_f )

	todo = (ffmpeg, '-i', input_file, '-bsf:v', "setts=TS/3", '-af', "atempo=3", '-y', out_f )
	todo = (ffmpeg, '-i', input_file, '-filter_complex', "[0:v]setpts=PTS/ 3 [v];[0:a]atempo= 3 [a]",'-map', "[v]", '-map', "[a]", '-y', out_f)

	print ( todo  )
	if run_ffm( todo, de_bug ):
		return out_f
	else:
		msj+= f"   = Failed to \n"
		raise Exception(msj)

	end_t = time.perf_counter()
	print(f'  -End: {TM.datetime.now():%T}\tTotal: {hm_time(end_t - str_t)}')
	return todo
##>>============-------------------<  End  >------------------==============<<##

@perf_monitor
def video_diff(file1, file2) :
	# XXX:  Visualy Compare in and out files
	# https://stackoverflow.com/questions/25774996/how-to-compare-show-the-difference-between-2-videos-in-ffmpeg
	str_t = time.perf_counter()
	msj = sys._getframe().f_code.co_name
	print(f"  +{msj} Start: {TM.datetime.now():%T}")

	file_name, _ = os.path.splitext( os.path.basename(file1) )
	out_f = '_Diff_'+file_name +stmpd_rad_str(3) +TmpF_Ex

	todo = (ffmpeg, '-i', file1, '-i', file2, '-filter_complex', "blend=all_mode=difference", '-c:v', 'libx265', '-preset', 'faster', '-c:a', 'copy', '-y', out_f)

	if run_ffm(todo):
		return out_f
	else:
		msj+= f"   = Failed to Compare Files >\n"
		raise Exception(msj)

	end_t = time.perf_counter()
	print(f'  -End: {TM.datetime.now():%T}\tTotal: {hm_time(end_t - str_t)}')

	return out_file
	##>>============-------------------<  End  >------------------==============<<##

@perf_monitor
def short_ver ( input_file, execu, *other ) :
	str_t = time.perf_counter()
	msj = sys._getframe().f_code.co_name
	print(f"  +{msj} Start: {TM.datetime.now():%T}")

	print (f" F: {input_file} E: {execu} O: {other}")

	str_at = '00:00:33'
	end_at = '00:05:55'
	print (f"Make short ver Start:{str_at} End:{end_at}" )

	file_name, _ = os.path.splitext( os.path.basename(input_file) )
	out_file = '_Short_' +file_name +stmpd_rad_str(3) +TmpF_Ex

	ff_head = [ execu, '-ss', str_at, '-t', end_at, '-i', input_file, '-max_muxing_queue_size', '2048']
	ff_com  = ['-map', '0:V?', '-map', '0:a?', '-map', '0:s?', '-c:s', 'mov_text' ]
	ff_tail = [ '-y', out_file ]

	todo = ff_head +ff_com +ff_tail
	if run_ffm( todo ) and os.path.exists(out_file) :
		msj+= f'\n{todo}\nNo Output Error'
		print(msj)
		time.sleep(1)
		return out_file
	else :
		print (" Failed")

	end_t = time.perf_counter()
	print(f'  -End: {TM.datetime.now():%T}\tTotal: {hm_time(end_t - str_t)}')
	return out_file
##>>============-------------------<  End  >------------------==============<<##
