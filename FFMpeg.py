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
version = f"Ffmpeg version: {SP.run(['ffmpeg', '-version'], stdout=SP.PIPE).stdout.decode('utf-8')[30:33]} (:)"
print( version )

##==============-------------------   End   -------------------==============##
spin_count = 0
def print_spinner(extra=""):
	global spin_count
	rel_path = extra.replace(Root, "").lstrip(os.sep)
	spin_char = "|/-o+\\"
	sys.stderr.write(f"\r | {spin_char[spin_count % len(spin_char)]} | {rel_path} \r")
	sys.stderr.flush()
	spin_count += 1

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
#	print(f"  +{msj} Start: {TM.datetime.now():%T}")

	if not input_file :
		raise FileNotFoundError(f"{msj} No input_file provided.")
	print_spinner (input_file[:90])

	cmd = [execu, '-hide_banner', '-i', input_file,
					'-analyzeduration', '100000000',
					'-probesize',        '50000000',
					'-v', 'fatal',       # XXX quiet, panic, fatal, error, warning, info, verbose, de_bug, trace
					'-of','json',        # XXX default, csv, xml, flat, ini
						'-show_programs',
						'-show_format',
						'-show_streams',
						'-show_error',
						'-show_data',
						'-show_private_data',
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
		msj += f" {msj} Error {e} getting metadata from file \n{input_file}"
		raise Exception(msj) from e

##>>============-------------------<  End  >------------------==============<<##


# XXX:  Returns encoded filename file_name
@perf_monitor
def ffmpeg_run(input_file: str, ff_com: list, skip_it: bool, execu: str = "ffmpeg", de_bug: bool = False, max_retries: int = 2, retry_delay: int = 3) -> str:
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
		msj = f"{msj} Skip={skip_it} = {input_file}"
		return None

	print(f"  +{msj} Start: {TM.datetime.now():%T}")

	file_name, _ = os.path.splitext(os.path.basename(input_file))
	out_file = os.path.normpath('_' + stmpd_rad_str(7, file_name[0:20]))
	out_file        = re.sub(r'[^\w\s_-]+', '', out_file).strip().replace(' ', '_') + TmpF_Ex

	try:
		ffmpeg_vers = SP.check_output([execu, "-version"]).decode("utf-8").splitlines()[0].split()[2]
	except SP.CalledProcessError as e:
		print(f"Error getting ffmpeg version: {e}")
		return ''

	ff_tail = [
		"-metadata", f"title={file_name} x256",
		"-metadata", f"comment={Skip_key}",
		"-metadata", "copyright=2023 Me",
		"-metadata", "author=Encoded by the One and only GeoHab",
		"-metadata", f"encoder=ffmpeg {ffmpeg_vers}",
		"-movflags", "+faststart",
		"-fflags", "+fastseek",
		"-fflags", "+genpts",
		"-y", out_file,
	]

	ff_head = [execu, "-thread_queue_size", "24", "-i", input_file, "-hide_banner"]

	for attempt in range(1, max_retries + 1):
		try:
			if attempt > 1:
				time.sleep(retry_delay)
				print(f"Attempt: {attempt}")
				de_bug = True
				ff_head = [execu, "-report", "-loglevel", "verbose", "-i", input_file, "-hide_banner"]
			todo = ff_head + ff_com + ff_tail

			if run_ffm(todo, de_bug=de_bug):
				return out_file
		except Exception as e:
			print(f"Attempt {attempt} failed: {e}")
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

			runit = SP.run( args,
				universal_newlines=True,
				encoding=console_encoding,
	#            stderr=SP.STDOUT,
	#            stdout=SP.PIPE,
				 )
			print("\nStd: ", runit.stdout)
			print("\nErr: ", runit.stdout)
			msj += f" Done\n"
			print(msj)

			time.sleep(3)
		else:
			with SP.Popen( args,
				universal_newlines=True,
				encoding=console_encoding,
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
				zz = ( process.returncode, args, out, err )
				print ( f" Procces returncode {zz}")
				raise SP.CalledProcessError(
					process.returncode, args, output=out, stderr=err )

	except Exception as e:
		msj += f" Exception: {e}"
		print( msj )
		return False

	return True
##==============-------------------   End   -------------------==============##

@perf_monitor
def parse_frmat(input_file: str, mta_dta: Dict[str, any], de_bug: bool) -> Tuple[List[str], List[str], List[str], List[str], bool]:
	''' Parse and extract data from file format '''
	msj = sys._getframe().f_code.co_name

	global glb_vidolen  # make it global so we can reuse for fmpeg check ...
	global glb_bitrate

	_Format = mta_dta.get('format', {})
	if de_bug: print(f"F: {json.dumps(_Format, indent=2)}\n ")

	if not _Format :
		msj += f" 'format' keyword not in\n{ json.dumps(mta_dta, indent=2) }\n"
		print ( msj )
		raise ValueError(msj)

	glb_vidolen = int(float(_Format.get('duration',     0.0)) )
	glb_bitrate = int(      _Format.get('bit_rate',     0) )

	nb_streams  = int(      _Format.get('nb_streams',   0) )
	nb_programs = int(      _Format.get('nb_programs',  0) )
	size        =           _Format.get('size',         0)
	filename    =           _Format.get('filename',     'No File Name')

	f_comment   =           _Format.get('tags',         {}).get('comment', 'No_comment')
	title       =           _Format.get('tags',         {}).get('title', 'No_title')

	_Streams = mta_dta.get('streams', [])

	# Initialize a dictionary to group streams by codec_type
	streams_by_type = defaultdict(list)

	for i, stream in enumerate(_Streams, start=1):
		codec_type = stream.get('codec_type','?')
		streams_by_type[codec_type].append(stream)

	if debug:
		print(f"S: {json.dumps(_Streams, indent=2)}\n ")
		# Collect all unknown or unrecognized codec_types
	known_types = {'video', 'audio', 'subtitle', 'data'}
	unknown_streams = {k: v for k, v in streams_by_type.items() if k not in known_types}
	if unknown_streams:
		for codec_type, streams in unknown_streams.items():
#			msj += f" Unrecognized codec_type '{codec_type}'\n found in streams: {json.dumps(streams, indent=2)}"
			msj += f" Unrecognized codec_type '{codec_type}' in stream {i}"
			print(msj)
	if i != nb_streams:
		print(f" i = {i} != nb_streams = {nb_streams}")
		input ("WTF")

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
	summary_msg = f"    |=Title|{title}|\n    |>FRMT<|Size: {hm_sz(size)}|Bitrate: {hm_sz(glb_bitrate)}|Length: {hm_time(glb_vidolen)}|"
	summary_msg += ''.join([f" {key}: {count}|" for key, count in stream_counts.items() if count != 0])

	# Check for skip condition
	_, ext = os.path.splitext(input_file)

	f_skip = (f_comment == Skip_key) and (ext == '.mp4')

	if f_skip:
		summary_msg += f" Key OK"        # {filename}
	else:
		summary_msg += f" Key not OK"    # {filename}

	print(f"\033[96m{summary_msg}\033[0m")

# Call parse functions for streams if they exist
	ff_com = []
	if video_streams:
		ff_video, v_skip = parse_video(video_streams, de_bug)
		ff_com.extend(ff_video)
		skip_it = f_skip and v_skip
		if de_bug : print (f"\nSkip={skip_it}, Fskip ={f_skip}, Vskip ={v_skip}\n" )
	else :
		msj += f"\nNo Video in: {input_file}\n"
		print(msj)
		raise ValueError(msj)

	if audio_streams:
		ff_audio, a_skip = parse_audio(audio_streams, de_bug)
		ff_com.extend(ff_audio)
		skip_it = skip_it and a_skip
		if de_bug : print (f"\nSkip={skip_it}, Askip = {a_skip}\n" )
	else :
		msj += f"\nNo Audio in: {input_file}\n"
		print(msj)
		raise ValueError(msj)

	if subtl_streams:
		ff_subtl, s_skip = parse_subtl(subtl_streams, de_bug)
		ff_com.extend(ff_subtl)
		skip_it = skip_it and s_skip
		if de_bug : print (f"\nSkip={skip_it}, Sskip = {s_skip}\n" )
	else :
		ff_subtl, s_skip  = add_subtl_from_file(input_file, de_bug=True)
		ff_com.extend(ff_subtl)
# XXX: 		skip_it = skip_it and s_skip #after fixing the add_subtitle ...
		skip_it = skip_it and True

	if datax_streams:
		ff_datat, d_skip = parse_extrd(datax_streams, de_bug)
	#	ff_com.extend(ff_datat)
		d_skip = True    # XXX: Avoid reencode
		skip_it = skip_it and d_skip
		if de_bug : print (f"\nSkip={skip_it}, Dskip = {d_skip}\n" )

	if de_bug:
#	if de_bug or f_skip:
#		print(f" Skip: {skip_it}\n FFmpeg: {ff_com}\n Nothing to do")
		return [], f_skip

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
			print("    | No External subtitle file found |")
	return [], True

##>>============-------------------<  End  >------------------==============<<##


# Define a helper function to select the appropriate encoder and options
def get_encoder_options(codec_name, src_pix_fmt, use_hw_accel):
	hw_pix_fmt = "p010le" if src_pix_fmt.endswith("10le") else "nv12"
	if use_hw_accel:
		# Use hevc_qsv (HEVC) encoder with QSV options
		return ['hevc_qsv', '-load_plugin', 'hevc_hw',
				'-init_hw_device', 'qsv=qsv:MFX_IMPL_hw_any',
				'-filter_hw_device', 'qsv',
				'-pix_fmt', hw_pix_fmt,
				'-look_ahead', '1',            # Enable lookahead
				'-look_ahead_depth', '50',    # Set lookahead depth to ? 40 frames
				'-global_quality',   '22',    # Use global_quality instead of CRF for QSV
				'-preset', 'slower']        # Encoder preset
	else:
		# Use libx265 (HEVC) or libx264 (H.264) encoder with software options and 10Bit
		return ['libx265', '-pix_fmt', 'yuv420p10le', '-crf', '22', '-preset', 'slow']
#        return ['libx265', '-pix_fmt', src_pix_fmt, '-crf', '22', '-preset', 'slow']
##>>============-------------------<  End  >------------------==============<<##

@perf_monitor
def parse_video(strm_in, de_bug=False, use_hw_accel=True ):
	''' Parse and extract data from video streams '''
	msj = sys._getframe().f_code.co_name

	if de_bug:
		print(f"    +{msj} Start: {TM.datetime.now():%T}")
	#    print(f" {json.dumps(strm_in, indent=2)}")

	global glb_vidolen  # make it global so we can reuse for fmpeg check ...
	global vid_width    # NOTE used by matrix_it
	global glb_totfrms

	ff_video = []

	for indx, this_vid in enumerate(strm_in):
		ff_vid = []
		skip_it = False
		extra = ''

		if 'codec_name' not in this_vid :
			msj += f"   No codec_name |<V:{this_vid}\n"
			print ( msj )
			raise Exception(msj)

		index               = this_vid.get('index', -1)
		codec_name          = this_vid.get('codec_name', 'XXX')
		pix_fmt             = this_vid.get('pix_fmt', '')
		vid_width,vid_heigh = this_vid.get('width', 2), this_vid.get('height', 1)

		tags            = this_vid.get('tags', {})
		handler_name    = tags.get('handler_name','Unknown')
		_vi_btrt = int(       this_vid.get('bit_rate', glb_bitrate * 0.8))
		frm_rate = divd_strn( this_vid.get('r_frame_rate'  , '25'))

		if 'bit_rate' not in this_vid:
			extra = ' Bit Rate Estimate '

		# If it's mjpeg or other unsupported just and delete
		if codec_name.lower() in ['mjpeg', 'png', 'XXX']:
			ff_vid = ['-map', f'-0:v:{indx}']
			msj = f"    |<V:{indx:2}>|{codec_name:^8}| Remouve it |"
			print(msj)
			continue
		else :
			# XXX: Estimate Average bits_per_pixel
			glb_totfrms = round(frm_rate * glb_vidolen)
			avbpp = 100000 * _vi_btrt / (glb_totfrms * vid_width * vid_heigh) +1
			max_vid_btrt = 5000000

			if pix_fmt.endswith("10le") :
				msj = f"{pix_fmt} 10 Bit"
				avbpp  *= 1.25
			else:
				msj = pix_fmt

			avbpp = round ( avbpp, 2 )

			mins,  secs = divmod(glb_vidolen, 60)
			hours, mins = divmod(mins, 60)

			original_ratio = vid_width / vid_heigh
			standard_ratios = {
				"4:3":     4 / 3,
				"16:9":    16 / 9,
				"3:2":     3 / 2,
				"1:1":     1,
				}
			aspct_r = min(standard_ratios, key=lambda k: abs(standard_ratios[k] - original_ratio))

			ff_vid = ['-map', f'0:v:{indx}', f'-c:v:{indx}']
			# Determine if codec copy or conversion is needed, and update ff_vid accordingly
			if codec_name == 'hevc':
#                print ( hm_sz( max_vid_btrt ) )
				if avbpp < 35 and _vi_btrt < max_vid_btrt :
					extra += ' => Copy'
					ff_vid.extend(['copy'])
					skip_it = True
				else:
					extra += f' |Reduce Avbpp {avbpp:>3} | Btrt: {hm_sz(max_vid_btrt):>6}|'
					encoder_options = get_encoder_options(codec_name, this_vid['pix_fmt'], use_hw_accel)
					ff_vid.extend(encoder_options)
			else:
				extra += ' => Convert to Hevc'
				encoder_options = get_encoder_options(codec_name, this_vid['pix_fmt'], use_hw_accel)
				ff_vid.extend(encoder_options)

			output = "SD"
			if   vid_width >= 7600 or vid_heigh >= 4300:
				output = "8K"
			elif vid_width >= 3800 or vid_heigh >= 2100:
				output = "4K"
			elif vid_width >= 2100 or vid_heigh > 1920:
				output = "2K"
			elif vid_width >= 1280 or vid_heigh >=  720:
				output = "HD"

			# Apply scaling for anything larger than HD
			if output in ["2K", "4K", "8K"]:
				nw = 1920
				nh = round( (nw / vid_width) * vid_heigh / 2) * 2  # Ensure nh is always even
				ff_vid = [
					'-map', f'0:v:{indx}',
					f'-c:v:{indx}', 'libx265',  # Specify the H.265 codec here
					'-pix_fmt', f"{this_vid['pix_fmt']}",
					'-crf', '22','-preset', 'slow',
					'-vf', f'scale={nw}:{nh}',
				]
				extra = f' {output} Scale {vid_width}x{vid_heigh} to {nw}x{nh}'
				skip_it = False

			if handler_name == 'VideoHandler x265':
				extra += f"  {handler_name}"
			else :
				extra += f" {handler_name} => Change to: VideoHandler"
				ff_vid.extend([f"-metadata:s:v:{indx}", "handler_name=VideoHandler x265"])
				skip_it = False

			message = f"    |<V:{indx:2}>|{codec_name:^8}|{vid_width:<4}x{vid_heigh:<4}|{aspct_r}|Btrt: {hm_sz(_vi_btrt):>6}|Avbpp: {avbpp:>3}|Fps: {frm_rate:>7}|Tfm: {hm_sz(glb_totfrms,'F'):>8}| {extra}"
			print(f"\033[91m{message}\033[0m")

		ff_video += ff_vid
		skip_it  &= skip_it

	if de_bug :
		print (f"V:= {ff_video} Skip = {skip_it}")

	if skip_it :
		print ("   .Skip Video" )

	return ff_video, skip_it

##>>============-------------------<  End  >------------------==============<<##
# XXX: Audio
@perf_monitor
def parse_audio(streams, de_bug=False):
	"""Parse and extract data from audio streams."""
	msj = sys._getframe().f_code.co_name

	only_audio = len(streams) == 1
	ffmpeg_audio_options = []
	all_skippable = True
	audio_kept = False

	english_streams = []
	non_english_streams = []
	previous_default = None
	best_audio_stream = None

	for indx, audio_stream in enumerate(streams):
		stream_options = []
		extra_info = ""
		skip_current = False

		index = audio_stream.get('index', -1)
		codec_name = audio_stream.get('codec_name', None)
		sample_rate = audio_stream.get('sample_rate', None)
		bitrate = int(audio_stream.get('bit_rate', 0))
		channels = audio_stream.get('channels', -100)

		tags = audio_stream.get('tags', {})
		language = tags.get('language', 'und')
		handler_name = tags.get('handler_name', 'Unknown')

		disposition = audio_stream.get('disposition', {})
		dispo_forced = disposition.get('forced', 0)
		dispo_default = disposition.get('default', 0)

		if dispo_default:
			previous_default = audio_stream
			extra_info += f" Previous default: {language} {channels}ch "

		# Estimate bitrate if missing
		if bitrate == 0:
			extra_info = 'BitRate Estimate'
			bitrate = int(glb_bitrate * 0.2)  # estimate 20% of total

		# Get channel info
		extra_info += {
			1: " Mono",
			2: " Stereo"
		}.get(channels, f"{channels - 1}.1 Channels" if 2 < channels < 8 else f" {channels} Channels")

		keep_language = language in Keep_langua or only_audio
		copy_codec = codec_name in ('aac', 'vorbis', 'mp2', 'mp3') and channels <= 8
		reduce_bitrate = bitrate > Max_a_btr

		if language == 'eng':
			english_streams.append((index, bitrate, channels, audio_stream))
		else:
			non_english_streams.append((index, bitrate, channels, audio_stream))

		if keep_language:
			audio_kept = True
			if copy_codec:
				stream_options = ['-map', f'0:a:{indx}', '-c:a', 'copy']
				skip_current = True
			else:
				extra_info += ' Convert to vorbis'
				stream_options = ['-map', f'0:a:{indx}', '-c:a', 'libvorbis', f'-q:a', '8']
		elif not only_audio and not audio_kept:
			stream_options = ['-map', f'0:a:{indx}', '-c:a', 'copy']
			skip_current = True
			audio_kept = True
		elif not only_audio:
			stream_options = ['-map', f'-0:a:{indx}']
			extra_info = f" Del: {language} | Remove it"
			skip_current = False
		else:
			stream_options = ['-map', f'0:a:{indx}']

			if copy_codec:
				if reduce_bitrate:
					extra_info += ' Reduce BitRate'
					stream_options.extend([f'-c:a:{indx}', 'libvorbis', f'-q:a', '8'])
				else:
					extra_info += ' Copy'
					stream_options.extend([f'-c:a:{indx}', 'copy'])
					skip_current = True
			else:
				extra_info += f" Convert {codec_name} to vorbis "
				stream_options.extend([f'-c:a:{indx}', 'libvorbis', f'-q:a', '8'])

		# Metadata and disposition
		if stream_options != ['-map', f'-0:a:{indx}']:  # Stream is not marked for deletion
			if handler_name == "SoundHandler":
				extra_info += f" handler_name: {handler_name}"
			else:
				extra_info += f" handler_name: {handler_name} Change to: SoundHandler"
				stream_options.extend([f"-metadata:s:a:{indx}", "handler_name=SoundHandler"])
				skip_current = False

		message = (f"    |<A:{indx:2}>|{codec_name:^8}|Br:{hm_sz(bitrate):>9}|{language}"
				   f"|Frq: {hm_sz(sample_rate, 'Hz'):>8}|Ch: {channels}|Dis: {dispo_default} Fr:{dispo_forced}| {extra_info}")
		print(f'\033[92m{message}\033[0m')

		ffmpeg_audio_options += stream_options
		all_skippable &= skip_current

	# Select the best English stream based on highest bitrate
	if english_streams:
		best_english_stream = max(english_streams, key=lambda s: s[1])  # Prefer higher bitrate
	else:
		best_english_stream = None

	# If no English streams, select the previous default or the best non-English stream based on highest bitrate
	if best_english_stream:
		best_stream = best_english_stream
	elif previous_default:
		best_stream = (previous_default.get('index', -1),
					   int(previous_default.get('bit_rate', 0)),
					   previous_default.get('channels', -100),
					   previous_default)
	else:
		best_stream = max(non_english_streams, key=lambda s: s[1]) if non_english_streams else None

	# Set the best stream as default and ensure only one stream is default
	if best_stream:
		index = best_stream[0]
		best_audio_stream = best_stream[3]
		language = best_audio_stream.get('tags', {}).get('language', 'und')
		ffmpeg_audio_options.extend([f'-metadata:s:a:{index}', f'language={language}', f'-disposition:s:a:{index}', '+default'])
		audio_kept = True
		extra_info = f" {language} {best_stream[2]}ch {hm_sz(best_stream[1])}"
#		print(f'\033[92m    New audio default: {extra_info} \033[0m')

	# Ensure only one default stream is set
	for indx, audio_stream in enumerate(streams):
		if audio_stream != best_audio_stream:
			index = audio_stream.get('index', -1)
			ffmpeg_audio_options.extend([f'-disposition:s:a:{index}', '0'])

	# Only if no audio stream was marked to be kept and there are streams available,
	# do we default to copying the first audio stream.
	if not audio_kept and streams:
		ffmpeg_audio_options = ['-map', '0:a:0', '-c:a:0', 'copy']
		all_skippable = False

	if de_bug:
		print(f"A:= {ffmpeg_audio_options}")

	if all_skippable:
		print("   .Skip Audio")

	return ffmpeg_audio_options, all_skippable


# XXX: Subtitle
@perf_monitor
def parse_subtl(streams_in, de_bug=False):
	"""Parse and extract data from subtitle streams."""
	msj = sys._getframe().f_code.co_name

	all_skippable = True  # Initialize variable to keep track of whether all audio streams can be skipped
	skip_it = False
	ff_subttl = []
	default_eng_set = False  # Track if default English subtitle has been set

	for indx, this_sub in enumerate(streams_in):
		ff_sub = []
		extra = ''

		index        = this_sub.get('index', -1)
		codec_name   = this_sub.get('codec_name', 'unknown?')
		codec_type   = this_sub.get('codec_type', 'unknown?')
		disposition  = this_sub.get('disposition', {'forced': 0, 'default': 0})

		tags         = this_sub.get('tags', {})
		handler_name = tags.get('handler_name', 'Unknown')
		language     = tags.get('language', 'und')

		if codec_name in ('hdmv_pgs_subtitle', 'dvd_subtitle', 'ass', 'unknown?'):
			ff_sub = [f'-map', f'-0:s:{indx}']
			extra += f" Delete: {codec_name} {language} |"
		elif codec_name in ('subrip', 'mov_text'):
			ff_sub = [f'-map', f'0:s:{indx}']
			if language == 'eng':
				if not default_eng_set:
					default_eng_set = True  # Now we have a default English subtitle
					extra += f" Keep: {codec_name} {language} | Set to Default"
					ff_sub.extend([f'-c:s:{indx}', 'mov_text', f'-metadata:s:s:{indx}', f'language={language}', f'-disposition:s:s:{indx}', 'default'])
				else:
					skip_it = True
					# Another English subtitle, but we've already set the default
					extra += f" Keep: {codec_name} {language} | Not Default"
			elif language in Keep_langua:
				extra += f" Keep: {codec_name} {language}"
				skip_it = True
#                ff_sub.extend([f'-c:s:{indx}', 'mov_text', f'-metadata:s:s:{indx}', f'language={language}'])
			else:
				ff_sub = [f'-map', f'-0:s:{indx}']
				extra += f" Delete: {codec_name} {language} X"

			if ff_sub != [f'-map', f'-0:s:{indx}'] :
				if handler_name == "SubtitlHandle":
					extra += f"  {handler_name}"
					skip_it = True
				else :
					extra += f" handler_name: {handler_name} Change to: SubtitlHandle"
					ff_sub.extend([f"-metadata:s:s:{indx}", "handler_name=SubtitlHandle"])
					skip_it = False

		# Print message for this subtitle stream
		message = f"    |<S:{indx:2}>|{codec_name[:8]:^8}|{codec_type[:8]}|{language:3}|Disp: default={disposition['default']}, forced={disposition['forced']}|{extra}"
		print(f"\033[94m{message}\033[0m")

		ff_subttl += ff_sub  # Only add if the list is not empty
		all_skippable &= skip_it

	ff_subttl.extend([f'-c:s', 'mov_text'])

	if de_bug :
		print(f"S:= {ff_subttl}")

	if all_skippable:
		print ("   .Skip Subtitle" )

	return ff_subttl, all_skippable

##>>============-------------------<  End  >------------------==============<<##

# XXX: Extra Data
@perf_monitor
def parse_extrd(streams_in, de_bug=False):
	"""Parse and extract data from data streams."""
	msj = sys._getframe().f_code.co_name

	ff_data = []
	all_skippable = True

	for indx, this_dat in enumerate(streams_in):
#        if de_bug:    print(f"    +{msj} Start: {TM.datetime.now():%T}\n {json.dumps(this_dat, indent=2)}")
		ff_dd = []

		index           = this_dat.get('index', -1)
		codec_name      = this_dat.get('codec_name', '')
		codec_lng_nam   = this_dat.get('codec_long_name', '')
		codec_type      = this_dat.get('codec_type', '')

		tags            = this_dat.get('tags', {})
		handler_name = tags.get('handler_name', 'Unknown')

		msj = f"    |<D:{index:2}>|{codec_name:^8}| {codec_lng_nam:<9}| {codec_type:^11} | {handler_name}"
		if handler_name == 'SubtitleHandler':
			msj += " | Keep it Subtitle"
			print(msj)
			return [], False

		ff_dd = ['-map', f'-0:d:{indx}' ]
		skip_it = False
		msj += " | Remouve it"

		print(msj)

		ff_data += ff_dd
		all_skippable &= skip_it

	if de_bug:
		print(f"D:= {ff_data}")

	if all_skippable :
		print ("   .Skip Data" )

	return ff_data, skip_it

##>>============-------------------<  End  >------------------==============<<##

@perf_monitor
def zabrain_run(input_file: str, mta_dta: Dict[str, any], de_bug: bool= False ) -> Tuple[bool, bool]:
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
#    print(f'  -End: {TM.datetime.now():%T}\tTotal: {hm_time(end_t - str_t)}')

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

	elif all(substr in line_to for substr in ["speed=", "fps=", "time="]):
		try:
			# Use compiled regular expressions
			regx_val = {}
			for key, regex in regex_dict.items():
				regx_val[key] = regex.search(line_to).group(1)

			fp, sp = regx_val["fps"], regx_val["speed"]
			fp_int = int(fp)

			if fp_int >= 1:
				sp_float = float(sp)
				time_parts = regx_val["time"].split(":")
				a_sec = int(time_parts[0]) * 3600 + int(time_parts[1]) * 60 + int(time_parts[2])
				dif = abs(glb_vidolen - a_sec)
				eta = round(dif / sp_float)
				mints, secs = divmod(int(eta), 60)
				hours, mints = divmod(mints, 60)
				_eta = f"{hours:02d}:{mints:02d}:{secs:02d}"
				_P = f"\r    | {sy} |Size: {hm_sz(regx_val['size']):>7}|Frames: {int(regx_val['frame']):>6}|Fps: {fp_int:>3}|BitRate: {hm_sz(regx_val['bitrate']):>6}|Speed: {sp_float:>5}|ETA: {_eta:>8}|  "
				if de_bug:
					print(f"\n {line_to}\n | {sy} |Size: {hm_sz(regx_val['size']):>7}|Frames: {int(regx_val['frame']):>6}|Fps: {fp_int:>3}|BitRate: {hm_sz(regx_val['bitrate']):>6}|Speed: {sp_float:>5}|ETA: {_eta:>8}|")
		except Exception as e:
			msj = f"Exception in {msj}: {e} {line_to}"
			print(msj)

	elif any(substr in line_to for substr in ["muxing overhead:", "global headers:", "encoded"]):
		_P = f"\r    |<+>| Done: {line_to}"

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
		return out_file
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
		time.sleep(3)
		return out_file
	else :
		print (" Failed")

	end_t = time.perf_counter()
	print(f'  -End: {TM.datetime.now():%T}\tTotal: {hm_time(end_t - str_t)}')
	return out_file
##>>============-------------------<  End  >------------------==============<<##
