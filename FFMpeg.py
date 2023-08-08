# -*- coding: utf-8 -*-

import os
import re
import sys
import json

import datetime	as TM
import subprocess as SP

from math	import gcd
from collections import defaultdict
from typing import Dict, List, Tuple, Optional

from My_Utils import *
from Yaml import *

ffmpg_bin = 'C:\\Program Files\\ffmpeg\\bin'

ffmpeg = os.path.join(ffmpg_bin, "ffmpeg.exe" )
ffprob = os.path.join(ffmpg_bin, "ffprobe.exe")
# ffplay = os.path.join(ffmpg_bin, "ffplay.exe")

if not os.path.exists(ffmpeg) or not os.path.exists(ffprob) :
	input(f"{ffmpeg}\nPath Does not Exist:")
	raise OSError

#SP.run( [ffmpeg, '-version'] )
version = f"ffmpeg version: {SP.run(['ffmpeg', '-version'], stdout=SP.PIPE).stdout.decode('utf-8')[14:20]} (:)"
print( version )
logging.info( version )

##==============-------------------   End   -------------------==============##

def ffprobe_create_index(input_file):
	"""
	Creates an index for each sub stream of an MPEG file using ffprobe.

	Args:
		input_file (str): The path to the MPEG file.

	Returns:
		dict: The index of the MPEG file.
	"""

	cmd = ['ffprobe',
			'-hide_banner',
			'-analyzeduration', '100000000',
			'-probesize', '50000000',
			'-v', 'warning',
			'-of', 'json',
			'-show_programs',
			'-show_format',
			'-show_streams',
			'-show_error',
			'-show_data',
			'-show_private_data',
			'-i', input_file]

	try:
		result = SP.run(cmd, capture_output=True, text=True, errors='ignore', encoding='utf-8')
		output = json.loads(result.stdout)

		# Get the format and streams metadata
		_format = output.get('format', {})
		streams = output.get('streams', [])

		# Initialize the indexes dictionary
		indexes = {'video': [], 'audio': [], 'subtitle': []}
		# Add the format variable to the indexes dictionary
		indexes['format'] = _format

		for stream in streams:
			codec_type = stream.get('codec_type', '')
			index = {
				'codec_type': codec_type,
				'codec_name': stream.get('codec_name', ''),
			}

			# Only retain the codec type and properties of the stream
			if codec_type == 'video':
				index['width'] = stream.get('width', 0)
				index['height'] = stream.get('height', 0)
				index['fps'] = stream.get('fps', 0)
				index['pixel_format'] = stream.get('pixel_format', '')
				index['display_aspect_ratio'] = stream.get('display_aspect_ratio', '')
				index['chroma_subsampling'] = stream.get('chroma_subsampling', '')
				indexes['video'].append(index)
			elif codec_type == 'audio':
				index['bit_rate'] = stream.get('bit_rate', 0)
				index['channels'] = stream.get('channels', 0)
				index['samplerate'] = stream.get('sample_rate', 0)
				indexes['audio'].append(index)
			elif codec_type == 'subtitle':
				index['language'] = stream.get('tags', {}).get('language', '')
				index['forced'] = stream.get('disposition', {}).get('forced', 0)
				indexes['subtitle'].append(index)

		return indexes
	except subprocess.CalledProcessError as e:
		print(f"Error running ffprobe: {e}")
		return None

@perf_monitor
def extract_file_info(input_file):
	cmd = ['ffprobe',
				'-hide_banner',
				'-analyzeduration', '100000000',
				'-probesize',		 '50000000',
				'-v', 'error',		# XXX quiet, panic, fatal, error, warning, info, verbose, de_bug, trace
				'-of','json',		# XXX default, csv, xml, flat, ini
					'-show_programs',
					'-show_format',
					'-show_streams',
					'-show_error',
					'-show_data',
					'-show_private_data',
				'-i', input_file]
	try:
		result = SP.run(cmd, capture_output=True, text=True, errors='ignore', encoding='utf-8')
		output = json.loads(result.stdout)
		# Use .get() method with default values
		# XXX: We could extract more info if needed
		'''
		_format = output.get('format', {})
		glb_vidolen = int(float(_format.get('duration', 0.0)) )
		glb_bitrate = int(      _format.get('bit_rate', 0) )
		nb_programs	= int(      _format.get('nb_programs', 0) )
		filename	=           _format.get('filename', 'No File Name')
		size		=           _format.get('size', 0)
		'''

		duration	= float(output.get('format', {}).get('duration', 0.0))
		if de_bug : print ( duration )
		return duration
	except (SP.SubprocessError, json.JSONDecodeError) as e:
		msj += f"Error {e} getting metadata from file\n{input_file}\n{cmd}"
		print( msj )
		return 0.0

##>>============-------------------<  End  >------------------==============<<##

@perf_monitor
def ffprobe_run(input_file, execu=ffprob, de_bug=False ) -> str :
	''' Run ffprobe returns a Json file with the info '''

	msj = sys._getframe().f_code.co_name
	print(f"  +{msj} Start: {TM.datetime.now():%T}")
	logging.info(f"{msj}")
	str_t = time.perf_counter()

	if not input_file :
		msj += "  no input_file"
		raise Exception(msj)

	cmd = [execu,
				'-hide_banner',
				'-analyzeduration', '100000000',
				'-probesize',		 '50000000',
				'-v', 'warning',	# XXX quiet, panic, fatal, error, warning, info, verbose, de_bug, trace
				'-of','json',		# XXX default, csv, xml, flat, ini
					'-show_programs',
					'-show_format',
					'-show_streams',
					'-show_error',
					'-show_data',
					'-show_private_data',
				'-i', input_file]

# XXX: TBD good stuff
	try:
		out    = SP.run(cmd, stdout=SP.PIPE)
		jsn_ou = json.loads(out.stdout.decode('utf-8'))
	except (SP.SubprocessError, json.JSONDecodeError) as e:
		msj += f"Error {e} getting metadata from file\n{input_file}\n{cmd}"
		print( msj )
		logging.error( msj )
		raise Exception(msj)

	end_t = time.perf_counter()
	print(f'  -End: {TM.datetime.now():%T}\tTotal: {hm_time(end_t - str_t)}')
	return jsn_ou
##>>============-------------------<  End  >------------------==============<<##

# XXX:  Returns encoded filename file_name
@perf_monitor
def ffmpeg_run(input_file: str, ff_com: str, skip_it: bool, execu: str = ffmpeg, de_bug: bool = False, max_retries: int = 2, retry_delay: int = 3 ) -> str:
	"""Create command line, run ffmpeg then retries and runs again if fails"""

	msj = sys._getframe().f_code.co_name
	logging.info(f"{msj}")
	str_t = time.perf_counter()

	if not input_file or skip_it:
		msj = f"{msj} Skip={skip_it} = {input_file}"
	#	print(msj)
		logging.info(msj)
		return ''

	print(f"  +{msj} Start: {TM.datetime.now():%T}")

	file_name, _ = os.path.splitext(os.path.basename(input_file))
	out_file = os.path.normpath('_' + stmpd_rad_str(7, file_name[0:20]))
	out_file = re.sub(r'[^\w\s_-]+', '', out_file).strip().replace(' ', '_') + TmpF_Ex

	ff_head = [execu, "-thread_queue_size", "24", "-i", input_file, "-hide_banner"]
# XXX: Disable Hardware acceleation for short vid
#	ffmpeg -ss 00:01:00 -i input.mp4 -t 00:00:30  # XXX: start at 1 minute and encode only 30 seconds
#	ff_head = [execu, '-ss', '00:06:00', "-thread_queue_size", "24", "-i", input_file, "-t", "00:0:45" ]

	ff_tail = [
		"-metadata", f"title={file_name} x256",
		"-metadata", f"comment={Skip_key}",
		"-metadata", "copyright= 2023 Me",
		"-metadata", "author= Encoded by the one and only GeoHab",
		"-metadata", "encoder= ffmpeg 6.00",
#		"-movflags", "+faststart",
#		"-fflags"  , "+fastseek",
		"-y", out_file,
			   #"-fflags", "+genpts,+igndts",
#		"-f", "matroska"
	]

#	p = psutil.Process()
#	p.nice(psutil.HIGH_PRIORITY_CLASS)
#	cpu_count = psutil.cpu_count(logical=False)
#	p.cpu_affinity(list(range(cpu_count)))

	for attempt in range(max_retries + 1):
#		breakpoint()  # Set a breakpoint here
		# XXX: not the first time :(
		if attempt > 0 :
			print (f" Attempt: = {attempt} ")
			ff_head = [execu, "-report", "-loglevel", "verbose", "-i", input_file, "-hide_banner"]
			todo = ff_head + ff_com + ff_tail
			msj  = f"   {msj} Failed\n   Retry: {attempt} of {max_retries} in {retry_delay} seconds..."
			msj += f"\n\nH:{ff_head}\nC:{ff_com}\nT:{ff_tail}\n\n"
			print (msj)
			time.sleep(retry_delay)
		todo = ff_head + ff_com + ff_tail
		if run_ffm(todo, de_bug=attempt):
			end_t = time.perf_counter()
			print(f'  -End: {TM.datetime.now():%T}\tTotal: {hm_time(end_t - str_t)}')
#			p.nice(psutil.NORMAL_PRIORITY_CLASS)
			return out_file
		elif attempt == max_retries:
			msj += f"Error failed attempt {attempt}"
			print (msj)
			logging.error( msj, exc_info=True)
#	p.nice(psutil.NORMAL_PRIORITY_CLASS)
			raise Exception( msj)

	return None  # Return None explicitly if the loop completes without returning a value

##>>============-------------------<  End  >------------------==============<<##
@perf_monitor
def run_ffm(args, de_bug=False):
	""" Run ffmpeg command """

	msj = sys._getframe().f_code.co_name
	logging.info(f"{msj}")

	if not args:
		print(f"{msj} Exit no args = {args}")
		return False

	try :
		if de_bug:
			print(f"\n{msj} Debug mode\n{' '.join(args)}\n")
			logging.info(f"Exec in DEBUG mode ")

			runit = SP.run( args,
				universal_newlines=True,
				encoding=console_encoding,
	#			stderr=SP.STDOUT,
	#			stdout=SP.PIPE,
				 )
			print("\nStd: ", runit.stdout)
			print("\nErr: ", runit.stdout)
			msj += f" Done\n"
			logging.info(msj)
			print(msj)

			time.sleep(3)
		else:
			logging.info(f"Exec ")
			with SP.Popen( args,
				universal_newlines=True,
				encoding=console_encoding,
				stderr=SP.STDOUT,
				stdout=SP.PIPE,
				bufsize=1 ) as process:

				loc = 0
				symbls = "|/-o+\\"
				sy_len = len(symbls)
				for line in iter(process.stdout.readline, ""):
#					print( line )
					zz = show_progrs( line, symbls[loc], de_bug=False)
					loc += 1
					if loc == sy_len:
						loc = 0
				out, err = process.communicate()
				if err or out:
					msj = f"\nError:\nL:{line}\nE:{err}\nO:{out}"
					print(msj)
					logging.error(msj, exc_info=True)
					time.sleep(3)

				process.stdout.close()

			if process.returncode != 0:
				zz = ( process.returncode, args, out, err )
				print ( f" Procces returncode {zz}")
				logging.error(f" {zz}", exc_info=True)
				raise SP.CalledProcessError(
					process.returncode, args, output=out, stderr=err )

	except Exception as e:
		msj += f"Exception:{e}"
		print (msj)
		logging.exception( msj ,exc_info=True, stack_info=True)
		return False

	return True
##==============-------------------   End   -------------------==============##

def pars_frmat(input_file: str, mta_dta: Dict[str, any], de_bug: bool) -> Tuple[List[str], List[str], List[str], List[str], bool]:
	''' Parse and extract data from file format '''
	msj = sys._getframe().f_code.co_name
	logging.info(msj)

	global glb_vidolen  # make it global so we can reuse for fmpeg check ...
	global glb_bitrate

	# XXX: Meta Data
	_format_ = {
		'bit_rate' 	 		: int,
		'nb_streams' 		: int,
		"nb_programs"		: int,
		'filename'			: str,
		"format_name"		: str,
		"format_long_name"	: str,
		"start_time"		: float,
		'duration' 	 		: float,
		'size'     	 		: float,
		'tags'		 		: {}  }

	_format = mta_dta.get('format', {})

	if not _format :
		msj += f" 'format' keyword not in\n{ json.dumps(mta_dta, indent=2) }\n"
		print ( msj )
		logging.error(msj)
		return [], True

	glb_vidolen = int(float(_format.get('duration', 0.0)) )
	glb_bitrate = int(      _format.get('bit_rate', 0) )
	nb_programs	= int(      _format.get('nb_programs', 0) )
	filename	=           _format.get('filename', 'No File Name')
	size		=           _format.get('size', 0)

	_strms = mta_dta.get('streams', [])

	# Initialize a dictionary to group streams by codec_type
	streams_by_type = defaultdict(list)

	for i, stream in enumerate(_strms):
		codec_type = stream.get('codec_type', None)
		streams_by_type[codec_type].append(stream)

	# Extract the lists of streams for each codec_type
	video_streams = streams_by_type['video']
	audio_streams = streams_by_type['audio']
	subtl_streams = streams_by_type['subtitle']
	datax_streams = streams_by_type['data']
	wtfis_streams = streams_by_type[None]  # Streams with no codec_type or unrecognized codec_type

	if wtfis_streams :
		print (f" WTF\n{wtfis_streams}\n")
		input ("WTF")

	if not video_streams:
		msj = f"\nNo Video in File: {input_file}\n"
		print(msj)
		logging.error(msj)
		raise ValueError(msj)

	if not audio_streams:
		msj = f"\nNo Audio in File: {input_file}\n"
		print(msj)
		logging.error(msj)
		raise ValueError(msj)

	stream_counts = {stream_type: len(streams) for stream_type, streams in
				 [('Prog', nb_programs), ('V', video_streams), ('A', audio_streams), ('S', subtl_streams), ('D', datax_streams)] if
				 streams}

	message = f"    |< FR >|Sz: {hm_sz(size)}|Br: {hm_sz(glb_bitrate)} |L: {hm_time(glb_vidolen)} "
	message += ''.join([f'|{stream_type} = {count}|' for stream_type, count in stream_counts.items() ])
	print(f"{message}")

	f_skip = False
	# Check if the right signature is found
	_, ext = os.path.splitext(input_file)
	if (_format.get('tags', {}).get('comment') == Skip_key) and (ext == '.mp4') :
		print(f"  Skip Format")
		# Check if filenames are the same and update f_skip accordingly
		f_skip = True if (input_file == filename) else False
		if f_skip:
#			print("  Same Filename")
			logging.info(f"Was processed: {os.path.basename(input_file)}")
		else:
			# Visual comparison of different filenames
			vis_compr(input_file, filename)
			print(f"InFile: {input_file} != Fnam: {filename}")
			input ("Press CR")
			raise Exception("FileName does not match")
	else:
		print("  Key Miss")
	#	print(f"\nInFile: {input_file}\nFname:  {filename}\n")
		if ( input_file != filename ) :
			print(f"InFile: {input_file} != Fnam: {filename}")
			vis_compr(input_file, filename)
			input ("Press CR")
			raise Exception("FileName does not match")

	ff_video, v_skip = (pars_video(video_streams, de_bug) if video_streams else (['-vn'], False) )
	ff_audio, a_skip = (pars_audio(audio_streams, de_bug) if audio_streams else (['-an'], False) )
	ff_subtl, s_skip = (pars_subtl(subtl_streams, de_bug) if subtl_streams else (['-sn'], True ) )
	ff_datat, d_skip = (pars_extrd(datax_streams, de_bug) if datax_streams else (['-dn'], True ) )

	skip_it = True if ( f_skip and v_skip and a_skip and s_skip and d_skip ) else False

# XXX:
#	ff_audio = ["-c:a", "copy"]
#	ff_subtl, s_skip = (['-sn'], False) # Remouve all Subtitle
#	ff_subtl = ['-c:s', 'mov_text']

	ff_com = ff_video + ff_audio + ff_subtl + ff_datat

#	de_bug = True
	if de_bug :print (f"L:{msj}\nFFcom: {ff_com} Skip:{skip_it}")

	if skip_it :
		print ( "  ! Skip All :)" )
	else :
		pass
#		print (f"com: {ff_com}\n")

	return ff_com, skip_it
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
				'-look_ahead', '1',			# Enable lookahead
				'-look_ahead_depth', '50',	# Set lookahead depth to ? 40 frames
				'-global_quality',   '23',	# Use global_quality instead of CRF for QSV
				'-preset', 'slower']		# Encoder preset
	else:
		# Use libx265 (HEVC) or libx264 (H.264) encoder with software options
		return ['libx265', '-pix_fmt', src_pix_fmt, '-crf', '23', '-preset', 'slow']

@perf_monitor
def pars_video(strm_in, de_bug, use_hw_accel=True ):
	''' Parse and extract data from video streams '''
	msj = sys._getframe().f_code.co_name
	if de_bug:
		print(f"    +{msj} Start: {TM.datetime.now():%T}")
		print(f" {json.dumps(strm_in, indent=2)}")

	global glb_vidolen  # make it global so we can reuse for fmpeg check ...
	global vid_width    # NOTE used by matrix_it
	global glb_totfrms

	ff_video = []
	skip_it = False
	for count, this_vid in enumerate(strm_in):
		ff_vid = []
		extra = ''

		if 'codec_name' not in this_vid or 'index' not in this_vid:
			msj = f"   No codec_name or index |<V:{this_vid}\n"
			print ( msj )
			raise Exception(msj)
		# If it's mjpeg or other unsupported just ignore and delete
		codec_name	= this_vid.get('codec_name', 'XXX')
		index		= this_vid.get('index', -1)
		if codec_name.lower() in ['mjpeg', 'png']:
			ff_video += ['-map', f'-0:v:{count}']
			msj = f"    |<V:{index:2}>|{codec_name:<8} |Del: {codec_name}:<7|"
			print(msj)
			continue

		pix_fmt		= this_vid.get('pix_fmt', '')
		tags		= this_vid.get('tags', {})
		_vi_btrt = int(this_vid.get('bit_rate', glb_bitrate * 0.8))
		frm_rate = divd_strn(this_vid.get('r_frame_rate'  , '25'))
		vid_width, vid_heigh = this_vid.get('width', 2), this_vid.get('height', 1)
		glb_totfrms = round(frm_rate * glb_vidolen)

		totl_pixl = vid_width * vid_heigh * glb_totfrms

		# XXX: Estimate bits_per_pixel
		bpp = 3000000

		if pix_fmt.endswith("10le") :
			msj = f"{pix_fmt} 10 Bit"
			totl_pixl	*= 1.25  # 15/12
			bpp 		*= 1.25
		else:
			msj = pix_fmt
		print (f"\nTotal Pixels= {totl_pixl}\t Bpp= {bpp}\t div= { round(totl_pixl / bpp) }")

		if 'bit_rate' not in this_vid:
			extra = ' BitRate Estimate '
		else:
			extra = f' {hm_sz(_vi_btrt)}'

		mins,  secs = divmod(glb_vidolen, 60)
		hours, mins = divmod(mins, 60)

		# Get aspect ratio using the greatest common divisor
		comm_div = gcd(vid_width, vid_heigh)
		smpl_width  = vid_width // comm_div
		smpl_height = vid_heigh // comm_div
		aspct_r = f"{smpl_width}:{smpl_height}"

		# XXX: Print Banner
		msj = f"    |< CT >|{vid_width:>4}x{vid_heigh:<4}| {aspct_r} |Tfm: {hm_sz(glb_totfrms,'F'):>8}|Tpx: {hm_sz(totl_pixl, 'P'):>8}|XBr: {hm_sz(bpp):>7}| {msj}"
		print(msj)
		if (de_bug): print(f"S:{index} C:{index}\n {ff_vid}")

		ff_vid = ['-map', f'0:v:{count}']
		output = "SD"

		# Determine if codec copy or conversion is needed, and update ff_vid accordingly
		if _vi_btrt < bpp :
			if codec_name == 'hevc':
				extra += ' => Copy'
				skip_it = True
				ff_vid.extend([f'-c:v:{count}', 'copy'])
			else:
				extra += ' => Convert to Hevc'
				encoder_options = get_encoder_options(codec_name, this_vid['pix_fmt'], use_hw_accel)
				ff_vid.extend([f'-c:v:{count}'] + encoder_options)
		else:
			if codec_name == 'hevc':
				extra += f'\tReduce < {hm_sz(bpp)}'
			else:
				extra += ' => Convert to Hevc'
			encoder_options = get_encoder_options(codec_name, this_vid['pix_fmt'], use_hw_accel)
			ff_vid.extend([f'-c:v:{count}'] + encoder_options)

		#Check it needs to be scaled down
		if vid_width >= 7600 or vid_heigh >= 4300:
			output = "8K"
		if vid_width >= 3800 or vid_heigh >= 2100:
			output = "4K"
		if vid_width >  2100 or vid_heigh >  1920:
			output = "2K"
			# XXX: Compute Scale
			nw = 1920
			nh = round((nw / vid_width) * vid_heigh)
			ff_vid = ['-map', f'0:v:{count}', f'-c:v:{count}', 'libx265',
								'-pix_fmt', f"{this_vid['pix_fmt']}",
								'-crf', '23', '-preset', 'slow',
								'-vf', f'scale={nw}:{nh}'
								]
			extra += f' {output} Scale {vid_width}x{vid_heigh} to {nw}x{nh}'
		elif vid_width >= 1280 or vid_heigh >= 720:
			output = "HD"

		handler_name = tags.get('handler_name')
#		if handler_name:
#			extra += f"  {handler_name}"

		msj = f"    |<V:{index:2}>|{codec_name:<8} |Br: {hm_sz(_vi_btrt):>9}|XBr: {hm_sz(bpp):>8}|Fps: {frm_rate:>7}| {extra}"
		print(f"\033[91m{msj}\033[0m")

		ff_video += ff_vid
		skip_it  &= skip_it

	if de_bug :	 print (f"V:= {ff_video}")
	if skip_it : print (  "  Skip Video" )

	return ff_video, skip_it

##>>============-------------------<  End  >------------------==============<<##
# XXX: Audio
@perf_monitor
def pars_audio(streams_in, de_bug=False):
	only_audio = len(streams_in) == 1
	ff_audio = []
	all_skippable = True  # Initialize variable to keep track of whether all audio streams can be skipped
	kept_audio = False  # Initialize variable to keep track of whether we have kept an audio stream

	for count, this_aud in enumerate(streams_in):
		ff_aud = []
		extra = ''
		skip_it = False  # Initialize variable to keep track of whether this audio stream can be skipped
		aud_sample_rate = this_aud.get('sample_rate', None)

		# Disposition and language
		language =		this_aud.get('tags', {}).get('language', 'und')
		disposition =	this_aud.get('disposition', {})
		dsp_forced =	disposition.get('forced', 0)
		dsp_default =	disposition.get('default', 0)

		# Bitrate
		codec_name =	this_aud.get('codec_name', None)
		au_bitrate =	int(this_aud.get('bit_rate', 0))
		if au_bitrate == 0:
			extra = 'BitRate Estimate '
			au_bitrate = int(glb_bitrate * 0.2)  # estimate 20% of total

		# Channels
		channels =		this_aud.get('channels', -100)
		extra += {
			1: "Mono",
			2: "Stereo"
		}.get(channels, f"{channels - 1}.1 Channels" if 2 < channels < 8 else f" {channels} Channels ")

		# Check if the language is in the list of languages to keep
		keep_language	= language in Keep_langua or only_audio
		copy_codec		= codec_name in ('aac', 'vorbis', 'mp2', 'mp3') and channels < 9
		reduce_bitrate	= au_bitrate > Max_a_btr

		if keep_language:
			if copy_codec:
				ff_aud = ['-map', f'0:a:{count}', f'-c:a:{count}', 'copy']
				skip_it = True
			else:
				extra += ' Convert to vorbis'
				ff_aud = ['-map', f'0:a:{count}', f'-c:a:{count}', 'libvorbis', f'-q:a:{count}', '8']
				if codec_name == 'vorbis':
					skip_it = True
			kept_audio = True
		elif not only_audio and not kept_audio:
			ff_aud = ['-map', f'0:a:{count}', f'-c:a:{count}', 'copy']
			skip_it = True
			kept_audio = True
		elif not only_audio:
			ff_aud = ['-map', f'-0:a:{count}']
			extra = f"Del: Delete language"
		else:
			ff_aud = ['-map', f'0:a:{count}']
			if copy_codec:
				if reduce_bitrate:
					extra += ' Reduce BitRate'
					ff_aud.extend([f'-c:a:{count}', 'libvorbis', f'-q:a:{count}', '8'])
				else:
					extra += ' Copy'
					ff_aud.extend([f'-c:a:{count}', 'copy'])
					skip_it = True
			else:
				extra += f" Convert {codec_name} to vorbis "
				ff_aud.extend([f'-c:a:{count}', 'libvorbis', f'-q:a:{count}', '8'])

		# Setting metadata and disposition
		ff_aud.extend([f'-metadata:s:a:{count}', f'language={language}'])
		if language == Default_lng:
			extra += " Set to Default"
			ff_aud.extend([f'-disposition:s:a:{count}', '+default+forced'])
		else:
			ff_aud.extend([f'-disposition:s:a:{count}', 'none'])

		# Optional handler_name
		handler_name = this_aud.get('tags', {}).get('handler_name')
#		if handler_name:
#			extra += f"  {handler_name}"


		# Print information
		msj = (f"    |<A:{this_aud['index']:2}>|{codec_name:<8} |Br: {hm_sz(au_bitrate):>9}|{language}"
				f"|Frq: {hm_sz(aud_sample_rate, 'Hz'):>8}|Ch: {channels}|Dis: {dsp_default} Fr:{dsp_forced}| {extra}")
		print(f'\033[92m{msj}\033[0m')

		# Append options for this audio stream to the final list
		ff_audio += ff_aud
		all_skippable &= skip_it
		if de_bug  or  language == 'und':
#			if count == 1 :
#				language ='eng'
#				ff_aud = [ f"-map, 0:a:{count}, -c:a:{count}, libvorbis, -q:a:{count}, 8, -metadata:s:a:{count}, language={language}" ]
#			else :
#				ff_aud = []
			print ( f"A: {ff_aud} L={language}")
#			input ('Next')

	# If no audio streams were kept, keep the first one (if available)
	if not kept_audio and streams_in:
		ff_audio = ['-map', f'0:a:0', f'-c:a:0', 'copy']
		all_skippable = False

	if de_bug:
		print(f"A:= {ff_audio}")
	if all_skippable:
		print("  Skip Audio")

	return ff_audio, all_skippable

# XXX: Subtitle
@perf_monitor
def pars_subtl(streams_in, de_bug=False):
	"""Parse and extract data from subtitle streams."""
	msj = sys._getframe().f_code.co_name

	extracted_subs = []  # List to store information about extracted subtitles
	ff_subttl = []
	skip_it = False
	all_skippable = True  # Initialize variable to keep track of whether all audio streams can be skipped
	default_scores = {}  # Dictionary to store scores for default language subtitles
	language_counts = {}  # Dictionary to store the counts of each language subtitle

	for count, this_sub in enumerate(streams_in):
		codec_name = this_sub.get('codec_name', 'unknown?')
		codec_type = this_sub.get('codec_type', 'unknown?')
		disposition = this_sub.get('disposition', {'forced': 0, 'default': 0})
		index = this_sub.get('index')
		tags = this_sub.get('tags', {})
		language = tags.get('language', 'und')

		extra = ''
		# Condition 1: Convert graphical or ASS subtitles to mov_text format
		ff_sub = ['-map', f'-0:s:{count}']
		if codec_name in ('hdmv_pgs_subtitle', 'dvd_subtitle', 'ass', 'unknown?'):
			extra += f" Del: {this_sub['codec_name']} {language} < |"
			# TBD Extract bitmap subtitles to a separate file
			output_filename = f"subtitle_stream{index}_{language}.sup"
			#ff_sub.extend([f'-c:s:{count}', 'copy', '-y', output_filename ])
			extracted_subs.append({
				'index': index,
				'language': language,
				'filename': output_filename
			})
		# Condition 2: Subtitle language is OK
		elif codec_name in ('subrip', 'mov_text'):
			skip_it = True
			if language == Default_lng:
				if disposition['default'] != 1:
					skip_it &= False
					extra += f" Move Text {codec_name} {language} Set to Default"
					ff_sub.extend([f'-c:s:{count}', 'mov_text', f'-metadata:s:s:{count}', f'language={language}', f'-disposition:s:s:{count}', 'default'])
				else:
					extra += f" Move Text {codec_name} {language} Already Default"
					ff_sub.extend([f'-c:s:{count}', 'mov_text'])

				# Calculate a score for this subtitle based on certain criteria
				score = 0
				if codec_name == 'mov_text':
					score += 1
				# Add more scoring criteria here if needed

				# Store the score for this subtitle
				default_scores[count] = score
			elif language in Keep_langua:
				extra += f" Move Text {codec_name} {language}"
				ff_sub.extend([f'-c:s:{count}', 'mov_text', f'-metadata:s:s:{count}', f'language={language}', f'-disposition:s:s:{count}', 'none'])
			else:
				skip_it &= False
				extra += f" Del: {codec_name} {language} < |"
				ff_sub = ['-map', f'-0:s:{count}']

		# Include handler name in the message
		handler_name = tags.get('handler_name', '')
#		if handler_name:
#			extra += f"  {handler_name}"

		# Print message for this subtitle stream
		message = f"    |<S:{index:2}>|{codec_name:<9}|{codec_type:<13}|{language:3}|Disp: default={disposition['default']}, forced={disposition['forced']}|{extra}"
		print(f"\033[94m{message}\033[0m")

		# Append options for this subtitle stream to the final list
		ff_subttl += ff_sub
		all_skippable &= skip_it
		if de_bug  and  language == 'und':
			print ( f"S: {ff_sub} L={language}")

		# Count the occurrence of each language subtitle
		language_counts[language] = language_counts.get(language, 0) + 1

	# Filter out duplicate language subtitles disabled
	for count, this_sub in enumerate(streams_in):
		tags = this_sub.get('tags', {})
		language = tags.get('language', 'und')
#        if language_counts.get(language, 0) > 1:
#            ff_subttl.extend(['-map', f'-0:s:{count}'])

	if de_bug:
		print(f"S:= {ff_subttl}")
	if all_skippable:
		print("  Skip Subtitle")

	return ff_subttl, all_skippable

##>>============-------------------<  End  >------------------==============<<##

# XXX: Extra Data
@perf_monitor
def pars_extrd(streams_in, de_bug=False):
	"""Parse and extract data from data streams."""
	msj = sys._getframe().f_code.co_name
	if de_bug:
		print(f"    +{msj} Start: {TM.datetime.now():%T}\n {json.dumps(streams_in, indent=2)}")

	ff_data = []
	all_skippable = True
	for count, this_dat in enumerate(streams_in):
		if de_bug:	print(f"    +{msj} Start: {TM.datetime.now():%T}\n {json.dumps(this_dat, indent=2)}")
		ff_dd = []
		extra = 		this_dat.get('tags', {}).get('handler_name', '')
		index = 		this_dat.get('index', -1)
		codec_name	= 	this_dat.get('codec_name', '')
		codec_lng_nam = this_dat.get('codec_long_name', '')
		codec_type = 	this_dat.get('codec_type', '')

		skip_it = False
		if extra == 'SubtitleHandler' :
			ff_dd = ['-map', f'0:d:{count}', f'-c:d:{count}', 'copy']
			msj = f"    |<D:{index:2}>| {codec_name:<8}| {codec_lng_nam:<9}| {codec_type:^11} |  {extra}"
			skip_it = True
		else:
			ff_dd = ['-map', f'-0:d:{count}' ]
			msj = f"    |<D:{index:2}>| {codec_name:<8}| {codec_lng_nam:<9}| {codec_type:^11} |  {extra}"

		print(msj)

		ff_data += ff_dd
		all_skippable &= skip_it

	if all_skippable :	print (  "  Skip Data" )

	return ff_data, skip_it

##>>============-------------------<  End  >------------------==============<<##
@perf_monitor
def	find_subtl ( input_file, de_bug ) :
	''' find subtitle files and select the best '''
	str_t = time.perf_counter()
	msj = sys._getframe().f_code.co_name
	if de_bug : print(f"    +{msj} Start: {TM.datetime.now():%T}\nFiles:{input_file}" )
	logging.info(f"{msj}")

	'''
	Let's setup the local directory to see what's in it
	os.path.sep (input_file)
	files = [file for file in files if '.png' in file or '.jpg' in file]
	'''
	top_dir, Filo = input_file.rsplit(os.path.sep, 1)
	Finam, ext = os.path.splitext(Filo)
	file_lst = os.listdir(top_dir)

# XXX: TO do it properly
#	srt_files = [file for file in file_lst if '.srt' in file ]
#	if srt_files :
#		print(f"    |<S: Extternal file|:{srt_files}")
#	else :
#		print(f"    |<S: NO Extternal file|:{srt_files}")

	file_lst.sort(key=lambda f: os.path.isfile(os.path.join(top_dir, f)))
#	if de_bug :
#		print(f"Dir:{top_dir}\nFile:{Filo}\nFn:{Finam}\t E:{ext}\n")
#		print("\nFile:".join(file_lst))
#		input ("WTF")
	ff_subtl = []
	for count, fname in enumerate(file_lst) :
		fnm, fex =  os.path.splitext(fname)
		if  fex == '.srt' :
			lng = 'und'
			symb, trnsf = Trnsformr_ ( Finam, fnm, de_bug )
			# XXX: simple way to see similarity 4 chars = .srt
			if len(trnsf)/3 <= 4 : # dived by 3 cause it sas 3 components location cr1 cr2 to transform from one to the other
				#print( len( trnsf), symb )
				print(f"    |<S: Ext file|:{fname}")
				if de_bug :
					print(f"1: {Finam}\nD: {symb}\n2: {fnm}\nLng={lng}\nLen:{len(trnsf)} ")
	#		ff_sub = (['-i', fname, '-c:s', 'copy', '-metadata:s', 'language=' + 'eng'])
	#		ff_sub = (['-i', fname, '-c:s', 'copy', '-metadata:s:s' + count, 'language=' + 'eng'])
			ff_sub = (['-i', fname, '-c:s', 'mov_text'])
		else :
			ff_sub = []
#			if de_bug : print (f'{count} {fnm}{fex} Not a Sub File')

		ff_subtl += ff_sub

	if '-sn' not in ff_subtl and not len(ff_subtl) :
		ff_subtl = ['-sn']

	if de_bug : print ("  ", ff_subtl ) , input("Next")

	# XXX:
	#ff_subtl = ['-sn']
	return( ff_subtl )

def Trnsformr_(str1, str2, de_bug, no_match_c='|', match_c='='):
	str_t = time.perf_counter()
	message = sys._getframe().f_code.co_name
	if de_bug : print(f"     +{message} Start: {str_t:%T}")

	#	print (f"\n1: {string1}\n2: {string2}\n ??")
	# XXX: # TODO: location of differences , chunking

	# XXX: Extend with space cause zip stops at the shortest
	delt = len(str2) - len(str1)
	if delt < 0:
		if de_bug : print ("lst1 longer")
		str2 += ' '*abs(delt)
	elif delt > 0:
		if de_bug : print ("lst2 longer")
		str1 += ' '*delt

	symb = ""
	trns = []
	for loc, (c1, c2) in enumerate(zip(str1, str2)) :
		if c1 == c2:
			symb += match_c
		else:
			symb += no_match_c
			trns.extend( [loc, c1, c2] )
	if de_bug : print (f"1: {str1}\nD: {symb}\n2: {str2}\n{trns} ")

	return symb, trns
#>=-------------------------------------------------------------------------=<#

@perf_monitor
def zabrain_run(input_file: str, mta_dta: Dict[str, any], de_bug: bool= False ) -> Tuple[bool, bool]:
	''' Decide what based on streams info '''
	msj = sys._getframe().f_code.co_name
	print(f"  +{msj} Start: {TM.datetime.now():%T}")
	str_t = time.perf_counter()
	logging.info(f"{msj}")

	if not input_file or not mta_dta :
		print (f"\n{msj}\nF:{input_file}\nM:{mta_dta} Exit:")
		return False , True

	try:
		ff_run_cmnd, skip_it = pars_frmat(input_file, mta_dta, de_bug)
	except ValueError as e:
		msj += f" Error: {e}"
		print ( msj )
		# Handle the exception here (e.g., log the error, skip the file, etc.)
		logging.error(msj)
		skip_it = True  # Skip the file if an error occurs in pars_frmat
		ff_run_cmnd = []  # Set ff_run_cmnd to an empty list
		raise ValueError(msj)
#		breakpoint()  # Set a breakpoint here

	end_t = time.perf_counter()
	print(f'  -End: {TM.datetime.now():%T}\tTotal: {hm_time(end_t - str_t)}')

	return ff_run_cmnd, skip_it
##>>============-------------------<  End  >------------------==============<<##

def show_progrs( line_to, sy, de_bug=False ) :
	msj = sys._getframe().f_code.co_name
	_P = ''
	if 'N/A' in line_to:
		return False
	elif 'global headers:' and "muxing overhead:" in line_to:
		_P = f'\r    |<+>| Done: {line_to}'
	elif 'encoded' in line_to:
		_P = f'    |>+<| Done: {line_to}'
#	elif 'speed=' and 'fps=' and 'time=' in line_to:
	elif all(substr in line_to for substr in ['speed=', 'fps=', 'time=']):
	# Do something when all three substrings are present in line_to

		try:
			br = re.search(r'bitrate=\s*([0-9\.]+)', line_to).group(1)
			fr = re.search(r'frame=\s*([0-9]+)',	 line_to).group(1)
			sp = re.search(r'speed=\s*([0-9\.]+)',	 line_to).group(1)
			sz = re.search(r'size=\s*([0-9]+)',		 line_to).group(1)
			tm = re.search(r'time=\S([0-9:]+)',		 line_to).group(1)
			fp = re.search(r'fps=\s*([0-9]+)',		 line_to).group(1)
			if int(fp) > 1 :
				a_sec = sum(int(x) * 60**i for i, x in enumerate(reversed(tm.split(":"))))
				dif   = abs(glb_vidolen - a_sec)
				eta   = round(dif / (float(sp)))
				mints, secs  = divmod(int(eta), 60)
				hours, mints = divmod(mints, 60)
				_eta = f'{hours:02d}:{mints:02d}:{secs:02d}'
				_P = f'\r    | {sy} |Size: {hm_sz(sz):>5}|Frames: {int(fr):>5}|Fps: {fp:>3}|BitRate: {br:>6}|Speed: {sp:>5}|ETA: {_eta:>8}|    '
				if de_bug :
					print (f'\n {line_to}\n | {sy} |Size: {hm_sz(sz):>5}|Frames: {int(fr):>5}|Fps: {fp:>3}|BitRate: {br:>6}|Speed: {sp:>5}|ETA: {_eta:>8}|' )

		except Exception as e:
			msj = f"Exception in {msj}: {e} ({line_to})"
			print(msj)
			logging.exception(msj, exc_info=True, stack_info=True )
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
	logging.info(f"{msj}")

	global glb_totfrms , vid_width

# XXX Create Matrix Colage:
	file_name, _ext = os.path.splitext(input_file)
	if _ext not in File_extn: # XXX: Very unlikely !!
		msj = f"Input: {input_file}\n Not video file."
		raise Exception (msj)

	'''
	# Get the directory path of the input file
	input_file_dir = os.path.dirname(input_file)

	# Get the basename of the input file
	infil_name, _  = os.path.splitext(os.path.basename(input_file))
#	print (infil_name)

	# Get a list of files and directories in the input file's directory
	all_entries = os.listdir(input_file_dir)

	for cnt, file in enumerate(all_entries) :
		zz, _ext = os.path.splitext(file)
	#	print (f" {cnt:02d}: {file}")
		if ( _ext == ext ) and ( zz == infil_name ):
	# XXX: 		print (f"{file} == {infil_name}")

	# Escape special characters in the file_name[:6] substring
	key_ = re.escape(file_name[5:8])
	# Create a pattern that matches files starting with the same first 6 characters and ending with .png
	pattern = fr'^{key_}.*\.{ext}$'
	# Filter the list to include only .png files that match the pattern
	matching_files = [entry for entry in all_entries if os.path.isfile(entry) and re.match(pattern, entry)]
	if matching_files :
		print(f'  We have= {matching_files}')
	'''
	if os.path.isfile(file_name + ext):
		print( f"   | PNG Exists ¯\_(%)_/¯ Skip")
		end_t = time.perf_counter()
		print(f'  -End: {TM.datetime.now():%T}\tTotal: {hm_time(end_t - str_t)}')
		return False

	else:
		file_name += ext
		width = str(vid_width)
		# We have 9 tiles plus a bit more
		slice = str(round( glb_totfrms / 9 + 1 ))

		skip0 = '00:01:13'
#		zzzle =  "[0:v]select=not(mod(n\," + slice + ")), scale=" + width + ":-1:, tile=3x3:nb_frames=9:padding=3:margin=3"
		zzzle = f"[0:v]select=not(mod(n\, {slice})), scale={width}:-1:, tile=3x3:nb_frames=9:padding=3:margin=3 "

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
			logging.error(f" {msj}", exc_info=True)
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

#	todo = (ffmpeg, '-i', input_file, '-filter_complex', "[0:v]setpts=PTS/3[v];[0:a]atempo=3[a]", '-map', "[v]", '-map', "[a]", '-y', out_f )

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
