# -*- coding: utf-8 -*-

from My_Utils import *
from Yaml import *

import os
import re
import sys
import json
import psutil
import datetime	as TM
import subprocess as SP

ffmpg_bin = 'C:\\Program Files\\ffmpeg\\bin'

ffmpeg = os.path.join(ffmpg_bin, "ffmpeg.exe")
ffprob = os.path.join(ffmpg_bin, "ffprobe.exe")
# ffplay = os.path.join(ffmpg_bin, "ffplay.exe")

if not os.path.exists(ffmpeg) or not os.path.exists(ffprob) :
	input(f"{ffmpeg}\nPath Does not Exist:")
	raise OSError

#SP.run( [ffmpeg, '-version'] )
##==============-------------------   End   -------------------==============##

# Runns ffprobe on input file then if all good
# Returns a Json list
@performance_check
def ffprobe_run(input_file, execu=ffprob, de_bug=False ) -> str :
	''' Run ffprobe on input_file
		return a Json file with all the info
	'''
	mesaj = sys._getframe().f_code.co_name
	print(f"  +{mesaj}=: Start: {TM.datetime.now():%T}")
	str_t = time.perf_counter()
	logging.info(f"{mesaj}")

	if not input_file :
		print (f" {mesaj} no input_file Exit:")
		return False

	ff_args = [execu,
				'-hide_banner',
				'-analyzeduration', '100000000',
				'-probesize',		 '50000000',
				'-v', 'warning',	# XXX quiet, panic, fatal, error, warning, info, verbose, de_bug, trace
				'-of', 'json',		# XXX default, csv, xml, flat, ini
				'-show_format',
				'-show_streams',
				'-show_error',
#				'-show_data',
				'-show_entries', 'format:stream',
#				'-show_private_data',
				'-i', input_file]

# XXX: TBD good stuff
	out = SP.run(	ff_args,
					encoding=console_encoding,
					stdout=SP.PIPE,
					stderr=SP.PIPE,
					universal_newlines=True)

	jsn_ou = json.loads(out.stdout)

	end_t = time.perf_counter()
	print(f'  -End: {TM.datetime.now():%T}\tTotal: {(end_t - str_t):.2f} sec')

	return jsn_ou
##>>============-------------------<  End  >------------------==============<<##

# XXX:  Returns encoded filename file_name
@performance_check
def ffmpeg_run(input_file:str, ff_com:[], skip_it:bool, execu:str=ffmpeg, de_bug:bool=False, max_retries:int=1, rety_delay:int=5) -> str:
	'''	Runs ffmpeg after creating the command line
		then retries and runs again if encoding fails'''
	mesaj = sys._getframe().f_code.co_name
	print(f"  +{mesaj}=: Start: {TM.datetime.now():%T}")
	logging.info(f"{mesaj}")

	p = psutil.Process()
	p.nice(psutil.HIGH_PRIORITY_CLASS)
	if not input_file or skip_it :
		logging.info(f" {mesaj}\n {input_file}\tCom: {ff_com}\n")
		return False

	file_name, _ = os.path.splitext( os.path.basename(input_file) )
	out_file = os.path.normpath('_' + stmpd_rad_str(7, file_name[0:21]))
	out_file = re.sub(r'[^\w\s_-]+', '', out_file).strip().replace(' ', '_') + TmpF_Ex

	ff_head = [execu, "-i", input_file, "-hide_banner" ]

	ff_tail = [ "-metadata", "title="    + file_name + " x256",
			   "-metadata", "comment=" + Skip_key,
			   "-metadata", "copyright=" + " 2023 Me",
			   "-metadata", "author=" + " Encoded by the one and only GeoHab",
			   "-metadata", "encoder=" + " ffmpeg 5.12",
			   "-movflags", "+faststart",
			   "-fflags", "+fastseek",
			   "-y", out_file,
#				"-fflags", "+genpts,+igndts",
			   "-f", "matroska"]
	todo = ff_head +ff_com +ff_tail

	attempt = 0
	while attempt <= max_retries:
		if attempt :
			print(f"    Retry: {attempt} of {max_retries}")

		if run_ffm(todo, debug=attempt):
			p.nice(psutil.NORMAL_PRIORITY_CLASS)
			return out_file
		else:
			attempt += 1
			if attempt <= max_retries:
				print(f"    Encode failed. Retrying in {rety_delay} seconds...")
				time.sleep(rety_delay)
	mesaj += f"   = ffmpeg Failed \n"
	p.nice(psutil.NORMAL_PRIORITY_CLASS)
	raise Exception(mesaj)


##>>============-------------------<  End  >------------------==============<<##

@performance_check
def run_ffm(args, callback=None, debug=False):
	"""Run an ffmpeg command and call a callback function for each new line of output.
	Args:
		args (list): A list of command line arguments to pass to ffmpeg.
		callback (callable): A function to call whenever a new line of output is available.
			The function should take a single string argument.
		debug (bool): Whether to print debugging information.
	Returns:
		True if the ffmpeg command completed successfully, False otherwise.
	"""
	mesaj = sys._getframe().f_code.co_name
	logging.info(f"{mesaj}")

	if not args :
		print(f"{mesaj} Exit no args = {args}")
		return False

	if debug:
		print(f"{mesaj} Debug mode\n{' '.join(args)}\n")
		runit    = SP.run( args,
			encoding=console_encoding,
			stderr=SP.STDOUT,
#						stdout=SP.PIPE,
			universal_newlines=True )
		print ("Std:", runit.stdout )
		print ("Err:", runit.stdout )
		mesaj += f" Done\n"
		print(mesaj)
		time.sleep(3)
	else:
		try:
			with SP.Popen( args,
				encoding=console_encoding,
				stderr=SP.STDOUT,
				stdout=SP.PIPE,
				universal_newlines=True,
				bufsize=1 ) as process:
				loc = 0
				symbls = "|/-o+\\"
				sy_len = len(symbls)
				for line in iter(process.stdout.readline, ""):
					if callback:
						callback(line.rstrip())
					show_progrs(line, symbls[loc])
					loc += 1
					if loc == sy_len:
						loc = 0
				out, err = process.communicate()
				if err or out:
					print(f"Error:\nE:{err}\nO:{out}")
					time.sleep(3)
				process.stdout.close()
			if process.returncode != 0:
				zz = ( process.returncode, args, out, err )
				logging.error(f" {zz}", exc_info=True)
				raise SP.CalledProcessError(
					process.returncode, args, output=out, stderr=err
				)
		except Exception as x:
			logging.error(f" {x}", exc_info=True)

			if debug:
				print(repr(x))
			return False


	return True
##==============-------------------   End   -------------------==============##

@performance_check
def pars_format(mta_dta, input_file, de_bug):
	''' Parse and extract data from file format '''
	str_t = time.perf_counter()
	mesaj = sys._getframe().f_code.co_name
	if de_bug :		print(f"  +{mesaj}=: Start: {TM.datetime.now():%T}")
	logging.info(f"{mesaj}")

	global glb_vidolen  # make it global so we can reuse for fmpeg check ...
	global glb_bitrate

	# XXX: Meta Data
	_format = {
		'filename'			: str,
		'nb_streams' 		: int,
		"nb_programs"		: int,
		"format_name"		: str,
		"format_long_name"	: str,
		"start_time"		: float,
		'duration' 	 		: float,
		'bit_rate' 	 		: int,
		'size'     	 		: float,
		'tags'		 		: {}
		}

	if 'format' not in mta_dta :
		mesaj+= f" 'format' keyword not in\n{ json.dumps(mta_dta, indent=2) }\n"
		print ( mesaj )
		raise ValueError(mesaj)
	else :
		frmt_ = mta_dta['format']
		if de_bug :
			print( f"\nFormat:\n{ json.dumps(mta_dta, indent=2) }\n")

	glb_vidolen = int(float(frmt_['duration']))
	glb_bitrate = int(      frmt_['bit_rate'])

	nb_streams	= int(		frmt_['nb_streams'])
	nb_programs	= int(		frmt_['nb_programs'])

	strms_ = [strg for strg in mta_dta['streams']]

	if de_bug : print(f" We have {len(strms_)} Streams: {strms_}" )

	if nb_streams != len( strms_) :
		print(f" Someting is wrong different numer of streams {nb_streams} != {len( strms_) }")

	skip_this = False
	if 'tags' in frmt_ :
		if 'comment' in frmt_['tags'] and frmt_['tags']['comment'] == Skip_key :
			skip_this = True
		if de_bug : print( json.dumps(frmt_['tags'], indent=2) )
	else :
		print("No Tags in Format ?", frmt_['tags']['comment'], Skip_key)

	show_cmd = de_bug

	all_videos = [stre for stre in strms_ if stre['codec_type'] == 'video']
	if len( all_videos) :
		ff_video = pars_video( all_videos, de_bug )
	else :
		ff_video = ['-vn']
		print("    |<V:no>|")
		raise Exception(f' no Video')
	if show_cmd :print (f"      {ff_video}")

	all_audios = [stre for stre in strms_ if stre['codec_type'] == 'audio']
	if len( all_audios) :
		ff_audio = pars_audio ( all_audios, de_bug )
	else :
		ff_audio = ['-an']
		print("    |<A:no>|")
	if show_cmd :print (f"      {ff_audio}")

	all_substrs = [stre for stre in strms_ if stre['codec_type'] == 'subtitle']
	if len( all_substrs) :
		ff_subtl= pars_subtl ( all_substrs, de_bug )
	else :
		ff_subtl = ['-sn']
# XXX: 		ff_subtl = find_subtl( input_file, de_bug )
		print("    |<S:no>|")
	if show_cmd :print (f"      {ff_subtl}")

	all_datass = [stre for stre in strms_ if stre['codec_type'] == 'data']
	if len( all_datass) :
		ff_data = pars_extdta( all_datass, de_bug )
	else :
		ff_data = ['-dn']
		print("    |<D:no>|")
	if show_cmd :print (f"      {ff_data}")

	minut,  secs = divmod(glb_vidolen, 60)

	Leng =		f"{minut:02d}m:{secs:2d}s"
	hours, minut = divmod(minut, 60)
	if hours :
		Leng =	f"{hours:02d}h:{minut:02d}m:{secs:2d}s"
	days,  hours = divmod(hours, 24)
	if days :
		Leng =	f"{days}D{hours:02d}h:{minut:02d}m:{secs:02d}s"

	# XXX: Print Banner
	mesaj = f"    |< FR >|Sz:{hm_sz( frmt_['size'])}|BRate: {hm_sz( glb_bitrate )} |L: {Leng} |Strms = {nb_streams}|Prog = {nb_programs}"
	if len( all_videos) :
		mesaj+= f'|V = {len(all_videos)}|'
	if len( all_audios) :
		mesaj+= f'|A = {len(all_audios)}|'
	if len(all_substrs) :
		mesaj+= f'|S = {len(all_substrs)}|'
	if len(all_datass) :
		mesaj+= f'|D = {len(all_datass)}|'
	print(f"{mesaj}")

	end_t = time.perf_counter()
	if de_bug : print(f'   Done: {TM.datetime.now():%T}\tTotal: {(end_t - str_t):.2f} sec')
	return ff_video, ff_audio, ff_subtl, ff_data, skip_this
##>>============-------------------<  End  >------------------==============<<##

@performance_check
def pars_video ( strm_in , de_bug ) :
	''' Parse and extract data from video streams '''

	mesaj = sys._getframe().f_code.co_name
	str_t = time.perf_counter()
	logging.info(f"{mesaj}")
	if de_bug:
		print(f"    +{mesaj}=: Start: {TM.datetime.now():%T}")
		print(f" {json.dumps( strm_in, indent=2)}" )

	_vdata = {
		'index'			:int,
		'codec_name'	:str,
		'width'			:int,
		'height'		:int,
		'coded_width'	:int,
		'coded_height'	:int,
		'bit_rate'		:int,
		'r_frame_rate'	:str,
		'avg_frame_rate':str,
		'pix_fmt'		:str,
		'disposition' :{},
		'tags' :{}
		}

	ff_video = []
	for count, this_vid in enumerate(strm_in) :
		extra = ''
		_strm = str(this_vid['index'])
		_cnt  = str(count)

		global vid_width	#NOTE used by matrix_it

		vid_width, vid_heigh = this_vid['width'], this_vid['height']

		totl_pixl = vid_width * vid_heigh
		expctd = totl_pixl * (2000/vid_heigh )

		if 'codec_name' not in this_vid:
			this_vid['codec_name'] = 'No Codec Name ?'
			mesaj = f"   No Video Codec |<V:{this_vid}\n"
			raise Exception(mesaj)

# If it's mjpeg or other unsuported just ignore and deleate
		if this_vid['codec_name'].lower() in ['mjpeg', 'png'] :
			extra = f" Rm: {this_vid['codec_name']} < |"
			ff_video += (['-map', '-0:v?' + _cnt ])
			mesaj = f"    |<V:{_strm:2}>|{this_vid['codec_name']:<8} | {extra}"
			print(mesaj)
			continue

		# NOTE Calc _vi_btrt
		if 'bit_rate' in this_vid and this_vid['bit_rate'] :
			_vi_btrt = int(this_vid ['bit_rate'])
		else:
			_vi_btrt = glb_bitrate * 0.8	# estimate 80% is video
			extra += ' BitRate Estimate '

			try :
				if 'tags' in this_vid and 'handler_name' in this_vid ['tags'] :
					extra += f"  {this_vid ['tags']['handler_name']}"
			except Exception as e:
				logging.error(f" {e}", exc_info=True)
				if de_bug : print ('  =>', e)
				pass
		try :
			frm_rate = 25
			if 'r_frame_rate' in this_vid :
				frm_ra    = divd_strn(this_vid['r_frame_rate'])
				if frm_ra in [ '0', 'NAN'] :
					r_frame_rate = 25
				else :
					frm_rate = frm_ra
			else :
				r_frame_rate = 25
				print ( "no r_frame_rate" )
		except Exception as e :
			logging.error(f" {e}", exc_info=True)
			print (e)

		if 'avg_frame_rate' in this_vid :
			av_frm = divd_strn(this_vid['avg_frame_rate'])
			if av_frm in [ '0', 'NAN'] :
				avg_frame_rate = 25
			else :
				av_frm_rate = av_frm
		else :
			avg_frame_rate = 25
			print ( "no avg_frame_rate" )

		if av_frm_rate != frm_rate :
			print(f'   Diff avg_frame_rate = {av_frm_rate} != r_frame_rate = {frm_rate}')

		global glb_totfrms
		glb_totfrms = round(frm_rate * glb_vidolen)

		if 'yuv420p' in this_vid['pix_fmt'] :
			mesaj = f"{this_vid['pix_fmt']}"
			if 'yuv420p10le' in this_vid['pix_fmt'] :
				mesaj += " 10 Bit"
				totl_pixl *= 1.2 # 30/24 +15% extra

		mins,  secs = divmod(glb_vidolen, 60)
		hours, mins = divmod(mins, 60)

	# XXX: Print Banner
		mesaj = f"    |< CT >|{vid_width:>4}x{vid_heigh:<4}|Tfm: {hm_sz(glb_totfrms,'F'):>8}|Tpx: {hm_sz(totl_pixl, 'P'):>8}|XBr: {hm_sz(round(totl_pixl)):>7}| {mesaj}"
		print(mesaj)
		if (de_bug): print( f"S:{_strm} C:{_strm }\n {ff_vid}" )

		ff_vid = ['-map', '0:V:' + _strm , '-c:v:' + _cnt ]
		output = "SD"
		if vid_width >= 7600 or vid_heigh >= 4300:
			output = "8K"
		if vid_width >= 3800 or vid_heigh >= 2100:
			output = "4K"
		if vid_width >  2100 or vid_heigh >  1240 :
			output = "2K"
			nw = 1920
			nh = round ((nw/vid_width) * vid_heigh)
			extra += f' {output} Scale {vid_width}x{vid_heigh} to {nw}x{nh}'
			ff_video = ['-map', f'0:v:{_cnt}', f'-c:v:{_cnt}', 'libx265', '-pix_fmt', f"{this_vid['pix_fmt']}", '-crf', '26', '-preset', 'slow', '-vf', f'scale={nw}:{nh}']
			mesaj = f"    |<V:{_strm:2}>|{this_vid ['codec_name']:<8} |Br: {hm_sz(_vi_btrt):>9}|XBr: {hm_sz(totl_pixl):>8}|Fps: {frm_rate:>7}|{extra}"
			print(f"\033[91m{mesaj}\033[0m")
			return ff_video
		elif  vid_width >= 1280 or vid_heigh >= 720:
			output = "FHD"
		elif vid_width >= 720 or vid_heigh >= 500:
			output = "HD"
		if _vi_btrt < (expctd) :
			if this_vid['codec_name'] == 'hevc':
				extra += ' Copy'
				ff_vid.extend(['copy'])
			else:
				extra += ' => Convert to Hevc'
				ff_vid.extend(['libx265', '-pix_fmt', f"{this_vid['pix_fmt']}", '-crf', '26', '-preset', 'slow'])
		else :
			ff_vid.extend(    ['libx265', '-pix_fmt', f"{this_vid['pix_fmt']}", '-crf', '26', '-preset', 'slow'])
			if this_vid['codec_name'] == 'hevc':
				extra += '\tReduce < ' +hm_sz(expctd)
			else:
				extra += ' => Convert to Hevc'
		mesaj = f"    |<V:{_strm:2}>|{this_vid ['codec_name']:<8} |Br: {hm_sz(_vi_btrt):>9}|XBr: {hm_sz(totl_pixl):>8}|Fps: {frm_rate:>7}|{extra}"
		print(f"\033[91m{mesaj}\033[0m")


		ff_video += ff_vid

	if de_bug : print (" ", ff_video)

	end_t = time.perf_counter()
#	print(f'   -End: {TM.datetime.now():%T}\tTotal: {round((end_t - str_t), 2)} sec')

	return ff_video
##>>============-------------------<  End  >------------------==============<<##

# XXX: Audio
@performance_check
def pars_audio ( strm_in, de_bug ) :
	''' Parse and extract data from audio streams '''
	mesaj = sys._getframe().f_code.co_name
	if de_bug :	print(f"    +{mesaj}=: Start: {TM.datetime.now():%T}")
	str_t = time.perf_counter()
	if de_bug :	print(f"{json.dumps( strm_in, indent=2)}" )
	logging.info(f"{mesaj}")

	_adata = {
		'index'			: int,
		'codec_name'	: str,
		"codec_type"	: str,
		'language'		: str,
		'title'			: str,
		"bit_rate"		: str,
		"sample_rate"	: int,
		"channels"		: int,
		"channel_layout": str,
		"time_base"		: str,
		"start_pts"		: int,
		"start_time"	: str,
		'disposition'	: {},
		'tags'			: {}
		}


	ff_audio = []
	is_deflt = False
	for count, this_aud in enumerate(strm_in) :
		ff_aud = []
		extra  = ''
		_strm = str(this_aud['index'])
		_cnt =  str(count)
		if de_bug :
			print ( json.dumps(this_aud, indent=3 ) )
			print (f"    +{mesaj}=: Start: {TM.datetime.now():%T}\n {json.dumps(this_aud, indent=2)}" )

#		try :
		if 'bit_rate' in this_aud:
			_au_btrt = int( this_aud['bit_rate'])
		else :
			_au_btrt = glb_bitrate * 0.2	# estimate 20% is video
			extra = 'BitRate Estimate '

		if 'channels' in this_aud:
			chnls = this_aud['channels']
			if chnls == 1:
				extra += "Mono"
			elif chnls == 2:
				extra += "Stereo"
			elif chnls > 2:
				extra += f"{chnls -1}.1 Channels"
		else:
			chnls = -100
			extra += f" Unknown Channels "

		if 'disposition' in this_aud:
			dsp_f = this_aud['disposition']['forced']
			dsp_d = this_aud['disposition']['default']

		ff_aud.extend( ['-map', '0:a:' + _cnt, '-c:a:' + _cnt])

		if this_aud['codec_name'] in ('aac', 'vorbis', 'mp2', 'mp3'):
			if _au_btrt < Max_a_btr :
				extra += ' Copy'
				# XXX: de_bug # XXX:
				if de_bug: print(json.dumps(this_aud, indent=2))
				ff_aud.extend(['copy'])
			else:
				extra += ' Reduce BitRate'
				ff_aud.extend(['libvorbis', '-q:a:'+ _cnt, '7'])
		else:
			extra += f" Convert to vorbis"
			ff_aud.extend(['libvorbis', '-q:a:'+ _cnt, '7'])

		if 'tags' in this_aud and 'language' in this_aud['tags'] :
			_lng = this_aud['tags']['language']
			if len(_lng) != 3 :
				print (f' Not Kosher Language {_lng}')
				_lng = 'und'
		else :
			_lng = 'und'

		if _lng == Default_lng :
			is_deflt +=1
			ff_aud.extend(['-metadata:s:a:'  + _cnt, 'language=' + _lng])
			if is_deflt == 1 :
				ff_aud.extend(['-disposition:s:a:' + _cnt, 'default'])
				extra += f" Dis: def={dsp_d}, force={dsp_f} => Set to Default"
			else :
				ff_aud.extend(['-disposition:s:a:' + _cnt, 'none'])
				extra += f" Dis: def={dsp_d}, force={dsp_f}"
		elif _lng in Keep_langua :
			ff_aud.extend(['-metadata:s:a:'  + _cnt, 'language=' + _lng])
			ff_aud.extend(['-disposition:s:a:' + _cnt, 'none'])
		# XXX: If only one Audio and it is und keep it# XXX: Let's Presume The Only one 'und' is 'eng' Enlish
		elif len(strm_in) == 1 :
			extra += f" Dis: def={dsp_d}, force={dsp_f} => Set to {Default_lng} + Default"
			ff_aud.extend(['-metadata:s:a:'  + _cnt, 'language=' + Default_lng])
			ff_aud.extend(['-disposition:s:a:' + _cnt, 'default'])
		else :
			extra = f" Rm: {this_aud['codec_name']} {_lng} < |"
			ff_aud = (['-map', '-0:a:' + _cnt, '-c:a:' + _cnt, 'copy'])

		try :
			if 'tags' in this_aud and 'handler_name' in this_aud['tags']:
				extra += f"  {this_aud['tags']['handler_name']}"
		except Exception as x:
			logging.error(f" {x}", exc_info=True)
			if de_bug : print ('  =>', repr(x))
			pass
		global aud_smplrt
		aud_smplrt = this_aud['sample_rate']
#		print ( f" {aud_smplrt}, {this_aud['sample_rate']} & {hm_sz(aud_smplrt)} )")
#		input ( 'WTFko')
		mesaj = f"    |<A:{_strm:2}>|{this_aud['codec_name']:<8} |Br: {hm_sz(_au_btrt):>9}|{_lng}|Frq: {hm_sz(aud_smplrt):>8},'Hz'|Ch: {chnls}| {extra}"
		print(f'\033[92m{ mesaj}\033[0m')
		if (de_bug) : print (ff_aud)

		ff_audio += ff_aud
	if de_bug :
		print("Audio", ff_audio )

	end_t = time.perf_counter()
#	print(f'   -End: {TM.datetime.now():%T}\tTotal: {round((end_t - str_t), 2)} sec')

	return ff_audio
##>>============-------------------<  End  >------------------==============<<##

# XXX: Subtitle
@performance_check
def pars_subtl ( strm_in , de_bug ) :
	''' Parse and extract data from subtitle streams '''
	mesaj = sys._getframe().f_code.co_name
	if de_bug :	print(f"    +{mesaj}=: Start: {TM.datetime.now():%T}")
	str_t = time.perf_counter()
	logging.info(f"{mesaj}")

	_sdata = {
		'index'		: int,
		'codec'		: str,
		'codec_name': str,
		'codec_type': str,
		'language'	: str,
#		'title'		: str,
		'map'		: int,
		'source'	: int,
		'path'		: str,
		'disposition': {},
		'tags':{}
	}

	numbr = len (strm_in)
	ff_subtl =[]
	for count, this_sub in enumerate(strm_in) :

	#	prs_frm_to ( this_sub, _sdata , de_bug)

		ff_sub = []
		extra  = ''
		_strm = str(this_sub['index'])
		_cnt  = str(count)
		if de_bug :
			print(f"    +{mesaj}=: Start: {TM.datetime.now():%T}\n {json.dumps(this_sub, indent=2)}" )
		if 'codec_name' not in this_sub :
			this_sub['codec_name'] = "undefined"
		#	print(f"    +{mesaj}= NO codec_name in {json.dumps(this_sub, indent=2)}" )

		try :
			if ('language') in this_sub['tags'] :
				_lng = this_sub['tags']['language']
			else :
				_lng = 'und'

			if 'disposition' in this_sub:
				dsp_f = this_sub['disposition']['forced']
				dsp_d = this_sub['disposition']['default']

			ff_sub.extend( ['-map', '0:s:' +_cnt , '-c:s:' +_cnt])
# XXX: bin_data TBD
			if this_sub['codec_name'].lower() in ['hdmv_pgs_subtitle', 'dvb_subtitle', 'ass' ]:
				extra += f" Rm: {this_sub['codec_name']} {_lng} < |"
				ff_sub = (['-map', '-0:s:' + _cnt])

			elif _lng == Default_lng :
				extra += f" Move Text {this_sub['codec_name']} {_lng} Default"
				ff_sub.extend(['mov_text', '-metadata:s:s:'  + _cnt, 'language=' + _lng])
				ff_sub.extend(['-disposition:s:s:' + _cnt, 'default'])

			elif _lng in Keep_langua :
				extra += f" Move Text {this_sub['codec_name']} {_lng}"
				ff_sub.extend(['mov_text', '-metadata:s:s:'  + _cnt, 'language=' + _lng])
				ff_sub.extend(['-disposition:s:s:' + _cnt, 'none'])

			else :
				extra += f" Rm: {this_sub['codec_name']} {_lng} < |"
				ff_sub = (['-map', '-0:s:' + _cnt, '-c:s:' + _cnt, 'mov_text'])

			try :
				if 'tags' in this_sub and 'handler_name' in this_sub['tags']:
					extra += f"  {this_sub['tags']['handler_name']}"
			except Exception as x:
				logging.error(f" {x}", exc_info=True)
				if de_bug : print ('  =>', repr(x))
				pass

			mesaj = f"    |<S:{_strm:2}>|{this_sub['codec_name']:<9}|{this_sub['codec_type']:<13}|{_lng:3}|Disp: default={dsp_d}, forced={dsp_f}|{extra}"
			print(f"\033[94m{mesaj}\033[0m")
			if (de_bug) : print (ff_sub)

		except Exception as e:
			logging.exception(f"{e}", stack_info=True)
			logging.error(f" {e}", exc_info=True)
			mesaj += f" Exception"
			Trace (mesaj, e)
		if de_bug :
			print(f"\n {json.dumps(this_sub, indent=2)}\n{ff_sub}\n" )
		ff_subtl += ff_sub
	if de_bug : print ("  ", ff_subtl )

	end_t = time.perf_counter()
#	print(f'   -End: {TM.datetime.now():%T}\tTotal: {round((end_t - str_t), 2)} sec')

	return( ff_subtl )
##>>============-------------------<  End  >------------------==============<<##

# XXX: Extra Data
@performance_check
def pars_extdta( strm_in, de_bug ) :
	''' Parse and extract data from data streams '''

	mesaj = sys._getframe().f_code.co_name
	str_t = time.perf_counter()
	if de_bug :
		print(f"    +{mesaj}=: Start: {TM.datetime.now():%T}\n {json.dumps( strm_in, indent=2)}" )
	logging.info(f"{mesaj}")

	_ddata = {
		'index'		: int,
		'codec'		: str,
		'codec_name': str,
		'codec_type': str,
#		'language'	: str,
		'tags'		:{}
	}
	# XXX: Data
	ff_data =[]
	for count, this_dat in enumerate(strm_in) :
		if de_bug : print(f"    +{mesaj}=: Start: {TM.datetime.now():%T}\n {json.dumps(this_dat, indent=2)}" )
		ff_dd = []
		try :
			if 'tags' in this_dat and 'handler_name' in this_dat ['tags'] :
				extra = f"  {this_dat['tags']['handler_name']}"

			if 'codec_name' in this_dat and 'codec_type' in this_dat :
				mesaj = f"    |<D:{this_dat['index']:2}>| {this_dat['codec_name']:<8}| {this_dat['codec_long_name']:<9}| {this_dat['codec_type']:^11} |  {extra}"
			elif 'codec_long_name' in this_dat :
				mesaj = f"    |<D:{this_dat['index']:2}>| {this_dat['codec_long_name']:<9}| {this_dat['codec_type']:^11} |  {extra}"
			else :
				mesaj = f"    |<D:{this_dat['index']:2}>| {this_dat['codec_type']:^11} |  {extra}"
			print(mesaj)

		except Exception as e:
			logging.error(f" {e}", exc_info=True)
			mesaj += f" Exception {e}"
			print(f"    +{mesaj}=!!\n{json.dumps(this_dat, indent=2)}" )
			Trace (mesaj, e)

		ff_data += ff_dd

	end_t = time.perf_counter()
#	print(f'   -End: {TM.datetime.now():%T}\tTotal: {round((end_t - str_t), 2)} sec')

	return ff_data

##>>============-------------------<  End  >------------------==============<<##
@performance_check
def	find_subtl ( input_file, de_bug ) :
	''' find subtitle files and select the best '''
	str_t = time.perf_counter()
	mesaj = sys._getframe().f_code.co_name
	if de_bug :
		print(f"    +{mesaj}=: Start: {TM.datetime.now():%T}\nFiles:{input_file}" )
	logging.info(f"{mesaj}")

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
		_cnt =str(count)
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
	#		ff_sub = (['-i', fname, '-c:s', 'copy', '-metadata:s:s' + _cnt, 'language=' + 'eng'])
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
	if de_bug : print(f"     +{message}=: Start: {str_t:%T}")

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

@performance_check
def zabrain_run(input_file, mta_dta, de_bug ):
	''' Decide what based on streams info '''

	mesaj = sys._getframe().f_code.co_name
	print(f"  +{mesaj}=: Start: {TM.datetime.now():%T}")
	str_t = time.perf_counter()
	logging.info(f"{mesaj}")

	if not input_file or not mta_dta :
		print (f" {mesaj} F:{input_file} M:{mta_dta} Exit:")
		return False

	ff_video, ff_audio, ff_subtl, ff_data, skip_this = pars_format( mta_dta, input_file, de_bug )

	ff_run_cmnd = (ff_video+ ff_audio + ff_subtl +ff_data), skip_this

	if de_bug :print (mesaj, ff_run_cmnd)

	end_t = time.perf_counter()
	print(f'  -End: {TM.datetime.now():%T}\tTotal: {(end_t - str_t):.2f} sec')

	return ff_run_cmnd
##>>============-------------------<  End  >------------------==============<<##

def show_progrs(line_to, sy=False):
	mesaj = sys._getframe().f_code.co_name

	_P = ''
	if 'N/A' in line_to:
		return False
	elif 'global headers:' and "muxing overhead:" in line_to:
		_P = f'\r    |<+>| Done: {line_to}'
	elif 'encoded' in line_to:
		_P = f'    |>+<| Done: {line_to}'
	elif 'speed=' and 'fps=' in line_to:
		try:
			fr = re.search(r'frame=\s*([0-9]+)',	 line_to).group(1)
			sz = re.search(r'size=\s*([0-9]+)',		 line_to).group(1)
			sp = re.search(r'speed=\s*([0-9\.]+)',	 line_to).group(1)
			br = re.search(r'bitrate=\s*([0-9\.]+)', line_to).group(1)
			tm = re.search(r'time=\S([0-9:]+)',		 line_to).group(1)
			fp = re.search(r'fps=\s*([0-9]+)',		 line_to).group(1)
			if int(fp) > 1 :
				a_sec = sum(int(x) * 60**i for i, x in enumerate(reversed(tm.split(":"))))
				dif = abs(glb_vidolen - a_sec)
				eta = round(dif / (float(sp)))
				mints, secs  = divmod(int(eta), 60)
				hours, mints = divmod(mints, 60)
				_eta = f'{hours:02d}:{mints:02d}:{secs:02d}'
				_P = f'\r    | {sy} |Size: {hm_sz(sz):>5}|Frames: {int(fr):>5}|Fps: {fp:>3}|BitRate: {br:>6}|Speed: {sp:>5}|ETA: {_eta:>8}|    '
				if de_bug :
					print (f'\n {line_to} | {sy} |Size: {hm_sz(sz):>5}|Frames: {int(fr):>5}|Fps: {fp:>3}|BitRate: {br:>6}|Speed: {sp:>5}|ETA: {_eta:>8}|' )

		except Exception as e:
			logging.error(f" {e}", exc_info=True)
			mesaj+= f"Line: {line_to} ErRor: {e}\n{repr(e)}:"
			print (mesaj)
#			Trace (mesaj, e)

	sys.stderr.write(_P)
	sys.stderr.flush
	return True
##>>============-------------------<  End  >------------------==============<<##

@performance_check
def matrix_it(input_file, execu=ffmpeg, ext ='.png'):
	''' Create a 3x3 matrix colage '''
	str_t = time.perf_counter()
	mesaj = sys._getframe().f_code.co_name
	print(f"  +{mesaj}=: Start: {TM.datetime.now():%T}")
	logging.info(f"{mesaj}")

	global glb_totfrms , vid_width
# XXX Create Matrix Colage:
	file_name, _ = os.path.splitext(input_file)
#	print(f' file  {input_file}')
	'''
	hack to eliminate dubles
	'''
	_name, _ = os.path.splitext(input_file)
	if _ in File_extn:
		file_name, _ = os.path.splitext(input_file)
	if os.path.isfile(file_name + ext):
		print( f"   | PNG Exists ¯\_(%)_/¯ Skip")
		end_t = time.perf_counter()
		print(f'  -End: {TM.datetime.now():%T}\tTotal: {(end_t - str_t):.2f} sec')
		return False
	else:
		file_name += ext

		skip0 = '00:00:58'
		width = str(vid_width)
		# We have 9 tiles plus a bit more
		slice = str(round( glb_totfrms / 9 + 1 ))
		zzzle = "[0:v]select=not(mod(n\," + slice + ")), scale=" + width + ":-1:, tile=3x3:nb_frames=9:padding=3:margin=3"

		if glb_totfrms > 6000:
			todo = (execu, '-ss', skip0, '-vsync', 'vfr', '-i',
					input_file, '-frames', '1', '-vf', zzzle, '-y', file_name)
		else:
			todo = (execu, '-vsync', 'vfr', '-i',
					input_file, '-frames', '1', '-vf', zzzle, '-y', file_name)
		# XXX:
		if run_ffm(todo):
			mesaj = f"\r    |3x3| Matrix Created {ext}"
			print(mesaj)
			end_t = time.perf_counter()
			print(f'  -End: {TM.datetime.now():%T}\tTotal: {(end_t - str_t):.2f} sec')
			return os.path.getsize(file_name)
		else:
			mesaj = f"   = Failed to Created .PNG >"
			logging.error(f" {mesaj}", exc_info=True)
			raise Exception(mesaj)

	end_t = time.perf_counter()
	print(f'  -End: {TM.datetime.now():%T}\tTotal: {(end_t - str_t):.2f} sec')

##>>============-------------------<  End  >------------------==============<<##
@performance_check
def speed_up ( input_file, *other) :
	''' Create a 4x sped up version '''
	str_t = time.perf_counter()
	mesaj = sys._getframe().f_code.co_name
	print(f"  +{mesaj}=: Start: {TM.datetime.now():%T}")
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
		mesaj+= f"   = Failed to \n"
		raise Exception(mesaj)

	end_t = time.perf_counter()
	print(f'  -End: {TM.datetime.now():%T}\tTotal: {(end_t - str_t):.2f} sec')
	return todo
##>>============-------------------<  End  >------------------==============<<##

@performance_check
def video_diff(file1, file2) :
	# XXX:  Visualy Compare in and out files
	# https://stackoverflow.com/questions/25774996/how-to-compare-show-the-difference-between-2-videos-in-ffmpeg
	str_t = time.perf_counter()
	mesaj = sys._getframe().f_code.co_name
	print(f"  +{mesaj}=: Start: {TM.datetime.now():%T}")

	file_name, _ = os.path.splitext( os.path.basename(file1) )
	out_f = '_Diff_'+file_name +stmpd_rad_str(3) +TmpF_Ex

	todo = (ffmpeg, '-i', file1, '-i', file2, '-filter_complex', "blend=all_mode=difference", '-c:v', 'libx265', '-preset', 'faster', '-c:a', 'copy', '-y', out_f)

	if run_ffm(todo):
		return out_file
	else:
		mesaj+= f"   = Failed to Compare Files >\n"
		raise Exception(mesaj)

	end_t = time.perf_counter()
	print(f'  -End: {TM.datetime.now():%T}\tTotal: {(end_t - str_t):.2f} sec')

	return out_file
	##>>============-------------------<  End  >------------------==============<<##

@performance_check
def short_ver ( input_file, execu, *other ) :
	str_t = time.perf_counter()
	mesaj = sys._getframe().f_code.co_name
	print(f"  +{mesaj}=: Start: {TM.datetime.now():%T}")

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
		mesaj+= f'\n{todo}\nNo Output Error'
		print(mesaj)
		time.sleep(3)
		return out_file
	else :
		print (" Failed")

	end_t = time.perf_counter()
	print(f'  -End: {TM.datetime.now():%T}\tTotal: {(end_t - str_t):.2f} sec')
	return out_file
##>>============-------------------<  End  >------------------==============<<##
