from My_Utils import *
from Yaml import *

import os
import re
import sys
import json
import datetime	as TM
import subprocess as SP

ffmpg_bin = 'C:\\Program Files\\ffmpeg\\bin'

ffmpeg = os.path.join(ffmpg_bin, "ffmpeg.exe")
ffprob = os.path.join(ffmpg_bin, "ffprobe.exe")
# ffplay = os.path.join(ffmpg_bin, "ffplay.exe")

if not ( os.path.exists(ffmpeg) and os.path.exists(ffprob) ):
	input(f"{ffmpeg}\nPath Does not Exist:")
	raise OSError

#SP.run( [ffmpeg, '-version'] )
##==============-------------------   End   -------------------==============##

# Runns ffprobe on input file then if all good
# Returns a Json list
@performance_check
def ffprobe_run(in_file, execu=ffprob, DeBug=False ):
	''' Run ffprobe '''
	mesaj = sys._getframe().f_code.co_name
	print(f"  +{mesaj}=: Start: {TM.datetime.now():%T}")
	str_t = time.perf_counter()

	ff_args = [execu,
				'-hide_banner',
				'-analyzeduration', '100000000',
				'-probesize',		 '50000000',
				'-v', 'warning',	# XXX quiet, panic, fatal, error, warning, info, verbose, debug, trace
				'-of', 'json',		# XXX default, csv, xml, flat, ini
				'-show_format',
				'-show_streams',
				'-show_error',
#				'-show_data',
				'-show_entries', 'format:stream',
#				'-show_private_data',
				'-i', in_file]

# XXX: TBD good stuff
	out = SP.run(	ff_args,
					encoding=console_encoding,
					stdout=SP.PIPE,
					stderr=SP.PIPE,
					universal_newlines=True)

	jsn_ou = json.loads(out.stdout)

	end_t = time.perf_counter()
	print(f'  -End: {TM.datetime.now():%T}\tTotal: {round((end_t - str_t), 2)} sec')

	return jsn_ou
##>>============-------------------<  End  >------------------==============<<##

# XXX:  Returns encoded filename file_name
@performance_check
def ffmpeg_run(input_file, ff_com, execu=ffmpeg, DeBug=False):
	str_t = TM.datetime.now()
	mesaj= sys._getframe().f_code.co_name
	print(f"  +{mesaj}=: Start: {str_t:%T}")
#	DeBug=True

	file_name, _ = os.path.splitext( os.path.basename(input_file) )
	out_file = '_' +stmpd_rad_str(7, file_name[0:21]) +TmpF_Ex

	ff_head = [execu, "-i", input_file, "-hide_banner" ]

	ff_tail = [ "-metadata", "title="    + file_name + " x256 ",
				"-metadata", "comment="  + Skip_key,
				"-metadata", "copyright="+"2023 Me",
				"-metadata", "author="   +"Encoded by the one and only GeoHab",
				"-metadata", "encoder="  +"FFmpeg 5.12",
				"-movflags", "+faststart",
				"-fflags",   "+fastseek",
#				"-fflags", "+genpts,+igndts",
				"-y", out_file,
				"-f", "matroska" ]
	DeBug = True
	if DeBug :
		# -to specifis end time, -t specifies duration
		str_at = '00:00:33'
		end_at = '00:05:55'

		print (f"DeBug Make short Start:{str_at} End:{end_at}" )

		if len(file_name) > 40 :
			out_file = '_Short_ '+file_name[0:40] +stmpd_rad_str(3) +TmpF_Ex
		else :
			out_file = '_Short_ '+file_name +stmpd_rad_str(3) +TmpF_Ex

		ff_head = [ execu, '-ss', str_at, '-t', end_at, '-i', input_file, '-max_muxing_queue_size', '2048']
		ff_com  = ['-map', '0:v?', '-map', '0:a?', '-map', '0:s?', '-c:s', 'mov_text' ]
		ff_tail = [ '-y', out_file ]

	todo = ff_head +ff_com +ff_tail

#	if DeBug :
#		print( todo )
#		time.sleep( 4 )

# XXX:
	'''
	Needs Work it's just a hack to rerun in debug mode if not succesful
	'''

	if run_ffm( todo ):
		if not os.path.exists(out_file) or os.path.getsize(out_file) < 1000 :
			mesaj+= f'\n{todo}\nNo Output Error'
			print(mesaj)
			time.sleep(3)
			make_matrx(input_file, execu, ext = '.png')
#			run_ffm(todo, mesaj)
			raise Exception(mesaj)
		else:
			end_t = TM.datetime.now()
			convert = time.strftime("%H:%M:%S", time.gmtime((end_t-str_t).total_seconds()))
			mesaj=f'  -End: {end_t:%T}\tTotal: {convert} sec'
			print(mesaj)
			return out_file
	else:
		mesaj+= f" -> FFMpeg failed :("
		raise Exception(mesaj)

	return out_file
##>>============-------------------<  End  >------------------==============<<##


def run_ffm(args, *DeBug):
#	DeBug=True
	mesaj= sys._getframe().f_code.co_name + ':'

	if DeBug :
		print(f'{mesaj} Debug Mode\n{args}\n')
		runit    = SP.run( args)
		mesaj+= f' Done \n'
		print(mesaj)
		time.sleep(3)
	else:
		try:
			runit = SP.Popen(args,
							encoding=console_encoding,
							stderr=SP.STDOUT,
							stdout=SP.PIPE,
							universal_newlines=True )
			loc = 0
			symbls = '|/-o+\\'
			sy_len = len(symbls)
			while not runit.poll() :
				try :
					lineo = runit.stdout.readline()
					if len(lineo) :
						show_progrs(lineo, symbls[loc])
						loc += 1
						if  loc == sy_len:
							loc = 0
					else:
						out, err = runit.communicate()
						if err or out :
							print(f'Error :\nE:{err}\nO:{out}')
							time.sleep(3)
						break
				except Exception as x:
					if DeBug : print (repr(x))
					continue
		except :
			mesaj+= f" Exception => So lets try DEBUG mode"
			print(mesaj)
			try :
				runit.kill()
				runit = SP.Popen(args)
				out, err = runit.communicate()
				if out or err :
					print (f'\nOut: {out}\nErr{err}\n' )
				print('\nDone Looking Good :D')
				return True
			except :
				print (':( Looking Bad :D')
				return False

	mesaj+= f"\n is Done"
	if DeBug : print (mesaj)

	return True
##==============-------------------   End   -------------------==============##

@performance_check
def pars_format(mta_dta, in_file, DeBug):
	''' Parse and extract data from file format '''
	str_t = TM.datetime.now()
	mesaj= sys._getframe().f_code.co_name + 'p:'

	if DeBug :		print(f"  +{mesaj}=: Start: {str_t:%T}")

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

	frmt_ = mta_dta['format']

	if not len(frmt_) :
		mesaj+= f"\n{ json.dumps(mta_dta, indent=2) }\n No Format\n"
		raise ValueError(mesaj)
	else :
		if DeBug :
			print( f"\nFormat:\n{ json.dumps(mta_dta, indent=2) }\n")

	glb_vidolen = int(float(frmt_['duration']))
	glb_bitrate = int(      frmt_['bit_rate'])

	nb_streams	= int(		frmt_['nb_streams'])
	nb_programs	= int(		frmt_['nb_programs'])

	strms_ = [strg for strg in mta_dta['streams']]

	if DeBug : print(f" We have {len(strms_)} Streams: {strms_}" )

	if nb_streams != len( strms_) :
		print(f" Someting is wrong different numer of streams {nb_streams} != {len( strms_) }")

	skip_this = False
	if 'tags' in frmt_ :
		if 'comment' in frmt_['tags'] and frmt_['tags']['comment'] == Skip_key :
			mesaj+= ' = Skip it all is well'
			skip_this = True
		if DeBug : print( json.dumps(frmt_['tags'], indent=2) )
	else :
		print("No Tags in Format ?", frmt_['tags']['comment'], Skip_key)

	show_cmd = DeBug

	all_videos = [stre for stre in strms_ if stre['codec_type'] == 'video']

	if len( all_videos) :
		ff_video = pars_video( all_videos, DeBug )
	else :
		ff_video = ['-vn']
		print("    |<V:no>|")
		raise Exception(f' no Video')
	if show_cmd :print (f"      {ff_video}")

	all_audios = [stre for stre in strms_ if stre['codec_type'] == 'audio']
	if len( all_audios) :
		ff_audio = pars_audio ( all_audios, DeBug )
	else :
		ff_audio = ['-an']
		print("    |<A:no>|")
	if show_cmd :print (f"      {ff_audio}")

	all_substrs = [stre for stre in strms_ if stre['codec_type'] == 'subtitle']
	if len( all_substrs) :
		ff_subtl= pars_subtl ( all_substrs, DeBug )
	else :
		ff_subtl = ['-sn']
		ff_subtl = find_subtl( in_file, DeBug )
		print("    |<S:no>|")
	if show_cmd :print (f"      {ff_subtl}")

	all_datass = [stre for stre in strms_ if stre['codec_type'] == 'data']
	if len( all_datass) :
		ff_data = pars_extdta( all_datass, DeBug )
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
	mesaj= f"    |< FR >|Sz:{hm_sz( frmt_['size'])}|BRate: {hm_sz( glb_bitrate )} |L: {Leng} |Strms = {nb_streams}|Prog = {nb_programs}"
	if len( all_videos) :
		mesaj+= f'|V = {len(all_videos)}|'
	if len( all_audios) :
		mesaj+= f'|A = {len(all_audios)}|'
	if len(all_substrs) :
		mesaj+= f'|S = {len(all_substrs)}|'
	if len(all_datass) :
		mesaj+= f'|D = {len(all_datass)}|'
	print(mesaj)

	end_t = time.perf_counter()
	#print(f"  Done {TM.datetime.now():%T}\tTotal: {round((end_t - str_t), 2)} sec")
	return ff_video, ff_audio, ff_subtl, ff_data, skip_this
##>>============-------------------<  End  >------------------==============<<##

@performance_check
def pars_video ( strm_in , DeBug ) :
	''' Parse and extract data from video streams '''

	mesaj = sys._getframe().f_code.co_name
	print(f"    +{mesaj}=: Start: {TM.datetime.now():%T}")
	str_t = time.perf_counter()

	if DeBug:	print(f" {json.dumps( strm_in, indent=2)}" )

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
		ff_vid =[]
		_strm = str(this_vid['index'])
		_cnt  = str(count)
		global vid_width
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
			ff_video += (['-map', '-0:' +_strm , '-c:', 'copy'])
			mesaj = f"    |<V:{_strm:2}>|{this_vid['codec_name']:<8} | {extra}"
			print(mesaj)
			continue

# Calc _vi_btrt
		if 'bit_rate' in this_vid :
			_vi_btrt = int(this_vid ['bit_rate'])
		else:
			_vi_btrt = glb_bitrate * 0.8	# estimate 80% is video
			extra = ' BitRate Estimate '

		'''
			If size larger than 1080p reduce to 1080p
		'''
	# NOTE: Expected Bitrate Kind of ... should be bigger 2.5x for low res and smaler for high 1.5 # XXX:
		ff_vid.extend( ['-map', '0:v:' + _strm , '-c:v:' + _cnt ])
	#	if size > 1080p : reduce to 1080p
	#		: vid_width, vid_heigh to cals reduction factor

		if  vid_width >= 7600 or vid_heigh >= 4300:
			output = "8K"
		elif vid_width >= 3800 or vid_heigh >= 2100:
			output = "4K"
		elif vid_width > 2592 or vid_heigh > 1920 :
			output = "2K"
		# XXX: Compute Scale
			div_w = round((vid_width / 1920))
			div_h = round((vid_heigh / 1080))
			if div_h == div_w :
				nw = round( vid_width/div_w )
				nh = round( vid_heigh/div_h )
			else :
				output +=f" W != H, {div_w}, {div_h}"
				fac= min( div_w, div_h )
				nw = round( vid_width/fac )
				nh = round( vid_heigh/fac )

			extra += f'  {output} Scale {vid_width}x{vid_heigh} to {nw}x{nh}'
			ff_vid.extend(['libx265', '-crf', '25', '-preset', 'slow', '-vf', 'scale=' +str(nw) +':' +str(nh) ])
		elif _vi_btrt < (expctd) :
			if this_vid['codec_name'] == 'hevc':
				extra += ' Copy'
				ff_vid.extend(['copy'])
			else:
				ff_vid.extend(['libx265', '-crf', '25', '-preset', 'slow'])
				if   vid_width >= 1280 or vid_heigh >= 720:
					output = "FHD"
				elif vid_width >= 720 or vid_heigh >= 500:
					output = "HD"
				else:
					output = "SD"
				extra += f' {output} Convert to x265'
		else:
			if this_vid['codec_name'] == 'hevc':
				extra += '\tReduce < ' +hm_sz(expctd)
				ff_vid.extend(['libx265', '-crf', '25'])
			else:
				extra += ' => Convert to Hevc'
				ff_vid.extend(['libx265', '-crf', '25'])
			ff_vid.extend(['-preset', 'slow'])
		try :
			frm_rate = 25
			if 'r_frame_rate' in this_vid :
				frm_ra    = divd_strn(this_vid['r_frame_rate'])
				if frm_ra in [ '0', 'NAN'] :
					r_frame_rate = 25
					pass
				else :
					frm_rate = frm_ra
			else :
				r_frame_rate = 25
				print ( "no r_frame_rate" )
		except Exception as e :
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

		if re.search (r"yuv[a-z]?[0-9]{3}", this_vid['pix_fmt']) :
			mesaj= f"{this_vid['pix_fmt']}"
			if re.search (r"10le", this_vid['pix_fmt']) :
				mesaj += " 10 Bit"
				totl_pixl *= 1.2 # 30/24 +15% extra

		mins,  secs = divmod(glb_vidolen, 60)
		hours, mins = divmod(mins, 60)

	# XXX: Print Banner
		mesaj= f"    |< CT >|{vid_width:>4}x{vid_heigh:<4}|Tfm: {hm_sz(glb_totfrms,'F'):>8}|Tpx: {hm_sz(totl_pixl, 'P'):>8}|XBr: {hm_sz(round(totl_pixl)):>7}| {mesaj}"
		print(mesaj)

		if 'bit_rate' in this_vid and this_vid['bit_rate'] :
			_vi_btrt = int(this_vid ['bit_rate'])
		else :
			print ( json.dumps(this_vid , indent=3 ) )
			_vi_btrt = glb_bitrate * 0.8	# estimate 80% is video
			extra = ' BitRate Estimate '

			try :
				if 'tags' in this_vid and 'handler_name' in this_vid ['tags'] :
					extra = f"  {this_vid ['tags']['handler_name']}"
				else :
					extra = ' hndl_nm?'
			except Exception as x:
				if DeBug : print ('  =>', repr(x))
				pass

		mesaj= f"    |<V:{_strm:2}>|{this_vid ['codec_name']:<8} |Br: {hm_sz(_vi_btrt):>9}|XBr: {hm_sz(totl_pixl):>8}|Fps: {frm_rate:>7}|{extra}"
		print(mesaj)

		if (DeBug): print( f"S:{_strm} C:{_strm }\n {ff_vid}" )

		ff_video += ff_vid

	if DeBug :
		print (" ", ff_video)

	end_t = time.perf_counter()
	print(f'   -End: {TM.datetime.now():%T}\tTotal: {round((end_t - str_t), 2)} sec')

	return ff_video
##>>============-------------------<  End  >------------------==============<<##

# XXX: Audio
@performance_check
def pars_audio ( strm_in, DeBug ) :
	''' Parse and extract data from audio streams '''
	mesaj = sys._getframe().f_code.co_name
	print(f"    +{mesaj}=: Start: {TM.datetime.now():%T}")
	str_t = time.perf_counter()

	if DeBug :	print(f"{json.dumps( strm_in, indent=2)}" )

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
		if DeBug :
			print ( json.dumps(this_aud, indent=3 ) )
			print (f"    +{mesaj}=: Start: {str_t:%T}\n {json.dumps(this_aud, indent=2)}" )

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
				# XXX: DeBug # XXX:
				if DeBug: print(json.dumps(this_aud, indent=2))
				ff_aud.extend(['copy'])
			else:
				extra += ' Reduce BitRate'
				ff_aud.extend(['libvorbis', '-q:a:'+ _cnt, '7'])
		else:
			extra += f" Convert to vorbis"
			ff_aud.extend(['libvorbis', '-q:a:'+ _cnt, '7'])

		if 'language' in this_aud['tags'] :
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
			if 'tags' in this_aud :
				extra += f"  {this_aud['tags']['handler_name']}"
		except Exception as x:
			if DeBug : print ('  =>', repr(x))
			pass

		mesaj = f"    |<A:{_strm:2}>|{this_aud['codec_name']:<8} |Br: {hm_sz(_au_btrt):>9}|{_lng}|Frq: {hm_sz(this_aud['sample_rate'],'Hz'):>8}|Ch: {chnls}| {extra}"
		print( mesaj )
		if (DeBug) : print (ff_aud)

		ff_audio += ff_aud
	if DeBug :
		print("Audio", ff_audio )

	end_t = time.perf_counter()
	print(f'   -End: {TM.datetime.now():%T}\tTotal: {round((end_t - str_t), 2)} sec')

	return ff_audio
##>>============-------------------<  End  >------------------==============<<##

# XXX: Subtitle
@performance_check
def pars_subtl ( strm_in , DeBug ) :
	''' Parse and extract data from subtitle streams '''
	mesaj = sys._getframe().f_code.co_name
	print(f"    +{mesaj}=: Start: {TM.datetime.now():%T}")
	str_t = time.perf_counter()

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
		ff_sub = []
		extra  = ''
		_strm = str(this_sub['index'])
		_cnt  = str(count)
		if DeBug :
			print(f"    +{mesaj}=: Start: {str_t:%T}\n {json.dumps(this_sub, indent=2)}" )
		try :
			if ('language') in this_sub['tags'] :
				_lng = this_sub['tags']['language']
			else :
				_lng = 'und'

			if 'NUMBER_OF_BYTES' in this_sub['tags'] :
				s_siz = this_sub['tags']['NUMBER_OF_BYTES']
			else :
				s_siz = 1

			if 'disposition' in this_sub:
				dsp_f = this_sub['disposition']['forced']
				dsp_d = this_sub['disposition']['default']

			ff_sub.extend( ['-map', '0:s:' +_cnt , '-c:s:' +_cnt])
# XXX: bin_data TBD
			if this_sub['codec_name'].lower() in ('hdmv_pgs_subtitle', 'dvd_subtitle', 'ass' ):
				extra += f" Rm: {this_sub['codec_name']} {_lng} < |"
				ff_sub = (['-map', '-0:s:' + _cnt, '-c:s:' + _cnt, 'mov_text'])

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
				if 'tags' in this_sub :
					extra += f"  {this_sub['tags']['handler_name']}"
			except Exception as x:
				if DeBug : print ('  =>', repr(x))
				pass

			mesaj = f"    |<S:{_strm:2}>|{this_sub['codec_name']:<9}|{this_sub['codec_type']:<13}|{_lng:3}|Siz: {hm_sz(s_siz):>8}|Dispo: default={dsp_d}, forced={dsp_f}|{extra}"
			print(mesaj)
			if (DeBug) : print (ff_sub)

		except Exception as e:
			mesaj += f" Exception"
			Trace (mesaj, e)
		if DeBug :
			print(f"\n {json.dumps(this_sub, indent=2)}\n{ff_sub}\n" )
		ff_subtl += ff_sub
	if DeBug : print ("  ", ff_subtl )

	end_t = time.perf_counter()
	print(f'   -End: {TM.datetime.now():%T}\tTotal: {round((end_t - str_t), 2)} sec')

	return( ff_subtl )
##>>============-------------------<  End  >------------------==============<<##

# XXX: Extra Data
def pars_extdta( strm_in, DeBug ) :
	''' Parse and extract data from data streams '''

	mesaj = sys._getframe().f_code.co_name
	print(f"  +{mesaj}=: Start: {TM.datetime.now():%T}")
	str_t = time.perf_counter()

	if DeBug :	print(f"    +{mesaj}=: Start: {str_t:%T}\n {json.dumps( strm_in, indent=2)}" )

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
		if DeBug : print(f"    +{mesaj}=: Start: {str_t:%T}\n {json.dumps(this_dat, indent=2)}" )
		ff_dd = []
		try :
			if 'tags' in this_dat :
				extra = f"  {this_dat['tags']['handler_name']}"
			mesaj = f"    |<D:{this_dat['index']:2}>| {this_dat['codec_name']:<8}| {this_dat['codec_long_name']:<9}| {this_dat['codec_type']:^11} |  {extra}"
			print(mesaj)

		except Exception as e:
			mesaj += f" Exception {e}"
			print(f"    +{mesaj}=!!\n{json.dumps(this_dat, indent=2)}" )
			Trace (mesaj, e)

		ff_data += ff_dd

	end_t = time.perf_counter()
	print(f'   -End: {TM.datetime.now():%T}\tTotal: {round((end_t - str_t), 2)} sec')

	return ff_data

##>>============-------------------<  End  >------------------==============<<##
@performance_check
def	find_subtl ( in_file, DeBug ) :
	''' find subtitle files and select the best '''
	str_t = TM.datetime.now()
	mesaj= sys._getframe().f_code.co_name + ':'
	if DeBug :
		print(f"    +{mesaj}=: Start: {str_t:%T}Files:{in_file}" )

	'''
	Let's setup the local directory to see what's in it
	os.path.sep (in_file)
	files = [file for file in files if '.png' in file or '.jpg' in file]
	'''
	top_dir, Filo = in_file.rsplit(os.path.sep, 1)
	Finam, ext = os.path.splitext(Filo)
	file_lst = os.listdir(top_dir)

# XXX: TO do it properly
#	srt_files = [file for file in file_lst if '.srt' in file ]
#	if srt_files :
#		print(f"    |<S: Extternal file|:{srt_files}")
#	else :
#		print(f"    |<S: NO Extternal file|:{srt_files}")

	file_lst.sort(key=lambda f: os.path.isfile(os.path.join(top_dir, f)))
#	if DeBug :
#		print(f"Dir:{top_dir}\nFile:{Filo}\nFn:{Finam}\t E:{ext}\n")
#		print("\nFile:".join(file_lst))
#		input ("WTF")
	ff_subtl = []
	for count, fname in enumerate(file_lst) :
		_cnt =str(count)
		fnm, fex =  os.path.splitext(fname)
		if  fex == '.srt' :
			lng = 'und'
			symb, trnsf = Trnsformr_ ( Finam, fnm, DeBug )
			# XXX: simple way to see similarity 4 chars = .srt
			if len(trnsf)/3 <= 4 : # dived by 3 cause it sas 3 components location cr1 cr2 to transform from one to the other
				#print( len( trnsf), symb )
				print(f"    |<S: Ext file|:{fname}")
				if DeBug :
					print(f"1: {Finam}\nD: {symb}\n2: {fnm}\nLng={lng}\nLen:{len(trnsf)} ")
	#		ff_sub = (['-i', fname, '-c:s', 'copy', '-metadata:s', 'language=' + 'eng'])
	#		ff_sub = (['-i', fname, '-c:s', 'copy', '-metadata:s:s' + _cnt, 'language=' + 'eng'])
			ff_sub = (['-i', fname, '-c:s', 'mov_text'])
		else :
			ff_sub = []
#			if DeBug : print (f'{count} {fnm}{fex} Not a Sub File')

		ff_subtl += ff_sub

	if '-sn' not in ff_subtl and not len(ff_subtl) :
		ff_subtl = ['-sn']

	if DeBug : print ("  ", ff_subtl ) , input("Next")

	# XXX:
	#ff_subtl = ['-sn']
	return( ff_subtl )

def Trnsformr_(str1, str2, DeBug, no_match_c='|', match_c='='):
	str_t = TM.datetime.now()
	message = sys._getframe().f_code.co_name + ':'
	if DeBug : print(f"     +{message}=: Start: {str_t:%T}")

	#	print (f"\n1: {string1}\n2: {string2}\n ??")
	# XXX: # TODO: location of differences , chunking

	# XXX: Extend with space cause zip stops at the shortest
	delt = len(str2) - len(str1)
	if delt < 0:
		if DeBug : print ("lst1 longer")
		str2 += ' '*abs(delt)
	elif delt > 0:
		if DeBug : print ("lst2 longer")
		str1 += ' '*delt

	symb = ""
	trns = []
	for loc, (c1, c2) in enumerate(zip(str1, str2)) :
		if c1 == c2:
			symb += match_c
		else:
			symb += no_match_c
			trns.extend( [loc, c1, c2] )

	if DeBug : print (f"1: {str1}\nD: {symb}\n2: {str2}\n{trns} ")

	return symb, trns
#>=-------------------------------------------------------------------------=<#

@performance_check
def zabrain_run(in_file, mta_dta, DeBug ):
	''' Decide what based on streams info '''

	mesaj = sys._getframe().f_code.co_name
	print(f"  +{mesaj}=: Start: {TM.datetime.now():%T}")
	str_t = time.perf_counter()

	if not mta_dta:
		mesaj+= ' No Metadata Nothing to do here'
		raise Exception(mesaj)

	ff_video, ff_audio, ff_subtl, ff_data, skip_this = pars_format( mta_dta, in_file, DeBug )

	ffmpeg_cmnd = ff_video+ ff_audio + ff_subtl +ff_data

# XXX: Do nothing if all is well needs rework to look in every line of comands
# Check if filenam and enbeded tag are the same.
	_, extens = os.path.splitext(in_file)
	if (extens.lower() == TmpF_Ex.lower()) and ('copy' in ff_video) and ('copy' or '-an' in ff_audio) : #and ('mov_text' or '-sn' in ff_subtl) and ('-dn' in ff_exdta) :
		pass
#		raise ValueError(Skip_key)
	if  skip_this  == True :
		raise ValueError(Skip_key)

	if DeBug :print (mesaj, ffmpeg_cmnd)

	end_t = time.perf_counter()
	print(f"   -End: {TM.datetime.now():%T}\tTotal: {round((end_t - str_t), 2)} sec")

	return ffmpeg_cmnd
##>>============-------------------<  End  >------------------==============<<##

def show_progrs(line_to, sy=False):
	mesaj= sys._getframe().f_code.co_name + '-:'

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
				a_sec = sum(int(x) * 60**i for i,
							x in enumerate(reversed(tm.split(":"))))
				dif = abs(glb_vidolen - a_sec)
				eta = round(dif / (float(sp)))
				mints, secs  = divmod(int(eta), 60)
				hours, mints = divmod(mints, 60)
				_eta = f'{hours:02d}:{mints:02d}:{secs:02d}'
				_P = f'\r    | {sy} |Size: {hm_sz(sz):>5}|Frames: {int(fr):>5}|Fps: {fp:>3}|BitRate: {br:>6}|Speed: {sp:>5}|ETA: {_eta:>8}|    '
				if DeBug :
					print (f'\n {line_to} | {sy} |Size: {hm_sz(sz):>5}|Frames: {int(fr):>5}|Fps: {fp:>3}|BitRate: {br:>6}|Speed: {sp:>5}|ETA: {_eta:>8}|' )

		except Exception as e:
			mesaj+= f"Line: {line_to} ErRor: {e}\n{repr(e)}:"
			print (mesaj)
#			Trace (mesaj, e)

	sys.stderr.write(_P)
	sys.stderr.flush
	return True
##>>============-------------------<  End  >------------------==============<<##
@performance_check
def make_matrx(input_file, execu=ffmpeg, ext = '.png'):
	''' Create a 3x3 matrix colage '''
	str_t = TM.datetime.now()
	mesaj= sys._getframe().f_code.co_name
	print(f"  +{mesaj}=: Start: {str_t:%T}")
	global glb_totfrms , vid_width
# XXX Create Matrix Colage:
	file_name, _ = os.path.splitext(input_file)
#	print(f' file  {input_file}')
	'''
	hack to eliminate doublee
	'''
	_name, _ = os.path.splitext(file_name)
	if _ in File_extn:
		file_name, _ = os.path.splitext(file_name)
	if os.path.isfile(file_name + ext):
		mesaj= f"    |PNG Exists ¯\_(%)_/¯ Skip"
		print(mesaj)
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
			mesaj= f"\r    |3x3| Matrix Created {ext}"
			print(mesaj)
			end_t = TM.datetime.now()
			convert = time.strftime("%H:%M:%S", time.gmtime((end_t-str_t).total_seconds()))
			mesaj=f'  -End: {end_t:%T}\tTotal: {convert} sec'
			return os.path.getsize(file_name)
		else:
			mesaj= f"   = Failed to Created .PNG >"
			raise Exception(mesaj)
##>>============-------------------<  End  >------------------==============<<##

def short_ver ( fname, *other ) :
	str_t = TM.datetime.now()
	mesaj= sys._getframe().f_code.co_name
	print(f"  +{mesaj}=: Start: {str_t:%T}")

# https://trac.ffmpeg.org/wiki/Map#Chooseallstreams
	ff_head = [execu, '-ss', "00:00:00", '-t', '00:03:30', '-i', fname, '-map', '0', '-c', 'copy' -y ]
	return todo
##>>============-------------------<  End  >------------------==============<<##

def speed_up ( fname, *other) :
# https://trac.ffmpeg.org/wiki/How%20to%20speed%20up%20/%20slow%20down%20a%20video
#	ffmpeg -i input.mkv -filter:v "setpts=0.5*PTS" output.mkv #Frame Rate Chage
	print(f"  +{mesaj}=: Start: {str_t:%T}")
	todo = [ '-filter_complex "[0:v]setpts=0.25*PTS[v];[0:a]atempo=2.0,atempo=2.0[a]" -map "[v]" -map "[a]" ']
	return todo
##>>============-------------------<  End  >------------------==============<<##

def video_diff(file1, file2) :
	# XXX:  Visualy Compare in and out files
	# https://stackoverflow.com/questions/25774996/how-to-compare-show-the-difference-between-2-videos-in-ffmpeg

	str_t = TM.datetime.now()
	mesaj= sys._getframe().f_code.co_name
	print(f"  +{mesaj}=: Start: {str_t:%T}")

	cmprd_file = get_new_fname(file1, "_Video_comp.mp4", TmpF_Ex)

	todo = (ffmpeg, '-i', file1, '-i', file2, '-filter_complex', "blend=all_mode=difference", '-c:v', 'libx265', '-preset', 'faster', '-c:a', 'copy', '-y', cmprd_file)

	if run_ffm(todo):
		return cmprd_file
	else:
		mesaj+= f"   = Failed to Compare Files >\n"
		raise Exception(mesaj)
##>>============-------------------<  End  >------------------==============<<##
