from My_Utils import *
from Yaml import *

import os
import re
import sys
import json
import datetime
import subprocess

ffmpg_bin = 'C:\\Program Files\\ffmpeg\\bin'

ffmpeg = os.path.join(ffmpg_bin, "ffmpeg.exe")
ffprob = os.path.join(ffmpg_bin, "ffprobe.exe")
# ffplay = os.path.join(ffmpg_bin, "ffplay.exe")

if not ( os.path.exists(ffmpeg) and os.path.exists(ffprob) ):
	input(f"{ffmpeg}\nPath Does not Exist:")
	raise OSError
else :
	subprocess.run( [ffmpeg, '-version'] )

##==============-------------------   End   -------------------==============##

# XXX:  Returns a Json list
def run_ffprob(in_file, execu=ffprob, DeBug=False ):
	str_t = datetime.datetime.now()
	messa = sys._getframe().f_code.co_name
	print(f"  +{messa}=: Start: {str_t:%T}")

	ff_args = (execu, '-hide_banner',
					  '-analyzeduration', '100000000',
					  '-probesize',		  '50000000',
					  '-v', 'warning',		# XXX quiet, panic, fatal, error, warning, info, verbose, DeBug, trace
					  '-of', 'json',		# XXX default, csv, xml, flat, ini
					  '-show_format',
					  '-show_streams',
					  '-show_error',
#					  '-show_data',
#					  '-show_private_data',
					  '-i', in_file )

	try:
		runit = subprocess.Popen( ff_args,	stderr=subprocess.PIPE,
											stdout=subprocess.PIPE )
		out, err = runit.communicate()
		jlist = json.loads(out.decode(console_encoding, errors='ignore'))

		if err :
			messa += f" Error: { repr(err) }\n"
		if len(jlist) < 2 :
			messa += f"F:{in_file}\nJson:\n{json.dumps(jlist, indent=2 )}"
			print(messa)
			time.sleep (3)
			raise Exception(' Ffprobe has NO data')
		if DeBug:
			messa += f"F:{in_file}\nJson:\n{json.dumps(jlist, indent=2 )}"
			print ( messa)

	except Exception as e:
		runit.kill()
		messa += f"\nException: {e}\n"
		raise Exception(messa)

	end_t = datetime.datetime.now()
	messa =f'  -End: {end_t:%T}\tTotal: {round((end_t-str_t).total_seconds(),2)} sec'
	print ( messa )

	return jlist

##>>============-------------------<  End  >------------------==============<<##


# XXX:  Returns encoded filename file_name
def ffmpeg_run(input_file, ff_com, execu=ffmpeg, DeBug=False):
	str_t = datetime.datetime.now()
	messa = sys._getframe().f_code.co_name
	print(f"  +{messa}=: Start: {str_t:%T}")
#	DeBug=True
	out_file = '_' + stmpd_rad_str(9) + TmpF_Ex

	file_name, _ = os.path.splitext( os.path.basename(input_file) )

	ff_head = [execu, "-i", input_file, "-hide_banner" ]

	if DeBug :
		print (f"Make a short Version in Debug Mode" )
		ff_head = [execu, '-ss', "00:00:00", '-t', '00:03:30', '-i', input_file ]

	ff_tail = [ "-metadata", "title=" + file_name + " x256 ",
				"-metadata", "copyright="	+"2023 Me",
				"-metadata", "comment=" +"An exercise in metadata creation",
				"-metadata", "author=" +"Encoded by the one and only GeoHab",
				"-metadata", "encoder=" +"FFmpeg 5.12",
				"-movflags", "+faststart",
#				"-fflags", "+genpts,+igndts",
				"-fflags", "+fastseek",
				"-y", out_file,
				"-f", "matroska"
				]

	todo = ff_head + ff_com + ff_tail
	if DeBug :
		print ( todo )
# XXX:
	if run_ffm( todo ):
		if not os.path.exists(out_file) or os.path.getsize(out_file) < 1000 :
			messa += f'\nCom:{todo}\nNo Output Error\n Exec in Debug'
			print(messa)
			run_ffm(todo, messa)
			time.sleep(3)
			raise Exception('$hit: ', messa)
		else:
			end_t = datetime.datetime.now()
			messa =f'  -End: {end_t:%T}\tTotal: {round((end_t-str_t).total_seconds(),2)} sec'
			print(messa)
			return out_file
	else:
		messa += f" -> FFMpeg failed :("
		raise Exception(messa)

	return out_file
##>>============-------------------<  End  >------------------==============<<##


def run_ffm(args, *DeBug):
#	DeBug=True
	messa = sys._getframe().f_code.co_name + ':'

	if DeBug :
		print(f'{messa} Debug Mode\n{args[4:]}\n')
		runit    = subprocess.Popen(args)
		out, err = runit.communicate()
		messa += f' Done\tErr: {err}\n'
		print(messa)
		time.sleep(5)
	else:
		try:
			runit = subprocess.Popen(args,
									encoding=console_encoding,
									stderr=subprocess.STDOUT,
									stdout=subprocess.PIPE,
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
			messa += f" Exception => So lets try DEBUG mode"
			print(messa)
			try :
				runit.kill()
				runit = subprocess.Popen(args)
				out, err = runit.communicate()
				if out or err :
					print (f'\nOut: {out}\nErr{err}\n' )
				print('\nDone Looking Good :D')
				return True
			except :
				print (':( Looking Bad :D')
				return False

	messa += f"\n is Done"
	if DeBug : print (messa)

	return True
##==============-------------------   End   -------------------==============##

def pars_format(mta_dta, DeBug):
	str_t = datetime.datetime.now()
	messa = sys._getframe().f_code.co_name + '/:'
	if DeBug :		print(f"  +{messa}=: Start: {str_t:%T}")

	global glb_vidolen  # make it global so we can reuse for fmpeg check ...
	global glb_bitrate  # make it global so we can reuse for fmpeg check ...
	global glb_totfrms

	# XXX: Meta Data
	all_formt = mta_dta.get('format')

	if len(all_formt) < 1 :
		messa += f"\n{ json.dumps(mta_dta, indent=2) }\n No Format\n"
		raise ValueError(messa)
	else :
		if DeBug :
			print( f"\nFormat:\n{ json.dumps(all_formt, indent=2) }\n")

	_format = {
		'filename'			: str,
		'nb_streams' 		: int,
		"nb_programs"		: int,
		'nb_streams' 		: int,
		"format_name"		: str,
  		"format_long_name"	: str,
  		"start_time"		: float,
		'duration' 	 		: float,
		'bit_rate' 	 		: int,
		'size'     	 		: float,
		'tags'		 		: {}
		}

	prs_frm_to(all_formt, _format, DeBug )

	if DeBug :
		print( f"\nParsed:\n{ json.dumps(_format, indent=2) }\n")

	time.sleep (4)

	glb_vidolen = int(float(_format['duration']))
	glb_bitrate = int(      _format['bit_rate'])

	minut,  secs = divmod(glb_vidolen, 60)
	Leng =		f"{minut:02d}m:{secs:2d}s"
	hours, minut = divmod(minut, 60)
	if hours :
		Leng =	f"{hours:02d}h:{minut:02d}m:{secs:2d}s"
		days,  hours = divmod(hours, 24)

	# XXX: Stream Data
	all_strms = mta_dta.get('streams')
	if len (all_strms) :
		all_video = []
		all_audio = []
		all_subtl = []
		all_datas = []
		for count, strm_x in enumerate(all_strms):
			if   strm_x['codec_type'] == 'video' :
				all_video.append(strm_x)
			elif strm_x['codec_type'] == 'audio' :
				all_audio.append(strm_x)
			elif strm_x['codec_type'] == 'subtitle' :
				all_subtl.append(strm_x)
			elif strm_x['codec_type'] == 'data' :
				if 'handler_name' in strm_x['tags'] and strm_x['tags']['handler_name'] == "SubtitleHandler":
					print("\t SubtitleHandler in fact") # XXX: Is Subtitle In fact
					all_subtl.append(strm_x)
				else:
					all_datas.append(strm_x)
			else:
				messa += f"\n Unknown Codec Type: {strm_x['codec_type']}\n{json.dumps(strm_x, indent=2)}"
				print (messa)
				time.sleep(5)
			if DeBug :	print (f"\nStreem: {count}\n{json.dumps(strm_x, indent=2)}")
	else :
		messa += f"\n{ json.dumps(mta_dta, indent=2) }\n\n No Streams\n"
		raise ValueError(messa)# XXX: Check it :)

	# XXX: Print Banner
	messa = f"    |< FR >|Sz:{hm_sz( _format['size'])}|BRate: {hm_sz( glb_bitrate )} |L: {Leng} |Strms = {_format['nb_streams']}"
	if len(all_video) :
		messa += f'|V = {len(all_video)}|'
	if len(all_audio) :
		messa += f'|A = {len(all_audio)}|'
	if len(all_subtl) :
		messa += f'|S = {len(all_subtl)}|'
	if len(all_datas) :
		messa += f'|D = {len(all_datas)}|'
	print ( messa )
	return all_video, all_audio, all_subtl, all_datas

##>>============-------------------<  End  >------------------==============<<##

def pars_video ( strm_in , DeBug ) :
	str_t = datetime.datetime.now()
	messa = sys._getframe().f_code.co_name + ':'
	if DeBug :	print(f"    +{messa}=: Start: {str_t:%T}\n {json.dumps( strm_in, indent=2)}" )
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
# XXX: Presumes that the first Video is the real one !
	ff_video = []
	for count, this_vid in enumerate(strm_in) :
		prs_frm_to(this_vid, _vdata, DeBug)
		extra = ''
		ff_vid =[]
		_strm = str(_vdata['index'])
		_cnt  = str(count)

		if 'codec_name' not in _vdata:
			_vdata['codec_name'] = 'No Codec Name ?'
			messa = f"   No Video Codec |<V:{_vdata}\n"
			raise Exception(messa)

		vid_width = _vdata['width']
		vid_heigh = _vdata['height']
	# NOTE: Expected Bitrate Kind of ... should be bigger 2.5x for low res and smaler for high 1.5 # XXX:
		totl_pixl = vid_width * vid_heigh
		if vid_heigh   < 720 :
			expctd = totl_pixl * 2.8
		elif vid_heigh < 1080 :
			expctd = totl_pixl * 1.8
		else :
			expctd = totl_pixl * 1.3

		if 'r_frame_rate' in _vdata :
			frm_rate = divd_strn(_vdata['r_frame_rate'])
		if not frm_rate :
			print(json.dumps(_vdata, indent=2))
			frm_rate = 25

		if 'avg_frame_rate' in _vdata :
			av_frm_rate = divd_strn(_vdata['avg_frame_rate'])

		if av_frm_rate != frm_rate :
			print(f' Diff fmr rate: avg_frame_rate = {av_frm_rate} != r_frame_rate = {frm_rate}')

		glb_totfrms = round(frm_rate * glb_vidolen)

		if re.search (r"yuv[a-z]?[0-9]{3}", _vdata['pix_fmt']) :
			messa = f"{_vdata['pix_fmt']}"
			if re.search (r"10le", _vdata['pix_fmt']) :
				messa  += " 10 Bit"
				expctd *= 1.2 # 30/24 +15% extra

		mins,  secs = divmod(glb_vidolen, 60)
		hours, mins = divmod(mins, 60)

# XXX: Print Banner
		messa = f"    |< CT >|{vid_width:>4}x{vid_heigh:<4}|Tfm: {hm_sz(glb_totfrms,'F'):>8}|Tpx: {hm_sz(totl_pixl, 'P'):>8}|XBr: {hm_sz(round(expctd)):>7}| {messa}"
		print(messa)

		if 'bit_rate' in _vdata:
			if (_vdata['bit_rate']) == "Pu_la" :
#				print ( json.dumps(_vdata, indent=3 ) )
				_vi_btrt = glb_bitrate * 0.8	# estimate 80% is video
				extra = ' BitRate Estimate'
			else :
				_vi_btrt = int(_vdata['bit_rate'])
		else:
			_vi_btrt = glb_bitrate * 0.8	# estimate 80% is video
			extra = ' BitRate Estimate'

		ff_vid.extend( ['-map', '0:v:' + _cnt, '-c:v:' + _cnt])

		if vid_width > 2592 or vid_heigh > 1920 :
			output = "2K"
			if   vid_width >= 7600 or vid_heigh >= 4300:
				output = "8K"
			elif vid_width >= 3800 or vid_heigh >= 2100:
				output = "4K"
# XXX: Compute Scale
			div_w = round((vid_width / 1920))
			div_h = round((vid_heigh / 1080))
			if div_h < div_w :
				output +=f"  W != H, {div_w}, {div_h}"
				nw = round( vid_width/div_w )
				nh = round( vid_heigh/div_w )
			elif div_h > div_w :
				nw = round( vid_width/div_h )
				nh = round( vid_heigh/div_h )
			else :
				nw = round( vid_width/div_w )
				nh = round( vid_heigh/div_h )
			extra += f'\t{output} Scale {vid_width}x{vid_heigh} to {nw}x{nh}'

			ff_vid.extend(['libx265', '-crf', '25', '-preset', 'slow', '-vf', 'scale=' +str(nw) +':' +str(nh) ])

		elif _vi_btrt < (expctd) :
			if _vdata['codec_name'] == 'hevc':
				extra += '\tCopy'
				ff_vid.extend(['copy'])
			else:
				ff_vid.extend(['libx265', '-crf', '25', '-preset', 'slow'])
				if   vid_width >= 1280 or vid_heigh >= 720:
					output = "FHD"
				elif vid_width >= 720 or vid_heigh >= 500:
					output = "HD"
				else:
					output = "SD"
				extra += f'  {output} Convert to x265'
		else:
			if _vdata['codec_name'] == 'hevc':
				extra += '\tReduce < ' +hm_sz(expctd)
				ff_vid.extend(['libx265', '-crf', '25'])
			else:
				extra += '\tConvert to Hevc'
				ff_vid.extend(['libx265', '-crf', '25'])
			ff_vid.extend(['-preset', 'slow'])

		try :
			if 'tags' in _vdata :
				extra += f"  {_vdata['tags']['handler_name']}"
		except Exception as x:
			if DeBug : print ('  =>', repr(x))
			pass

		if _vdata['codec_name'].lower() in ['mjpeg', 'png'] :
			extra = f"| > Remove {_vdata['codec_name']}< |"
#			ff_vid = (['-map', '-0:v:' +str(_strm), '-c:v:' +str(_strm), 'copy'])

		messa = f"    |<V:{_strm:2}>|{_vdata['codec_name']:<8} |Br: {hm_sz(_vi_btrt):>9}|XBr: {hm_sz(expctd):>8}|Fps: {frm_rate:>7}|{extra}"
		print(messa)

		if (DeBug) :print (ff_vid)

	ff_video += ff_vid
	if DeBug :print ("  ", ff_video)

	return ff_video
##>>============-------------------<  End  >------------------==============<<##

# XXX: Audio
def pars_audio ( strm_in , DeBug ) :
	str_t = datetime.datetime.now()
	messa = sys._getframe().f_code.co_name + ':'
	if DeBug :	print(f"    +{messa}=: Start: {str_t:%T}\n {json.dumps( strm_in, indent=2)}" )

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
	is_deflt = 0
	for count, this_aud in enumerate(strm_in) :
		prs_frm_to(this_aud, _adata, DeBug )
		ff_aud = []
		extra  = ''
		_strm = str(_adata['index'])
		_cnt =  str(count)
		if DeBug :
			print ( json.dumps(_adata, indent=3 ) )
			print (f"    +{messa}=: Start: {str_t:%T}\n {json.dumps(_adata, indent=2)}" )

		try :
			if 'bit_rate' in _adata:
				if (_adata['bit_rate']) == "Pu_la" :
					_au_btrt = glb_bitrate * 0.2	# estimate 20% is video
					extra = 'BitRate Estimate '
				else :
					_au_btrt = int( _adata['bit_rate'])
			else :
				print (json.dumps(strm_in, indent=2))
				extra = "Audio Bitrate Extimate"
				_au_btrt = 3.1415926535

			if 'channels' in _adata:
				chnls = _adata['channels']
			else:
				chnls = -100
				extra += f" Unknown Channels "

			if chnls == 1:
				extra += "Mono"
			elif chnls == 2:
				extra += "Stereo"
			elif chnls > 2:
				extra += f"{chnls -1}.1 Channels"

			if 'disposition' in _adata:
				dsp_f = _adata['disposition']['forced']
				dsp_d = _adata['disposition']['default']

			ff_aud.extend( ['-map', '0:a:' + _cnt, '-c:a:' + _cnt])

			if _adata['codec_name'] in ('aac', 'vorbis', 'mp2', 'mp3'):
				if _au_btrt < Max_a_btr :
					extra += ' Copy'
					# XXX: DeBug # XXX:
					if DeBug: print(json.dumps(_adata, indent=2))
					ff_aud.extend(['copy'])
				else:
					extra += ' Reduce BitRate'
					ff_aud.extend(['libvorbis', '-q:a:'+ _cnt, '7'])
			else:
				extra += f" {_adata['codec_name']} Convert to vorbis"
				ff_aud.extend(['libvorbis', '-q:a:'+ _cnt, '7'])

			if 'language' in _adata['tags'] :
				_lng = _adata['tags']['language']
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
				extra = f"| > Remove {_lng} < |"
				ff_aud = (['-map', '-0:a:' + _cnt, '-c:a:' + _cnt, 'copy'])

			try :
				if 'tags' in _adata :
					extra += f"  {_adata['tags']['handler_name']}"
			except Exception as x:
				if DeBug : print ('  =>', repr(x))
				pass

			messa = f"    |<A:{_strm:2}>|{_adata['codec_name']:<8} |Br: {hm_sz(_au_btrt):>9}|{_lng}|Frq: {hm_sz(_adata['sample_rate'],'Hz'):>8}|Ch: {chnls}| {extra}"
			print( messa )
			if (DeBug) : print (ff_aud)

		except Exception as e:
			messa += f" Exception"
			Trace (messa, e)

		ff_audio += ff_aud
	if DeBug : print("  ", ff_audio )

	return ff_audio
##>>============-------------------<  End  >------------------==============<<##

# XXX: Subtitle
def pars_subtl ( strm_in , DeBug ) :
	str_t = datetime.datetime.now()
	messa = sys._getframe().f_code.co_name + ':'
	if DeBug :	print(f"    +{messa}=: Start: {str_t:%T}\n {json.dumps( strm_in, indent=2)}" )

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
		prs_frm_to(this_sub, _sdata, DeBug )
		ff_sub = []
		extra  = ''
		_strm = str(_sdata['index'])
		_cnt  = str(count)
		if DeBug :
			print(f"    +{messa}=: Start: {str_t:%T}\n {json.dumps(_sdata, indent=2)}" )
		try :
			if ('language') in _sdata['tags'] :
				_lng = _sdata['tags']['language']
			else :
				_lng = 'und'

			if 'NUMBER_OF_BYTES' in _sdata['tags'] :
				s_siz = _sdata['tags']['NUMBER_OF_BYTES']
			else :
				s_siz = 1

			if 'disposition' in _sdata:
				dsp_f = _sdata['disposition']['forced']
				dsp_d = _sdata['disposition']['default']

			ff_sub.extend( ['-map', '0:s:' +_cnt , '-c:s:' +_cnt])

			if _sdata['codec_name'].lower() in ('hdmv_pgs_subtitle', 'dvd_subtitle', 'ass', 'bin_data' ):
				extra += f" Skip {_sdata['codec_name']}"
				ff_sub = (['-map', '-0:s:' + _cnt, '-c:s:' + _cnt, 'mov_text'])

			elif _lng == Default_lng :
				extra += f" Move Text {_sdata['codec_name']} {_lng} Default"
				ff_sub.extend(['mov_text', '-metadata:s:s:'  + _cnt, 'language=' + _lng])
				ff_sub.extend(['-disposition:s:s:' + _cnt, 'default'])

			elif _lng in Keep_langua :
				extra += f" Move Text {_sdata['codec_name']} {_lng}"
				ff_sub.extend(['mov_text', '-metadata:s:s:'  + _cnt, 'language=' + _lng])
				ff_sub.extend(['-disposition:s:s:' + _cnt, 'none'])

			else :
				extra += f" Remove {_lng}"
				ff_sub = (['-map', '-0:s:' + _cnt, '-c:s:' + _cnt, 'mov_text'])

			try :
				if 'tags' in _sdata :
					extra += f"  {_sdata['tags']['handler_name']}"
			except Exception as x:
				if DeBug : print ('  =>', repr(x))
				pass

			messa = f"    |<S:{_strm:2}>|{_sdata['codec_name']:<9}|{_sdata['codec_type']:<13}|{_lng:3}|Siz: {hm_sz(s_siz):>8}|Dispo: default={dsp_d}, forced={dsp_f}|{extra}"
			print(messa)
			if (DeBug) : print (ff_sub)

		except Exception as e:
			messa += f" Exception"
			Trace (messa, e)
		if DeBug :
			print(f"\n {json.dumps(_sdata, indent=2)}\n{ff_sub}\n" )
		ff_subtl += ff_sub
	if DeBug : print ("  ", ff_subtl )

	return( ff_subtl )
##>>============-------------------<  End  >------------------==============<<##

# XXX: Extra Data
def pars_extdta( strm_in, DeBug ) :
	str_t = datetime.datetime.now()
	messa = sys._getframe().f_code.co_name + ':'
	if DeBug :	print(f"    +{messa}=: Start: {str_t:%T}\n {json.dumps( strm_in, indent=2)}" )

	_ddata = {
		'index'		: int,
		'codec'		: str,
		'codec_name': str,
		'codec_type': str,
		'language'	: str,
		'tags'		:{}
	}
	# XXX: Data
	ff_dext =['-dn']
	for count, this_dat in enumerate(strm_in) :
		prs_frm_to(this_dat, _ddata, DeBug )
		if DeBug : print(f"    +{messa}=: Start: {str_t:%T}\n {json.dumps(_ddata, indent=2)}" )

		try :
			if 'tags' in _ddata :
				extra = f"  {_ddata['tags']['handler_name']}"

			messa = f"    |<D:{_ddata['index']:2}>|{_ddata['codec']:<9}|{_ddata['codec_name']:<8}|{_ddata['codec_type']:^11} |  {extra}"
			print(messa)

		except Exception as e:
			messa += f" Exception"
			Trace (messa, e)

	return ff_dext
##>>============-------------------<  End  >------------------==============<<##
def	find_subtl ( in_file, DeBug ) :
	str_t = datetime.datetime.now()
	messa = sys._getframe().f_code.co_name + ':'
	if DeBug :
		print(f"    +{messa}=: Start: {str_t:%T}\nDir:{top_dir}\nFiles:{file_lst}" )

	'''
	Let's setup the local directory to see what's in it
	os.path.sep (in_file)

	'''
	top_dir, Filo = in_file.rsplit(os.path.sep, 1)
	Finam, ext = os.path.splitext(Filo)
	file_lst = os.listdir(top_dir)
	file_lst.sort(key=lambda f: os.path.isfile(os.path.join(top_dir, f)))
	if DeBug :
		print(f"Dir:{top_dir}\nFile:{Filo}\nFn:{Finam}\t E:{ext}\n")
		print("\nFile:".join(file_lst))
#		input ("WTF")
	ff_subtl = []
	for count, fname in enumerate(file_lst) :
		_cnt =str(count)
		if  os.path.splitext(fname)[1] == '.srt' :
			print(f"    |<S: Extternal file|:{fname}")
			sub_file_pt = os.path.join(top_dir, fname)
			ff_sub = (['-i', fname, '-c:s', 'srt', '-metadata:s:s' + _cnt, 'language=' + 'eng'])
		else :
			ff_sub = []
			if DeBug : print (f'{count} No Sub File')

		ff_subtl += ff_sub

	if '-sn' not in ff_subtl and not len(ff_subtl) :
		ff_subtl = ['-sn']

	if DeBug : print ("  ", ff_subtl )
	return( ff_subtl )

def thza_brain(in_file, mta_dta, DeBug ):
	str_t = datetime.datetime.now()
	messa = sys._getframe().f_code.co_name

	if DeBug : print(f" +{messa}=: Start: {str_t:%T}")

	if not mta_dta:
		messa += ' No Metadata Nothing to do here'
		raise Exception(messa)

	try:
		vdo_strm, aud_strm, sub_strm, ext_data = pars_format( mta_dta, DeBug )
		# XXX: Video
		show_cmd = False

		if len ( vdo_strm) :
			ff_video = pars_video( vdo_strm, DeBug )
		else :
			ff_video = ['-vn']
			print("    |<V:no>|")
		if show_cmd :print (f"      {ff_video}")

		# XXX: Audio
		if len ( aud_strm ):
			ff_audio = pars_audio( aud_strm, DeBug )
		else :
			ff_audio = ['-an']
			print("    |<A:no>|")
		if show_cmd :print (f"      {ff_audio}")

		# XXX Subtitle
		if len ( sub_strm ):
			ff_subtl = pars_subtl( sub_strm, DeBug )
		else :
			print("    |<S:no>|")
			ff_subtl = ['-sn']
			ff_subtl = find_subtl( in_file, DeBug )
		if show_cmd :print (f"      {ff_subtl}")

		# XXX Data
		if len ( ext_data ):
			ff_exdta = pars_extdta( ext_data, DeBug  )
		else :
			print("    |<D:no>|")
			ff_exdta = ['-map_chapters', '-1']
		if show_cmd :print (f"      {ff_exdta}")

	except Exception as e:
		messa += f" FFZa_Brain: Exception"
		Trace (messa, e)
		raise Exception(messa)

	ffmpeg_cmnd = ff_video +ff_audio +ff_subtl +ff_exdta

# XXX: Do nothing if all is well needs rework to look in every line of comands
# Check if filenam and enbeded tag are the same.
	_, extens = os.path.splitext(in_file)
	if (extens.lower() == TmpF_Ex.lower()) and ('copy' in ff_video) and ('copy' or '-an' in ff_audio) : #and ('mov_text' or '-sn' in ff_subtl) and ('-dn' in ff_exdta) :
		pass
		raise ValueError(f"  | <¯\\_(%)_/¯>  Skip |")
	end_t = datetime.datetime.now()
	messa =f'  -End: {end_t:%T}\tTotal: {round((end_t-str_t).total_seconds(),2)} sec'
	if DeBug :print (messa)

#	raise ValueError(f"  | <¯\\_(%)_/¯>  Skip |")
	if (DeBug) :
		print (ffmpeg_cmnd)

	return ffmpeg_cmnd
##>>============-------------------<  End  >------------------==============<<##

def show_progrs(line_to, sy=False):
	messa = sys._getframe().f_code.co_name + '-:'

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
			messa += f"Line: {line_to} ErRor: {e}\n{repr(e)}:"
			print (messa)
#			Trace (messa, e)

	sys.stderr.write(_P)
	sys.stderr.flush
	return True
##>>============-------------------<  End  >------------------==============<<##

def make_matrx(input_file, execu=ffmpeg, ext = '.png'):
	str_t = datetime.datetime.now()
	messa = sys._getframe().f_code.co_name
	print(f"  +{messa}=: Start: {str_t:%T}")

# XXX Create Matrix Colage:
	file_name, _ = os.path.splitext(input_file)
#	print(f' file  {input_file}')
	'''
	hack to eliminate doublee
	'''
	_name, _ = os.path.splitext(file_name)
	if _ in File_extn:
		file_name, _ = os.path.splitext(file_name)
#	print(f' looking for {file_name}')
#	_, _, filenames = next(os.walk(input_file))
#	print (filenames)
#	input ('next')
	if os.path.isfile(file_name + '.jpg') or os.path.isfile(file_name + ext):
		messa = f"    |PNG Exists ¯\_(%)_/¯ Skip"
		print(messa)
		return False

	else:
		file_name += ext

		skip0 = '00:00:51'
		width = str(vid_width)
		# We have 9 tiles plus a bit more
		slice = str(round(glb_totfrms / 9))
		zzzle = "[0:v]select=not(mod(n\," + slice + ")), scale=" + width + ":-1:, tile=3x3:nb_frames=9:padding=3:margin=3"

		if glb_totfrms > 6600:
			todo = (execu, '-ss', skip0, '-vsync', 'vfr', '-i',
					input_file, '-frames', '1', '-vf', zzzle, '-y', file_name)
		else:
			todo = (execu, '-vsync', 'vfr', '-i',
					input_file, '-frames', '1', '-vf', zzzle, '-y', file_name)
		# XXX:
		if run_ffm(todo):
			messa = f"\r    |3x3| Matrix Created {ext}"
			print(messa)
			end_t = datetime.datetime.now()
			messa = f" -make_matrx Done !!: {end_t:%T}\tTotal: {round((end_t-str_t).total_seconds(),2)} sec"
			return os.path.getsize(file_name)
		else:
			messa = f"   = Failed to Created .PNG >"
			raise Exception(messa)
##>>============-------------------<  End  >------------------==============<<##

def short_ver ( fname, *other ) :
	str_t = datetime.datetime.now()
	messa = sys._getframe().f_code.co_name
	print(f"  +{messa}=: Start: {str_t:%T}")

# https://trac.ffmpeg.org/wiki/Map#Chooseallstreams
	ff_head = [execu, '-ss', "00:00:00", '-t', '00:03:30', '-i', fname, '-map', '0', '-c', 'copy' -y ]
	return todo
##>>============-------------------<  End  >------------------==============<<##

def speed_up ( fname, *other) :
# https://trac.ffmpeg.org/wiki/How%20to%20speed%20up%20/%20slow%20down%20a%20video
#	ffmpeg -i input.mkv -filter:v "setpts=0.5*PTS" output.mkv #Frame Rate Chage
	print(f"  +{messa}=: Start: {str_t:%T}")
	todo = [ '-filter_complex "[0:v]setpts=0.25*PTS[v];[0:a]atempo=2.0,atempo=2.0[a]" -map "[v]" -map "[a]" ']
	return todo
##>>============-------------------<  End  >------------------==============<<##

def video_diff(file1, file2) :
	# XXX:  Visualy Compare in and out files
	# https://stackoverflow.com/questions/25774996/how-to-compare-show-the-difference-between-2-videos-in-ffmpeg

	str_t = datetime.datetime.now()
	messa = sys._getframe().f_code.co_name
	print(f"  +{messa}=: Start: {str_t:%T}")

	cmprd_file = get_new_fname(file1, "_Video_comp.mp4", TmpF_Ex)

	todo = (ffmpeg, '-i', file1, '-i', file2, '-filter_complex', "blend=all_mode=difference", '-c:v', 'libx265', '-preset', 'faster', '-c:a', 'copy', '-y', cmprd_file)

	if run_ffm(todo):
		return cmprd_file
	else:
		messa += f"   = Failed to Compare Files >\n"
		raise Exception(messa)
##>>============-------------------<  End  >------------------==============<<##
