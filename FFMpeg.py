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
	message += f"{ffmpeg}\nPath Does not Exist:"
	input(message)
	raise OSError

##==============-------------------   End   -------------------==============##

def run_ffprob(in_file, execu=ffprob, DeBug=False ):
# Returns Json list
	str_t = datetime.datetime.now()
	message = sys._getframe().f_code.co_name
	print(f"  +{message}=: Start: {str_t:%T}")

	ff_args = (execu, '-hide_banner',
					  '-analyzeduration', '100000000',
					  '-probesize',		  '50000000',
					  '-v', 'warning',		# XXX quiet, panic, fatal, error, warning, info, verbose, debug, trace
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
			message += f" Error: { repr(err) }\n"
		if len(jlist) < 2 :
			message += f"F:{in_file}\nJson:\n{json.dumps(jlist, indent=2 )}"
			print(message)
			time.sleep (3)
			raise Exception(' Ffprobe has NO data')

	except Exception as e:
		runit.kill()
		message += f"\nException: {e}\n"
		raise Exception(message)

	end_t = datetime.datetime.now()
	message =f'  -End: {end_t:%T}\tTotal: {round((end_t-str_t).total_seconds(),2)} sec'
	print ( message )

	return jlist

##>>============-------------------<  End  >------------------==============<<##


def ffmpeg_run(input_file, ff_com, execu=ffmpeg):
# Returns encoded filename file_name
	str_t = datetime.datetime.now()
	message = sys._getframe().f_code.co_name
	print(f"  +{message}=: Start: {str_t:%T}")

	out_file = '_' + stmpd_rad_str(7) + TmpF_Ex

	file_name, _ = os.path.splitext( os.path.basename(input_file) )

	ff_head = [execu, "-i", input_file, "-hide_banner" ]

	banner = "title= " + file_name + " x256 "

	ff_tail =	[ "-metadata", banner,
				"-movflags", "+faststart",
				"-fflags", "+fastseek",
#				"-fflags", "+genpts,+igndts",
				"-y", out_file,
#				"-f", "matroska"
				]

	todo = ff_head + ff_com + ff_tail
# XXX:
	if run_ffm( todo ):
		if not os.path.exists(out_file) or os.path.getsize(out_file) < 1000 :
			message += f'\n No Output Error\n Execute in Debug Mode for more details'
			print(message)
	#		run_ffm(todo, message)
			time.sleep(3)
			raise Exception('$hit ', message)
		else:
			end_t = datetime.datetime.now()
			message =f'  -End: {end_t:%T}\tTotal: {round((end_t-str_t).total_seconds(),2)} sec'
			print(message)
			return out_file
	else:
		message += f" -> FFMpeg failed :("
		raise Exception(message)

	return out_file
##>>============-------------------<  End  >------------------==============<<##


def run_ffm(args, *DeBug):
#	DeBug = True
	message = sys._getframe().f_code.co_name + '|:'
	if DeBug :
		print(f'{message}\nDebug Mode\n{args[4:]}\n')
		runit = subprocess.Popen(args)
		out, err = runit.communicate()
		message += f'\nDone\tOut: {out}\nErr: {err}\n'
		print(message)
		time.sleep(5)
	else:
		try:
			runit = subprocess.Popen(args,	stderr=subprocess.STDOUT,
											stdout=subprocess.PIPE,
											universal_newlines=True,
											encoding=console_encoding)
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
			message += f" Exception => So lets try DEBUG mode"
			print(message)
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

	message += f"\n is Done"
	if DeBug : print (message)

	return True
##==============-------------------   End   -------------------==============##

# XXX: New Code
def pars_mtdta ( js_info , DeBug=False ) :
	str_t = datetime.datetime.now()
	message = sys._getframe().f_code.co_name + 'c:'
	if DeBug :	print(f"   +{message}=: Start: {str_t:%T}\n {json.dumps(js_info, indent=2)}" )

	global glb_vidolen  # make it global so we can reuse for fmpeg check ...
	global glb_bitrate  # make it global so we can reuse for fmpeg check ...
	global glb_totfrms

	_mtdta = dict(	filename='',
					nb_streams=int(0),
					duration=float(0),
					bit_rate=int(0),
					size=float(0)
					)
	prs_frm_to(js_info, _mtdta, DeBug )

	glb_vidolen = int(_mtdta['duration'])
	glb_bitrate = int(_mtdta['bit_rate'])

	minut,  secs = divmod(glb_vidolen, 60)
	Leng =		f"{minut:02d}m:{secs:2d}s"
	hours, minut = divmod(minut, 60)
	if hours :
		Leng =	f"{hours:02d}h:{minut:02d}m:{secs:2d}s"
		days,  hours = divmod(hours, 24)
		if days  :
			Leng =	f"{days:02}d:{hours:02d}h:{minut:02d}m:{secs:2d}s"
			weeks, days  = divmod(days, 7)
			if weeks :
				Leng =	f"{weeks:02}w:{days:02}d:{hours:02d}h:{minut:02d}m:{secs:2d}s"
				months, weeks = divmod(weeks, 4)
				if months :
					Leng =	f"{months:02d}m:{weeks:02}w:{days:02}d:{hours:02d}h:{minut:02d}m:{secs:2d}s"
					years , months = divmod(months, 12)
					if years :
						Leng =	f"{years:02d}y:{months:02d}m:{weeks:02}w:{days:02}d:{hours:02d}h:{minut:02d}m:{secs:2d}s"
	# XXX: Print Banner
	message = f"    |< MD >|Size: {hm_sz( _mtdta['size'])} |L: {Leng} |Strms: {_mtdta['nb_streams']} |BitRate: {hm_sz( glb_bitrate )} |"
	print(message)

##>>============-------------------<  End  >------------------==============<<##

def pars_video ( js_info , DeBug=False ) :
	str_t = datetime.datetime.now()
	message = sys._getframe().f_code.co_name + ':'
	if DeBug :	print(f"    +{message}=: Start: {str_t:%T}\n {json.dumps(js_info, indent=2)}" )
	_vdata = {
		'index':int,
		'codec_name':'',
		'width':int,
		'height':int,
		'coded_width':int,
		'coded_height':int,
		'bit_rate':int,
		'r_frame_rate': '',
		'avg_frame_rate': '',
		'pix_fmt': '',
		'disposition' :{},
		'tags' :{}
		}
# XXX: Presumes that the first Video is the real one !
	ff_video = []
	for count, this_vid in enumerate(js_info) :
		extra = ''

		prs_frm_to(this_vid, _vdata, DeBug)

		vid_width = _vdata['width']
		vid_heigh = _vdata['height']
		frm_rate = divd_strn(_vdata['r_frame_rate'])
		if not frm_rate :
			print(json.dumps(_vdata, indent=2))
			frm_rate = 25
		if 'avg_frame_rate' in _vdata :
			av_frm_rate = divd_strn(_vdata['avg_frame_rate'])
			if av_frm_rate != frm_rate :
				print(f' Diff fmr rate avg_frame_rate = {av_frm_rate} != r_frame_rate = {frm_rate}')

		glb_totfrms = round(frm_rate * glb_vidolen)

		# NOTE: Expected Bitrate Kind of ... should be bigger 2 for low res and smaler for high 1.2 # XXX:
		totl_pixl = vid_width * vid_heigh
		if vid_heigh  < 720 :
			expctd    = totl_pixl * 3
		elif vid_heigh < 1080 :
			expctd    = totl_pixl * 2.2
		else :
			expctd    = totl_pixl * 1.5

		if re.search (r"yuv[a-z]?[0-9]{3}", _vdata['pix_fmt']) :
			messa = f" {_vdata['pix_fmt']}"
			if re.search (r"10le", _vdata['pix_fmt']) :
				messa += " 10 Bit"
				expctd *= 1.25 # 30/24

		mins,  secs = divmod(glb_vidolen, 60)
		hours, mins = divmod(mins, 60)

# XXX: Print Banner
		message = f"    |< CT >|{vid_width:>4}x{vid_heigh:<4}|Tfm: {hm_sz(glb_totfrms,'F'):7}|Tpx: {hm_sz(totl_pixl, 'P')}|Xbr: {hm_sz(round(expctd),'x')}|V: {len(js_info)}| {messa}"
		print(message)

		if 'codec_name' not in _vdata:
			_vdata['codec_name'] = 'No Codec Name ?'
			message = f"   No Video Codec |<V:{_vdata}\n"
			raise Exception(message)
		elif _vdata['codec_name'].lower() in ['mjpeg', 'png'] :
			message = f"    |<V:{_vdata['index']:2}>|{_vdata['_vdata']:8} | > Skip this "
			print(message)
			continue

		if 'bit_rate' in _vdata:
			if (_vdata['bit_rate']) == "Pu_la" :
#				print ( json.dumps(_vdata, indent=3 ) )
				_vi_btrt = glb_bitrate * 0.8	# estimate 80% is video
				extra = ' BitRate Estimate'
			else :
				_vi_btrt = int(_vdata['bit_rate'])
		else:
			_vi_btrt = int(_vdata['bit_rate'])

		ff_vid = ['-map', '0:' + str(_vdata['index']), '-c:v:' + str(count)]

		if vid_width > 2592 or vid_heigh > 1280 :
			output = "2K"
			if   vid_width >= 7600 or vid_heigh >= 4300:
				output = "8K"
			elif vid_width >= 3800 or vid_heigh >= 2100:
				output = "4K"
			factore_w = round(vid_width / 1920)
			factore_h = round(vid_heigh / 1080)
			if factore_h != factore_w :
				output +=f"  W != H, {factore_w}, {factore_h}"
			nw = round( vid_width/factore_w)
			nh = round( vid_heigh/factore_h)
			extra += f'\t{output} Scale {vid_width}x{vid_heigh} to {nw}x{nh}'
			scalare = 'scale= ' +str(nw) +':' +str(nh)
			ff_vid.extend(['libx265', '-vf', scalare, '-preset', 'slow'])

		elif _vi_btrt < (expctd) :
			if _vdata['codec_name'] == 'hevc':
				extra += '\tCopy'
				ff_vid.extend(['copy'])
			else:
				ff_vid.extend(['libx265'])
				if   vid_width >= 1280 or vid_heigh >= 720:
					output = "FHD"
					ff_vid.extend(['-preset', 'slow'])
				elif vid_width >= 720 or vid_heigh >= 500:
					output = "HD"
					ff_vid.extend(['-preset', 'slow'])
				else:
					output = "SD"
					ff_vid.extend(['-preset', 'medium'])
				extra += f'\t{output} Convert to x265'
		else:
			if _vdata['codec_name'] == 'hevc':
				extra += '\tReduce to: ' + hm_sz(expctd)
				#				['libx265', '-b:v', str(expctd)])
				ff_vid.extend(['libx265'])
			else:
				extra += '\tConvert to Hevc'
				ff_vid.extend(['libx265'])
			ff_vid.extend(['-preset', 'slow'])
		message = f"    |<V:{_vdata['index']:2}>|{_vdata['codec_name']:8} |Br: {hm_sz(_vi_btrt,'R'):>9}|Xbr: {hm_sz(expctd,'R'):>8}|Fps: {frm_rate:>7}| {extra}"
		print(message)
		if (DeBug) :print (ff_vid)

	ff_video += ff_vid
	if DeBug :print ("  ", ff_video)

	return ff_video
##>>============-------------------<  End  >------------------==============<<##
def pars_audio ( js_info , DeBug=False ) :
	str_t = datetime.datetime.now()
	message = sys._getframe().f_code.co_name + ':'
	_adata = {
		'index': int,
		'codec_name': str,
		"codec_type": str,
		'language': str,
		'title': str,
		"sample_rate": int,
		"channels": int,
		"channel_layout": str,
		"time_base": str,
		"start_pts": int,
		"start_time": str,
		"bit_rate": str,
		'disposition': {},
		'tags':{}
		}
	# XXX: Audio
	ff_audio =[]
	for count, this_aud in enumerate(js_info) :
		prs_frm_to(this_aud, _adata, DeBug )
		extra = ''

#		print ( json.dumps(_adata, indent=3 ) )

		if DeBug :
			print(f"    +{message}=: Start: {str_t:%T}\n {json.dumps(_adata, indent=2)}" )
		try :
			ff_aud = ['-map', '0:' + str(_adata['index']), '-c:a:' + str(count)]

			if 'bit_rate' in _adata:
				if (_adata['bit_rate']) == "Pu_la" :
# XXX: Probably aac ?
					_au_btrt = glb_bitrate * 0.2	# estimate 20% is video
					extra = ' BitRate Estimate'
				else :
					_au_btrt = int( _adata['bit_rate'])
			else :
				print (json.dumps(js_info, indent=2))
				extra = "Audio Bitrate Extimate"
				_au_btrt = 123
			if 'language' in _adata['tags'] :
				_lng = _adata['tags']['language']
			else :
				_lng = 'wtf'
			if 'disposition' in _adata:
				_disp = _adata['disposition']['forced']
				_dflt = _adata['disposition']['default']

			if _adata['codec_name'] in ('aac', 'vorbis'):
				if _au_btrt <= Max_a_btr :
					extra += '\tCopy'
					ff_aud.extend(['copy'])
				else:
					extra += '\tReduce BitRate'
					ff_aud.extend(['libvorbis', '-q:a', '8'])
			else:
				extra += '\tConvert'
				ff_aud.extend(['libvorbis', '-q:a', '8'])
			message = f"    |<A:{_adata['index']:2}>|{_adata['codec_name']:8} |Br: {hm_sz(_au_btrt):>8}|Fq : {hm_sz(_adata['sample_rate']):>8}|Cha: {_adata['channels']}|Ln: {_lng}|Dispo: default={_dflt}, forced={_disp}| {extra}"
			print( message )
			if (DeBug) : print (ff_aud)

		except Exception as e:
			message += f" Exception"
			Trace (message, e)

		ff_audio += ff_aud
	if DeBug : print("  ", ff_audio )

	return ff_audio
##>>============-------------------<  End  >------------------==============<<##
def pars_subtl ( js_info , DeBug=False ) :
	str_t = datetime.datetime.now()
	message = sys._getframe().f_code.co_name + ':'
	_sdata = {
		'index': int,
		'codec': str,
		'codec_name': str,
		'codec_type': str,
		'language': str,
		'title': str,
		'map': int,
		'source': int,
		'path': str,
		'disposition': {},
		'tags':{}
	}
	# XXX: Subtitle
	ff_subtl =[]
	for count, this_sub in enumerate(js_info) :
		extra = ''
		prs_frm_to(this_sub, _sdata, DeBug )
		if DeBug :
			print(f"    +{message}=: Start: {str_t:%T}\n {json.dumps(_sdata, indent=2)}" )
		try :
			ff_sub = ['-map', '0:' + str(_sdata['index']), '-c:s:' + str(count)]
# XXX:
			if ('tags' and 'language') in _sdata['tags'] :
				_lng = _sdata['tags']['language']
			else :
				_lng = 'wtf'

# XXX: #https://askubuntu.com/questions/214199/how-do-i-add-and-or-keep-subtitles-when-converting-video
			if _sdata['codec_name'].lower() in ('hdmv_pgs_subtitle', 'dvd_subtitle', 'ass'):
				extra += f"Skip {_sdata['codec_name']}"
				ff_sub = []
			else :
				if _lng == 'eng':
					extra += f"Move Text & Make Default {_sdata['codec_name']}"
					ff_sub.extend(['mov_text'])
#						['mov_text', '-metadata:s:s:' + str(count), 'language=' + _lng, '-disposition:s:s:' + str(count), 'forced'])
				else:
					extra += f"Move Text {_sdata['codec_name']}"
					ff_sub.extend(['mov_text'])
#						['mov_text', '-metadata:s:s:' + str(count), "title="+ _sdata['codec_name']] )

			message = f"    |<S:{_sdata['index']:2}>|{_sdata['codec_name']:8}|{_sdata['codec_type']:^11} |{_lng:3}| {extra}"
			print(message)
			if (DeBug) : print (ff_sub)
		except Exception as e:
			message += f" Exception"
			Trace (message, e)
		if DeBug :
			print(f"\n {json.dumps(_sdata, indent=2)}\n{ff_sub}\n" )
		ff_subtl += ff_sub
	if DeBug : print ("  ", ff_subtl )

	return( ff_subtl )
##>>============-------------------<  End  >------------------==============<<##

def pars_extdta( js_info, DeBug ) :
	str_t = datetime.datetime.now()
	message = sys._getframe().f_code.co_name + ':'
	_ddata = {
		'index': int,
		'codec': str,
		'codec_name': str,
		'codec_type': str,
		'language': str,
		'tags':{}
	}
	# XXX: Data
	ff_dext =[]
	for this_dat in js_info :
		prs_frm_to(this_dat, _ddata, DeBug )
		if DeBug : print(f"    +{message}=: Start: {str_t:%T}\n {json.dumps(_ddata, indent=2)}" )
		try :
			ff_dat = ['-map', '0:' + str(_ddata['index']), '-dn']
			message = f"    |<D:{_ddata['index']:2}>|{_ddata['codec']:8}|{_ddata['codec_name']:8}|{_ddata['codec_type']:^11} |"
			print(message)
			if (DeBug) : print (ff_dat)

		except Exception as e:
			message += f" Exception"
			Trace (message, e)
		ff_dext += ff_dat

	return ff_dext
##>>============-------------------<  End  >------------------==============<<##


def pars_metadta(mta_dta, DeBug=False):
	str_t = datetime.datetime.now()
	message = sys._getframe().f_code.co_name + '/:'
	if DeBug :		print(f"  +{message}=: Start: {str_t:%T}")

	all_formt = mta_dta.get('format')

	all_strms = mta_dta.get('streams')
	if len (all_strms) :
		all_video = []
		all_audio = []
		all_subtl = []
		all_datas = []
		for strm_x in all_strms:
			if   strm_x['codec_type'] == 'video' :
				all_video.append(strm_x)
			elif strm_x['codec_type'] == 'audio' :
				all_audio.append(strm_x)
			elif strm_x['codec_type'] == 'subtitle' :
				all_subtl.append(strm_x)
			elif strm_x['codec_type'] == 'data' :
				all_datas.append(strm_x)
			else:
				message += f"\n Unknown Codec Type: {strm_x['codec_type']}\n"
				print (message)
				print (json.dumps(strm_x, indent=2))
				time.sleep(5)
			if DeBug :	print (json.dumps(strm_x, indent=2))
	else :
		message += f"\n{ json.dumps(mta_dta, indent=2) }\n\n No Streams\n"
		raise ValueError(message)# XXX: Check it :)

	return all_formt, all_video, all_audio, all_subtl, all_datas
##>>============-------------------<  End  >------------------==============<<##

def thza_brain(in_file, mta_dta, DeBug=False):
	str_t = datetime.datetime.now()
	message = sys._getframe().f_code.co_name
	print(f"  +{message}=: Start: {str_t:%T}")

	if not mta_dta:
		message += ' No Metadata Nothing to do here'
		raise Exception(message)
	try:
		frmt_inf, vdo_strm, aud_strm, sub_strm, ext_data = pars_metadta( mta_dta, DeBug )

		# XXX: Meta Data
		if len(frmt_inf) :
			pars_mtdta ( frmt_inf, DeBug )
		else :
			message += f"\n{ json.dumps(mta_dta, indent=2) }\n No Format\n"
			raise ValueError(message)

		# XXX: Video
		if len ( vdo_strm) :
			ff_video = pars_video( vdo_strm, DeBug )
		else :
			ff_video = ['-vn']
			print("    |<no:V>|")

		# XXX: Audio
		if len ( aud_strm ):
			ff_audio = pars_audio( aud_strm, DeBug )
		else :
			ff_audio = ['-an']
			print("    |<A:no>|")

		# XXX Subtitle
		if len ( sub_strm ):
			ff_subtl = pars_subtl( sub_strm, DeBug )
			if not len (ff_subtl) :
				ff_subtl = ['-sn']
		else :
			ff_subtl = ['-sn']
			print("    |<S:no>|")

		# XXX Data
		if len ( ext_data ):
			ff_exdta = pars_extdta( ext_data, DeBug  )
		else :
			ff_exdta = []
			print("    |<D:no>|")

	except Exception as e:
		message += f" FFZa_Brain: Exception"
		Trace (message, e)
		raise Exception(message)

# XXX: Do nothing if all is well needs to rework it a bit
	_, extens = os.path.splitext(in_file)
	if (extens.lower() == TmpF_Ex.lower()) and ('copy' in ff_video) and ('copy' or '-an' in ff_audio) and ('copy' or '-sn' in ff_subtl) :# and ('-dn' in ff_exdta) :
		raise ValueError(f"| <¯\_(%)_/¯>  Skip |")
	else:
		ffmpeg_cmnd = ff_video + ff_audio + ff_subtl +ff_exdta

	if (DeBug) :
		print (ffmpeg_cmnd)

	end_t = datetime.datetime.now()
	message =f'  -End: {end_t:%T}\tTotal: {round((end_t-str_t).total_seconds(),2)} sec'
	print (message)

	return ffmpeg_cmnd
##>>============-------------------<  End  >------------------==============<<##

def show_progrs(line_to, sy=False):
	message = sys._getframe().f_code.co_name + '-:'

	_P = ''
	if 'N/A' in line_to:
		return False
	elif 'global headers:' and "muxing overhead:" in line_to:
		_P = f'\r    |<+>| Done: {line_to}'
	elif 'encoded' in line_to:
		_P = f'    |>+<| Done: {line_to}'
	elif 'speed=' in line_to:
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
			message += f" {line_to}\n ErRor in Procesing data {e}:"
			Trace (message, e)

	sys.stderr.write(_P)
	sys.stderr.flush
	return True
##>>============-------------------<  End  >------------------==============<<##

def make_matrx(input_file, execu=ffmpeg, ext = '.png'):
	str_t = datetime.datetime.now()
	message = sys._getframe().f_code.co_name
	print(f"  +{message}=: Start: {str_t:%T}")

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
		message = f"    |PNG Exists ¯\_(%)_/¯ Skip"
		print(message)
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
			message = f"\r    |3x3| Matrix Created {ext}"
			print(message)
			end_t = datetime.datetime.now()
			message = f" -make_matrx Done !!: {end_t:%T}\tTotal: {round((end_t-str_t).total_seconds(),2)} sec"
			return os.path.getsize(file_name)
		else:
			message = f"   = Failed to Created .PNG >"
			raise Exception(message)
##>>============-------------------<  End  >------------------==============<<##

def short_ver ( fname, *other ) :
	str_t = datetime.datetime.now()
	message = sys._getframe().f_code.co_name
	print(f"  +{message}=: Start: {str_t:%T}")

# https://trac.ffmpeg.org/wiki/Map#Chooseallstreams
	todo = [ '-map', '0', '-c', 'copy']
	return todo
##>>============-------------------<  End  >------------------==============<<##

def speed_up ( fname, *other) :
# https://trac.ffmpeg.org/wiki/How%20to%20speed%20up%20/%20slow%20down%20a%20video
#	ffmpeg -i input.mkv -filter:v "setpts=0.5*PTS" output.mkv #Frame Rate Chage
	print(f"  +{message}=: Start: {str_t:%T}")
	todo = [ '-filter_complex "[0:v]setpts=0.25*PTS[v];[0:a]atempo=2.0,atempo=2.0[a]" -map "[v]" -map "[a]" ']
	return todo
##>>============-------------------<  End  >------------------==============<<##

def video_diff(file1, file2) :
	# XXX:  Visualy Compare in and out files
	# https://stackoverflow.com/questions/25774996/how-to-compare-show-the-difference-between-2-videos-in-ffmpeg

	str_t = datetime.datetime.now()
	message = sys._getframe().f_code.co_name
	print(f"  +{message}=: Start: {str_t:%T}")

	cmprd_file = get_new_fname(file1, "_Video_comp.mp4", TmpF_Ex)

	todo = (ffmpeg, '-i', file1, '-i', file2, '-filter_complex', "blend=all_mode=difference", '-c:v', 'libx265', '-preset', 'faster', '-c:a', 'copy', '-y', cmprd_file)

	if run_ffm(todo):
		return cmprd_file
	else:
		message += f"   = Failed to Compare Files >\n"
		raise Exception(message)
##>>============-------------------<  End  >------------------==============<<##
