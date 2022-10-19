from Yaml import *

import os
import re
import sys
import json
import locale
import datetime
import subprocess

from My_Utils import *
console_encoding = locale.getdefaultlocale()[1] or 'UTF-8'

ffmpg_bin = 'C:\\Program Files\\ffmpeg\\bin'

ffmpeg = os.path.join(ffmpg_bin, "ffmpeg.exe")
ffprob = os.path.join(ffmpg_bin, "ffprobe.exe")
# ffplay = os.path.join(ffmpg_bin, "ffplay.exe")

if not ( os.path.exists(ffmpeg) and os.path.exists(ffprob) ):
	message += f"{ffmpeg}\nPath Does not Exist:"
	input(message)
	raise OSError

##==============-------------------   End   -------------------==============##


def run_ffprob(in_file, execu=ffprob):
#Returns Json list
	str_t = datetime.datetime.now()
	message = sys._getframe().f_code.co_name
	print(f"  +{message}=: Start: {str_t:%T}")

	ff_args = (execu, '-hide_banner',
					  '-probesize',			'50M',
					  '-analyzeduration', '100M',
					  '-v', 'warning',		# XXX quiet, panic, fatal, error, warning, info, verbose, debug, trace
					  '-of', 'json',		# XXX default, csv, xml, flat, ini
					  '-show_error',
					  '-show_format',
					  '-show_streams',
#					  '-show_data',
#					  '-show_private_data',
					  '-i', in_file )
	try:
		runit = subprocess.Popen( ff_args,	stderr=subprocess.PIPE,
											stdout=subprocess.PIPE )
		out, err = runit.communicate()

		jlist = json.loads(out.decode(console_encoding, errors='ignore'))
		if DeBug  :
			print ( json.dumps(jlist, indent=2 ) )
		if err :
			message += f" Error:{err}"
			if DeBug : message += f"\n{json.dumps(jlist, indent=2 )}\n"
			print(message)
		if len(jlist) < 2 :
			message += f"F:{in_file}\nJson:\n{json.dumps(jlist, indent=2 )}"
			print(message)
			time.sleep (3)
			raise Exception(message)
	except Exception as e:
		runit.kill()
		message += f"\nException: {e}\n"
		raise Exception(message)


	end_t = datetime.datetime.now()
	message =f'  -End  : {end_t:%T}\tTotal: {round((end_t-str_t).total_seconds(),1):,}'
	print ( message )
	if DeBug :
		exit ()
	return jlist

##>>============-------------------<  End  >------------------==============<<##


def ffmpeg_run(input_file, ff_com, execu=ffmpeg):
#Returns file_name
	str_t = datetime.datetime.now()
	message = sys._getframe().f_code.co_name
	print(f"  +{message}=: Start: {str_t:%T}")

	out_file = '_' + stmpd_rad_str(7) + TmpF_Ex

	file_name, _ = os.path.splitext( os.path.basename(input_file) )

	ff_head = [execu, "-i", input_file, "-hide_banner"]

	banner = "title= " + file_name + " x256 "

	ff_tail = [ "-dn", "-metadata", banner,
				"-movflags", "+faststart",
				"-fflags", "+fastseek",
#				"-fflags", "+genpts,+igndts",
				"-y", out_file,
				"-f", "matroska" ]

	todo = ff_head + ff_com + ff_tail
# XXX:
	if run_ffm(todo):
		if not os.path.exists(out_file) or os.path.getsize(out_file) < 1000 :
			message += f'\n No Output Error\n Execute in Debug Mode for more details'
			print(message)
			run_ffm(todo, message)
			time.sleep(3)
			raise Exception('$hit ', message)
		else:
			end_t = datetime.datetime.now()
			message = f"  -Done:  -End  : {end_t:%T}\tTotal: {round((end_t-str_t).total_seconds(),1):,}"
			print(message)
			return out_file
	else:
		message += f" -> FFMpeg failed :("
		raise Exception(message)

	return out_file
##>>============-------------------<  End  >------------------==============<<##


def run_ffm(args, *debug):
#	debug = True
	message = sys._getframe().f_code.co_name + '|:'
	if debug :
		print(f'{message}\nDebug Mode\n{args[4:]}\n')
		runit = subprocess.Popen(args)
		out, err = runit.communicate()
		message = f'\nDone\tOut: {out}\nErr: {err}'
		print(message)
		time.sleep(5)
	else:
		try:
			runit = subprocess.Popen(args,	stderr=subprocess.STDOUT,
											stdout=subprocess.PIPE,
											universal_newlines=True,
											encoding='utf-8')
			loc = 0
			symbls = '|/-o+\\'
			sy_len = len(symbls)
			while not runit.poll() :
				lineo = runit.stdout.readline()
				if len(lineo) :
					show_progrs(lineo, symbls[loc])
					loc += 1
					if  loc == sy_len:
						loc = 0
				else:
					out, err = runit.communicate()
					if err or out :
						print(f'Erro:\nE:{err}\nO:{out}')
						time.sleep(3)
					break
		except (Exception, PermissionError, OSError) as e:
			runit.kill()
			message += f" Exception => {e}\n"
			print(message)
			return False
	message += f"\n Done"
	if DeBug : print (message)

	return True

##==============-------------------   End   -------------------==============##


def short_ver ( fname, *other ) :
	str_t = datetime.datetime.now()
	message = sys._getframe().f_code.co_name
	print(f"  +{message}=: Start: {str_t:%T}")

# https://trac.ffmpeg.org/wiki/Map#Chooseallstreams
	todo = [ '-map', '0', '-c', 'copy']
	return todo

def speed_up ( fname, *other) :
# https://trac.ffmpeg.org/wiki/How%20to%20speed%20up%20/%20slow%20down%20a%20video
#	ffmpeg -i input.mkv -filter:v "setpts=0.5*PTS" output.mkv #Frame Rate Chage
	print(f"  +{message}=: Start: {str_t:%T}")
	todo = [ '-filter_complex "[0:v]setpts=0.25*PTS[v];[0:a]atempo=2.0,atempo=2.0[a]" -map "[v]" -map "[a]" ']
	return todo


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
		width = str(round(vid_width))
		# We have 9 tiles plus a bit more
		slice = str(round(totl_frms / 9))
		zzzle = "[0:v]select=not(mod(n\," + slice + ")), scale=" + width + ":-1:, tile=3x3:nb_frames=9:padding=3:margin=3"

		if totl_frms > 6600:
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
			message = f" -make_matrx Done !!:  -End  : {end_t:%T}\tTotal: {round((end_t-str_t).total_seconds(),1):,}"
			return os.path.getsize(file_name)
		else:
			message = f"   = Failed to Created .PNG >"
			raise Exception(message)
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


def parse_mtdata(mta_dta, DeBug=False):
	str_t = datetime.datetime.now()
	message = sys._getframe().f_code.co_name + '/:'
	if DeBug :		print(f"  +{message}=: Start: {str_t:%T}")

	all_formt = mta_dta.get('format', 'WTF?')
	if not len(all_formt) :
		message += f"\n{ json.dumps(mta_dta, indent=2) }\n"
		message += f' No Format\n'
		raise ValueError(message)

	_frmt = dict( filename='',
					nb_streams=int(0),
					duration=float(0),
					size=float(0),
					bit_rate=int(0),
					tags= {}
					)
	prs_frm_to( all_formt, _frmt , DeBug)

	all_strms = mta_dta.get('streams', 'WTF?')
	if not len(all_strms) :
		message += f"\n{ json.dumps(mta_dta, indent=2) }\n"
		message += f' No Stream\n'
		raise ValueError(message)

	all_audio = []
	all_video = []
	all_subtl = []

	for strm_x in all_strms:
		if   strm_x['codec_type'] == 'video' :
			all_video.append(strm_x)
		elif strm_x['codec_type'] == 'audio' :
			all_audio.append(strm_x)
		elif strm_x['codec_type'] == 'subtitle':
			all_subtl.append(strm_x)
		elif strm_x['codec_type'] == 'data':
			message += f" \nType: {strm_x['codec_type']}\n"
		else:
			message += f" \nType: {strm_x['codec_type']}\n{ json.dumps(strm_x, indent=2) }"
		if DeBug :	print (message)
# XXX: Check it :)
	if not len(all_video) :
		message += f"\n{ json.dumps(mta_dta, indent=2) }\n"
		message += f'File  !! No Video => Canot convert\n'
		raise ValueError(message)
	if not len(all_audio) :
		message += f"\n{ json.dumps(mta_dta, indent=2) }\n"
		message += f'File: !! No Audio\n'
		time.sleep(1)
#?		raise  ValueError( message )
	if not len(all_subtl) :
		message += f"\n{ json.dumps(mta_dta, indent=2) }\n"
		message += f'File: !! No Subtitle\n'
		time.sleep(1)
#?		raise  ValueError( message )
	if DeBug :
		print("\n", "^" * 70)
		print ('Meta Data :\n', json.dumps(mta_dta, indent=2) )
		print("\n", "=" * 70)
		print ('Streams Dt:\n', json.dumps(all_strms, indent=2) )
		print("\n", "=" * 70)
		print ('Format  Dt:\n', json.dumps(all_formt, indent=2) )
		print("\n", "f" * 70)
		print ('File Frmt :\n', json.dumps(_frmt, indent=2) )
		input ("Press any Key")

	return all_video, all_audio, all_subtl, all_formt
##>>============-------------------<  End  >------------------==============<<##


def thza_brain(in_file, mta_dta, DeBug=False):
	global vid_lengt  # make it global so we can reuse for fmpeg check ...
	global totl_frms
	global vid_width

	str_t = datetime.datetime.now()
	message = sys._getframe().f_code.co_name
	print(f"  +{message}=: Start: {str_t:%T}")
	if not mta_dta:
		message += ' on NoMetadata Nothing to do here'
		raise Exception(message)
	special = False

	try:
		vdo_strm, aud_strm, sub_strm, frmt_inf = parse_mtdata( mta_dta, DeBug )

		_mtdta = dict(filename='',
					  nb_streams=int(0),
					  duration=float(0),
					  bit_rate=int(0),
					  size=float(0)
					  )
		prs_frm_to(frmt_inf, _mtdta , DeBug )

		_vdata = dict(index=int(0),
					  codec_name='',
					  width=int(0),
					  height=int(0),
					  coded_width=int(0),
					  coded_height=int(0),
					  bit_rate=int(0),
					  r_frame_rate = '',
					  pix_fmt = ''
					  )
# XXX: Presumes that the first Video is the real one !
		prs_frm_to(vdo_strm[0], _vdata, DeBug )

		vid_lengt = int(_mtdta['duration'])
		vid_width = _vdata['width']
		vid_heigh = _vdata['height']

		match = re.search (r"yuv[a-z]?[0-9]{3}", _vdata['pix_fmt'])
		if match:
			print ("    Pixel Format  ",_vdata['pix_fmt'])
		mins,  secs = divmod(vid_lengt, 60)
		hours, mins = divmod(mins, 60)

		frm_rate = float(divd_strn(_vdata['r_frame_rate']))
		if not frm_rate :
			print(json.dumps(_vdata, indent=2))
			frm_rate = 25
		'''
# XXX: Compute expected BitRate
		totl_frms = round(frm_rate * vid_lengt)
		totl_pixl = vid_width * vid_heigh * totl_frms
	# XXX: (width x height) / 1000 * fps * BPP (bits per pixel)
	# NOTE: Set BPP expectation between 0.1 to 0.15.
		exp_bitpp    = 0.25
		expctd = round( totl_pixl / 10000 * exp_bitpp )
# XXX: Bitrate = file size / (number of minutes * .0075).
		'''
		# NOTE: Expected Bitrate
		totl_frms = round(frm_rate * vid_lengt)
		totl_pixl = vid_width * vid_heigh * totl_frms
		exp_bitpp = 0.1
		expctd = round( totl_pixl * exp_bitpp  / 2000 )
		# XXX: Fixed
		expctd = 3330000

# XXX: Print Banner
		message = f"    |< CT >|{f'{hours:02d}h:{mins:02d}m'} |  {vid_width:>4}x{vid_heigh:<4} |Tfm: {hm_sz(totl_frms,'F'):7}|Tpx: {hm_sz(totl_pixl, 'P')}|Xbr: {hm_sz(expctd,'R')}|Vid: {len(vdo_strm)}|Aud: {len(aud_strm)}|Sub: {len(sub_strm)}|"
		print(message)

# XXX: Video
		ff_video = []
		if len(vdo_strm):
			count = -1
			for _vid in vdo_strm:
				extra = ''
				count += 1
				if 'codec_name' not in _vid:
					_vid['codec_name'] = 'No Codec Name ?'
					message = f"    |<V:{_vid} | WTF"
					raise Exception(message)
				elif _vid['codec_name'].lower() in ['mjpeg', 'png'] :
					message = f"    |<V:{_vid['index']:2}>|{_vid['codec_name']:7} | > Skip this "
					print(message)
					continue
				if 'bit_rate' in _vid:
					_vi_btrt = int(_vid['bit_rate'])
				else:
					# XXX approximation 80% is video
					if _mtdta['bit_rate'] != "Pu_la":
						_vi_btrt = int(float(_mtdta['bit_rate']) * 0.80)
# XXX: There must be a better way
					else:
						_vi_btrt = 123456
						input ( 'No Bitrate')
					extra = ' BitRate Estimate'
# XXX: Print Banner

#			safe['pix_fmt'] = "yuv420p"

#					ff_video.extend ( ['-map', '0:' + str(_vid['index']), '-c:v:' + str(count), 'mjpeg'] )

				ff_video.extend(['-map', '0:' + str(_vid['index']), '-c:v:' + str(count)])

				if frm_rate > Max_frm_r:
#					ff_video.extend( [ '-r', '25', 'libx265', '-crf', '26' ] )
					message = f"  Frame rate convert {frm_rate} to 30"
					print(message)
					time.sleep(2)

				if   vid_width >= 7600 or vid_heigh >= 4300:
					output = "8K"
					special = True
					extra += f'\t\t{output} Scale {vid_width}x{vid_heigh} to 1080p'
					ff_video.extend(['libx265', '-vf', 'scale=1920:-1', '-preset', 'slow'])
				elif vid_width >= 3800 or vid_heigh >= 2100:
					output = "4K"
					special = True
					nw = (vid_width/2)
					nh = (vid_heigh/2)
					extra += f'\t\t{output} Scale {vid_width}x{vid_heigh} to 1080p {nw}x{nh} '
					scalare = 'scale= ' +str(nw) +':' +str(nh)
					ff_video.extend(['libx265', '-vf', 'scalare' , '-preset', 'slow'])
				elif _vi_btrt < expctd :
					if _vid['codec_name'] == 'hevc':
						extra += '\t\t\tCopy'
						ff_video.extend(['copy'])
					else:
						ff_video.extend(['libx265'])
						if   vid_width >= 1280 or vid_heigh >= 720:
							output = "FHD"
							ff_video.extend(['-preset', 'slow'])
						elif vid_width >= 720 or vid_heigh >= 500:
							output = "HD"
							ff_video.extend(['-preset', 'medium'])
						else:
							output = "SD"
							ff_video.extend(['-preset', 'fast'])
						extra += f'\t\t\t{output} Convert to x265'
				else:
					if _vid['codec_name'] == 'hevc':
						extra += '\t\t\tReduce BitRate: ' + hm_sz(expctd)
							#							['libx265', '-b:v', str(expctd)])
						ff_video.extend(['libx265'])
					else:
						extra += '\t\t\tConvert to Hevc'
						ff_video.extend(['libx265'])
					ff_video.extend(['-preset', 'slow'])

				message = f"    |<V:{_vid['index']:2}>|{_vid['codec_name']:7} |Br: {hm_sz(_vi_btrt,'R'):>8}|Fps: {frm_rate:>7}| {extra}"
				print(message)
			if (DeBug) :
				print (ff_video)
		else:
			print('    |<V:No>| No Video')

# XXX: Audio
		ff_audio = []
		if len(aud_strm):
			aud_typ = []
			_au_btrt = 0
			count = -1
			for _aud in aud_strm:
				extra = ''
				count += 1
				_disp = dict(default=int(0), forced=int(0))
				_lng = dict(language='')
				if 'bit_rate' in _aud:
					_au_btrt = int(_aud['bit_rate']) / int(_aud['channels'])
				# XXX:  aproximation
				else:
					_au_btrt = int(_mtdta['bit_rate'] *
								   0.02 * _aud['channels'])
					extra += ' BitRate Estimate'

				if 'tags' in _aud:
					prs_frm_to(_aud['tags'], _lng)
					if 'Pu_la' in _lng:
						_lng['language'] = '_wtf?_'
				if 'disposition' in _aud:
					prs_frm_to(_aud['disposition'], _disp)

				ff_audio.extend(
					['-map', '0:' + str(_aud['index']), '-c:a:' + str(count)])

				if _aud['codec_name'] in ('aac', 'vorbis'):
					if _au_btrt <= Max_a_btr *1.33:
						extra += '\tCopy'
						ff_audio.extend(['copy'])
					else:
						extra += '\tReduce BitRate'
						ff_audio.extend(['libvorbis', '-q:a', '8'])
				else:
					extra += '\tConvert'
					ff_audio.extend(['libvorbis', '-q:a', '8'])

				aud_typ.append(_lng['language'])

			if len(aud_typ) > 1:
				if ('eng' in aud_typ) and (('rus' in aud_typ) or ('und' in aud_typ)):
					# XXX: Select English only
					extra += f"Map only English"
					ff_audio = ['-map', '0:m:language:eng']
					print(
						f"    | {len (aud_typ)} AuD: {aud_typ} => {ff_audio}")
					special = True

			message = f"    |<A:{_aud['index']:2}>|{_aud['codec_name']:7} |Br: {hm_sz(_au_btrt):>8}|Fq : {hm_sz(_aud['sample_rate']):>6}|Ch: {_aud['channels']}|{_lng['language']}|{_disp['default']}| {extra}"
			print(message)
			if (DeBug) :
				print (ff_audio)

		else:
			print('    |<A:No>| No Audio')

# XXX subtitle
		ff_subtl = []
		if len(sub_strm):
			count = -1
			for _sub in sub_strm:
				extra = ''
				count += 1
				_lng = dict(language='')
				if 'tags' in _sub:
					prs_frm_to(_sub['tags'], _lng)
					if 'Pu_la' in _sub['tags']:
						_lng['language'] = 'wtf'
				ff_subtl.extend(
					['-map', '0:' + str(_sub['index']), '-c:s:' + str(count)])

# XXX: #https://askubuntu.com/questions/214199/how-do-i-add-and-or-keep-subtitles-when-converting-video
				if _sub['codec_name'].lower() in ('hdmv_pgs_subtitle', 'dvd_subtitle', 'ass' ):
					extra += f"Skip {_sub['codec_name']}"
					ff_subtl = [ '-sn']
				else:
					if _lng['language'] == 'eng':
						extra += f"Move Text & Make Default {_sub['codec_name']}"
						ff_subtl.extend(
							['mov_text', '-metadata:s:s:' + str(count), 'language=' + _lng['language'], '-disposition:s:s:' + str(count), 'forced'])
					else:
						extra += f"Move Text Default {_sub['codec_name']}"
						ff_subtl.extend(
							['mov_text', '-metadata:s:s:' + str(count), "title="+ _sub['codec_name']] )

				message = f"    |<S:{_sub['index']:2}>|{_sub['codec_name']:8}|{_sub['codec_type']:^11} |{_lng['language']:3}| {extra}"
				print(message)
				if (DeBug) :
					print (ff_subtl)

		else:
			print('    |<S:No>|NO Subtitle|')

	except Exception as e:
		message += f" FFZa_Brain: Exception"
		Trace (message, e)
		raise Exception(message)

	else:
		ffmpeg_cmnd = ff_video + ff_audio + ff_subtl

		_, extens = os.path.splitext(in_file)
		if len(aud_strm):
			_au_code = _aud['codec_name']
		else:
			_au_code = 'HoFuckingAudio'

		end_t = datetime.datetime.now()
		message = f'  -End  : {end_t:%T}\tTotal: {round((end_t-str_t).total_seconds(),1):,}'
		if not special and (extens.lower() == TmpF_Ex.lower()) and (_vid['codec_name'] == 'hevc') and (_au_code in ('aac', 'opus', 'vorbis', 'nofuckingaudio')):
			# XXX: 33% Generous Bitrate forgiveness for Video and audio
			if (_vi_btrt < expctd ) and (_au_btrt < Max_a_btr ) :
				message = f"    | {_vid['codec_name']} | {_au_code:^5} | <¯\_(%)_/¯>  Skip"
				if True :
					raise ValueError(message)

		end_t = datetime.datetime.now()
		message = f'  -End  : {end_t:%T}\tTotal: {round((end_t-str_t).total_seconds(),1):,}'
		if (DeBug) :
			print (ffmpeg_cmnd)

	return ffmpeg_cmnd
##>>============-------------------<  End  >------------------==============<<##

def show_progrs(line_to, sy=False):
#	return True
#	DeBug = True
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
				dif = abs(vid_lengt - a_sec)
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
			time.sleep(3)
#			raise Exception(message)

	sys.stderr.write(_P)
	sys.stderr.flush
	return True
##>>============-------------------<  End  >------------------==============<<##
