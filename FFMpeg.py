# -*- coding: utf-8 -*-
#!/usr/bin/python3
__author__ = 'GeoHaZen'
'''
@author: 	  GeoHaZen
# XXX KISS
# XXX:ToDo: multiple languages
			raise exception to safe specifi files
			pass comand line and atributes for behaiviour
'''
import os
import re
import sys
import json
import datetime
import traceback
import subprocess

from My_Utils import *
from Yaml import *

ffmpg_bin = 'C:\\Program Files\\ffmpeg\\bin'
ffmpeg = os.path.join(ffmpg_bin, "ffmpeg.exe")
ffprob = os.path.join(ffmpg_bin, "ffprobe.exe")

if os.path.exists(ffmpeg) and os.path.exists(ffprob):
	pass
else:
	message += f"{ffmpeg} Path Does not Exist:"
	input(message)
	raise OSError
##==============-------------------   End   -------------------==============##

def Run_ff(args, **kwargs):
#	DeBug = True
	'''
	Returns True or if compleated
	'''
	message = sys._getframe().f_code.co_name + '|:'

	try:
		if DeBug:
			print(f'{message} Debug Mode\n{args}\n')
			time.sleep(5)

			Run = subprocess.Popen(args)
			out, err = Run.communicate()
			message += f'\nDone\tO= {out}\tE= {err}'
			print(message)
			return True
		else:
			loc = 0
			symbls = '|/-+\\'
			Run = subprocess.Popen(args, stderr=subprocess.STDOUT,
								   stdout=subprocess.PIPE,
								   universal_newlines=True)
#								   encoding='utf-8')
			while Run.poll() is None:
				lineo = Run.stdout.readline()
				Prog_cal(lineo, symbls[loc])
				loc += 1
				if loc == len(symbls):
					loc = 0
			return True
	except Exception as e:
		Run.kill()
		message += f"Exception => {e}\n"
		print(message)
		return False
	else:
		return False
##==============-------------------   End   -------------------==============##


def Run_FFProbe(File_in, Execute=ffprob):
	DeBug = True

	str_t = datetime.datetime.now()
	message = sys._getframe().f_code.co_name
	print(f"  +{message}=: Start: {str_t:%H:%M:%S}")

	Comand = (Execute,
			  '-i', File_in,
			  '-analyzeduration', '200000000000',
			  '-probesize',       '200000000000',
			  '-v', 'verbose',		# XXX quiet, panic, fatal, error, warning, info, verbose, debug, trace
			  '-of', 'json',		# XXX default, csv, xml, flat, ini
			  '-hide_banner',
			  '-show_format',
			  '-show_error',
			  '-show_streams')

	try:
		Run = subprocess.Popen(Comand, stderr=subprocess.PIPE,
							   stdout=subprocess.PIPE)
		out, err = Run.communicate()
		if Run.poll():
			message += f'\nError:\nO= {out}\nE= {err}'
			raise Exception (message)

	except Exception as e:
		Run.kill()
#		message += f"\nException => {e.args}\n"
		print (message)
		if DeBug :
			message += f"Json:\n{ json.dumps(json.loads(out), indent=2) }\n"

	else:
		jlist = json.loads(out)
		if len(jlist) < 2:
			message += f"Json out to small\n{File_in}\n{jlist}"
			if DeBug:
				print(message), input(" Jlist to small ")
			raise Exception(message)
		else :
			end_t = datetime.datetime.now()
			print(
				f'  -End  : {end_t:%H:%M:%S}\tTotal: {(end_t-str_t).total_seconds()}')
			return jlist

##>>============-------------------<  End  >------------------==============<<##


def Run_FFMpego(Fmpg_in_file, Za_br_com, Execute=ffmpeg):
	##	DeBug = True
	global Tot_Frms
	global Vid_With

	str_t = datetime.datetime.now()
	message = sys._getframe().f_code.co_name
	print(f"  +{message}=: Start: {str_t:%H:%M:%S}")

	Fmpg_ou_file = '_' + Random_String(11) + TmpF_Ex

	Sh_fil_name = os.path.basename(Fmpg_in_file).title()
	Sh_fil_name, _ = os.path.splitext(Sh_fil_name)
# TODO: Sanitize / parse name

# XXX Convert to HEVC FileName for the Title ...
	# TODO: If .mp4 exists skip

	ff_head = [Execute, '-i', Fmpg_in_file, '-hide_banner']

	ff_tail = ['-dn', '-movflags', '+faststart', '-fflags',
			   'genpts', '-flags', 'global_header', '-y', Fmpg_ou_file]

	Cmd = ff_head + Za_br_com + ff_tail

	if DeBug :
		print("    |>-:", Cmd )  # XXX:  Skip First 4 and Last 6

	if Run_ff(Cmd):
		if not os.path.exists(Fmpg_ou_file):
			message += f'\n No Outpu Error'
			print(message)
			raise Exception('$hit ', message)
		else:
			end_t = datetime.datetime.now()
			message = f"  -FFMpeg Done !!:  -End  : {end_t:%H:%M:%S}\tTotal: {(end_t-str_t).total_seconds()}"
			print(message)
			return Fmpg_ou_file
	else:
		message += f" -> FFMpeg failed :("
		raise Exception(message)

	return Fmpg_ou_file
##>>============-------------------<  End  >------------------==============<<##

def Make_Matrix( Fmpg_in_file, Execute=ffmpeg, Embed_Subtitle = False, Compare_Files = False ):
	str_t = datetime.datetime.now()
	message = sys._getframe().f_code.co_name
	print(f"  +{message}=: Start: {str_t:%H:%M:%S}")

# XXX Create Matrix Colage:
	Sh_fil_name, _ = os.path.splitext(Fmpg_in_file)
	Sh_fil_name += '.png'

	if not os.path.isfile(Sh_fil_name):
		'''
		To make multiple screenshots and place them into a single image file (creating tiles), you can use FFmpeg's tile video filter, like this:
		https://ffmpeg.org/ffmpeg-filters.html#Examples-129
		"ffmpeg -loglevel panic -i \"$MOVIE\" -y -frames 1 -q:v 1 -vf \"select=not(mod(n\,$NTH_FRAME)),scale=-1:${HEIGHT},tile=${COLS}x${ROWS}\" \"$OUT_FILEPATH\""
		# `-loglevel panic` We don’t want to see any output. You can remove this option if you’re having any problem to see what went wrong
		# `-i "$MOVIE"` Input file
		# `-y` Override any existing output file
		# `-frames 1` Tell `ffmpeg` that output from this command is just a single image (one frame).
		# `-q:v 3` Output quality where `0` is the best.
		# `-vf \"select=` That's where all the magic happens. Selector function for [video filter](https://trac.ffmpeg.org/wiki/FilteringGuide).
		# # `not(mod(n\,58))` Select one frame every `58` frames [see the documentation](https://www.ffmpeg.org/ffmpeg-filters.html#Examples-34).
		# # `scale=-1:120` Resize to fit `120px` height, width is adjusted automatically to keep correct aspect ration.
		# # `tile=${COLS}x${ROWS}` Layout captured frames into this grid

		Produce 8x8 PNG tiles of all keyframes (-skip_frame nokey) in a movie:
		ffmpeg -skip_frame nokey -i file.avi -vf 'scale=128:72,tile=8x8' -an -vsync 0 keyframes%03d.png
		The -vsync 0 is necessary to prevent ffmpeg from duplicating each output frame to accommodate the originally detected frame rate.

		Display 5 pictures in an area of 3x2 frames, with 7 pixels between them, and 2 pixels of initial margin, using mixed flat and named options:
		tile=3x2:nb_frames=5:padding=7:margin=2

		ffmpeg -ss 00:00:10 -i movie.avi -frames 1 -vf "select=not(mod(n\,1000)),scale=320:240,tile=2x3" out.png
		That will seek 10 seconds into the movie, select every 1000th frame, scale it to 320x240 pixels and create 2x3 tiles in the output image out.png, which will look like this:
		-frames 1 -vf "select=not(mod(n\,400)),scale=160:120,tile=4x3" tile.png
		-vf select='gt(scene\,0.4)', scale=160:120, tile -frames:v 1 Yosemite_preview.png
		'-skip_frame', 'nokey', '-vsync', 'vfr',
		'''
		skip0 = '00:00:29'
		width = str(round(Vid_With / 3))
		slice = str(round(Tot_Frms / 9.01))  # We have 9 tiles plus a bit more
		zzzle = "[0:v]select=not(mod(n\," + slice + ")), scale=" + \
				width + ":-1:, tile=3x3:nb_frames= 9:padding= 2:margin= 2 "
	#	slice = str( round( Tot_Frms / 16.003 ) ) # We have 16 tiles plus a bit more
	#	zzzle = "[0:v]select=not(mod(n\," + slice + ")), scale=-1:280, tile=4x4:nb_frames=16:padding=4:margin=4"

		if Tot_Frms > 6000:
			Cmd = (Execute, '-ss', skip0, '-vsync', 'vfr', '-i',
				   Fmpg_in_file, '-frames', '1', '-vf', zzzle, '-y', Sh_fil_name)
		else:
			Cmd = (Execute, '-vsync', 'vfr', '-i',
				   Fmpg_in_file, '-frames', '1', '-vf', zzzle, '-y', Sh_fil_name)
		# XXX:
		if DeBug :
			print("    |>-:", Cmd )  # XXX:  Skip First 4 and Last 6
		if Run_ff(Cmd):
			message = f"   = PNG Created >"
			print(message)
			end_t = datetime.datetime.now()
			message = f" -Make_Matrix Done !!:  -End  : {end_t:%H:%M:%S}\tTotal: {(end_t-str_t).total_seconds()}"
			return os.path.getsize(Sh_fil_name)
		else:
			message = f"   = Faled to Created PNG >"
			raise Exception (message)
	else :
		message = f"    |PNG Exists ¯\_(%)_/¯_Skip"
		print(message)
		return False

# XXX:  Enbed Subtitle file
	if Embed_Subtitle:
		Sh_fil_name, _ = os.path.splitext(Fmpg_in_file)
		Sh_fil_name += '.srt'

		if os.path.isfile(Sh_fil_name) :
			Fmpg_ou_ = Sh_fil_name + TmpF_Ex

			ff_head = [ffmpeg, '-i', Fmpg_in_file,
							   '-f', 'srt', '-i', Sh_fil_name]
			ff_tail = ['-c:v', 'copy', '-c:a', 'copy',
							   '-c:s', 'mov_text', '-y', Fmpg_ou_]
			Cmd = ff_head + ff_tail
			if DeBug  or True :
				print("    |>-:", Cmd)  # XXX:
				input ( " Ready to do srt add? " )
			if Run_ff(Cmd):
				message = f"   = Embed Subtitle >\n"
				print(message)
			else:
				message = f"   = Faled to Embed Subtitle >\n"
				raise Exception (message)
		else:
			input (" No .srt file with the same name")
# XXX:  Visualy Compare in and out files
# https://stackoverflow.com/questions/25774996/how-to-compare-show-the-difference-between-2-videos-in-ffmpeg

	if Compare_Files:
		ff_head = [ffmpeg, '-i', Fmpg_in_file, '-i', Fmpg_ou_file]
		ff_tail = ['-filter_complex', "blend=all_mode=difference",
				   "format=yuv420p", '-c:v', 'libx265', '-c:a', 'copy', '-y', "out.mp4"]
		Cmd = ff_head + ff_tail
		if DeBug :
			print("    |>-:", Cmd)  # XXX:
		if Run_ff(Cmd):
			message = f"   = Compare Files >\n"
			print(message)
		else:
			message = f"   = Faled to Compare Files >\n"
			raise Exception (message)

	print(message)
##>>============-------------------<  End  >------------------==============<<##


def Pars_MetaData(Meta_dta, verbose=False):

	str_t = datetime.datetime.now()
	message = sys._getframe().f_code.co_name + '/:'
	#	print(f"    /{message}\t\tStart: {str_t:%H:%M:%S}")

	All_Audio = []
	All_Video = []
	All_Subtt = []

	Al_strms = Meta_dta.get('streams', 'WTF?')
	for Strm_X in Al_strms:
		if 'video' in Strm_X['codec_type']:
			All_Video.append(Strm_X)
		elif 'audio' in Strm_X['codec_type']:
			All_Audio.append(Strm_X)
		elif 'subtitle' in Strm_X['codec_type']:
			All_Subtt.append(Strm_X)
		else:
			message += f" Type: {Strm_X['codec_type']}\n{ json.dumps(Strm_X, indent=2) }"

	Al_strms = Meta_dta.get('format', 'WTF?')

# XXX: Check it :)
	if len(Al_strms) == 0:
		message += f"\n{ json.dumps(Meta_dta, indent=2) }\n"
		message += f'File: !! No MetaData\n'
		raise ValueError(message)
	if len(All_Video) == 0:
		message += f"\n{ json.dumps(Meta_dta, indent=2) }\n"
		message += f'File  !! No Video => Can\'t convert\n'
		raise ValueError(message)
	if len(All_Audio) == 0:
		message += f"\n{ json.dumps(Meta_dta, indent=2) }\n"
		message += f'File: !! No Audio\n'
#		raise  ValueError( message )
	if len(All_Subtt) == 0:
		message += f"\n{ json.dumps(Meta_dta, indent=2) }\n"
		message += f'File: !! No Subtitle\n'
#		raise  ValueError( message )

	end_t = datetime.datetime.now()
#	print(f'    -End  : {end_t:%H:%M:%S}\tTotal: {(end_t-str_t).total_seconds()}')

	return All_Video, All_Audio, All_Subtt, Al_strms
##>>============-------------------<  End  >------------------==============<<##


def FFZa_Braino(Ini_file, Meta_dta, verbose=False):
	global Vi_Dur  # make it global so we can reuse for fmpeg check ...
	global Tot_Frms
	global Vid_With

#	global DeBug
	DeBug = True

	str_t = datetime.datetime.now()
	message = sys._getframe().f_code.co_name
	print(f"  +{message}=: Start: {str_t:%H:%M:%S}")
	if not Meta_dta :
		message +=' Nothing to do here move on\nNoMetadata'
		raise Exception(message)
	try:
		Vi_strms, Au_strms, Su_strms, Metata = Pars_MetaData(
			Meta_dta, verbose=False)

		_mtdta = dict(filename='',
					  nb_streams=int(0),
					  duration=float(0),
					  bit_rate=int(0),
					  size=float(0)
					  )
		Parse_from_to(Metata, _mtdta)

		_vdata = dict(index=int(0),
					  codec_name='',
					  width=int(0),
					  height=int(0),
					  coded_width=int(0),
					  coded_height=int(0),
					  bit_rate=int(0),
					  avg_frame_rate=''
					  )
# XXX: Presumes that the first Video is the real one !
		Parse_from_to(Vi_strms[0], _vdata)
		if 'Pu_la' in _mtdta.values():
			message += f" :O: Meta_dta has Pu_la\n"
			if 'Pu_la' in _mtdta['duration']:
				_mtdta['duration'] = '55'
			elif 'Pu_la' in _mtdta['bit_rate']:
				_mtdta['bit_rate'] = '66666'
			else:
				print(message)
				input('Meta WTF')
				raise ValueError(json.dumps(_mtdta, indent=2))

		mins,  secs = divmod(int(_mtdta['duration']), 60)
		hours, mins = divmod(mins, 60)

		Vi_Dur = f'{hours:02d}:{mins:02d}:{secs:02d}'
		frm_rate = float(String_div(_vdata['avg_frame_rate']))
		if frm_rate == 0:
			print(json.dumps(_vdata, indent=2))
			frm_rate = 25
		Tot_Frms = round(frm_rate * int(_mtdta['duration']))
		Vid_With = _vdata['width']

# XXX: Compute expected BitRate
		TotPixls = int(_vdata['width'] * _vdata['height'])
		# NOTE: Set BPP expectation between 0.1 to 0.05.
		BitPerPix = 0.075 * (1080 / Vid_With)
		# NOTE: Expected Bitrate
		XBitRt = round(TotPixls * BitPerPix * frm_rate)

# XXX: Print Banner
		message = f"    |< CT >|{f'{hours:02d}h:{mins:02d}m'} |{Vid_With:>4}x{_vdata['height']:<4} |Tfm: {HuSa(Tot_Frms):6}|Tpx: {HuSa(TotPixls)}|Xbr: {HuSa(XBitRt)}|Vid: {len(Vi_strms)}|Aud: {len(Au_strms)}|Sub: {len(Su_strms)}|"
		print(message)

# XXX: Video
		ff_video = []
		if len(Vi_strms) :
			extra = ''
			streamCounter = -1
			for _vid in Vi_strms:
				streamCounter += 1
				if 'bit_rate' in _vid:
					_vi_btrt = int(_vid['bit_rate'])
				else:
					# XXX approximation 80% video
					if _mtdta['bit_rate'] != "Pu_la":
						_vi_btrt = int(float(_mtdta['bit_rate']) * 0.80)
# XXX: There must be a better way
					else:
						_vi_btrt = 123456
					extra = ' BitRate Estimate'
# XXX: Print Banner
				if 'codec_name' not in _vid:
					_vid['codec_name'] = 'No Name ?'

				if _vid['codec_name'].lower() == 'mjpeg':
					print("    > Skipo MjpeG ")
#					ff_video.extend ( ['-map', '0:' + str(_vid['index']), '-c:v:' + str(_vid['index']), 'mjpeg'] )
					continue

				ff_video.extend(
					['-map', '0:' + str(_vid['index']), '-c:v:' + str(streamCounter)])

				if frm_rate > Max_frm_r:
					#					ff_video.extend( [ '-r', '25', 'libx265', '-crf', '26' ] )
					message = f"    ! FYI Frame rate convert {frm_rate} to 25"
					print(message)

				Bl_and_Wh = False

				if Bl_and_Wh:  # Black and White
					ff_video.extend(['libx265', '-vf', 'hue=s=0'])
					input('Black and White')
				elif _vid['height'] > 2160:  # # XXX: Scale Down to
					extra += '\t\tScale Down'
					ff_video.extend(['-vf scale=-1:1440 libx265'])
				elif _vi_btrt < XBitRt :  # XXX: 25% grace :D
					if _vid['codec_name'] == 'hevc':
						extra += '\t\t\tHevc'
						ff_video.extend(['copy'])
					else:
						extra += '\t\t\tConvert to Hevc '
						if _vid['height'] > 620:
							ff_video.extend(['libx265'])
						elif _vid['height'] > 240:
							ff_video.extend(['libx265'])
						else:
							ff_video.extend(['libx265'])
				else:
					if _vid['codec_name'] == 'hevc':
						extra += '\t\t\tHevc Reduce Bitrate'
						ff_video.extend(
							['libx265', '-b:v', str(XBitRt)])
					else:
						extra += '\t\t\tConvert to Hevc'
						ff_video.extend(
							['libx265'])

				message = f"    |<V:{_vid['index']:2}>|{_vid['codec_name']:7} |Br: {HuSa(_vi_btrt):>6}|Fps: {frm_rate:>6}| {extra}"
				print(message)

		else:
			print('    |<V:No>| No Video')

# XXX: Audio
		Special = False
		ff_audio = []
		if len(Au_strms) :
			Aud_typ = []
			_au_btrt = 0
			extra = ''
			streamCounter = -1
			for _aud in Au_strms:
				streamCounter += 1
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
					Parse_from_to(_aud['tags'], _lng)
					if 'Pu_la' in _lng:
						_lng['language'] = '_wtf?_'
				if 'disposition' in _aud:
					Parse_from_to(_aud['disposition'], _disp)

				ff_audio.extend(
					['-map', '0:' + str(_aud['index']), '-c:a:' + str(streamCounter)])

				if (_aud['codec_name'] in ('aac', 'opus', 'vorbis')) and (_aud['channels'] < 3):
					if _au_btrt <= Max_a_btr:
						extra += '\tCopy'
						ff_audio.extend(['copy'])
					else:
						extra += '\tReduce BitRate'
						ff_audio.extend(['libvorbis', '-q:a', '7'])
				else:
					extra += '\tConvert'
					ff_audio.extend(['libvorbis', '-q:a', '7', '-ac', '2'])

				Aud_typ.append(_lng['language'])

			if len(Aud_typ) > 1:
				if ('eng' in Aud_typ) and (('rus' in Aud_typ) or ('und' in Aud_typ)):
					# XXX: Select English only
					extra += f"Map only English"
					ff_audio = ['-map', '0:m:language:eng']
					print(f"    | {len (Aud_typ)} AuD: {Aud_typ} => {ff_audio}")
					Special = True

			message = f"    |<A:{_aud['index']:2}>|{_aud['codec_name']:7} |Br: {HuSa(_au_btrt):>6}|Fq : {HuSa(_aud['sample_rate']):>6}|Ch: {_aud['channels']}|{_lng['language']}|{_disp['default']}| {extra}"
			print(message)
		else:
			print('    |<A:No>| No Audio')

# XXX subtitle
		ff_subtl = []
		if len(Su_strms) :
			streamCounter = -1
			for _sub in Su_strms:
				streamCounter += 1
				extra = ''
				_lng = dict(language='')
				if 'tags' in _sub:
					Parse_from_to(_sub['tags'], _lng)
					if 'Pu_la' in _sub['tags']:
						_lng['language'] = 'wtf'
				ff_subtl.extend(
					['-map', '0:' + str(_sub['index']), '-c:s:' + str(streamCounter)])

# XXX: #https://askubuntu.com/questions/214199/how-do-i-add-and-or-keep-subtitles-when-converting-video
				if _sub['codec_name'] in ('hdmv_pgs_subtitle', 'dvd_subtitle'):
					extra += f"Skip {_sub['codec_name']}"
					ff_subtl = []  # XXX: Dont know how to do it :(
				else:
					if _lng['language'] == 'eng':
						extra += f"Move Make Default {_sub['codec_name']}"
						ff_subtl.extend(
							['mov_text', '-metadata:s:s:' + str(streamCounter) , 'language=' + _lng['language'], '-disposition:s:s:'+ str(streamCounter), 'forced'])
#                    elif _lng['language'] not in ('eng', 'rus'):
#                        Sub_fi_name = Ini_file + '_' + \
#                            str(_lng['language']) + '_' + \
#                            str(_sub['index']) + '.srt'
#                        extra = '* Xtract *'
#                        ff_subtl.extend(['mov_text', '-y', Sub_fi_name])
					else:
						#						ff_subtl = ['-map', '0:s', '-c:s', 'mov_text', '-metadata:s:s:0', 'language=' + _lng['language'] ]
						extra += f"Move Default {_sub['codec_name']}"
						ff_subtl.extend(
							['mov_text', '-metadata:s:s:' + str(streamCounter), 'language=' + _lng['language']])

				message = f"    |<S:{_sub['index']:2}>|{_sub['codec_name']:7}|{_sub['codec_type']:^10} |{_lng['language']:3}| {extra}"
				print(message)

		else:
			print('    |<S:No>| No Subtitle')

	except Exception as e:
		message = f"FFZa_Brain: Exception => {e}\n"
		print(message)
		raise Exception(message)

	else:
		FFM_cmnd = ff_video + ff_audio + ff_subtl

		_, extens = os.path.splitext(Ini_file)
		if len(Au_strms) :
			_au_code  = _aud['codec_name']
		else :
			_au_code  = 'HoFuckingAudio'

		if not Special and (extens.lower() in TmpF_Ex.lower()) and (_vid['codec_name'] == 'hevc') and (_au_code in ('aac', 'opus', 'vorbis', 'nofuckingaudio')):
			# XXX: 25% Generous Bitrate forgiveness for Video and audio
			if _vi_btrt <= XBitRt * 1.25 and _au_btrt <= Max_a_btr * 1.25 and _vid['height'] <= 1440 :
				message = f"    |V={_vid['codec_name']}|A={_au_code:^5}| =>  _Skip_it"
				raise ValueError( message )
#				FFM_cmnd = ['-c', 'copy']

		end_t = datetime.datetime.now()
		print(
			f'  -End  : {end_t:%H:%M:%S}\tTotal: {(end_t-str_t).total_seconds()}')

	return FFM_cmnd
##>>============-------------------<  End  >------------------==============<<##


def Prog_cal(line_to, sy=False):
	global Vi_Dur
	message = sys._getframe().f_code.co_name + '-:'

	if 'size=N/A' in line_to:
		_P = f"\r    | {sy} | Work:"
	elif 'global headers:' and "muxing overhead:" in line_to:
		_P = f'\n    |>+<| Done: {line_to}'
	elif 'speed=' in line_to:
		try:
			fr = re.search(r'frame=\s*([0-9]+)',	 line_to).group(1)
			fp = re.search(r'fps=\s*([0-9]+)',		 line_to).group(1)
			sz = re.search(r'size=\s*([0-9]+)',		 line_to).group(1)
			tm = re.search(r'time=\S([0-9:]+)',		 line_to).group(1)
			# Can have value of N/A
			br = re.search(r'bitrate=\s*([0-9\.]+)', line_to).group(1)
			# Can have value of N/A
			sp = re.search(r'speed=\s*([0-9\.]+)',	 line_to).group(1)
			if int(fp) > 0:
				a_sec = sum(int(x) * 60**i for i,
							x in enumerate(reversed(tm.split(":"))))
				b_sec = sum(int(x) * 60**i for i,
							x in enumerate(reversed(Vi_Dur.split(":"))))
				dif = abs(b_sec - a_sec)
				eta = round(dif / (float(sp)))
				mins, secs = divmod(int(eta), 60)
				hours, mins = divmod(mins, 60)
				_eta = f'{hours:02d}:{mins:02d}:{secs:02d}'
				_P = f'\r    | {sy} |Size: {HuSa(sz):>5}|Frames: {int(fr):>5}|Fps: {fp:>3}|BitRate: {br:>6}|Speed: {sp:>5}|ETA: {_eta:>8}|'
			else:
				_P = ''
		except Exception as e:
			print(line_to)
			message += f" ErRor: in Procesing data {e}:"
			raise Exception(message)
	else:
		_P = f'\n    |WTF| {line_to}'
		return False
#	print(_P, end='')
	sys.stderr.write(_P)
	sys.stderr.flush

	return True
##>>============-------------------<  End  >------------------==============<<##
