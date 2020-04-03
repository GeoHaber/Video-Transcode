# -*- coding: utf-8 -*-
#!/usr/bin/python3
__author__ = 'GeoHaZen'
'''
@author: 	  GeoHaZen
# XXX KISS
'''

import os
import re
import sys
import json
import cgitb
import shutil
import random
import datetime
import traceback
import subprocess
from My_Utils import *

DeBug = False

Vi_Dur = '30:00'

Tmp_F_Ext	= '.mp4'

Excepto	= 'C:\\Users\\Geo\\Desktop\\Except'

# XXX:

ffmpeg_bin  = 'C:\\Program Files\\ffmpeg\\bin'
ffmpeg_exe  = "ffmpeg.exe"
ffprobe_exe = "ffprobe.exe"
ffmpeg		= os.path.join( ffmpeg_bin, ffmpeg_exe  )
ffprobe		= os.path.join( ffmpeg_bin, ffprobe_exe )
##>>============-------------------<  End  >------------------==============<<##

def FFProbe_run (File_in, Execute= ffprobe ):
#	DeBug = True

	start_time = datetime.datetime.now()
	message = sys._getframe().f_code.co_name + '|:'
	print(f"  {message}\t\tStart: {start_time:%H:%M:%S}")

	if os.path.exists(File_in):
		file_size = os.path.getsize(File_in)
		message = f"\n{File_in}\t{HuSa(file_size)}\n"
		if DeBug:
			print(message)
	else:
		message += f"No Input file:( \n {File_in}"
		print(message)
		if DeBug:
			input('Now WTF?')
		return False

	Comand = [Execute,
			  '-analyzeduration', '2000000000',
			  '-probesize',       '2000000000',
			  '-i', File_in,
			  '-v', 'verbose',		# XXX quiet, panic, fatal, error, warning, info, verbose, debug, trace
			  '-of', 'json',		# XXX default, csv, xml, flat, ini
			  '-hide_banner',
			  '-show_format',
			  '-show_error',
			  '-show_streams']

	try:
		ff_out = subprocess.run(Comand,
								stdout=subprocess.PIPE,
								stderr=subprocess.PIPE,
								universal_newlines=True,
								encoding='utf-8')
	except subprocess.CalledProcessError as err:  # XXX: TBD Fix error in some rare cases
		message += f" FFProbe: CalledProcessError {err}"
		if DeBug:
			print(message), input('Next')
		raise Exception(message)
	else:
		out = ff_out.stdout
		err = ff_out.stderr
		bad = ff_out.returncode
		if bad:
			message += f"Oy vey {bad}\nIst mir {err}\n"
			if DeBug:
				print(message), input("Bad")
			raise ValueError(message)
		else:
			jlist = json.loads(out)
			if len(jlist) < 2:
				message += f"Json out to small\n{File_in}\n{jlist}"
				if DeBug:
					print(message), input(" Jlist to small ")
				raise Exception(message)
		end_time = datetime.datetime.now()
		Tot_time = end_time - start_time
		Tot_time = Tot_time.total_seconds()
		print(f'   End  : {end_time:%H:%M:%S}\tTotal: {Tot_time}')
		return jlist
##>>============-------------------<  End  >------------------==============<<##

def FFMpeg_run ( Fmpg_in_file, Za_br_com, Execute= ffmpeg ) :
#	DeBug = True
	global Tot_Frms

	start_time = datetime.datetime.now()
	message = sys._getframe().f_code.co_name + '-:'
	print(f"  {message}\t\tStart: {start_time:%H:%M:%S}")

# XXX FileName for the Title ...
	Sh_fil_name = os.path.basename(Fmpg_in_file).title()
	Sh_fil_name, xt = os.path.splitext(Sh_fil_name)
	Sh_fil_name += Tmp_F_Ext

	Fmpg_ou_file  = '_'+ Random_String( 11 ) + Tmp_F_Ext

	Title = 'title=\" ' + Sh_fil_name + " x265 Encoded By: " + __author__ + " Master "

	ff_head = [Execute, '-i', Fmpg_in_file, '-hide_banner']
	ff_tail = ['-metadata', Title, '-movflags', '+faststart',
			   '-fflags', 'genpts', '-y', Fmpg_ou_file]

	Za_br_com = ['-map 0']

	Cmd = ff_head + Za_br_com + ff_tail

	loc = 0
	symbs = '|/-+\\'
	try:
		if DeBug or True:
			print("    |>-", Cmd)
			input("Ready to Do it? ")
			ff_out = subprocess.run(Cmd,
									universal_newlines=True,
									encoding='utf-8')
			errcode = ff_out.returncode
			if errcode:
				message += f" ErRor: ErRorde {errcode}"
				print(message)
				input('Next')
				raise Exception('$hit ', message)
			input("Are we Done?")
			return Fmpg_ou_file
		else:
			print("    |>=", Cmd[4:-8])  # XXX:  Skip First 4 and Last 6
			ff_out = subprocess.Popen(Cmd,
									  stdout=subprocess.PIPE,
									  stderr=subprocess.STDOUT,
									  universal_newlines=True,
									  encoding='utf-8')
	except subprocess.CalledProcessError as err:
		ff_out.kill()
		message += f" ErRor: {err} CalledProcessError :"
		if DeBug:
			print(message), input('Next')
		if os.path.exists(Fmpg_ou_file):
			os.remove(Fmpg_ou_file)
		raise Exception('$hit ', message)
	except Exception as e:
		ff_out.kill()
		message += f" ErRor: Exception {e}:"
		if DeBug:
			print(message), input('Next')
		if os.path.exists(Fmpg_ou_file):
			os.remove(Fmpg_ou_file)
		raise Exception('$hit ', message)
	else:
		while ff_out.poll() is None:
			lineo = ff_out.stdout.readline()
			stderri = ff_out.stderr
			if DeBug:
				print(f"<{lineo}>")
			errcode = ff_out.returncode
			if errcode:
				message += f" ErRor: ErRorde {errcode} stderr {stderri}:"
				print(message)
				raise ValueError('$hit ', message)
			elif 'frame=' in lineo:
				Prog_cal(lineo, symbs[loc])
				loc += 1
				if loc == len(symbs):
					loc = 0
			elif 'global headers:' and "muxing overhead:" in lineo:
				print(f'\n|>+<| {lineo}')

	end_time = datetime.datetime.now()
	Tot_time = end_time - start_time
	Tot_time = Tot_time.total_seconds()
	print(f'   End  : {end_time:%H:%M:%S}\tTotal: {Tot_time}')

	if not os.path.exists(Fmpg_ou_file):
		message += ' No Out File Error '
		print(message)
		raise Exception('$hit ', message)
	elif os.path.getsize(Fmpg_ou_file) < Min_fsize:
		message += ' File Size Error'
		print(message)
		os.remove(Fmpg_ou_file)
		raise Exception('$hit ', message)
	else:
		message += "   FFMpeg Done !!"

	print(message)
	return Fmpg_ou_file
##>>============-------------------<  End  >------------------==============<<##


def FFClean_up(Inp_file, Out_file):
	#	DeBug	= True
	start_time = datetime.datetime.now()
	message = sys._getframe().f_code.co_name + '\\:'
	print("  {}\t\tStart: {:%H:%M:%S}".format(message, start_time))

# XXX: Check that in and out Files are okay
	Ou_fi_sz = os.path.getsize(Out_file)
	In_fi_sz = os.path.getsize(Inp_file)

	Ratio = round(100 * ((Ou_fi_sz - In_fi_sz) / In_fi_sz))
	if DeBug:
		if Ratio < 2:
			print("\tNew {} {} Smaller".format(Ratio, '%'))
		elif Ratio > -2:
			print("\tNew {} {} Larger ".format(abs(Ratio), '%'))
		else:
			print("\tSimilar {} {}".format(Ratio, '%'))
		print("Ini\t{}\tOut\t{}\tRat\t{}".format(In_fi_sz, Ou_fi_sz, Ratio))
		input('Next')
	try:
		# Create "Delete Me File" from the Original file (Inp_file)
		To_delete = New_File_Name(Inp_file, "DeletMe.old", Tmp_F_Ext)
		check_path = os.path.exists(To_delete)
		if check_path:  # File alredy exists TODO let's create another one ??
			print("\t.old Alredy Exists Rm:", To_delete)
			if DeBug:
				print(
					f"\nRnm: {os.path.basename(Inp_file)}\nTo:  {os.path.basename(To_delete)}")
			time.sleep(2)
			os.remove(To_delete)
		os.rename(Inp_file, To_delete)
# XXX: Extract ext remouve and change to Tmp_F_Ext for ImpFile
		f_name, xt = os.path.splitext(Inp_file)
		New_out_File = New_File_Name(Inp_file, Tmp_F_Ext, xt)
		if DeBug:
			print("Creating", New_out_File), input("Shall we?")
		check_path = os.path.exists(New_out_File)
		if check_path:  # File alredy exists TODO let's deleate it to create another one
			print("\t Alredy Exists Must Rm:", New_out_File)
			time.sleep(2)
			os.remove(New_out_File)
		shutil.move(Out_file, New_out_File)
		os.utime(New_out_File, None)  # XXX:  None means now !

		check_path = os.path.exists(New_out_File)
		if not check_path:
			print(message, " Big BuBu ", New_out_File, Out_file)
			return False
		else:
			if DeBug:
				print(
					f"\nRnm: {os.path.basename(Out_file)}\nTo:  { os.path.basename(New_out_File)}")

	except Exception as ex:
		message += f" FFClean_up Exception {ex}"
		if DeBug:
			print(message), input('Next')
		raise ValueError('$hit ', message)

	else:
		message = "File: {}\nWas: {}\tIs: {}\tSaved: {} = {} % \n".format(
			os.path.basename(Inp_file), HuSa(In_fi_sz), HuSa(Ou_fi_sz), HuSa(In_fi_sz - Ou_fi_sz), Ratio)
		print(message)
		# XXX Create the Lock file with utf-8 encode for non english caracters ... # XXX:
		end_time = datetime.datetime.now()
		Tot_time = end_time - start_time
		Tot_time = Tot_time.total_seconds()
		print('   End  : {:%H:%M:%S}\tTotal: {}'.format(end_time, Tot_time))

		return (1 + abs(In_fi_sz - Ou_fi_sz))
##>>============-------------------<  End  >------------------==============<<##


def Prog_cal(line_to, sy=False):
	#	DeBug = True
	global Vi_Dur
	message = sys._getframe().f_code.co_name + '-:'

	_P = ''
	if DeBug:
		print("\r", line_to, sy), input(message)
	if not line_to and sy:
		sy = f"\r    | {sy} |Working:"
		sys.stderr.write(sy)
		sys.stderr.flush
	elif 'frame=' and 'bitrate=' and not 'N/A' in line_to:
		try:
			fr = re.search(r'frame=\s*([0-9]+)',		line_to).group(1)
			fp = re.search(r'fps=\s*([0-9]+)',		line_to).group(1)
			sz = re.search(r'size=\s*([0-9]+)',		line_to).group(1)
			tm = re.search(r'time=\S([0-9:]+)',		line_to).group(1)
			# Can have value of N/A
			br = re.search(r'bitrate=\s*([0-9\.]+)',	line_to).group(1)
			# Can have value of N/A
			sp = re.search(r'speed=\s*([0-9\.]+)',	line_to).group(1)
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
				_P = f'\r    | {sy} |Size: {HuSa(sz):7}|Frames: {int(fr):6,}|Fps: {fp}|BitRate: {HuSa(br)}|Speed: {sp}|ETA: {_eta}|   '

		except Exception as e:
			print(line_to)
			message += f" ErRor: in Procesing data {e}:"
			raise Exception(message)
		else:
			sys.stderr.write(_P)
			sys.stderr.flush
	else:
		_P = '\r' + line_to
		sys.stderr.write(_P)
		sys.stderr.flush
	return True
##>>============-------------------<  End  >------------------==============<<##

if __name__=='__main__':
#	DeBug = True

	cgitb.enable(format='text')

	message = __file__ +'-:'
	print( message )

	start_time	= datetime.datetime.now()
	message 	= sys._getframe().f_code.co_name + '-:'
	print("{}\t\tStart: {:%H:%M:%S}".format( message, start_time ) )
	print ("{}".format ('='*60))
#	cgitb.enable(format='text')
##>>============-------------------<  End  >------------------==============<<##
