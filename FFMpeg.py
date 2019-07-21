# -*- coding: utf-8 -*-
#!/usr/bin/python3
__author__ = 'GeoHaZen'
'''
@author: 	  GeoHaZen
#XXX KISS
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
from   My_Utils  import *

DeBug		= False

Vi_Dur		= '0:0'

Out_F_typ	= '.mkv'
Tmp_F_Ext	= '_XY_' + Out_F_typ

Excepto	= 'C:\\Users\\Geo\\Desktop\\Except'

# XXX:

ffmpeg_bin  = 'C:\\Program Files\\ffmpeg\\bin'
ffmpeg_exe  = "ffmpeg.exe"
ffprobe_exe = "ffprobe.exe"
ffmpeg		= os.path.join( ffmpeg_bin, ffmpeg_exe  )
ffprobe		= os.path.join( ffmpeg_bin, ffprobe_exe )

def FFProbe_run (File_in, Execute= ffprobe ):
#	global DeBug
#	DeBug = True

	start_time	= datetime.datetime.now()
	message 	= sys._getframe().f_code.co_name + '|:'
	print("  {}\t\tStart: {:%H:%M:%S}".format( message, start_time ) )

	if os.path.exists (File_in) :
		file_size = os.path.getsize(File_in)
		message = "\n{}\t{}\n".format(
					File_in, HuSa(file_size) )
		if DeBug :	print ( message )
	else :
		message += "No Input file:( \n {}" .format( File_in )
		print (message)
		if DeBug : input ('Now WTF?')
		return False

	Comand = [ Execute ,
		'-analyzeduration', '2000000000',
		'-probesize',       '2000000000',
		'-i', File_in,
		'-v', 'verbose',		# XXX quiet, panic, fatal, error, warning, info, verbose, debug, trace
		'-of', 'json',			# XXX default, csv, xml, flat, ini
		'-hide_banner',
		'-show_format',
		'-show_error',
	#		'-count_frames',
	#		'-show_programs',
	#		'-show_pixel_formats',
	#		'-show_private_data',
		'-show_streams' ]

	try :
		ff_out = subprocess.run( Comand,
				stdout	= subprocess.PIPE,
				stderr	= subprocess.PIPE,
				universal_newlines = True,
				encoding='utf-8')
	except subprocess.CalledProcessError as err:	# XXX: TBD Fix error in some rare cases
		message += " FFProbe: CalledProcessError" .format( err )
		if DeBug : print( message ), input ('Next')
		raise  Exception( message )
	else:
		out	= ff_out.stdout
		err = ff_out.stderr
		bad = ff_out.returncode
		if bad  :
			message += "Oy vey {!r}\nIst mir {!r}\n".format( bad, err)
			if DeBug : print( message ), input("Bad")
			raise ValueError( message )
		else:
			jlist = json.loads( out )
			if len (jlist) < 2 :
				message += "Json out to small\n{}\n{!r}".format( File_in, jlist )
				if DeBug : print( message ), input(" Jlist to small ")
				raise  Exception( message )
		end_time    = datetime.datetime.now()
		Tot_time	= end_time - start_time
		Tot_time 	= Tot_time.total_seconds()
		print('   End  : {:%H:%M:%S}\tTotal: {}'.format( end_time, Tot_time ) )
		return jlist
##===============================   End   ====================================##

def FFMpeg_run ( Fmpg_in_file, Za_br_com, Execute= ffmpeg ) :
#	global DeBug
#	DeBug = True

	start_time	= datetime.datetime.now()
	message 	= sys._getframe().f_code.co_name + '-:'
	print("  {}\t\tStart: {:%H:%M:%S}".format( message, start_time ) )

#XXX FileName for the Title ...
	Sh_fil_name    = os.path.basename( Fmpg_in_file ).title()
	Sh_fil_name,xt = os.path.splitext( Sh_fil_name )
	Sh_fil_name   += Out_F_typ

	Fmpg_ou_file  = '_'+ Random_String( 15 ) + Tmp_F_Ext

	Title         = 'title=\" ' +Sh_fil_name + " (x265-aac) Encoded By: " + __author__ + " Master "

	ff_head  = [Execute, '-i', Fmpg_in_file, '-hide_banner' ]
	ff_tail  = [ '-metadata', Title, '-movflags', '+faststart', '-fflags', 'genpts', '-y', Fmpg_ou_file ]

	Cmd      = ff_head + Za_br_com + ff_tail

	loc		= 0
	symbs	= '|/-+\\'
	try :
		if DeBug  :
			print ("    |>-:", Cmd )
			input ("Ready to Do it? ")
			ff_out = subprocess.run( Cmd,

						universal_newlines=True,
						encoding='utf-8' )
			message += "\n Out: {!r}\n".format( ff_out )
			errcode  = ff_out.returncode
			stderri  = ff_out.stderr
			if errcode or stderri :
				message += " ErRor: ErRorde {!r} Stderr {!r}:".format( errcode, stderri )
				print( message )
				input('Next')
				raise Exception( '$hit ', message )
			input("Are we Done?")
			return Fmpg_ou_file
		else :
			print ("    |>-:", Cmd[4:-8] )	## XXX:  Skip First 4 and Last 6
			ff_out = subprocess.Popen( Cmd,
						stdout=subprocess.PIPE,
						stderr=subprocess.STDOUT,
						universal_newlines=True,
						encoding='utf-8' )
	except subprocess.CalledProcessError as err:
		message += " ErRor: {} CalledProcessError {!r}  {!r} :".format(err, err.returncode, err.output )
		if DeBug : print( message ) , input('Next')
		raise Exception( '$hit ', message )
	except Exception as e:
		message += " ErRor: Exception {}:".format( e )
		if DeBug : print( message ) , input('Next')
		raise Exception( '$hit ', message )
	else:
		while True :
			try:
				lineo    = ff_out.stdout.readline()
				if DeBug : print("<{!r}>".format (lineo))
				errcode  = ff_out.returncode
				stderri  = ff_out.stderr
				if errcode or stderri :
					message += " ErRor: ErRorde {!r} stderr {!r}:".format( errcode, stderri )
					print( message )
					raise ValueError ( '$hit ', message )
				elif 'frame=' in lineo :
					Prog_cal (lineo , symbs[loc])
					last_fr  = lineo.rstrip('\r\n')
					loc += 1
					if loc == len(symbs) :
						loc = 0
				elif 'global headers:' and "muxing overhead:" in lineo :
					last_fr = lineo.rstrip('\r\n')
# 			video:611001kB audio:55396kB subtitle:0kB other streams:0kB global headers:6kB muxing overhead: 0.480537%
					vis	= re.search( r'video:\s*([0-9\.]+)',	last_fr ).group(1)	#Can have value of N/A
					aus	= re.search( r'audio:\s*([0-9\.]+)',	last_fr ).group(1)	#Can have value of N/A
					print(  '\t  |><| Video = {} Audio = {}'.format (vis, aus) )
					print( ff_out.stdout )
					'''
					global Vi_Dur
					try :
						tm    = re.search ( r'time=\S([0-9:]+)', last_fr ).group(1)
						a_sec = sum( int(x)*60**i for i,x in enumerate( reversed(tm.split(":"))) )
						b_sec = sum( int(x)*60**i for i,x in enumerate( reversed(Vi_Dur.split(":"))) )
						dif   = abs (b_sec - a_sec)
						if dif :
							print ("\t  [-)] Lenght was={} is={} dif={}".format( Vi_Dur ,tm, dif ) )
						else :
							print ("\t  [:)] Lenght={}".format( Vi_Dur ) )
						# XXX: make it 1 percent a fixed number
						if dif < ( b_sec / 100 ) :
							pass
						else :
							print ("Diff to Big :( ",dif)
						if DeBug : time.sleep (2), input ("So ?")
					except Exception as e:
						message += " Got Time ErRor: Exception {}:".format( e )
						if DeBug : print( message ) , input('Next')
						raise ValueError ( '$hit ', message )
					'''
# XXX: ?? Not sure if it's the right way to deal with problem streems XXX
				elif lineo == '' :
					if DeBug :
						print("Done {!r}".format (lineo))
						print('\nStderr : {!r}'.format(ff_out.stderr) )
						print('\nStdout : {!r}'.format(ff_out.stdout) )
					if ff_out.poll() is not None:
						if DeBug :
							time.sleep(10)
#							input ("WWWWW")
						break
			except Exception as e:
				message += " ErRor: in Procesing data {!r}:".format( e )
				if DeBug : print( message ) , input('Next')
				raise ValueError ( '$hit ', message )
	finally :
		file_size	= os.path.getsize(Fmpg_in_file)
		check_path	= os.path.exists (Fmpg_ou_file)
		if check_path :
			n_file_size = os.path.getsize(Fmpg_ou_file)
			if n_file_size < (file_size/64) :	# XXX Kind of imposible
				message += " Size to small :" +os.path.basename(Fmpg_in_file) +' ' +HuSa(n_file_size)
				if DeBug : print( message ) , input('Next')
				raise ValueError ( '$hit ', message )
		else :
			message += ": Output Path not found "+ os.path.basename(Fmpg_in_file)
			if DeBug : print( message ) , input('Next')
			raise ValueError ( '$hit ', message )
		end_time    = datetime.datetime.now()
		Tot_time	= end_time - start_time
		Tot_time 	= Tot_time.total_seconds()
		print('   End  : {:%H:%M:%S}\tTotal: {}'.format( end_time, Tot_time ) )
		message +="   FFMpeg Done !!"
		print ( message )
		return Fmpg_ou_file
##===============================   End   ====================================##

def FFClean_up ( Inp_file, Out_file ):
#	global DeBug
#	DeBug = True

	start_time	= datetime.datetime.now()
	message 	= sys._getframe().f_code.co_name + '\\:'
	print("  {}\t\tStart: {:%H:%M:%S}".format( message, start_time ) )

## XXX: Check that in and out Files are okay
	check_path    = os.path.exists (Out_file )
	Out_file_size = os.path.getsize(Out_file)
	Ini_file_size = os.path.getsize(Inp_file)

	if not check_path:
		print ("\tPath not found: " + Out_file )
		if DeBug > 1 : input ("WTF ?")
		return False
	if not Out_file_size:
		print ("\tFile size Error" , Out_file_size )
		print ("\tNo New File Aborting script.")
		if DeBug > 1 : input ("WTF ?")
		return False
	if DeBug :
		print ("Pre :\n{} \t\t{}".format (Inp_file, HuSa(Ini_file_size)) )
		print ("Post:\n{} \t\t{}".format (Out_file, HuSa(Out_file_size)) )

	Ratio   =  round( 100*(( Out_file_size - Ini_file_size) / Ini_file_size))
	if DeBug:
		if  Ratio  <  2 :
			print ("\tNew {} {} Smaller".format(Ratio, '%'))
		elif Ratio > -2 :
			print ("\tNew {} {} Larger ".format(abs(Ratio), '%'))
		else :
			print ("\tSimilar {} {}".format(Ratio, '%'))
	if DeBug :
		print ("Ini\t{}\tOut\t{}\tRat\t{}".format( Ini_file_size, Out_file_size, Ratio ))
		input ('Next')
	try :
		# Create "Delete Me File" from the Original file (Inp_file)
		To_delete  = New_File_Name ( Inp_file, "DeletMe.old", Tmp_F_Ext  )
		check_path = os.path.exists( To_delete )
		if check_path : # File alredy exists TODO let's create another one ??
			print ("\t.old Alredy Exists Rm:", To_delete )
			time.sleep( 2 )
			try:
				os.remove( To_delete )
			except OSError as e:  ## if failed, report it back to the user ##
				message += " OSErRor: removing File {} {!r}:".format( To_delete, e )
				if DeBug : print( message ) , input('Next')
				raise ValueError ( '$hit ', message )
		os.rename ( Inp_file, To_delete)
		check_path = os.path.exists( To_delete )
		if check_path :
			if DeBug : print ("\tRnm: {}\tTo: {}".format( os.path.basename(Inp_file), os.path.basename(To_delete)) )
		else :
			print (message, " Big BuBu ", To_delete, Inp_file )
			return False
## XXX: Extract ext remouve and change to Out_F_typ for ImpFile
		f_name, xt 	 = os.path.splitext (Inp_file)
		New_out_File = New_File_Name (Inp_file, Out_F_typ, xt)
		if DeBug :
			print ("Creating" , New_out_File)
			input ("Shall we?")
		check_path   = os.path.exists (New_out_File)
		if check_path : # File alredy exists TODO let's deleate it to create another one
			print ("\t Alredy Exists Must Rm:", New_out_File )
			time.sleep( 3 )
			try:
				os.remove( New_out_File)
			except OSError as e:  ## if failed, report it back to the user ##
				message += " OSErRor: removing File {} {!r}:".format( New_out_File, e )
				if DeBug : print( message ) , input('Next')
				raise ValueError ( '$hit ', message )

		shutil.move ( Out_file, New_out_File)
		os.utime 	( New_out_File, None) 			## XXX:  None means now !

		check_path = os.path.exists( New_out_File )
		if not check_path :
			print (message, " Big BuBu " , New_out_File , Out_file )
			return False
		else :
			if DeBug : print ("\tRnm: {}\tTo: {}".format( os.path.basename(Out_file), os.path.basename(New_out_File)) )

	except Exception as ex :
		message += " FFClean_up Exception {!r}:".format( ex )
		if DeBug : print( message ) , input('Next')
		raise ValueError ( '$hit ', message )

	else :
		message = "File: {}\nWas: {}\tIs: {}\tSaved: {} = {} % \n".format(
					os.path.basename(Inp_file), HuSa(Ini_file_size), HuSa(Out_file_size), HuSa(Ini_file_size - Out_file_size), Ratio )
		print (message)
		# XXX Create the Lock file with utf-8 encode for non english caracters ... # XXX:
		end_time    = datetime.datetime.now()
		Tot_time	= end_time - start_time
		Tot_time 	= Tot_time.total_seconds()
		print('   End  : {:%H:%M:%S}\tTotal: {}'.format( end_time, Tot_time ) )

		return (1 + abs ( Ini_file_size - Out_file_size ) )
##===============================   End   ====================================##


def Prog_cal (line_to, sy=False ) :
#	global DeBug
#	DeBug = True
	global Vi_Dur
	message  = sys._getframe().f_code.co_name + '-:'

	_P =''
	last_fr  = line_to.rstrip('\r\n')
	if DeBug :
		print ("\r>" , last_fr)
	if 'frame=' and 'bitrate=' and not 'N/A' in last_fr :
		try :
			fr	= re.search( r'frame=\s*([0-9]+)',		last_fr ).group(1)
			fp	= re.search( r'fps=\s*([0-9]+)',		last_fr ).group(1)
			sz	= re.search( r'size=\s*([0-9]+)',		last_fr ).group(1)
			tm	= re.search( r'time=\S([0-9:]+)',		last_fr ).group(1)
			br	= re.search( r'bitrate=\s*([0-9\.]+)',	last_fr ).group(1)	#Can have value of N/A
			sp	= re.search( r'speed=\s*([0-9\.]+)',	last_fr ).group(1)	#Can have value of N/A
			if int(fp) > 0 :
				a_sec = sum( int(x)*60**i for i, x in enumerate( reversed( tm.split(":"))) )
				b_sec = sum( int(x)*60**i for i, x in enumerate( reversed( Vi_Dur.split(":"))) )
				dif   = abs( b_sec - a_sec )
				eta  = round( dif / (float(sp) ))
				mins, secs  = divmod(int(eta), 60)
				hours, mins = divmod( mins, 60)
				_Dur = '{:02d}:{:02d}:{:02d}'.format(hours, mins, secs)
				_P   = '\r| {} |Frame: {:>7}|Fps: {}|Siz: {:>6}|BitRate : {}|Speed: {}|Time Left: {}|  '.format(sy, fr, fp, sz, br, sp, _Dur)

		except Exception as e:
			print (last_fr)
			message += " ErRor: in Procesing data {!r}:".format( e )
			raise  Exception( message )
	else :
		print (last_fr)
	sys.stderr.write( _P )
	sys.stderr.flush
	return True
##===============================   End   ====================================##

if __name__=='__main__':
#	global DeBug
#	DeBug = True

	start_time	= datetime.datetime.now()
	message 	= sys._getframe().f_code.co_name + '-:'
	print("{}\t\tStart: {:%H:%M:%S}".format( message, start_time ) )
	print ("{}".format ('='*60))
#	cgitb.enable(format='text')
