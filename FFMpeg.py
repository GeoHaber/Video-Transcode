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

Vi_Dur		= '30:00'

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
		if DeBug :
			print ("    |>-:", Cmd )
			input ("Ready to Do it? ")
			ff_out = subprocess.run( Cmd,
				universal_newlines=True,
				encoding='utf-8' )
			errcode  = ff_out.returncode
			if errcode :
				message += " ErRor: ErRorde {!r}".format( errcode )
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
		ff_out.kill()
		message += " ErRor: {!r} CalledProcessError :".format( err )
		if DeBug : print( message ) , input('Next')
		raise Exception( '$hit ', message )
	except Exception as e:
		ff_out.kill()
		message += " ErRor: Exception {!r}:".format( e )
		if DeBug : print( message ) , input('Next')
		raise Exception( '$hit ', message )
	else:
		while ff_out.poll() is None:
			lineo    = ff_out.stdout.readline()
			errcode  = ff_out.returncode
			if DeBug : print("<Line:{} Err:{}>".format( lineo.rstrip('\r\n') ), errcode)
#			stderri  = ff_out.stderr
			if errcode :
				message += " ErRor: ErRorde {!r} stderr {!r}:".format( errcode )
				print( message )
				raise ValueError ( '$hit ', message )
			elif 'frame=' in lineo :
				Prog_cal (lineo, symbs[loc])
				loc += 1
				if loc == len(symbs) :
					loc = 0
			elif 'global headers:' and "muxing overhead:" in lineo :
				print('\n|>+<| {}'.format( lineo ) )
			else :
				if DeBug : print("<{}>".format( lineo) )
				if lineo is None :
					break

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

def Prog_cal ( line_to, sy=False ) :
#	global DeBug
#	DeBug = True
	global Vi_Dur
	message  = sys._getframe().f_code.co_name + '-:'

	cnt = 0
	_P =''
	if DeBug : print ("\r", line_to, sy), input(message)
	if not line_to and sy :
		sy = "\r    | {} |Working:".format(sy)
		sys.stderr.write( sy )
		sys.stderr.flush
	elif 'frame=' and 'bitrate=' and not 'N/A' in line_to :
		try :
			fr	= re.search( r'frame=\s*([0-9]+)',		line_to ).group(1)
			fp	= re.search( r'fps=\s*([0-9]+)',		line_to ).group(1)
			sz	= re.search( r'size=\s*([0-9]+)',		line_to ).group(1)
			tm	= re.search( r'time=\S([0-9:]+)',		line_to ).group(1)
			br	= re.search( r'bitrate=\s*([0-9\.]+)',	line_to ).group(1)	#Can have value of N/A
			sp	= re.search( r'speed=\s*([0-9\.]+)',	line_to ).group(1)	#Can have value of N/A
			if int(fp) > 0 :
				a_sec = sum( int(x)*60**i for i, x in enumerate( reversed( tm.split(":"))) )
				b_sec = sum( int(x)*60**i for i, x in enumerate( reversed( Vi_Dur.split(":"))) )
				dif   = abs( b_sec - a_sec )
				eta   = round( dif / (float(sp) ))
				mins, secs  = divmod(int(eta), 60)
				hours, mins = divmod( mins, 60)
				_Dur = '{:02d}:{:02d}:{:02d}'.format(hours, mins, secs)
				if not cnt :
					fp_av = float( fp )
					sp_av = float( sp )
				else :
					fp_av += round( (float( fp ) - fp_av ) / cnt )
					sp_av += round( (float( sp ) - sp_av ) / cnt )
				_P   = '\r    | {} |Frame= {}|Fps= {}|BitRate= {}|Speed= {}|Size= {}Kb|Time Left= {}| '.format(sy, fr, fp_av, br, sp_av, sz, _Dur)
				cnt += 1

		except Exception as e:
			print (line_to)
			message += " ErRor: in Procesing data {!r}:".format( e )
			raise  Exception( message )
		else:
			sys.stderr.write( _P )
			sys.stderr.flush
	else :
		_P = '\r' + line_to
		sys.stderr.write( _P )
		sys.stderr.flush
	return True
##===============================   End   ====================================##

if __name__=='__main__':
#	global DeBug
#	DeBug = False

	start_time	= datetime.datetime.now()
	message 	= sys._getframe().f_code.co_name + '-:'
	print("{}\t\tStart: {:%H:%M:%S}".format( message, start_time ) )
	print ("{}".format ('='*60))
#	cgitb.enable(format='text')
