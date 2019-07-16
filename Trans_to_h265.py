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
from	My_Utils import *
from	FFMpeg	 import *

DeBug		= False

Skip_First	= False

Vi_Dur		= '0:0'

Max_v_btr	= 2000000
Max_a_btr	= 380000
Max_frm_rt	= 35

Out_F_typ	= '.mkv'

Tmp_F_Ext	= '_XY_' + Out_F_typ

Excepto	= 'C:\\Users\\Geo\\Desktop\\Except'

Folder	= 'C:\\Users\\Geo\\Desktop\\downloads'
Folder = 'C:\\Users\\Geo\\Desktop\\Test'
#Folder	= 'E:\\Media\\Movie'
#Folder = '\\\\NAS-Q\\Multimedia\Movie'
#Folder	= 'D:\\Media'
#Folder	= 'C:\\Users\\Geo\\Desktop\\Except'

VIDEO_EXTENSIONS = ['.3g2', '.3gp', '.3gp2', '.3gpp', '.60d', '.ajp', '.asf', '.asx', '.avchd', '.avi', '.bik',
					'.bix', '.box', '.cam', '.dat', '.divx', '.dmf', '.dv', '.dvr-ms', '.evo', '.flc', '.fli',
					'.flic', '.flv', '.flx', '.gvi', '.gvp', '.h264', '.m1v', '.m2p', '.m2ts', '.m2v', '.m4e',
					'.m4v', '.mjp', '.mjpeg', '.mjpg', '.mkv', '.moov', '.mov', '.movhd', '.movie', '.movx', '.mp4',
					'.mpe', '.mpeg', '.mpg', '.mpv', '.mpv2', '.mxf', '.nsv', '.nut', '.ogg', '.ogm', '.ogv', '.omf',
					'.ps', '.qt', '.ram', '.rm', '.rmvb', '.swf', '.ts', '.vfw', '.vid', '.video', '.viv', '.vivo',
					'.vro', '.wm', '.wmv', '.wmx', '.wrap', '.wvx', '.wx', '.x264', '.xvid']

VIDEO_EXTENSIONS = [ '.avchd', '.avi', '.dat', '.divx', '.dv', '.flic', '.flv', '.flx', '.h264', '.m4v', '.mkv',
					'.moov', '.mov', '.movhd', '.movie', '.movx', '.mp4', '.mpe', '.mpeg', '.mpg', '.mpv', '.mpv2',
					'.ram', '.rm', '.rmvb', '.swf', '.ts', '.vfw', '.vid', '.video', '.viv', '.vivo',
					'.vro', '.wm', '.wmv', '.wmx', '.wrap', '.wvx', '.wx', '.x264', '.xvid']

This_File  = sys.argv[0].strip ('.py')
Log_File   = This_File + '_run.log'
Bad_Files  = This_File + '_bad.txt'
Good_Files = This_File + '_good.txt'

ffmpeg_bin  = 'C:\\Program Files\\ffmpeg\\bin'
ffmpeg_exe  = "ffmpeg.exe"
ffprobe_exe = "ffprobe.exe"
ffmpeg		= os.path.join( ffmpeg_bin, ffmpeg_exe  )
ffprobe		= os.path.join( ffmpeg_bin, ffprobe_exe )

##===============================   End   ====================================##

def Move_Del_File (src, dst, DeBug=False ):
#	DeBug = True
	message = sys._getframe().f_code.co_name + '-:'

	if DeBug :
		message += ' Src : {}\n Dest : {}' .format( src, dst)
		print( message )
		if DeBug > 3 :	input( message )
	try :
		if os.path.isdir(src) and os.path.isdir(dst) :
			shutil.copytree(src, dst)
		else:
			shutil.copy2(src, dst)
			if not DeBug :
				try :
					os.remove( src )
				except OSError as e:  ## if failed, report it back to the user ##
					message += "\n!Error: {}\n{} - {}\n".format(src, e.filename, e.strerror)
					if DeBug : print( message )
					raise  Exception( message )
			else:
				time.sleep ( 1 )
	except OSError as e:  ## if failed, report it back to the user ##
		message += "\n!Error: Src {} , Dst {}\n{} - {}\n".format(src, dst, e.filename, e.strerror)
		if DeBug : print( message )
		raise  Exception( message )
	else:
		if DeBug :
			print ( "\n", "=" *40)
			print( "Stack:{}\n".format( traceback.print_stack (limit=5) ) )
			print ( "\n", "-" *40)
			print( "Exec: {}\n".format( traceback.print_exc   (limit=5) ) )
			print ( "\n", "-" *40)
			input("\nNow what")
		return True
##===============================   End   ====================================##

def Create_File (dst, msge= '', times=1, DeBug=False ):
#	DeBug = True
	message = sys._getframe().f_code.co_name + '-:'
	if DeBug :
		message += "Dst {}\nMsg {} Times {}\n Press Any Key to go on" .format( dst, msge, times )
		input ( message )
		return  False
	else :
		try :
			Cre_lock = open( dst, "w",encoding="utf-8" )
			if Cre_lock :
				Cre_lock.write ( msge * times )
				Cre_lock.flush()
		except OSError as e:  ## if failed, report it back to the user ##
			message += "\n!Error: {}\n{} - {}\n".format( dst, e.filename, e.strerror)
			if DeBug : print (message)
			raise Exception( message )
	return True
##===============================   End   ====================================##

def Parse_year ( FileName ) :
#	DeBug = True
	message = sys._getframe().f_code.co_name + '-:'
	if DeBug :	print( message )
	try :
		yr	= re.findall( r'[\[\(]?((?:19[4-9]|20[0-1])[0-9])[\]\)]?', FileName )
		if yr :
			va	= sorted(yr, key=lambda pzd: int(pzd), reverse=True)
			za  = int(va[0])
			if za > 2019 or za < 1930 :
				za = 1954
			if DeBug : print ( FileName, yr, len(yr), va, za )
		else :
			za = 1890
	except :
		za = 1
	return za
##===============================   End   ====================================##

""" =============== The real McCoy =========== """
def Build_List ( Top_dir, Ext_types, Sort_loc=2, Sort_ord=True  ) : # XXX: Sort_ord=True (Big First) Sort_loc = 2 => File Size: =4 => year_made
	'''
	Create the list of Files to be proccesed
	'''
#	DeBug = True
	message    = sys._getframe().f_code.co_name + '-:'

	cnt 		= 0
	queue_list 	= []

	print("=" * 60)
	start_time = datetime.datetime.now()
	value = HuSa (get_tree_size ( Top_dir ) )
	print ( value )
	print('Start: {:%H:%M:%S}'.format( start_time ))

	# a Directory ?
	if os.path.isdir ( Top_dir ) :
		message  += "\n Directory Scan :{}".format( Top_dir )
		print ( message )
		for root, dirs, files in os.walk( Top_dir ):
			for one_file in files:
				x, extens = os.path.splitext( one_file.lower() )
				if extens in Ext_types :
					fi_path     = os.path.normpath( os.path.join( root, one_file ) )
					fi_size     = os.path.getsize( fi_path )
					fi_info     = os.stat( fi_path )
					year_made	= Parse_year( fi_path )
#XXX  |[0] Extension |[1] Full Path |[2] File Size |[3] File Info |[4] Year Made XXX
					Save_items	= extens, fi_path, fi_size, fi_info, year_made
					queue_list += [ Save_items ]
					if DeBug : print ("Add :{:>4} {:<70} {:^9} {} {:^6}".format(
								extens, one_file, HuSa(fi_size), fi_info, year_made ))
	# a File ?
	elif os.path.isfile ( Top_dir ) :
		message += " -> Single File Not a Directory: {}".format( Top_dir )
		print ( message )
		x,extens  = os.path.splitext( Top_dir.lower() )
		fi_path   = (Top_dir)
		fi_size   = os.path.getsize( fi_path)
		fi_info   = os.stat( fi_path)
		year_made = Parse_year ( fi_path)
#XXX  |[0] Extension |[1] Full Path |[2] File Size |[3] File Info |[4] Year Made XXX
		Save_items	= extens, fi_path, fi_size, fi_info, year_made
		queue_list += [ Save_items ]
		if DeBug : print ("Add :{:>4} {:<70} {:^9} {} {:^6}".format(
					extens, one_file, HuSa(fi_size), fi_info, year_made ))

	elif os.path.islink( Top_dir ) :
		print (Folder, " It's a Link")
		input ("WTF should I do now?")
		return False

	elif os.path.ismount( Top_dir) :
		print (Folder, " It's a Mountpoint")
		input ("WTF should I do now?")
		return False

	else :
		# XXX TODO : Read from file if flag -f XXX for now just read the same file
		message = "{} -> ./Excepton.log File ".format( message)
		print ( message )
		input ( "Ready ? Press CR")
		Exep_File = open("./Excepton.log", "r", encoding="utf-8")
		for line in Exep_File:
			cnt += 1
			if DeBug : print (line)
			fi_path   = line.replace("\n",'')
			if os.path.isfile ( fi_path ) :
				x, extens	= os.path.splitext( fi_path.lower() )
				fi_path		= os.path.normpath( os.path.join( root, one_file ) )
				fi_size		= os.path.getsize( fi_path)
				fi_info		= os.stat( fi_path)
				year_made	= Parse_year( fi_path)
#XXX  |[0] Extension |[1] Full Path |[2] File Size |[3] File Info |[4] Year Made XXX
				Save_items	= extens, fi_path, fi_size, fi_info, year_made
				queue_list += [Save_items]
				if DeBug : print ("Add :{:>4} {:<70} {:^9} {} {:^6}".format(
							extens, one_file, HuSa(fi_size), fi_info, year_made ))
			else :
				print ("No file named {}".format( fi_path))
				return False

# XXX: https://wiki.python.org/moin/HowTo/Sorting
# XXX: Sort based in item [2] = filesize defined by Sort_loc :)
	queue_list = sorted( queue_list, key=lambda Item: Item[Sort_loc], reverse=Sort_ord ) ## XXX: sort defined by caller

	if DeBug :
		print ("="*90)
		for each in queue_list :
			print ( each )
		input ("Next:")

	end_time    = datetime.datetime.now()
	print('End  : {:%H:%M:%S}\tTotal: {}'.format( end_time, end_time-start_time ) )
	return queue_list
##===============================   End   ====================================##

def Skip_Files ( One_descr, Min_fsize=10240 ) :
#	DeBug = True

	message = sys._getframe().f_code.co_name + '-:'

#XXX  |[0] Extension |[1] Full Path |[2] File Size |[3] File Info |[4] Year Made XXX
	The_file  = One_descr[1]
	fi_size   = One_descr[2]

# List Files in the to be prccesed Directory
	if DeBug > 1 :
		Dir, Fil = os.path.split ( The_file )
		print ("Dir : {}\nFile: {}".format( Dir, Fil))
		for root, dirs, files in os.walk( Dir ):
			for fi in files :
				print ("\tFile :",fi)

## XXX: File does not exist :(
	if not os.path.exists ( The_file ) :
		message += "\n File Not Found {}\n".format( The_file )
		print ( message )
		Exeptions_File.write(message)
		Exeptions_File.flush()
		sys.stdout.flush()
		if DeBug : input ("? Skip it ?")
		return False

# XXX Big enough to be video ??
	if fi_size < Min_fsize : # 256K should be One Mega byte 1048576
		message += "\n To Small:| {:9} | {}\n".format( HuSa(fi_size), The_file )
		print ( message )
		Exeptions_File.write(message)
		Exeptions_File.flush()
		sys.stdout.flush()
		if DeBug : input ("? Skip it ?")
		return False

## XXX:  Ignore files that have been Locked (Procesed before)
	Lock_File = The_file + ".lock"

	if os.path.exists ( Lock_File ) :
		message += "Locked {}\n".format( os.path.basename( Lock_File) )
		print (message)
		Succesful_File.write( message )
		Succesful_File.flush()
		sys.stdout.flush()
		return False

# XXX Scrub the Directory XXX New function
	if  Tmp_F_Ext in The_file :	#XXX Delete Unfinished files
		message += " Rm File:| {:9} | {}\n".format( HuSa(fi_size), The_file )
		try:
			os.remove( The_file )
		except OSError as e:  ## if failed, report it back to the user ##
			message += "\n!Error: Rm {}\n{} - {}.".format( The_file, e.filename, e.strerror)
			if DeBug : print (message)
			raise Exception( message )
		print ( message )
		Exeptions_File.write(message)
		Exeptions_File.flush()
		sys.stdout.flush()
		if DeBug : input ("? Delete it ?")

	return Lock_File
##===============================   End   ====================================##

def Do_it ( List_of_files, Excluded ='' ):
#	global DeBug
#	DeBug = True

	message = sys._getframe().f_code.co_name +'-:'
	print("=" * 60)
	print( message )
	print (' Total of {} Files to Procces'. format( len( List_of_files ) ) )

	if DeBug : print ("Proccesing {} one by one".format( List_of_files ) )

	if not List_of_files :
		raise ValueError( message, 'No files to procces' )

	elif len ( Excluded ) :
		message += " Excluding" + len (Excluded) + Excluded
		raise ValueError( message, 'Not Implemented yet :( ' )
# XXX: TBD Skip those in the List

	queue_list 	=[]
	Fnum  		= 0
	Saving 		= 0
	cnt         = len(List_of_files)

	for One_descr in List_of_files:
		start_time = datetime.datetime.now()
		print("-" * 20)
		Fnum     += 1
#XXX  |[0] Extension |[1] Full Path |[2] File Size |[3] File Info |[4] Year Made XXX
		extens	  = One_descr[0]
		The_file  = One_descr[1]
		fi_size   = One_descr[2]
		year_made = One_descr[4]
		message = ("{} / {}, {}, ({}), {}".format( ordinal(Fnum), cnt, HuSa(fi_size), year_made, The_file, extens ) )
		print ( message )
		print ( ' Start: {:%H:%M:%S}'.format(start_time))
		# XXX Scrub the Directory XXX New function
		Lock_File = Skip_Files( One_descr )
		if Lock_File :
			#XXX Do it XXX
			try:
				if DeBug : print ("\nDo> FFProbe_run")
				all_good = FFProbe_run( The_file )
				if  all_good :
					if DeBug : print ("\nDo> FFZa_Brain")
					all_good = FFZa_Brain( The_file, all_good )
					if  all_good :
						if DeBug : print ("\nDo> FFMpeg_run")
						all_good = FFMpeg_run( The_file, all_good )
						if  all_good :
							if DeBug : print ("\nDo> FFClean_up")
							all_good = FFClean_up( The_file, all_good )
							if  all_good :
								Saving += all_good
								if DeBug : print ("\nDo> Create_File Log ")
								all_good = Create_File ( Lock_File, message )
								if  all_good :
									cnt -= 1
									queue_list += [The_file]
									Succesful_File.write( The_file )
									Succesful_File.flush()
									if DeBug : print ('\nThe List ... \n{}'.format( json.dumps(queue_list, indent=2 )) )	#XXX should be One_descr after it was Modifyed XXX
									print ("  Total Saved {}".format( HuSa( Saving )) )
# XXX: Someting is Fish :O
								else :
									message = "Post FFClean_up : Lock_File Create ErRor: {} \n".format( Lock_File )
									print (message)
									Exeptions_File.write(message)
									Exeptions_File.flush()
									if os.path.exists (Lock_File) :
										try:
											os.remove(Lock_File)
										except OSError as e:  ## if failed, report it back to the user ##
											message += "\n!Error: Rm {}\n{} - {}.".format( Lock_File, e.filename, e.strerror)
											print (message)
											input("2x WTF! Lock File Create Error !")
											raise Exception( message )
										print ("Lock_File Removed: " ,Lock_File )
							else :
								message += '\n FFClean_up  ErRor :'
								message += '\n Copy & Delete {}\n' .format ( The_file )
								print (message)
								Exeptions_File.write(message)
	#							Create_File   ( Lock_File, message, 10, DeBug=True )
								Move_Del_File ( The_file, Excepto, DeBug=True )
						else:
							message += '\n FFMpeg_run  ErRor :'
							message += '\n Copy & Delete {}\n' .format ( The_file )
							print (message)
							Exeptions_File.write(message)
#							Create_File   ( Lock_File, message, 10, DeBug=True )
							Move_Del_File ( The_file, Excepto, DeBug=True )
					else :
						message += '\n FFZa_Brain  ErRor :'
						message += '\n Copy & Delete {}\n' .format ( The_file )
						print (message)
						Exeptions_File.write(message)
#						Create_File   ( Lock_File, message, 10, DeBug=True )
						Move_Del_File ( The_file, Excepto, DeBug=True )
				else :
					message += '\n FFProb   ErRor :'
					message += '\n Copy & Delete {}' .format ( The_file )
					print (message)
					Exeptions_File.write(message)
#					Create_File   ( Lock_File, message, 10 )
					Move_Del_File ( The_file, Excepto, DeBug=True )
			except ValueError as err:
				message += "\n\n ValueError Exception " .format ( err.args )
				print (message)
				if '_Skip_it :' in message :
					Succesful_File.write( message )
#					Create_File ( Lock_File, message )
				else:
					Exeptions_File.write( message )
					message += '\n Copy & Delete {}' .format ( The_file )
#					Create_File   ( Lock_File, message, 10, DeBug=True )
					Move_Del_File ( The_file, Excepto, DeBug=True )
			except Exception as e:
				message += " WTF? General Exception {}" .format(e)
				print ( "\n", "=" *40)
				print (message)
				print( "Stack:{}\n".format( traceback.print_stack (limit=5) ) )
				print ( "\n", "-" *40)
				print( "Exec: {}\n".format( traceback.print_exc   (limit=5) ) )
				print ( "\n", "-" *40)
#				Create_File   ( Lock_File, message, 100, DeBug=True )
				Move_Del_File ( The_file, Excepto, DeBug=True )
				Exeptions_File.flush()
				Succesful_File.flush()
				sys.stdout.flush()
				input ("## Bad Error :")
			else:
				pass
			Exeptions_File.flush()
			Succesful_File.flush()
			sys.stdout.flush()
		else :
			cnt -= 1
			queue_list += [The_file]
			Exeptions_File.flush()
			Succesful_File.flush()

		sys.stdout.flush()
		end_time    = datetime.datetime.now()
		print( ' End  : {:%H:%M:%S}\tTotal: {} '.format( end_time, end_time-start_time ) )
		print( '='*20)

	return queue_list
##===============================   End   ====================================##


def FFZa_Brain ( Ini_file, Meta_dta, verbose=False ) :
	global Vi_Dur	# make it global so we can reuse for fmpeg check ...
#	global DeBug
#	DeBug = True

	start_time	= datetime.datetime.now()
	message 	= sys._getframe().f_code.co_name + '/:'
	print("  {}\t\tStart: {:%H:%M:%S}".format( message ,start_time ) )

	frm_rate = 0

	Prst_all = []
	Au_strms = []
	Vi_strms = []
	Su_strms = []
	Dt_strms = []
	Xt_strms = []

	ff_subtl = []
	ff_audio = []
	ff_video = []

# TODO Parse Format
	try :
		_mtdta = dict(	nb_streams		= int(0),
						duration		= float(0),
						bit_rate		= int(0),
						size 			= float(0) )
		Parse_from_to ( Meta_dta['format'], _mtdta)
		Prst_all.append (_mtdta)
		if 'Pu_la' in _mtdta.values() :
			message += ":O: Meta_dta has Pu_la\n{}\n{}" .format( json.dumps( Meta_dta, indent=2, sort_keys=True ), _mtdta )
			print (message)
			if DeBug : input ('Meta WTF')
#			raise ValueError( message )
		_kdat = dict(	codec_type 		=''	)
		for rg in range ( _mtdta['nb_streams'] ) :
			Strm_X = Meta_dta['streams'][ rg ]
			key    = Parse_from_to ( Strm_X, _kdat )
			if DeBug > 1 :
				print ("  HAS :", key, '\n\n', json.dumps( Strm_X, indent=2, sort_keys=False ) )
				input ("  Next:")
# XXX: Is Video
			if key == 'video' 		:
				_vdata = dict(  index			= int(0),
								codec_name		= '',
								width			= int(0),
								height			= int(0),
								coded_width		= int(0),
								coded_height	= int(0),
								bit_rate 		= int(0),
								avg_frame_rate	= '',
								r_frame_rate	= '')
				Parse_from_to  (Strm_X, _vdata)
				Prst_all.append(_vdata)
				Vi_strms.append(_vdata)	# procces all before building command
				if DeBug > 1 :
					print ('\t{} in {}'.format( key, Vi_strms))
					print ('\tindx:{} : {} = {}\n{}'.format(_vdata['index'], key, Vi_strms , _vdata))
					input ("VID")
# XXX: Is Audio
			elif key == 'audio' 	:
				_adata = dict(	index			= int(0),
								codec_name		='',
								channels		=int(0),
								sample_rate 	=float(0),
								bit_rate		=int(0),
								disposition 	= '',
								tags 			= '')
				Parse_from_to  (Strm_X, _adata)
				Prst_all.append(_adata)
				Au_strms.append(_adata)	# procces all before building command
				if DeBug > 1 :
					print ('\t{} in {}'.format( key, Au_strms))
					print ('\tindx:{} : {} = {}\n{}'.format(_adata['index'], key, Au_strms ,_adata))
					input ("AUD")
# XXX: Is subtitle
			elif key == 'subtitle'	:
				_sdata	= dict(	index				= int(0),
								codec_name			= '',
								codec_type			= '',
								duration 			= float(0),
								disposition 		= '',
								tags 				= '' )
				Parse_from_to  (Strm_X, _sdata)
				Prst_all.append(_sdata)
				Su_strms.append(_sdata)
				if DeBug > 1 :
					print ('\t{} in {}'.format( key, Su_strms))
					print ('\tindx:{} : {} = {}\n{}'.format(_sdata['index'], key, Su_strms , _sdata))
					input ("SUB")
## XXX: Is Data
			elif key == 'data'		:
				_ddata	= dict(	index				= int(0),
								codec_name			= '',
								codec_type			= '',
								duration 			= float(0),
								disposition 		= '',
								tags 				= '' )
				Parse_from_to  (Strm_X, _ddata)
				Prst_all.append(_ddata)
				Dt_strms.append(_ddata)
				if DeBug > 1 :
					print ('\t{} in {}'.format( key, Dt_strms))
					print ('\tindx:{} : {} = {}\n{}'.format(_ddata['index'], key, Dt_strms , _ddata))
					input ("DTA")
## XXX: Is attachment
			elif key =='attachment'	:
				_atach	= dict(	index				= int(0),
								codec_name			= '',
								codec_type			= '',
								duration 			= float(0),
								disposition 		= '',
								tags 				= '' )
				Parse_from_to  (Strm_X, _atach)
				Prst_all.append(_atach)
				Xt_strms.append(_atach)
				if DeBug > 1 :
					print ('\t{} in {}'.format( key, Xt_strms))
					print ('\tindx:{} : {} = {}\n{}'.format(_atach['index'], key, Xt_strms, _atach))
					input ("ATT")
## XXX: Is WTF ?
			else :
				print ("Key:\n",	  json.dumps(key,      indent=2, sort_keys=False))
				print ("Strm_X:\n",	  json.dumps(Strm_X,   indent=2, sort_keys=False))
				print ("Meta_dta:\n", json.dumps(Meta_dta, indent=2, sort_keys=False))
				message += ' Cant Parse Streams WTF? \n{}\n' .format( Ini_file )
				print( message )
				input( 'Next ' )
				raise ValueError( message )

# XXX: Parsing Done lets Procces
		if  _mtdta['bit_rate'] == 'Pu_la':
			_mtdta['bit_rate'] = 100
			Vi_Dur = '11:080'
		else :
			mins,  secs = divmod(int(_mtdta['duration']), 60)
			hours, mins = divmod(mins, 60)
			Vi_Dur = '{:02d}:{:02d}:{:02d}'.format(hours, mins, secs)
		message = "    |< CT >|{}| {:^5}x{:^5} | {} Vid| {} Aud| {} Sub|".format(
					Vi_Dur, _vdata['width'], _vdata['height'], len(Vi_strms), len(Au_strms), len(Su_strms) )
		print (message)

# XXX: Check it :)
		if len(Vi_strms) == 0 :
			message = 'File \n{}\n Has no Video => Can\'t convert' .format( Ini_file )
			if DeBug : print( message ), input ('Next ?')
			raise ValueError( message )
		if len(Au_strms) == 0 :
			message = 'File \n{}\n Has no Audio => Can\'t convert' .format( Ini_file )
			if DeBug : print( message ), input ('Next ?')
			return False

# XXX: Video
		if DeBug : input ("VID !!")
		NB_Vstr = 0
		extra = ''
		for _vid in Vi_strms :
			if DeBug >1 :  print ('Vid : {}\n{}'.format(NB_Vstr, _vid ) )
			if _vid['codec_name'] == 'mjpeg' :
				continue
			if 'Pu_la' in _vid.values() :
				if DeBug > 1 : print ( json.dumps( _vid, indent=2, sort_keys=False)), input ('ZZ')
				extra = 'has Pu_la'
				if  _vid['bit_rate'] == 'Pu_la':
					if DeBug: print ( _vid.items() ),	time.sleep(1)
					_vid['bit_rate'] = round( _mtdta['bit_rate'] * 0.85 )	# XXX approximation 80% video
				else:
					print ( json.dumps( _vid, indent=2, sort_keys=True ) )
					input("Pu_la is here")
			frm_rate  = float( Util_str_calc(_vid['avg_frame_rate']) )
			message = "    |<V:{:2}>| {:^6} |Br: {:>9}|Fps: {:>5}| {}".format(
						_vid['index'], _vid['codec_name'], HuSa(_vid['bit_rate']), frm_rate, extra )
			print (message)

			zzz = '0:' + str(_vid['index'])
			ff_video.extend( [ '-map', zzz ] )

			if   _vid['height'] > 1090 :	# Scale to 1080p
				ff_video.extend( [ '-vf', 'scale = -1:1080', '-c:v', 'libx265', '-crf', '26', '-preset', 'slow' ] )
			elif _vid['codec_name'] == 'hevc' :
				if _vid['bit_rate'] > Max_v_btr :
#					ff_video.extend( [ '-c:v', 'libx265', '-preset', 'slow', '-lossless' ] )
					ff_video.extend( [ '-c:v', 'libx265', '-crf', '26', '-preset', 'medium'  ] )
				else:
					ff_video.extend( [ '-c:v', 'copy'])
			else :
				if   _vid['height'] > 680 :
					ff_video.extend( [ '-c:v', 'libx265', '-crf', '26', '-preset', 'slow'  ] )
				elif _vid['height'] > 340 :
					ff_video.extend( [ '-c:v', 'libx265', '-crf', '27', '-preset', 'medium'] )
				elif _vid['height'] > 240 :
					ff_video.extend( [ '-c:v', 'libx265', '-preset', 'medium'] )
				else :
					ff_video.extend( [ '-c:v', 'libx265', '-preset', 'fast'  ] )
			if frm_rate > Max_frm_rt :
#				ff_video.extend( = [ '-r', '25' ] )
				message = " Should Frame rate convert from {} to 25" .format( frm_rate )
				print (message)
				time.sleep(2)
			NB_Vstr += 1
		if DeBug : message = "    {}".format( ff_video ), print (message)

# XXX: audio
		_disp = dict(	default = int(0),
						dub 	= int(0),
						comment = 0,
						lyrics	= 0,
						karaoke	= 0,
						forced  = int(0),
						hearing_impaired= 0,
						visual_impaired	= 0,
						clean_effects	= 0 )
		if DeBug > 1 : print ( json.dumps( Au_strms, indent=3, sort_keys=False ) )
		if DeBug : input ("AUD !!")
		NB_Astr = 0
		extra = ''
		'''
		for _aud in Au_strms :
			for i, v in enumerate( _aud.items() ):
				print ('\nItm:', i, '\tVal:', v)
				if 'tags' in v :
					if 'eng' in v[1]['language'] :
						input ('WTff')
		'''
		indx  = 0
		leave = 0
		# XXX: Do we have Englis Audio?
		for _aud in Au_strms :
			if DeBug  > 1 : print ( json.dumps( _aud, indent=3, sort_keys=False ) )
			for i, v in _aud.items() :
				if DeBug : print ('\nItm:', i, '\tVal:', v)
				if 'tags' in i :
					if 'Pu_la' in v :
						if DeBug : print ( " Pu_la ", v)
					elif 'eng' in v['language'] :
#						input ('Got you')
						leave = True
						break
			if leave :
				break
			indx += 1
		# XXX: If Yes then use it dischard the rest
		if  indx <= len(Au_strms) and leave == True :
			_aud = Au_strms[indx]
			if 'eng' in _aud['tags']['language'] :
				print ('Oh yes Index ', _aud['index'])
			Parse_from_to ( _aud['disposition'], _disp )
			if 'Pu_la' in _aud.values() :
				if DeBug > 1 : print (json.dumps( _aud, indent=2, sort_keys=False)), input ('ZZ')
				extra = 'has Pu_la'
				if  _aud['bit_rate'] == 'Pu_la':
					if DeBug: print ( _aud.items() ), time.sleep(1)
					_aud['bit_rate'] = round( _mtdta['bit_rate'] * 0.15 / _aud['channels'] )	## XXX:  aproximation
				else:
					pass
			_lng = dict ( language = '' )
			if  _aud['tags'] == 'Pu_la':
				_lng['language'] = 'wtf'
			else:
				Parse_from_to ( _aud['tags'], _lng )
			message = "    |<A:{:2}>| {:^6} |Br: {:>9}|Fq: {:>5}|Ch: {}|{}|{}| {}".format(
						_aud['index'], _aud['codec_name'], HuSa(_aud['bit_rate']),
						HuSa(_aud['sample_rate']), _aud['channels'], _lng['language'], _disp['default'], extra)
			zzz = '0:' + str( _aud['index'] )
			ff_audio.extend([ '-map', zzz ])
			zzz = '-c:a:' + str( _aud['index'] )
			if  (_aud['codec_name'] == ('aac' or 'opus' or 'vorbis')) :
				if _aud['bit_rate'] <= Max_a_btr : # and _aud['channels'] < 3 :
					ff_audio.extend( [ zzz, 'copy'] )
				else:
					ff_audio.extend( [ zzz, 'libvorbis', '-q:a', '6'] )
			else :
				ff_audio.extend( [ zzz, 'libvorbis', '-q:a', '7'] )
			message += " * Yey *"
			print (message)
		# XXX: Oh well do what needs to be Done
		else :
			if DeBug : print ("No English found")
			for _aud in Au_strms :
				Parse_from_to ( _aud['disposition'], _disp )
				if 'Pu_la' in _aud.values() :
					if DeBug > 1 : print (json.dumps( _aud, indent=2, sort_keys=False)), input ('ZZ')
					extra = 'has Pu_la'
					if  _aud['bit_rate'] == 'Pu_la':
						if DeBug: print ( _aud.items() ), time.sleep(1)
						_aud['bit_rate'] = round( _mtdta['bit_rate'] * 0.15 / _aud['channels'] )	## XXX:  aproximation
					else:
						pass
				_lng = dict ( language = '' )
				if  _aud['tags'] == 'Pu_la':
					_lng['language'] = 'wtf'
				else:
					Parse_from_to ( _aud['tags'], _lng )
				message = "    |<A:{:2}>| {:^6} |Br: {:>9}|Fq: {:>5}|Ch: {}|{}|{}| {}".format(
							_aud['index'], _aud['codec_name'], HuSa(_aud['bit_rate']),
							HuSa(_aud['sample_rate']), _aud['channels'], _lng['language'], _disp['default'], extra)
				zzz = '0:' + str( _aud['index'] )
				ff_audio.extend([ '-map', zzz ])
				zzz = '-c:a:' + str( _aud['index'] )
				if  _aud['codec_name'] == 'aac' or 'opus' or 'vorbis' :
					if _aud['bit_rate'] <= Max_a_btr : # and _aud['channels'] < 3 :
						ff_audio.extend( [ zzz, 'copy'] )
					else:
						ff_audio.extend( [ zzz, 'libvorbis', '-q:a', '6'] )
				else :
					ff_audio.extend( [ zzz, 'libvorbis', '-q:a', '7'] )

				if  len(Au_strms) == 1 :
					break
				print (message)
				NB_Astr += 1
		if DeBug : message = "    {}".format( ff_audio ), print (message)

#XXX subtitle
		if DeBug : input ("SUB !!")
		NB_Sstr = 0
		if len(Su_strms ) == 0 :
			print ('    |<S: No Subtitle>|' )
			if DeBug : input ('Next ?')
		extra = ''
		for _sub in Su_strms :
			if DeBug >1 : print ('Sub : {}\n{}'.format(NB_Sstr, _sub ) )
			if 'Pu_la' in _sub.values() :
				if DeBug > 1 : print ( json.dumps( _sub, indent=2, sort_keys=False)), input ('ZZ')
				extra = 'has Pu_la'

			_lng = dict ( language = '' )
			Parse_from_to ( _sub['tags'], _lng )
			if 'Pu_la' in _lng['language'] :
				_lng['language'] = 'wtf'

			message = "    |<S:{:2}>|{:^6}|{:^10}|{:3}| {}".format(
				_sub['index'], _sub['codec_name'], _sub['codec_type'], _lng['language'], extra )
## XXX:
			if _sub['codec_name'] == 'hdmv_pgs_subtitle' or 'dvd_subtitle' :
				if _lng['language'] == 'eng' :
					Sub_fi_name	= Ini_file + '.' + str(_lng['language']) + '.dvd_subtitle'
#					ff_subtl.extend( [ zzz, 'copy', Sub_fi_name ] )
				else :
					message += 'Skip :('
					print (message)
					if DeBug : input ('Next Sub ?')
					continue
			else:
				print (message)
				zzz = '0:' + str(_sub['index'])
				ff_subtl.extend( ['-map', zzz ])
				zzz = '-c:s:' + str(_sub['index'])
				ff_subtl.extend( [ zzz, 'copy' ] )

			NB_Sstr += 1
		if DeBug :
			message = "    {}".format( ff_subtl )
			if NB_Sstr > 0 : print (message)

	except Exception as e:
		message += "\n FFZa_Brain: Exception => {}".format( e )
		if DeBug : print( message ), input ('Next')
		raise  Exception( message )

	else :
		FFM_cmnd = ff_video + ff_audio + ff_subtl

		if  frm_rate < Max_frm_rt and _vid['codec_name'] == 'hevc' and _aud['codec_name'] == ('aac' or 'opus' or 'vorbis') :
			if DeBug: print ('    | Vcod {}| Acod {}| Vhgt {}| VBtr {} : {}| ABtr {} : {}' .format( _vid['codec_name'], _aud['codec_name'], _vid['height'], round( _vid['bit_rate'] ), Max_v_btr, round( _aud['bit_rate'] ), Max_a_btr ) )
			if _vid['bit_rate'] <= Max_v_btr and _vid['height'] <= 1090 and _aud['bit_rate'] <= Max_a_btr :
				if DeBug :	input ('Nothing to do just escape')
				message = "_Skip_it : Nothing to Do {} | Vid :{} Aud :{}\n" .format( Ini_file,  _vid['codec_name'], _aud['codec_name'] )
##				print (message)
				FFM_cmnd = ['-benchmark_all']	# XXX: Make it more useful

		for pu in Prst_all :
			if 'Pu_la' in pu.values() :
				print ('Had some Pu_la ¯\_(ツ)_/¯')
				break
		end_time    = datetime.datetime.now()
		print('   End  : {:%H:%M:%S}\tTotal: {}'.format( end_time, end_time-start_time ) )
		return FFM_cmnd
##===============================   End   ====================================##


if __name__=='__main__':
#	global DeBug
#	DeBug = False

	cgitb.enable(format='text')

	message = __file__ +'-:'
	print( message )

	start_time = datetime.datetime.now()
	print(' Start: {:%H:%M:%S}'.format(start_time))

	sys.stdout 		= Tee( sys.stdout,	open( Log_File,   'w', encoding="utf-8" ) )
	Exeptions_File 	= 					open( Bad_Files,  'w', encoding="utf-8" )
	Succesful_File 	= 					open( Good_Files, 'w', encoding="utf-8" )

	if not Resource_Check( Folder ) :
		print ("Aborting Not Enough resources")
		exit()

#XXX  |[0] Extension |[1] Full Path |[2] File Size |[3] File Info |[4] Year Made Sort = True => Largest First XXX
	Qlist_of_Files  = Build_List( Folder, VIDEO_EXTENSIONS, Sort_loc=2, Sort_ord=True  )
	if DeBug > 2 :  print (Qlist_of_Files), input ("Next :")

	QExeption_ = Do_it( Qlist_of_Files )
	if DeBug :
		for filedesc in QExeption_ :
			print (filedesc.replace('\n',''))
		print ("Total files :", len (QExeption_) )

	Exeptions_File.close()
	Succesful_File.close()
	sys.stdout.flush()

	end_time = datetime.datetime.now()
	print(' \tEnd  : {:%H:%M:%S}\tTotal: {}'.format( end_time, end_time-start_time ) )
	input('All Done')
	exit()
##===============================   End   ====================================##
