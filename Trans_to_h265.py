# -*- coding: utf-8 -*-
#!/usr/bin/python3

'''
@author: 	  CompreZen
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

__author__ = 'CompreZen'

DeBug        = False
Rusnano		 = False	# TODO if it's a russion Movie
Vi_Dur		 = 0

Except_fold = 'C:\\Users\\George\\Desktop\\Except'
#Except_fold = 'C:\\Users\\George_Yo\\Desktop\\Except'

Folder		= 'D:\\Media\\Movie'
#Folder		= '-f ./Exception.log'
#Folder		= "\\\\198.18.0.177\\BkUp\\"
#Folder = 'C:\\Users\\George_Yo\\Desktop\\Except'	## XXX:
#Folder		= Except_fold
Folder		= 'C:\\Users\\George\\Desktop\\TestIng'
Folder		= 'C:\\Users\\George\\Desktop\\downloads'

# XXX: Movie Lenght there is a better way but this works for Now

VIDEO_EXTENSIONS = ['.3g2', '.3gp', '.3gp2', '.3gpp', '.60d', '.ajp', '.asf', '.asx', '.avchd', '.avi', '.bik',
					'.bix', '.box', '.cam', '.dat', '.divx', '.dmf', '.dv', '.dvr-ms', '.evo', '.flc', '.fli',
					'.flic', '.flv', '.flx', '.gvi', '.gvp', '.h264', '.m1v', '.m2p', '.m2ts', '.m2v', '.m4e',
					'.m4v', '.mjp', '.mjpeg', '.mjpg', '.mkv', '.moov', '.mov', '.movhd', '.movie', '.movx', '.mp4',
					'.mpe', '.mpeg', '.mpg', '.mpv', '.mpv2', '.mxf', '.nsv', '.nut', '.ogg', '.ogm', '.ogv', '.omf',
					'.ps', '.qt', '.ram', '.rm', '.rmvb', '.swf', '.ts', '.vfw', '.vid', '.video', '.viv', '.vivo',
					'.vro', '.wm', '.wmv', '.wmx', '.wrap', '.wvx', '.wx', '.x264', '.xvid']
#					'.vob'

Tmp_F_Ext	= '_XY_.mp4'

This_File  = sys.argv[0].strip ('.py')
Log_File   = This_File + '_run.log'
Bad_Files  = This_File + '_bad.txt'
Good_Files = This_File + '_good.txt'

ffmpeg_bin  = 'C:\\Program Files\\ffmpeg\\'
ffmpeg_exe  = "ffmpeg.exe"
ffprobe_exe = "ffprobe.exe"
ffmpeg		= os.path.join( ffmpeg_bin, ffmpeg_exe  )
ffprobe		= os.path.join( ffmpeg_bin, ffprobe_exe )

Stat_colect ={}

##############################################################################
def Run_stats(dictio):
# TODO Oh man lots to do XXX
	cnt =0
	for item in tuple(sorted(dictio.items(), key=lambda t: len(t[0]))) :
		cnt += 1
		print (item)
		for compo in  (item[1]) :
			print (len (item[1]))
			for po in compo :
				print ("\t",po)
				for p in po.items() :
					print ("\t\t",p )
		print('-'*35)
	print ("Total of", cnt )
##############################################################################

def Parse_year ( FileName ) :
#	DeBug = True
	message = sys._getframe().f_code.co_name +'-:'
	if DeBug :	print("{}".format( message ) )
	try :
#		yr 	 = re.findall( r'[\[\(]?((?:19[0-9]|20[01])[0-9])[\]\)]?' , FileName )
		yr 	 = re.findall( r'[\[\(]?((?:19[4-9]|20[0-1])[0-9])[\]\)]?' , FileName ) ## XXX: TODO exclude _at end
		if yr :
			va	= sorted(yr, key=lambda pzd: int(pzd), reverse=True) ## XXX:
			za  = int(va[0])
			if za > 2019 or za < 1930 :
				za = 1954
			if DeBug : print (FileName, yr , len(yr) , va, za)
		else :
			za = 1357
	except :
		za = 1
	return za
##############################################################################

""" =============== The real McCoy =========== """
def Build_List ( Top_dir, F_trypes, sort=True, sort_loc = 2 ) : # XXX: Sort=True (Big First) Sort_loc = 2 => File Size: =4 => year_made
	'''
	Create the list of Files to be proccesed
	'''
#	DeBug = True
	print("=" * 60)
	message    = sys._getframe().f_code.co_name + '-:'
	start_time = datetime.datetime.now()
	print('Start: {:%H:%M:%S}'.format( start_time))

	cnt 		= 0
	queue_list 	= []
	# Is it a Directory ?
	if os.path.isdir ( Top_dir ) :
		message  += " -> Directory Scan: {}".format( Top_dir )
		print ( message )
		for root, dirs, files in os.walk( Top_dir ):
			for one_file in files:
				x, extens = os.path.splitext( one_file.lower() )
				if extens in F_trypes :
					fi_path     = os.path.normpath( os.path.join( root, one_file ) )
					fi_info     = os.stat( fi_path)
					fi_size     = os.path.getsize( fi_path)
					year_made	= Parse_year( fi_path)
#XXX  |[0] Extension |[1] Full Path |[2] File Size |[3] File Info |[4] Year Made XXX
					Save_items	= extens, fi_path, fi_size, fi_info, year_made
					queue_list += [Save_items]
					if DeBug :
						print ("Add :{:>4} {:<70} {:^9} {} {:^6}".format( extens, one_file, HuSa(fi_size), fi_info, year_made ))
	# Is it a File ?
	elif os.path.isfile ( Top_dir ) :
		message += " -> Single File Not Directory: {}".format( Top_dir )
		print ( message )
		x,extens  = os.path.splitext( Top_dir.lower() )
		fi_path   = (Top_dir)
		fi_info   = os.stat( fi_path)
		fi_size   = os.path.getsize( fi_path)
		year_made = Parse_year ( fi_path)
#XXX  |[0] Extension |[1] Full Path |[2] File Size |[3] File Info |[4] Year Made XXX
		Save_items	= extens, fi_path, fi_size, fi_info, year_made
		queue_list += [Save_items]
		if DeBug :
			print ("Add :{:>4} {:<70} {:^9} {} {:^6}".format( extens, one_file, HuSa(fi_size), fi_info, year_made ))

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
		message = "{} -> ./Exception.log File ".format( message)
		print ( message )
		input ( "Ready ? Press CR")
		Exep_File = open("./Exception.log", "r", encoding="utf-8")
		for line in Exep_File:
			cnt += 1
			if DeBug : print (line)
			fi_path   = line.replace("\n",'')
			if os.path.isfile ( fi_path ) :
				x, extens = os.path.splitext( fi_path.lower() )
				fi_path     = os.path.normpath( os.path.join( root, one_file ) )
				fi_info     = os.stat( fi_path)
				fi_size     = os.path.getsize( fi_path)
				year_made	= Parse_year( fi_path)
#XXX  |[0] Extension |[1] Full Path |[2] File Size |[3] File Info |[4] Year Made XXX
				Save_items	= extens, fi_path, fi_size, fi_info, year_made
				queue_list += [Save_items]
				if DeBug :
					print ("Add :{:>4} {:<70} {:^9} {} {:^6}".format( extens, one_file, HuSa(fi_size), fi_info, year_made ))
			else :
				print ("No file named {}".format( fi_path))
				return False

#TODO Categories based on extension !!
	print ("\nTotal of {} files, {} Categorie's".format( cnt, len (queue_list)) )
	# XXX Sort based o file size XXX True for Bigest first XXX
	# XXX: Sort based in item [2] = filesize defined by sort_loc :)
	queue_list = sorted(queue_list, key=lambda pzd: (pzd[sort_loc], pzd[2]), reverse=sort) ## XXX: sort defined by caller

	if DeBug :
		print ("="*99)
		cnt 	= 0
		for each in queue_list :
			cnt +=1
			print (each)
#			print ("+" *cnt)
		input ("Next:")
	print (message, "Total of %s Files to Procces" % len (queue_list) )
	end_time    = datetime.datetime.now()
	print('End  : {:%H:%M:%S}\tTotal: {} '.format( end_time, end_time-start_time ) )
	return queue_list
##############################################################################

def Skip_Files ( One_descr, Min_fsize=10240 ) :
	message = sys._getframe().f_code.co_name+'-:'

#XXX  |[0] Extension |[1] Full Path |[2] File Size |[3] File Info |[4] Year Made XXX
	The_file  = One_descr[1]
	fi_size   = One_descr[2]

# List Files in the to be prccesed Directory
	if DeBug :
		Dir, Fil = os.path.split ( The_file )
		print ("Dir : {}\nFile: {}".format(Dir,Fil))
		for root, dirs, files in os.walk( Dir ):
			for fi in files :
				print ("\tFile :",fi)

## XXX: File does not exist :(
	if not os.path.exists ( The_file ) :
		message += "\n WTF ? Not Found {}\n".format( The_file )
		print ( message )
		Exeptions_File.write(message)
		Exeptions_File.flush()
		sys.stdout.flush()
		if DeBug :input ("? Skip it ?")
		return False

# XXX Big enough to be video ??
	if fi_size < Min_fsize : # 256K should be One Mega byte 1048576
		message += " To Small:| {:9} | {}\n".format( HuSa(fi_size), The_file )
		print ( message )
		Exeptions_File.write(message)
		Exeptions_File.flush()
		sys.stdout.flush()
		if DeBug :input ("? Skip it ?")
		return False

## XXX:  Ignore files that have been Locked (Procesed before)
	Lock_File = The_file + ".lock"

	if os.path.exists ( Lock_File ) :
		message += " Skip Locked: {}\n".format( os.path.basename( Lock_File) )
		print (message)
		Succesful_File.write( message )
		Succesful_File.flush()
		sys.stdout.flush()
		return False

	return Lock_File
##############################################################################

def Do_it ( List_of_files ,Excluded =''):
#	DeBug = False
	print("=" * 60)
	message = sys._getframe().f_code.co_name +'-:'
	print("{}".format( message ) )
	print ("Proccesing %s one by one" % len(List_of_files) )
	if (len (Excluded)) :
		print (" Excluding %s" , len (Excluded))
	if not List_of_files :
		print ("Mising Input %s" )
		print ("Aborting script.")
		return False
	Saving		= 0
	queue_list 	=[]
	Fnum  		= 0
	cnt         = len(List_of_files)
	for One_descr in List_of_files:
		message = sys._getframe().f_code.co_name+'+:'
		start_time = datetime.datetime.now()
		print("-" * 20)
		Fnum     += 1
#XXX  |[0] Extension |[1] Full Path |[2] File Size |[3] File Info |[4] Year Made XXX
		extens	  = One_descr[0]
		The_file  = One_descr[1]
		fi_size   = One_descr[2]
		year_made = One_descr[4]
		print ("{} / {}, {}, ({}), {}".format( ordinal(Fnum), cnt, HuSa(fi_size), year_made, The_file ) )
		print(' Start: {:%H:%M:%S}'.format(start_time))
		# XXX Scrub the Directory XXX New function
		Lock_File = Skip_Files ( One_descr )
		if Lock_File :
			#XXX Do it XXX
			try:
				all_good = FFProbe_run( The_file )
				sys.stdout.flush()
				if  all_good :
					all_good = Za_Brain( The_file, all_good )
					sys.stdout.flush()
					if  all_good :
						all_good = FFMpeg_run( The_file, all_good )
						sys.stdout.flush()
						if  all_good :
							all_good = Clean_up( The_file, all_good )
							sys.stdout.flush()
							if  all_good :
								was      = all_good[0]
								isn      = all_good[1]
								rat      = all_good[2]
								message = "{} File {} \nDone :)\tWas: {}\tIs: {}\tSaved: {} => {} {}\n".format(
											ordinal(Fnum), The_file, HuSa(was), HuSa(isn), HuSa(was - isn), round(rat,1), '%' )
								print (message)
								# XXX Create the Lock file with utf-8 encode for non english caracters ... # XXX:
								all_good = open( Lock_File , "w", encoding="utf-8"  )
								sys.stdout.flush()
								if  all_good :
									cnt -= 1
									all_good.write( message )
									all_good.flush()
									Succesful_File.write( message )
									Succesful_File.flush()
									if DeBug : print ('And Now the results ... {}'.format( json.dumps(queue_list, indent=4 )) )	#XXX should be One_descr after it was Modifyed XXX
									Saving += was - isn
									print ("  Total Saved {}".format( HuSa(Saving)) )
									sys.stdout.flush()
								else :
									message = ": Lock_File Create ErRor: {} \n".format( Lock_File )
									print (message)
									Exeptions_File.write(message)
									Exeptions_File.flush()
									sys.stdout.flush()
									if os.path.exists (Lock_File) :
										try:
											os.remove(Lock_File)
										except OSError as e:  ## if failed, report it back to the user ##
											print ("Error: {} - {}.".format(e.filename, e.strerror))
										print ("Lock_File Removed: " ,Lock_File )
							else :
								message += ": Clean_up   ErRor:| {} & {}\n".format( str(all_good),The_file )
								print (message)
								Exeptions_File.write(message)
								Exeptions_File.flush()
								sys.stdout.flush()
						else :
							message += ": FFMpeg ErRor:| {}\n".format( The_file )
							message += "\nCopy: {}\t{}\n".format( The_file, Except_fold )
							print (message)
							Exeptions_File.write(message)
							Exeptions_File.flush()
							sys.stdout.flush()
							try:
								shutil.copy2( The_file, Except_fold)
							except OSError as e:  ## if failed, report it back to the user ##
								print ("Error: {} - {}.".format(e.filename, e.strerror))
							if not DeBug :
								Cre_lock = open( Lock_File, "w", encoding="utf-8" )
								if Cre_lock :
									Cre_lock.write (message *17)
									Cre_lock.flush()
									print ("Lock File Created")
					else :
						message += ": Za_Brain   ErRor:| {} & {}\n".format( str(all_good),The_file )
						print (message)
						Exeptions_File.write(message)
						Exeptions_File.flush()
						sys.stdout.flush()
				else :
					message += ": FFProb ErRor:| {}\n".format( The_file )
					message += "Copy: {}\n{}\n".format( The_file, Except_fold )
					print( message )
					Exeptions_File.write(message)
					Exeptions_File.flush()
					sys.stdout.flush()
					shutil.copy2( The_file, Except_fold)
					if not DeBug :
						Cre_lock = open( Lock_File , "w",encoding="utf-8" )
						if Cre_lock :
							Cre_lock.write (message *10)
							Cre_lock.flush()
							print ("Lock File Created")
				sys.stdout.flush()
				end_time    = datetime.datetime.now()
				print( ' End  : {:%H:%M:%S}\tTotal: {} '.format( end_time, end_time-start_time ) )
				print( '='*20)
			except ValueError as err:
#				print(err.args)
				print( '{}' .format (err) )
#				break
			except Exception as e:
				message += " WTF? Exception " + repr(e)
				print( message )
				print( "Error: {}".format( traceback.print_exc()   ) )
				print( "Is:    {}".format( traceback.print_stack() ) )
				Exeptions_File.flush()
				Succesful_File.flush()
				sys.stdout.flush()

		else :
			cnt -= 1

	return queue_list
##############################################################################

def FFProbe_run (File_in, Execute=ffprobe ) :
#	DeBug = True
	start_time	= datetime.datetime.now()
	message 	= sys._getframe().f_code.co_name+'|:'
	print("  {}\t\tStart: {:%H:%M:%S}".format( message ,start_time ) )
	if os.path.exists (File_in) :
		file_size = os.path.getsize(File_in)
		message = "\n{}\t{}\n".format(
					File_in, HuSa(file_size) )
		if DeBug :	print ( message )
	else :
		print ("No Input file:(\n {}" .format( File_in ))
		if DeBug : input ('Now WTF?')
		return False

	Comand = [ Execute ,
			'-i', File_in,
			'-v', 'info',			# XXX quiet, panic, fatal, error, warning, info, verbose, debug, trace
			'-of','json',			# XXX default, csv, xml, flat, ini
			'-hide_banner',
			'-show_format',
#			'-show_chapters',
#			'-show_programs',
#			'-show_pixel_formats',
#			'-show_private_data',
			'-show_streams' ]
	if DeBug :
		print ("    |>:" , Comand)
#		input ("Ready to run FFProbe? ")
	else :
		print ("    |>:" , Comand[3:])
	jlist = []
	err = 'WTF?'
	out = 'WTF?'
	Succes = True
	try :
		ff_out = subprocess.run( Comand,
				stdout = subprocess.PIPE,
				stderr = subprocess.PIPE,
				universal_newlines = True,
				encoding='utf-8')
	except subprocess.CalledProcessError as err:
		Succes = False
		message += " ErRor: CalledProcessError = {!r}, {!r} :".format( err.returncode , err.output )
		print( message )
		if DeBug : input("subprocess.CalledProcessError")
	except Exception as e:
		Succes = False
		message += " ErRor: Exception = {}:".format( e )
		print( message )
		if DeBug : input("Exception")
# No Exception's lets see the data ...
	else :
		bad = ff_out.returncode
		err = ff_out.stderr
		out	= ff_out.stdout
		if bad :	# Exit with ErRor Code :(
			Succes 	 = False
			message += "\n{!r} Error Code :{!r}\n".format(err, bad )
			print (message)
#			raise
#		if DeBug : input ('Next')
# No ErRor all seems to be well
		jlist	 = json.loads (out)
		if len (jlist) < 2 :
			Succes = False
			message += " WTF? No Output {}\n{!r}".format( File_in , jlist )
			if DeBug :
				print( message )
				input(" Jlist to small ")
		if Succes :
			if DeBug :
				message += "\n" + json.dumps(jlist, indent=2 )
				print (message)
				input("FFparse Done")
			end_time    = datetime.datetime.now()
			print('   End  : {:%H:%M:%S}\tTotal: {}'.format( end_time, end_time-start_time ) )
			return jlist
		else :
			message  =' Stdout:\n{!r}\n'.format(ff_out.stdout)
			message +=' Stderr:\n{!r}\n'.format(ff_out.stderr)
			message +=' Out   : {}\n'.format( json.dumps(jlist, indent=2 ))
			Exeptions_File.write(message)
			Exeptions_File.flush()
			print ( message )
			if DeBug :
				input("FFParse NOT Done")
			return False
##############################################################################

def Za_Brain ( Ini_file, Meta_dta, verbose=False ) :
#	DeBug = True
	'''
	The Heavy Lifting Probe Parse and Decide what to do
	Return ffmpeg Comand
	'''
	start_time	= datetime.datetime.now()
	message 	= sys._getframe().f_code.co_name + '/:'
	print("  {}\t\tStart: {:%H:%M:%S}".format( message ,start_time ) )

	global Vi_Dur	# make it global so we can reuse for fmpeg check ...

#	if DeBug : print ("File Part : {}\nJson File : {} ".format(Ini_file ,json.dumps(Meta_dta, indent=2,sort_keys=False) ) )
	Is_Fukt = 0

	Parsed_all = []
	Au_strms = []
	Vi_strms = []
	Su_strms = []
	Ot_strms = []

	ff_subtl = []
	ff_audio = []
	ff_video = []
	try :
		# TODO Parse Format
		_mtdta = dict(	nb_streams		= int(0),
						duration		= float(0),
						bit_rate		= int(0),
						size 			= float(0) )
		Parse_from_to ( Meta_dta['format'], _mtdta)
		Parsed_all.append (_mtdta)
		if 'Pu_la' in _mtdta.values() :
			print (_mtdta)
			input ('Meta_dta has Pu_la :(')
			return False

		_kdat = dict(	codec_type 		=''	)
		for rg in range ( _mtdta['nb_streams'] ) :
			Strm_X = Meta_dta['streams'][ rg ]
			key    = Parse_from_to ( Strm_X, _kdat )
			Parsed_all.append (_kdat)
			if DeBug > 1 :
				print ("  HAS :", key, '\n\n' ,json.dumps( Strm_X, indent=2, sort_keys=False ) )
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
				Parse_from_to ( Strm_X, _vdata)
				Parsed_all.append(_vdata)
				Vi_strms.append  (_vdata)	# procces all before building command
				if DeBug > 1 :
					print ('\t{} in {}'.format( key, Vi_strms))
					print ('\tindx:{} : {} = {}\n{}'.format(_vdata['index'], key, Vi_strms , _vdata))
					print (ff_video)
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
				Parse_from_to ( Strm_X, _adata)
				Parsed_all.append(_adata)
				Au_strms.append  (_adata)	# procces all before building command
				if DeBug > 1 :
					print ('\t{} in {}'.format( key, Au_strms))
					print ('\tindx:{} : {} = {}\n{}'.format(_adata['index'], key, Au_strms ,_adata))
					print (ff_audio)
					input ("AUD")
# XXX: Is subtitle
			elif key == 'subtitle'	:
				_sdata	= dict(	index				= int(0),
								codec_name			= '',
								codec_type			= '',
								duration 			= float(0),
								disposition 		= '',
								tags 				= '' )
				Parse_from_to ( Strm_X, _sdata)
				Parsed_all.append (_sdata)
				Su_strms.append (_sdata)
				if DeBug > 1 :
					print ('\t{} in {}'.format( key, Su_strms))
					print ('\tindx:{} : {} = {}\n{}'.format(_sdata['index'], key, Su_strms , _sdata))
					print (ff_subtl)
					input ("SUB")
## XXX: Is Data
			elif key == 'data'		:
				_ddata	= dict(	index				= int(0),
								codec_name			= '',
								codec_type			= '',
								duration 			= float(0),
								disposition 		= '',
								tags 				= '' )
				Parse_from_to ( Strm_X, _ddata)
				Parsed_all.append (_ddata)
				Ot_strms.append (_ddata)
				if DeBug > 1 :
					print ('\t{} in {}'.format( key, Ot_strms))
					print ('\tindx:{} : {} = {}\n{}'.format(_ddata['index'], key, Ot_strms , _ddata))
					input ("DTA")
## XXX: Is attachment
			elif key =='attachment'	:
				_atach	= dict(	index				= int(0),
								codec_name			= '',
								codec_type			= '',
								duration 			= float(0),
								disposition 		= '',
								tags 				= '' )
				Parse_from_to ( Strm_X, _atach)
				Parsed_all.append (_atach)
				Ot_strms.append (_atach)
				if DeBug :
					print ('\t{} in {}'.format( key, Ot_strms))
					print ('\tindx:{} : {} = {}\n{}'.format(_atach['index'], key, Ot_strms , _atach))
					input ("ATT")
## XXX: Is WTF ?
			else :
				Is_Fukt += 1
				print ("Key:\n",	  json.dumps(key,      indent=2,sort_keys=False))
				print ("Strm_X:\n",	  json.dumps(Strm_X,   indent=2,sort_keys=False))
				print ("Meta_dta:\n", json.dumps(Meta_dta, indent=2,sort_keys=False))
				input ("WTF is this?")

		if len(Vi_strms) == 0 :
			message += ' No Video => Can not convert \n{}\n' .format( Ini_file )
			print (message)
			Exeptions_File.write(message)
			Exeptions_File.flush()
			sys.stdout.flush()
			if DeBug : input('Press CR')
			return False

		mins,  secs = divmod(int(_mtdta['duration']), 60)
		hours, mins = divmod(mins, 60)
		Vi_Dur = '{:02d}:{:02d}:{:02d}'.format(hours, mins, secs)

		message = "    |< G >|{}| {:^5}x{:^5} | {} Vid| {} Aud| {} Sub|".format(
					Vi_Dur, _vdata['width'],_vdata['height'], len(Vi_strms), len(Au_strms), len(Su_strms) )
		print (message)
## XXX:  Video Commands
		NB_Vstr = 0
		for _vid in Vi_strms :
			if DeBug :  print ('Vid : {}\n{}'.format(NB_Vstr, _vid ) )
			if 'Pu_la' in _vid.values() :
				Is_Fukt +=1
				if  _vid['bit_rate'] == 'Pu_la':
					_vid['bit_rate'] = _mtdta['bit_rate'] * 0.8	# XXX approximation 80%
			if _vid[ 'codec_name'] == 'mjpeg' :		# XXX Add wait
				break
			frm_rate  = float( Util_str_calc(_vid['avg_frame_rate']) )
			message = "    |<V:{}>| {:^6} |Br: {:>9}|Fps: {:>5}|".format(
						_vid['index'], _vid['codec_name'] , HuSa(_vid['bit_rate']), frm_rate )
			print (message)
			zzz = '0:' + str(_vid['index'])
			if frm_rate > 30 :
### XXX: Best but slow 			ff_video = ['-vf', 'minterpolate=fps=30', '-map', zzz ]
				ff_video = ['-r', '25', '-map', zzz ]
				input ('Frame Rate Conversion to 25?')
			else :
				ff_video = ['-map', zzz ]

## XXX: Check Bit Rate < 1024 _vid['bit_rate']
			if   _vid['height'] > 1080 :	# Scale down to 1080p
					ff_video.extend( ['-vf', 'scale = -1:1080', '-c:v', 'libx265', '-preset', 'slow', '-x265-params', 'lossless'] )
			elif _vid['codec_name'] == 'hevc' and _vid['bit_rate'] > 1600000 :
				ff_video.extend( [ '-c:v', 'libx265', '-b:v', '1.5M'])
			elif _vid['codec_name'] == 'hevc' :
				ff_video.extend( [ '-c:v', 'copy'])
			else :
#				-vf scale=-1:720
				if   _vid['height'] > 1020 :
					ff_video.extend( [ '-c:v', 'libx265', '-preset', 'slow',   '-x265-params', 'lossless'] )
				elif _vid['height'] > 600 :
					ff_video.extend( [ '-c:v', 'libx265', '-preset', 'medium', '-x265-params', 'lossless'] )
				elif _vid['height'] > 300 :
					ff_video.extend( [ '-c:v', 'libx265', '-preset', 'fast',   '-x265-params', 'lossless'] )
				else :
					ff_video.extend( [ '-c:v', 'libx265', '-preset', 'faster', '-x265-params', 'lossless'] )
			if DeBug :
				print (ff_video)
			NB_Vstr += 1

# XXX: audio
#		sorted(dictio.items(), key=lambda t: len(t[0]))
#	queue_list = sorted(queue_list, key=lambda pzd: (pzd[sort_loc] , pzd[2]), reverse=sort) ## XXX: sort defined by caller

		Siz = len(Au_strms)
		NB_Astr = 0
		if DeBug :
			print (Au_strms)
		if Siz > 1 and Rusnano == True :
			print ("\t Reverse")
			Au_strms.reverse ()
		elif Siz > 1 :
			for i in range( Siz) :
				if Au_strms[i]['tags'] == 'Pu_la' or 'language' not in Au_strms[i]['tags'] :
					print ('ZZZ = ', Au_strms[i]['tags'])
					if DeBug : input ("Pu_la")
					pass
## XXX: Reverse streems for English First
				elif Au_strms[i]['tags']['language'] == 'eng' and i != 0 :
					Au_strms.reverse ()
					print ("\t   Audio Chanel Reverse")
					if DeBug : input ("reverse")
				if DeBug :
					print (Au_strms[i]['tags']['language'])
			if DeBug : input ('Au Sorted?')
#### XXX: For Ru films with only 2 languages simple .reverse
		for _aud in Au_strms :
			if DeBug :
				print ('Aud : {}\n{}'.format(NB_Astr, _aud ) )
			lng = 'unknown'
			if 'Pu_la' in _aud.values() :
				Is_Fukt +=1
				if  _aud['bit_rate'] == 'Pu_la':
					_aud['bit_rate'] = _mtdta['bit_rate'] *0.2	# aproximation
			if _aud['tags'] != 'Pu_la':
				_lng = dict ( language = '' )
				Parse_from_to ( _aud['tags'], _lng )
				lng =_lng['language']
			if DeBug :	print (_aud)
			message = "    |<A:{}>| {:^6} |Br: {:>9}|Fq: {:>5}|Ch: {}| {}|".format(
						_aud['index'], _aud['codec_name'], HuSa(_aud['bit_rate']), HuSa(_aud['sample_rate']), _aud['channels'], lng )
			print (message)

			zzz = '0:' + str(_aud['index'])
			if NB_Astr == 0 :
				ff_audio = ['-map', zzz ]
			else :
				ff_audio.extend(['-map', zzz ])
			zzz = '-c:a:' + str(NB_Astr)
## # XXX:
			if _aud['codec_name'] == 'aac' and _aud['bit_rate'] < 262144 and _aud['channels'] < 3 :
				ff_audio.extend( [ zzz, 'copy'])
			elif _aud['bit_rate'] > 262144 :
				ff_audio.extend( [ zzz, 'aac', '-b:a', '256k', '-ac', '2'] )
			else :
				ff_audio.extend( [ zzz, 'aac', '-b:a', str(_aud['bit_rate']), '-ac', '2'] )
			NB_Astr += 1

#	-metadata:s:1 language=eng
#	sets metadata language to eng on the stream id 1 (which is, in typical cases, the first audio stream). Whereas the option
#	-metadata:s:s:0 language=eng
#	sets the metadata language to eng on the first subtitle stream.
#			zzz = '-disposition:a:'+ str(_aud['index'])
#			ff_audio.extend( [ zzz ])
#			if lng == 'eng' :
#				ff_audio.extend( ['+default +forced'] )
#			else :
#				ff_audio.extend( ['0'] )
			if DeBug :
				print (ff_audio)

#XXX subtitle
		Siz = len(Su_strms)
		if Siz > 1 :	# More than 1 subtitle
			for i in range( Siz ) :
				if Su_strms[i]['tags'] == 'Pu_la' or 'language' not in Su_strms[i]['tags'].values() :
					pass
				elif Su_strms[i]['tags']['language'] == 'eng' and i != 0 :
					Su_strms.reverse ()
#		if DeBug :
#			print (Su_strms)
#			input ('Su Sorted?')

		NB_Sstr = 0
		for _sub in Su_strms :
			if DeBug :
				print ('Sub : {}\n{}'.format(NB_Sstr, _sub ) )
			lng = 'unknown'
			if 'Pu_la' in _sub.values() :
				Is_Fukt +=1
			if _sub['tags'] != 'Pu_la':
				_lng = dict ( language = '' )
				Parse_from_to ( _sub['tags'], _lng )
				lng =_lng['language']

			sin =_sub['index']
			message = "    |<S:{}>| {:^6} | {:^12}| {}|".format(
				sin, _sub['codec_name'], _sub['codec_type'], lng  )
			print (message)
## XXX:
			zzz = '0:' + str(sin)
			Sub_fi_name	= Ini_file + '.' + str(lng) + '.' + str(sin) + '.srt'

			if NB_Sstr == 0 :
				ff_subtl = ['-y', '-map', zzz ]
			else :
				ff_subtl.extend( ['-map', zzz ])
			zzz = '-c:s:' + str(sin)
			ff_subtl.extend( [ zzz, 'copy' , Sub_fi_name ] )
			NB_Sstr += 1
			if DeBug :
				print (ff_subtl)

		for pu in Parsed_all :
			if 'Pu_la' in pu.values() :
				Is_Fukt += 1
				if DeBug     : print ("\t  | XXX | Pu_la :", pu)
				if DeBug > 2 : print ("WTF ?? " ,key ,'\n', json.dumps(Meta_dta, indent=2,sort_keys=False))
		Bild_Dict ( Ini_file, Parsed_all , Stat_colect )
		if DeBug >2 : print ("Parsed_all :\n", json.dumps( Parsed_all, indent=2,sort_keys=False))

	except Exception as e:
		message = "Za_Brain: Exception => {}:".format( e )
		print( message )
		print ("WTF ?? ",key ,'\n', json.dumps(Meta_dta, indent=2,sort_keys=False))
		print( "Error: {}".format( traceback.print_exc()   ) )
		print( "Is:    {}".format( traceback.print_stack() ) )
		input ("Got you")
		return False
	else :
## XXX: if  _vid['height'] < 1080 and _vid['bit_rate'] < 1400000 and _vid['codec_name'] == 'hevc' and _aud['codec_name'] == 'aac' :
		if  _vid['codec_name'] == 'hevc' and _vid['bit_rate'] < 1600000 and  _vid['height'] <= 1080 and  _aud['codec_name'] == 'aac' and _aud['bit_rate'] < 500000 :
			message = "    H265 & aac"
			print (message)
#			raise ValueError('happened', _vid, 'bar', 'baz')
			raise ValueError('  OK:', _vid['codec_name'], _aud['codec_name'])
			input ('escape')
		FFM_cmnd = ff_video + ff_audio + ff_subtl
		Bild_Dict ( Ini_file , FFM_cmnd , Stat_colect )
		if DeBug:
			print ("Za_Brain Done :", FFM_cmnd)
			input ('Do it ?')
		end_time    = datetime.datetime.now()
		print('   End  : {:%H:%M:%S}\tTotal: {}'.format( end_time, end_time-start_time ) )
		return FFM_cmnd
##############################################################################

def FFMpeg_run ( Fmpg_in_file, Comand, Execute = ffmpeg ) :
#	DeBug = True
	start_time	= datetime.datetime.now()
	message 	= sys._getframe().f_code.co_name+'-:'
	print("  {}\t\tStart: {:%H:%M:%S}".format( message ,start_time ) )

## XXX: THIS IS WHRE the MAPPING HAPPENS !!

	ff_head  = [Execute, '-i', Fmpg_in_file, '-hide_banner', '-fflags', '+genpts' ]

#XXX FileName for the Title ...
	Sh_fil_name    = os.path.basename(Fmpg_in_file).title()
	Sh_fil_name,xt = os.path.splitext(Sh_fil_name)
	Sh_fil_name   += '.mp4'
#	Title         = 'title=\"' +Sh_fil_name +" => Encoded By: Mr. _XY_, Universal CompreZen Master " +'\"\''
	Title         = 'title=\"' +Sh_fil_name +" HAVC AAC => Encoded By: CompreZen Master " +'\"\''
#	Fmpg_ou_file  = New_File_Name( Fmpg_in_file, )
	Fmpg_ou_file  = Random_String(11) + Tmp_F_Ext
#	Fmpg_ou_file  = os.path.normpath(Fmpg_ou_file)

	ff_tail  = [ '-metadata' ,Title , '-y', Fmpg_ou_file ]

	Cmd      = ff_head + Comand + ff_tail
	symbs = '+|/-\\'
	loc   = 0
	n_sym = len(symbs)
	Succes = False
	Special= DeBug
	try :
		if Special :
			Succes = True
			print ("    |>:", Cmd )
			if DeBug : input ("Ready to Do it? ")

			ff_out = subprocess.run( Cmd,
						universal_newlines=True,
						encoding='utf-8'
					)
			out = ff_out
			message += "\n Out: {!r}\n".format( out )
#			print( message )
			errcode  = ff_out.returncode
			if errcode :
				message += "\n ErRor: Code {!r}\n".format( errcode )
#				print( message )
			return Fmpg_ou_file
		else :
			print ("    |>:", Cmd[6:-4] )
			ff_out = subprocess.Popen( Cmd,
						stdout=subprocess.PIPE,
						stderr=subprocess.STDOUT,
						universal_newlines=True,
						encoding='utf-8'
	#					bufsize=1
					)
	except subprocess.CalledProcessError as err:
		message += " ErRor: CalledProcessError {!r},  {!r} :".format( err.returncode , err.output )
		print( message )
		raise
	except Exception as e:
		message += " ErRor: Exception {}:".format( e )
		print( message )
		raise
	else:
		while True :
			try:
				lineo    = ff_out.stdout.readline()
				if DeBug : print("<{!r}>".format (lineo))
				errcode  = ff_out.returncode
				if errcode :
					message += " ErRor: ErRorde {!r} :".format( errcode )
					print( message )
					break
				elif 'frame=' in lineo :
					Prog_cal (lineo , symbs[loc])
					last_fr  = lineo.rstrip('\r\n')
					loc += 1
					if loc == n_sym :
						loc = 0
				elif 'video:' and "muxing overhead:" in lineo :
					print ('\r\t  |><|', last_fr )
					print (  '\t  |><|', lineo.rstrip('\r\n') )
					global Vi_Dur
					try :
						tm = re.search ( r'time=\S([0-9:]+)', last_fr ).group(1)
						if DeBug :
							print ("Lenght was:{} is:{}".format( Vi_Dur ,tm ) )
						a_sec = sum( int(x)*60**i for i,x in enumerate( reversed(tm.split(":"))) )
						b_sec = sum( int(x)*60**i for i,x in enumerate( reversed(Vi_Dur.split(":"))) )
						dif  = abs (b_sec - a_sec)
						print ("\t  [-)]Lenght was:{} is:{} dif:{}".format( Vi_Dur ,tm, dif ) )
#						if dif < 15 : # 15 seconds rule
						# XXX: make it 1 percent a fixed number
						if dif < ( b_sec / 100 ) :
							Succes = True
						else :
							print ("Diff to Big :( ",dif)
						if DeBug :
							time.sleep(20)	#2 minutes
#							input ("So ?")
					except Exception as e:
						message += " Got Time ErRor: Exception {}:".format( e )
						print( message )
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
				print( message )
#			else :
#				if DeBug : print( " All is fine so far")

	finally :
		file_size	= os.path.getsize(Fmpg_in_file)
		check_path	= os.path.exists (Fmpg_ou_file)
		if check_path :
			n_file_size = os.path.getsize(Fmpg_ou_file)
			if n_file_size < (file_size/64) :	# XXX Kind of imposible
				Succes = False
				message += " Size to small :" +os.path.basename(Fmpg_in_file) +' ' +HuSa(n_file_size)
				if DeBug :
					print ( message )
					time.sleep(30)
#					input ("\nOutput to Small\n")
		else :
			Succes = False
			message += ": Output Path not found "+ os.path.basename(Fmpg_in_file)
			if DeBug :
				message +="\nNo Output Created\n"
				print ( message )
				time.sleep(10)

		end_time    = datetime.datetime.now()
		print('   End  : {:%H:%M:%S}\tTotal: {}'.format( end_time, end_time-start_time ) )
		if 	Succes :
			if DeBug :
				message +="\n\t FFMpeg Done !!"
				print ( message )
				time.sleep(10)
			return Fmpg_ou_file
		else :
			if not Special :
				ff_out.kill()
			message +='\nStderr : {!s}'.format( ff_out.stderr )
			message +='\nStdout : {!s}'.format( ff_out.stdout )
			message +='\nDlt Out: {}\t'.format( os.path.basename(Fmpg_ou_file) )
			try:
				os.remove( Fmpg_ou_file )
			except OSError as e:  ## if failed, report it back to the user ##
				message += "\n!Error: {} - {}.".format(e.filename, e.strerror)
			print ( message )
			Exeptions_File.write(message)
			Exeptions_File.flush()
			if DeBug :
				message +="\n\t FFMpeg Not Done :("
				print ( message )
				time.sleep(10)
			return False
##############################################################################

def Prog_cal (line_to, sy=False ) :
#	DeBug = True
	message  = sys._getframe().f_code.co_name+'-:'
	last_fr  = line_to.rstrip('\r\n')
	_P =''
	if DeBug :
		print ("\r>" , last_fr)
		_P   = '\r {} .. {}'.format( sy, last_fr )
	if 'frame=' and 'bitrate=' and not 'N/A' in last_fr :
		try :
			global Vi_Dur
			fr 	 = re.search( r'frame=\s*([0-9]+)'    	,last_fr ).group(1)
			fp	 = re.search( r'fps=\s*([0-9]+)'      	,last_fr ).group(1)
			sz   = re.search( r'size=\s*([0-9]+)'     	,last_fr ).group(1)
			tm =   re.search( r'time=\S([0-9:]+)'		,last_fr ).group(1)
#			tm   = re.search( r'time=\S([0-9\.:]+)'		,last_fr ).group(1)	#Can start with -
			br	 = re.search( r'bitrate=\s*([0-9\.]+)'	,last_fr ).group(1)	#Can have value of N/A
			sp	 = re.search( r'speed=\s*([0-9\.]+)'	,last_fr ).group(1)	#Can have value of N/A

			if int(fp) > 0 :
				a_sec = sum( int(x)*60**i for i,x in enumerate( reversed(tm.split(":"))) )
				b_sec = sum( int(x)*60**i for i,x in enumerate( reversed(Vi_Dur.split(":"))) )
				dif   = abs( b_sec - a_sec )
				eta  = round( dif / (float(sp) ))
				mins, secs  = divmod(int(eta), 60)
				hours, mins = divmod( mins, 60)
				_Dur = '{:02d}:{:02d}:{:02d}'.format(hours, mins, secs)
				_P   = '\r| {} |Frame: {}|Fps: {}|Siz: {}|BitRate : {}|Speed: {}|Time Left: {}|  '.format(sy, fr, fp, sz, br, sp, _Dur)

		except Exception as e:
			print (last_fr)
			message += " ErRor: in Procesing data {!r}:".format( e )
			print( message )
	else :
		print (last_fr)
	sys.stderr.write( _P )
	sys.stderr.flush
	return True
##############################################################################

def Clean_up ( Inp_file , Out_file ):
#	DeBug = True
	start_time	= datetime.datetime.now()
	message 	= sys._getframe().f_code.co_name+'\\:'
	print("  {}\t\tStart: {:%H:%M:%S}".format( message ,start_time ) )

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
		print ("Post:\n{} \t\t{}".format (Out_file  , HuSa(Out_file_size)) )

	Ratio   =  round( 100*(( Ini_file_size - Out_file_size) / Out_file_size))
	if DeBug :
		print ("Ini\t{}\tOut\t{}\tRat\t{}".format( Ini_file_size , Out_file_size , Ratio ))
		input ('Next')
	if  Ratio  >  5 :
		print ("\tNew {} {} Smaller".format(Ratio, '%'))
	elif Ratio < -5 :
		print ("\tNew {} {} Larger ".format(abs(Ratio), '%'))
#		return ( Ini_file_size,  Out_file_size, Ratio )
	else :
		print ("\tSimilar {} {}".format(Ratio, '%'))
#		return ( Ini_file_size,  Out_file_size, Ratio )
	Succes = True
	try :
		# Create "Delete Me File" from the Original file (Inp_file)
		To_delete  = New_File_Name ( Inp_file, "DeletMe.old", Tmp_F_Ext  )
		check_path = os.path.exists( To_delete )
		if check_path : # File alredy exists TODO let's create another one ??
			print ("\t.old Alredy Exists Rm:", To_delete )
			try:
				os.remove( To_delete)
			except OSError as e:  ## if failed, report it back to the user ##
				print ("Error: %s - %s." % (e.filename,e.strerror))
		os.rename ( Inp_file, To_delete)
		check_path = os.path.exists( To_delete )
		if not check_path :
			print (message, " Big BuBu " , To_delete , Inp_file )
			Succes = False
		else :
			if DeBug : print ("\tRnm: {}\tTo: {}".format( os.path.basename(Inp_file), os.path.basename(To_delete)) )

## XXX: Extract ext remouve and change to .mp4 for ImpFile
		f_name, xt 	 = os.path.splitext (Inp_file)
		New_out_File = New_File_Name (Inp_file, ".mp4", xt)
		if DeBug :
			print ("Creating" , New_out_File)
			input ("Shall we?")
		check_path   = os.path.exists (New_out_File)
		if check_path : # File alredy exists TODO let's deleate it to create another one
			print ("\t .mp4 Alredy Exists Must Rm:", New_out_File )
			input ('WTF')
			try:
				os.remove( New_out_File)
			except OSError as e:  ## if failed, report it back to the user ##
				print ("Error: %s - %s." % (e.filename, e.strerror))
		shutil.move ( Out_file, New_out_File)
		os.utime 	( New_out_File, None) 			## XXX:  None means now !

		check_path = os.path.exists( New_out_File )
		if not check_path :
			print (message, " Big BuBu " , New_out_File , Out_file )
			Succes = False
		else :
			if DeBug : print ("\tRnm: {}\tTo: {}".format( os.path.basename(Out_file), os.path.basename(New_out_File)) )
	except Exception as ex :
		print ( repr(ex) )
		Succes = False
		input ("Clean_up Exception")
	if (Succes) :
		end_time    = datetime.datetime.now()
		print('   End  : {:%H:%M:%S}\tTotal: {}'.format( end_time, end_time-start_time ) )
		return ( Ini_file_size,  Out_file_size, Ratio )
	else :
		return False
##############################################################################

if __name__=='__main__':
#	DeBug = False
	cgitb.enable(format='text')

	message = __file__ +'-:'
	print( message )

	start_time = datetime.datetime.now()
	print(' Start: {:%H:%M:%S}'.format(start_time))

	sys.stdout 		= Tee( sys.stdout, open( Log_File , 'w',encoding="utf-8" ) )

	Exeptions_File 	= open( Bad_Files , 'w', encoding="utf-8" )
	Succesful_File 	= open( Good_Files, 'w', encoding="utf-8" )

	if not Resource_Check( Folder ) :
		print ("Aborting Not Enough resources")
		exit()

#XXX  |[0] Extension |[1] Full Path |[2] File Size |[3] File Info |[4] Year Made XXX
#	Qlist_of_Files  = Build_List( Folder, VIDEO_EXTENSIONS, sort=False , sort_loc=2 )	# Smalest Size File
	Qlist_of_Files  = Build_List( Folder, VIDEO_EXTENSIONS , sort=True , sort_loc=2 )
	if DeBug >2 :
		print (Qlist_of_Files)
		input ("Next :")

	QExeption_ = Do_it( Qlist_of_Files )
	if DeBug >2 :
		for filedesc in QExeption_ :
			print (filedesc.replace('\n',''))
		input ("Next :")

#	print("\nAll Done:\n" )
#	print ("Scaned   : %s\nSelected : %s\nFinished :" % ( (Qlist_of_Files) , (QExeption_)) )

#	Run_stats( Stat_colect )

	Exeptions_File.close()
	Succesful_File.close()

	end_time = datetime.datetime.now()
	print(' \tEnd  : {:%H:%M:%S}\tTotal: {}'.format( end_time, end_time-start_time ) )

	input("Done")
	exit()
##############################################################################
