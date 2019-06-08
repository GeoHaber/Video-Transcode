#!/usr/bin/python
# -*- coding: utf-8 -*-

# XXX KISS
import os
import sys
import time
import ctypes
import random
import string
import logging
import datetime
import platform

class Tee(object):
	""" tee for python
	"""
	def __init__(cls, _fd1, _fd2):
		cls.fd1 = _fd1
		cls.fd2 = _fd2
	def __del__(cls):
		if ((cls.fd1 != sys.stdout) and (cls.fd1 != sys.stderr)):
			cls.fd1.close()
		if ((cls.fd2 != sys.stdout) and (cls.fd2 != sys.stderr)):
			cls.fd2.close()
	def write(cls, text):
		cls.fd1.write(text)
		cls.fd2.write(text)
	def flush(cls):
		cls.fd1.flush()
		cls.fd2.flush()

	# STDOUT:
	@classmethod
	def stdout_start(cls, logfilename='stdout.log', append=True):
		cls.stdoutsav = sys.stdout
		if (append):
			cls.LOGFILE = open(logfilename, 'a')
		else:
			cls.LOGFILE = open(logfilename, 'w')
		sys.stdout = tee(cls.stdoutsav, cls.LOGFILE)
		return cls.LOGFILE
	@classmethod
	def stdout_stop(cls):
		cls.LOGFILE.close()
		sys.stdout = cls.stdoutsav

	# STDERR:
	@classmethod
	def stderr_start(cls, errfilename='stderr.log', append=True):
		cls.stderrsav = sys.stderr
		if (append):
			cls.ERRFILE = open(errfilename, 'a')
		else:
			cls.ERRFILE = open(errfilename, 'w')
		sys.stderr = tee(cls.stderrsav, cls.ERRFILE)
		return cls.ERRFILE
	@classmethod
	def stderr_stop(cls):
		cls.ERRFILE.close()
		sys.stderr = cls.stderrsav
##==============-------------------   End   -------------------==============##

def Custom_logger( name ):
	Frmt 	     = logging.Formatter(fmt='%(asctime)s %(levelname)-8s %(message)s',
								  datefmt='%Y-%m-%d %H:%M:%S')
	Hndlr 	     = logging.FileHndlr('log.txt', mode='w')
	Hndlr.setFormatter(Frmt)
	screen_Hndlr = logging.StreamHndlr(stream=sys.stdout)
	screen_Hndlr.setFormatter(Frmt)
	logger       = logging.getLogger(name)
	logger.setLevel(logging.DEBUG)
	logger.addHndlr(Hndlr)
	logger.addHndlr(screen_Hndlr)
	return logger
##==============-------------------   End   -------------------==============##

def Util_str_calc (the_string, DeBug=False ) :

	message = sys._getframe().f_code.co_name
	'''
	Returns the result for string (a/b)
	'''
	(dividend, divisor) = the_string.split('/')
	if DeBug : print ( "Util", the_string, dividend, divisor)
	if  divisor  == '0' :
		message += " ! Division by Zero ! {} / {}".format( dividend , divisor )
		print ( message )
		if DeBug :
			input ('Wait')
		return False
	elif dividend == '0' :
		message += " ! Zero Divided by  ! {} / {}".format( dividend , divisor )
		print ( message )
		if DeBug :
			input ('Wait')
		return 0
	else :
		Rounded = float(dividend) / float(divisor)
		if Rounded > 1 :
			Rounded = round ( Rounded, 2 )
		else :
			Rounded = round ( 1 / Rounded, 2 )
		return Rounded
##==============-------------------   End   -------------------==============##

def Random_String( length= 17 ):
	rand_string = '_'
	for letter in random.sample('_' + string.ascii_letters + string.hexdigits, length):
		rand_string += letter
	return rand_string
##==============-------------------   End   -------------------==============##

def ordinal( num ):
	'''
	Returns the ordinal number of a given integer, as a string.
	eg. 1 -> 1st, 2 -> 2nd, 3 -> 3rd, etc.
	'''
	if 10 <= num % 100 < 20:
		return '{0}\'th'.format(num)
	else:
		ord = {1 : '\'st', 2 : '\'nd', 3 : '\'rd'}.get(num % 10, '\'th')
		return '{0}{1}'.format(num, ord)
##==============-------------------   End   -------------------==============##

def New_File_Name ( file_name , new_extension='', strip='' ) :
	'''
	Returns a new filename derived from the Old File by adding and or removing
	'''
	filename, extensi	= os.path.splitext(file_name)
	if len(strip) :
		filename = filename.strip(strip)
	extensi = extensi.replace('.','_')
	New		= filename + extensi + new_extension
#	New		= filename + new_extension
	return New
##==============-------------------   End   -------------------==============##

def HuSa( nbyte ):
	'''
	Returns a human readable string from a number
	'''
	suffixes = ['B', 'K', 'M', 'G', 'T', 'P', 'Zilion']
	byte_val = float (nbyte)
	indx     = 0
	while byte_val >= 1024 and indx < len(suffixes):
		byte_val   /= 1024
		indx	   += 1
	res = ( round (byte_val,1) )
	return '%s %s' % (res, suffixes[indx])
##==============-------------------   End   -------------------==============##

def Bild_Dict (key, value, TheDick) :
	if key in TheDick : ## XXX:  Add item to key location
		TheDick[ key ].append ( [value] )
	else : 				## XXX:  The is the first, create key location
		TheDick[ key ] = [ value ]
	return TheDick
##==============-------------------   End   -------------------==============##

def Parse_from_to ( Stream, Dictio, DeBug=False ) :
	message = sys._getframe().f_code.co_name
	if DeBug : print(message,':\n', len(Stream), Stream, '\n', len(Dictio), Dictio)
	Result = Dictio
	Pu_la_cnt = 0

	if not Stream  :
		print ("WTF:? " ,message, " No Stream" ,'\t', Stream ,'\n', Dictio )
		for key in ( Dictio.keys() ) :
			Dictio[key] = 'Pu_la'
		return tuple( Dictio.values() )
	elif not Dictio :
		print ("WTF:? ", message, " No Dictio", '\t', Stream, '\n', Dictio )
		return False

	if DeBug : print ("\n>", repr(Dictio) )
	try :
		for key in Dictio.keys() :
			if DeBug  : print( key, Dictio[key] )
			item = Stream.get (key,'Pu_la')
			if item == 'Pu_la' :
				Result[key] = 'Pu_la'
				Pu_la_cnt += 1
				if DeBug : print ("\nPu_la_la ", key, '\n' )
			else :
				ty = type (item)
				dy = type (Dictio[key])
				if DeBug : print(ty, item ,'\n', dy , Dictio[key])
				if   ty == str and dy == int :
					Result[key] = int (item)
				elif ty == str and dy == float :
					Result[key] = float (item)
				else :
					Result[key] = item
				if DeBug : print("Got : ", item )
		if DeBug : print (message , " Out ", repr(Dictio) , "\nPu_la_cnt = ", Pu_la_cnt), input("N")
	except Exception as e:
		print ("{} -> {!r}".format(message, e))
		input ("All Fuked up")
	if len(Dictio) > 1 :
		return tuple( Dictio.values() )
	else :
		return Dictio[key]
##==============-------------------   End   -------------------==============##

def Resource_Check (Folder='./') :
	print("=" * 60)
	message = sys._getframe().f_code.co_name
	print (datetime.datetime.now().strftime('\n%A: %m/%d/%Y %H:%M:%S %p'))
	print('\n:', message )

	print ("Python is:")
	print ('\n'.join(sorted(sys.path)) )

	print('\nFile       :', __file__)
	print('Access time  :', time.ctime(os.path.getatime(__file__)))
	print('Modified time:', time.ctime(os.path.getmtime(__file__)))
	print('Change time  :', time.ctime(os.path.getctime(__file__)))
	print('Size         :', HuSa(      os.path.getsize( __file__)))

	if os.path.isfile( Folder ) :
		print (Folder, " is a File")
	elif os.path.isdir( Folder ) :
		print (Folder, " is a Folder" )
	elif os.path.islink( Folder ) :
		print (Folder, " is a Link")
	elif os.path.ismount (Folder) :
		print (Folder, " is a Mountpoint")
	else :
		print (Folder, " is WTF?")
	'''
	stat 	= os.stat(Folder)
	print ("Size     :" , HuSa( stat.st_size ))
	print ("Created  :" , time.asctime( time.gmtime( stat.st_ctime ) ) )
	print ("Modifyed :" , time.asctime( time.gmtime( stat.st_mtime ) ) )
	print ("Accesed  :" , time.asctime( time.gmtime( stat.st_atime ) ) )
	'''
	try :
		sys_is = platform.uname()
		print ('\nSystem Name :', sys_is.node , sys_is.system , sys_is.release, '(',sys_is.version,')', sys_is.processor )

		if not (os.path.exists( ffmpeg_bin )) :
			print (message, " ffMpeg Path Does not Exist:")
			return False

		print ("Log File :" ,Log_File)
		print ("FFmpeg   :" ,ffmpeg_bin )

		total, free = ctypes.c_ulonglong(), ctypes.c_ulonglong()
		if sys.version_info >= (3,) or isinstance(path, unicode):
			fun = ctypes.windll.kernel32.GetDiskFreeSpaceExW
		else:
			fun = ctypes.windll.kernel32.GetDiskFreeSpaceExA
		ret = fun( None , None , ctypes.byref(total), ctypes.byref(free))
		if ret == 0:
			raise ctypes.WinError()
			return False
		if ( free.value / total.value ) < 0.25 :
			print (message, "Not Enough Space on Disk")
			return False
		print ("\nTotal : %s  Free %s %s %s"
			% (HuSa(total.value) ,HuSa(free.value) , round( free.value/total.value *100 ) ,'%') )
	except Exception as e:
		message = " WTF? Exception " + type(e)
		print (message + message )
		print( traceback.format_exc()  )
		print( traceback.print_stack() )
	finally :
		print ("\nResources OK\n" )
		return True
##==============-------------------   End   -------------------==============##
