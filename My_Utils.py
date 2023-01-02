#!/usr/bin/python
# -*- coding: utf-8 -*-

# XXX KISS

import os
import sys
import time
import json
import shutil
import ctypes
import random
import string
import datetime
import platform
import traceback

# XXX: C:\Users\Geo\Documents\GitHub\yolov5\utils\general.py

def file_age(path=__file__):
	# Return days since last file update
	dt = (datetime.now() - datetime.fromtimestamp(Path(path).stat().st_mtime))  # delta
	return dt.days  # + dt.seconds / 86400  # fractional days
def file_update_date(path=__file__):
	# Return human-readable file modification date, i.e. '2021-3-26'
	t = datetime.fromtimestamp(Path(path).stat().st_mtime)
	return f'{t.year}-{t.month}-{t.day}'
def file_size(path):
	# Return file/dir size (MB)
	mb = 1 << 20  # bytes to MiB (1024 ** 2)
	path = Path(path)
	if path.is_file():
		return path.stat().st_size / mb
	elif path.is_dir():
		return sum(f.stat().st_size for f in path.glob('**/*') if f.is_file()) / mb
	else:
		return 0.0
##>>============-------------------<  End  >------------------==============<<##

# XXX: https://shallowsky.com/blog/programming/python-tee.html
class Tee (list):
	def __init__(self, *targets):
		self.targets = targets

	def __del__(self):
		for ftarg in self.targets:
			if ftarg != sys.stdout and ftarg != sys.stderr:
				ftarg.close()

	def write(self, obj):
		DeBug = False
		for ftarg in self.targets:
			try:
				ftarg.write(obj)
				ftarg.flush()
			except Exception as x:
				if DeBug : print (repr(x))
				continue
	def flush(self):
		return

##>>============-------------------<  End  >------------------==============<<##


def copy_move(src, dst, keep_it=False):
	# https://stackoverflow.com/questions/7419665/python-move-and-overwrite-files-and-folders
	messa = sys._getframe().f_code.co_name + '-:'

	do_it = shutil.move
	if keep_it:
		do_it = shutil.copy2
		print(f"Placebo Copied to {dst} Not Deleted:\n{src}")
		time.sleep(1)
	try:
#        if os.path.exists(dst) and os.path.samefile(src, dst):
#            os.utime(dst, None)
#            os.remove(src)
#            return True

		do_it(src, dst)

	except (PermissionError, IOError, OSError) as er:
		messa += f' Exception: '
		print (messa, er)
		time.sleep(3)
#		input ("Delete?")
#		os.remove(src)
	return True
	##==============-------------------   End   -------------------==============##

def print_alighned(list_of_strings):
	'''
	print formated table with the values provided
	'''

	lens = []
	for col in zip(*list_of_strings):
		lens.append(max([len(v) for v in col]))
	format = "  ".join(["{:<" + str(l) + "}" for l in lens])
	for row in list_of_strings:
		print(format.format(*row))
##==============-------------------   End   -------------------==============##


def divd_strn( val ):
	messa = sys._getframe().f_code.co_name
	'''
	Returns floating point resul for string (a/b)
	'''
#	input ( val )
	if '/' in val:
		n, d = val.split('/')
		n = float(n)
		d = float(d)
		if n > 0.0 and d > 0.0:
			r = n / d
		elif n == 0 :
			messa += f" ! Zero Divided by     ! {n} / {d}"
			print (messa)
			return 0
		else :
			messa += f" NAN Division by Zero   ! {n} / {d}"
			print (messa)
			return 0
	elif '.' in val:
		r = float(val)
	return round( r, 2)
##==============-------------------   End   -------------------==============##


def stmpd_rad_str(length=13):
	_time = datetime.datetime.now()
	rand_string = f"{_time:%Y %j %H-%M-%S }"
	for char in random.sample( string.ascii_letters + string.hexdigits, length):
		rand_string += char
	return rand_string
##==============-------------------   End   -------------------==============##


def ordinal(num):
	'''
	Returns the ordinal number of a given integer, as a string.
	eg. 1 -> 1st, 2 -> 2nd, 3 -> 3rd, etc.
	'''
	if 10 <= num % 100 < 20:
		return '{0}\'th'.format(num)
	else:
		ord = {1: '\'st', 2: '\'nd', 3: '\'rd'}.get(num % 10, '\'th')
		return f'{num}{ord}'
##==============-------------------   End   -------------------==============##


def get_new_fname(file_name, new_extension='', strip=''):
	'''
	Returns a new filename derived from the Old File by adding and or removing
	'''
	filename, ext = os.path.splitext(file_name)
	if len(strip):
		filename = filename.strip(strip)
	if new_extension == strip :
		return filename + new_extension
	else:
		return filename + ext + new_extension
##==============-------------------   End   -------------------==============##


def hm_sz( nbyte, type = "B" ):
	'''
	Returns a human readable string from a number
	+ or -
	'''
	if not nbyte:
		return '0 B'

	sufix = ['', 'K', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y']

	indx = 0
	sign = ''
	if int(nbyte) < 0 :
		sign = '-'

	valu = abs(float(nbyte))
	while valu >= 1024 and indx < len(sufix) - 1:
		valu /= 1024
		indx += 1

	return f'{sign}{round(valu + 0.05,1)} {sufix[indx]}{type}'
##==============-------------------   End   -------------------==============##

def prs_frm_to(strm, dictio, DeBug =False ):
#	messa = sys._getframe().f_code.co_name
#	str_t = datetime.datetime.now()

	resul = dictio
	try:
		for key in dictio.keys():
			item = strm.get(key, 'Pu_la')
			if item == 'Pu_la':
				resul[key] = 'Pu_la'
				if DeBug : print ("\nPu_la in:", key, '\n' )
			else:
				ty = type(item)
				dy = type(dictio[key])

#				if DeBug : print('Before Key:',key,'\n', ty, item ,'\n', dy , dictio[key])

				if   ty == str and dy == int:
					resul[key] = int(item)
				elif ty == str and dy == float:
					resul[key] = float(item)
				elif dy == dict:
					resul[key] = dict(item)
				else:
					resul[key] = item
#				if DeBug : print('After  Key:',key,'\n', dy , dictio[key])

	except Exception as e:
		print( '\n', len(strm), strm, '\n', len(dictio), dictio )
		messa += f'\n{len(strm)}\n{json.dumps(strm, indent=2)}\n{len(resul)}\n{json.dumps(dictio, indent=2)}'
		print(messa)
		Trace(messa, e)
		input("All Fuked up")

	if len(dictio) > 1:
		return tuple(dictio.values())
	else:
		return dictio[key]
	'''
	rsult = dict ()
	# XXX: Fast version only defined data is copied if done reversed walk over js_info could see the extra unused information  :D
	for key in _mtdta.keys():
		rsult[key] = js_info.get(key)
		if not rsult[key]  :
			pass
		if DeBug:	print (key, ' = ', rsult[key] )
	if DeBug:	print ( json.dumps( rsult, indent=2 ) )

	if DeBug :
		print(f"  +{messa}=: Start: {str_t:%T}")

		if not strm  :
			print ("WTF:? No Stream for Dictio" ,'\n', dictio )
			for key in ( dictio.keys() ) :
				dictio[key] = 'Pu_la'
			raise Except

		if not dictio :
			print ("WTF:? No Dictio for Strm ",'\n', repr(dictio) )
			raise Except

		print( type (strm),  '\n', len(strm),  'Items:\n', json.dumps(strm,   indent=2), '\n',
			   type(dictio), '\n', len(dictio),'Items:\n', json.dumps(dictio, indent=2) )
	'''
##==============-------------------   End   -------------------==============##

def Trace ( message, e, DeBug= False ) :
	messa = sys._getframe().f_code.co_name
	str_t = datetime.datetime.now()
	print("+-"*40)
	print(f'{message}\nError: {e}' )
	if DeBug : print( {repr(e)} )
	print("-+"*40)

#	print("%20s | %10s | %5s | %10s" %("File Name", "Method Name", "Line Number", "Line"))
	print("^"*80,)
	stack = traceback.extract_stack()
	template = ' {filename:<26} | {lineno:5} | {funcname:20} | {source:9}'
	for filename, lineno, funcname, source in stack:
		if  funcname != '<module>':
			funcname = funcname + '()'
		print(template.format(
				filename=os.path.basename(filename),
				lineno=lineno,
				source=source,
				funcname=funcname)
				)
	print("^"*80,)
	print("-"*80)

	exc_type, exc_value, exc_traceback = sys.exc_info()
	for frm_comp in traceback.extract_tb(exc_traceback):
#		print( (frm_comp) )
		print(f" {os.path.basename(frm_comp.filename):<26} | {frm_comp.lineno:5} | {frm_comp.name:20} | {frm_comp.line:9}" )
		print("-"*80)

	time.sleep(2)
##==============-------------------   End   -------------------==============##


def res_chk(folder='.'):
	messa = sys._getframe().f_code.co_name
	print("=" * 60)
	print(datetime.datetime.now().strftime('\n%a:%b:%Y %T %p'))
	print('\n:>', messa)
	print(os.getcwd())

	print("Python is:", '\n'.join(sorted(sys.path)), '\n')

	print('\nFile       :', __file__)
	print('Access time  :', time.ctime(os.path.getatime(__file__)))
	print('Modified time:', time.ctime(os.path.getmtime(__file__)))
	print('Change time  :', time.ctime(os.path.getctime(__file__)))
	print('Size         :', hm_sz(os.path.getsize(__file__)))

	if os.path.isfile(folder):
		print('\n', folder, " is a File")
	elif os.path.isdir(folder):
		print('\n', folder, " is a folder")
	elif os.path.islink(folder):
		print('\n', folder, " is a Link")
	elif os.path.ismount(folder):
		print('\n', folder, " is a Mountpoint")
	else:
		print('\n', folder, " is WTF?")

	try:
		sys_is = platform.uname()
		print('\nSystem : ', sys_is.node, sys_is.system, sys_is.release,
			  '(', sys_is.version, ')', sys_is.processor)

		print("FFmpeg   :", ffmpath)
		if not (os.path.exists(ffmpath)):
			print(messa, " ffMpeg Path Does not Exist:")
			return False

		print("Log File :", Log_File)

		total, free = ctypes.c_ulonglong(), ctypes.c_ulonglong()
		if sys.version_info >= (3,) or isinstance(path, unicode):
			fun = ctypes.windll.kernel32.GetDiskFreeSpaceExW
		else:
			fun = ctypes.windll.kernel32.GetDiskFreeSpaceExA
		ret = fun(None, None, ctypes.byref(total), ctypes.byref(free))
		if ret == 0:
			raise ctypes.WinError()
		if (free.value / total.value) < 0.30:
			print(messa, " Less that 30% Space on Disk")
			return False
		print("\nTotal : %s  Free %s %s %s"
			  % (hm_sz(total.value), hm_sz(free.value), round(free.value / total.value * 100), '%'))
	except Exception as e:
		messa += " WTF? Exception "
		Trace (messa, e)

	finally:
		print("\nResources OK\n")
		return True
##==============-------------------   End   -------------------==============##


def get_tree_size(path):
	"""Return total size of files in path and subdirs. If
	is_dir() or stat() fails, print an error messa to stderr
	and assume zero size (for example, file has been deleted).
	"""
	messa = sys._getframe().f_code.co_name + '-:'

	total = 0
	for entry in os.scandir(path):
		try:
			is_dir = entry.is_dir(follow_symlinks=False)
		except (IOError, OSError) as err:
			print('Error calling is_dir():', err, file=sys.stderr)
			Trace ( messa, err )
#			summary = traceback.StackSummary.extract( traceback.walk_stack(None) )
#			print("\n Err:",{err},"\n",''.join(summary.format()))
			continue
		if is_dir:
			total += get_tree_size(entry.path)
		else:
			try:
				total += entry.stat(follow_symlinks=False).st_size
			except (IOError, OSError) as err:
				print('Error calling stat():', err, file=sys.stderr)
				Trace ( messa, err )

	return total
##==============-------------------   End   -------------------==============##
