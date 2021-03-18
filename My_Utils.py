#!/usr/bin/python
# -*- coding: utf-8 -*-

# XXX KISS
import os
import sys
import time
import shutil
import ctypes
import random
import string
import datetime
import platform
import traceback

# XXX: https://shallowsky.com/blog/programming/python-tee.html

class Tee (list):

    def __init__(self, *targets):
        self.targets = targets

    def __del__(self):
        for ftarg in self.targets:
        	if ftarg != sys.stdout and ftarg != sys.stderr:
        		ftarg.close()

    def write(self, obj):
        for ftarg in self.targets:
            ftarg.write(obj)
            ftarg.flush()

    def flush(self):
        for ftarg in self.targets:
            ftarg.flush()

##>>============-------------------<  End  >------------------==============<<##


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


def divd_strn(the_string, dbg=False):
	messa = sys._getframe().f_code.co_name
	'''
	Returns floating resul for string (a/b)
	'''
	(dividend, divisor) = the_string.split('/')
	if divisor == '0':
		messa += f" ! Division by Zero ! {dividend} / {divisor}"
		print(messa)
		return False
	elif dividend == '0':
		messa += f" ! Zero Divided by  ! {dividend} / {divisor}"
#		print ( messa )
		return False
	else:
		rsul = float(dividend) / float(divisor)
		if rsul > 1:
			rsul = round(rsul,   2)
		else:
			rsul = round(1 / rsul, 2)
		return rsul
##==============-------------------   End   -------------------==============##


def random_string(length=13):
	_time = datetime.datetime.now()
	rand_string = f"{_time:%j-%H-%M-%S_}"
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


def hm_sz(nbyte):
	'''
	Returns a human readable string from a number
	+ or -
	'''
	sufix = ['B', 'K', 'M', 'G', 'Tera', 'Peta', 'Exa', 'Zetta', 'Yotta']

	indx = 0
	sign = ''
	if int(nbyte) < 0 :
		sign = '-'
	valu = abs(float(nbyte))
	while valu >= 1024 and indx < len(sufix) - 1:
		valu /= 1024
		indx += 1

	return f'{sign}{round(valu + 0.05,1)} {sufix[indx]}'
##==============-------------------   End   -------------------==============##


def prs_frm_to(strm, dictio, dbg=False):
	messa = sys._getframe().f_code.co_name
	resul = dictio

	try:
		for key in dictio.keys():
			item = strm.get(key, 'Pu_la')
			if item == 'Pu_la':
				resul[key] = 'Pu_la'
			else:
				ty = type(item)
				dy = type(dictio[key])
				if ty == str and dy == int:
					resul[key] = int(item)
				elif ty == str and dy == float:
					resul[key] = float(item)
				else:
					resul[key] = item
	except Exception as e:
		messa += f':\n {len(strm)}\t{strm}\n{len(dictio)}\t{dictio} '
		print(f"\n{messa}\n{e}")
		input("All Fuked up")
	if len(dictio) > 1:
		return tuple(dictio.values())
	else:
		return dictio[key]
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
		messa = " WTF? Exception " + type(e)
		print(messa + messa)
		print(traceback.format_exc())
		print(traceback.print_stack())
	finally:
		print("\nResources OK\n")
		return True
##==============-------------------   End   -------------------==============##


def get_tree_size(path):
	"""Return total size of files in path and subdirs. If
	is_dir() or stat() fails, print an error messa to stderr
	and assume zero size (for example, file has been deleted).
	"""
	total = 0
	for entry in os.scandir(path):
		try:
			is_dir = entry.is_dir(follow_symlinks=False)
		except OSError as error:
			print('Error calling is_dir():', error, file=sys.stderr)
			continue
		if is_dir:
			total += get_tree_size(entry.path)
		else:
			try:
				total += entry.stat(follow_symlinks=False).st_size
			except OSError as error:
				print('Error calling stat():', error, file=sys.stderr)
	return total
##==============-------------------   End   -------------------==============##

def copy_move(src, dst, keep_it=False):
# https://stackoverflow.com/questions/7419665/python-move-and-overwrite-files-and-folders
#	message = sys._getframe().f_code.co_name + '-:'

	do_it = shutil.move
	if keep_it:
		do_it = shutil.copy2
		print(f" ! Placebo will NOT delete: {src}")
		time.sleep(1)
	try:
		if os.path.exists(dst):
			# in case of the src and dst are the same file
			if os.path.samefile(src, dst):
				os.utime(dst, None)
				return True
		do_it ( src, dst )

	except (PermissionError, OSError) as e:
		print (f'Exception: {e}')
#		input ("Delete?")
		time.sleep(1)
		os.remove( src )
	return True
	##==============-------------------   End   -------------------==============##
