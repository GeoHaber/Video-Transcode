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
##==============-------------------   End   -------------------==============##


def Print_Aligned(List_of_Strings):
	'''
	print formated table with the values provided
	'''

	lens = []
	for col in zip(*List_of_Strings):
		lens.append(max([len(v) for v in col]))
	format = "  ".join(["{:<" + str(l) + "}" for l in lens])
	for row in List_of_Strings:
		print(format.format(*row))
##==============-------------------   End   -------------------==============##


def String_div(the_string, DeBug=False):
	messa = sys._getframe().f_code.co_name
	'''
	Returns floating result for string (a/b)
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
		Resul = float(dividend) / float(divisor)
		if Resul > 1:
			Resul = round(Resul,   2)
		else:
			Resul = round(1 / Resul, 2)
		return Resul
##==============-------------------   End   -------------------==============##


def Random_String(length=17):
	rand_string = '_'
	for letter in random.sample('_' + string.ascii_letters + string.hexdigits, length):
		rand_string += letter
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


def New_File_Name(file_name, new_extension='', strip=''):
	'''
	Returns a new filename derived from the Old File by adding and or removing
	'''
	filename, extensi = os.path.splitext(file_name)
	if len(strip):
		filename = filename.strip(strip)
#	extensi = extensi.replace('.','_')
	New		= filename + extensi + new_extension
#    New = filename + new_extension
	return New
##==============-------------------   End   -------------------==============##


def HuSa(nbyte):
	'''
	Returns a human readable string from a number
	'''
	messa = sys._getframe().f_code.co_name
	suffixes = ['B', 'K', 'M', 'G', 'T', 'P', 'Zilion']
	try:
		byte_val = float(nbyte)
		indx = 0
		while byte_val >= 1024 and indx < len(suffixes) - 1:
			byte_val /= 1024
			indx += 1
		res = (round(byte_val + 0.05 ))
	except Exception as e:
		messa += e
		input(messa)
	return '%s %s' % (res, suffixes[indx])
##==============-------------------   End   -------------------==============##


def Parse_from_to(Stream, Dictio, DeBug=False):
	#	DeBug = True
	messa = sys._getframe().f_code.co_name
	if DeBug:
		print(messa, ':\n', len(Stream), Stream, '\n', len(Dictio), Dictio)
	Result = Dictio
	Pu_la_cnt = 0

	if not Stream:
		print("WTF:? ", messa, " No Stream", '\t', Stream, '\n', Dictio)
		for key in (Dictio.keys()):
			Dictio[key] = 'Pu_la'
		return tuple(Dictio.values())
	elif not Dictio:
		print("WTF:? ", messa, " No Dictio", '\t', Stream, '\n', Dictio)
		return False

	if DeBug:
		print("\n>", repr(Dictio))
	try:
		for key in Dictio.keys():
			if DeBug:
				print(key, Dictio[key])
			item = Stream.get(key, 'Pu_la')
			if item == 'Pu_la':
				Result[key] = 'Pu_la'
				Pu_la_cnt += 1
				if DeBug:
					print("\nPu_la_la ", key, '\n')
			else:
				ty = type(item)
				dy = type(Dictio[key])
				if DeBug:
					print(ty, item, '\n', dy, Dictio[key])
				if ty == str and dy == int:
					Result[key] = int(item)
				elif ty == str and dy == float:
					Result[key] = float(item)
				else:
					Result[key] = item
				if DeBug:
					print("Got : ", item)
		if DeBug:
			print(messa, " Out ", repr(Dictio),
				  "\nPu_la_cnt = ", Pu_la_cnt), input("N")
	except Exception as e:
		messa += f':\n {len(Stream)}\t{Stream}\n{len(Dictio)}\t{Dictio} '
		print(f"\n{messa}\n{e}")
#		print("Is:    {}".format( traceback.print_stack() ) )
		print("Error: {}".format(traceback.print_exc()))
		input("All Fuked up")
	if len(Dictio) > 1:
		return tuple(Dictio.values())
	else:
		return Dictio[key]
##==============-------------------   End   -------------------==============##


def Resource_Check(Folder='.'):
	messa = sys._getframe().f_code.co_name
	print("=" * 60)
	print(datetime.datetime.now().strftime('\n%A: %m/%d/%Y %H:%M:%S %p'))
	print('\n:>', messa)
	print(os.getcwd())

	print("Python is:", '\n'.join(sorted(sys.path)), '\n')

	print('\nFile       :', __file__)
	print('Access time  :', time.ctime(os.path.getatime(__file__)))
	print('Modified time:', time.ctime(os.path.getmtime(__file__)))
	print('Change time  :', time.ctime(os.path.getctime(__file__)))
	print('Size         :', HuSa(os.path.getsize(__file__)))

	if os.path.isfile(Folder):
		print('\n', Folder, " is a File")
	elif os.path.isdir(Folder):
		print('\n', Folder, " is a Folder")
	elif os.path.islink(Folder):
		print('\n', Folder, " is a Link")
	elif os.path.ismount(Folder):
		print('\n', Folder, " is a Mountpoint")
	else:
		print('\n', Folder, " is WTF?")

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
			return False
		if (free.value / total.value) < 0.30:
			print(messa, " Less that 30% Space on Disk")
			return False
		print("\nTotal : %s  Free %s %s %s"
			  % (HuSa(total.value), HuSa(free.value), round(free.value / total.value * 100), '%'))
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

#https://stackoverflow.com/questions/7419665/python-move-and-overwrite-files-and-folders
def Move_or_Copy(src, dst, Dont_del=False):

	message = sys._getframe().f_code.co_name + '-:'
	if os.path.exists(src) :
		message += f"Ok\n{src}"
		try:
			if os.path.exists(dst):
				# in case of the src and dst are the same file
				if os.path.samefile(src, dst):
					return True
			shutil.copy2(src, dst)
			if not Dont_del:
				os.remove(src)
			else:
				print(f" ! Placebo did NOT delete: {src}")
		except PermissionError as e:
			message += f' Permission Error\n{e.args} '
			input(message)

		except OSError as e:  # if failed, report it back to the user ##
			message += f"\n!Error: Src: {src} -> Dst: {dst}\n{e}"
			input(message)
			raise Exception(message)
		else:
			return True
	else:
		message += f"\n!Error: No Src File:\n{src}\n| =>  _Skip_it"
		input(message)
		raise ValueError( message )
	'''
	If Debug then files are NOT deleted, only copied
	str_t = datetime.datetime.now()
	messa  = sys._getframe().f_code.co_name
#	print(f"  +{messa}=: Start: {str_t:%H:%M:%S}")
	Do = shutil.move
	if Dont_del:
		Do = shutil.copy2
		print(f" ! Placebo did NOT delete: {src}")
		time.sleep(1)
	try:
		if os.path.exists(dst):
			# in case of the src and dst are the same file
			if os.path.samefile(src, dst):
				return True
			os.remove(dst)
		Do ( src, dst )
	except PermissionError as ex:
		print (f' Permission: {ex.args}')
		os.chmod (dst, S_IWUSR)
		os.remove(dst)
		Do ( src, dst )
		if not Dont_del:
			input(messa)
		raise Exception(messa)
	return True
	'''
	##==============-------------------   End   -------------------==============##
