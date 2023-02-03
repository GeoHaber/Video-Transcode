#!/usr/bin/python
# -*- coding: utf-8 -*-

# XXX KISS

import	os
import	sys
import	time
import	json
import	shutil
import	ctypes
import	random
import	string
import	datetime as TM
import	platform
import	traceback
import	tracemalloc
import	logging
from	functools	import wraps

# XXX: C:\Users\Geo\Documents\GitHub\yolov5\utils\general.py

#  DECORATORS
def trycatch(func):
	""" Wraps the decorated function in a try-catch. If function fails print out the exception. """
	@wraps(func)
	def wrapper(*args, **kwargs):
		try:
			res = func(*args, **kwargs)
			return res
		except Exception as e:
			print(f"Exception in {func.__name__}: {e}")
		finally :
			pass
	return wrapper

def handle_exception(func):
	"""Decorator to handle exceptions."""
	@wraps(func)
	def wrapper(*args, **kwargs):
		try:
			res = func(*args, **kwargs)
			return res
#			return func(*args, **kwargs)
		except Exception as e:
			print(f"Exception in {func.__name__}: {e}")
			logging.error("Error: %s", e, exc_info=True)
#			sys.exit(1)
		except TypeError :
			print(f"{func.__name__} wrong data types")
			logging.error("Error: %s", e, exc_info=True)
		except IOError:
			print("Could not write to file.")
			logging.error("Error: %s", e, exc_info=True)
			logging.error("uncaught exception: %s", traceback.format_exc())
		except :
			print("someting Else")
			logging.error("uncaught exception: %s", traceback.format_exc())
		else:
			print("No Exceptions")
		finally:
			pass
	return wrapper

def performance_check(func):
	"""Measure performance of a function"""
	@wraps(func)
	def wrapper(*args, **kwargs):
		tracemalloc.start()
		start_time =	time.perf_counter()
		try:
			res = func(*args, **kwargs)
		except Exception as e:
			print(f"Exception in {func.__name__}: {e}")
			logging.error("Error: %s", e, exc_info=True)
			handle_exception( func(*args, **kwargs))
			return wrapper
		finally:
			duration =		time.perf_counter() - start_time
			current, peak = tracemalloc.get_traced_memory()
			tracemalloc.stop()
			print(	f"{'.'*60}"
					f"\n{func.__name__} ({func.__doc__})\n"
					f" Mem awrg: {current / 10**6:.6f} MB"
					f" Mem peak: {peak    / 10**6:.6f} MB"
					f" Time: {duration:.5f} sec"
					f"\n{'.'*60}" )
		return res
	return wrapper

##>>============-------------------<  End  >------------------==============<<##

#  CLASES
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

#FUNCTIONS
def file_age(path=__file__):
	# Return days since last file update
	dt = (datetime.now() - TM.fromtimestamp(Path(path).stat().st_mtime))  # delta
	return dt.days  # + dt.seconds / 86400  # fractional days
def file_update_date(path=__file__):
	# Return human-readable file modification date, i.e. '2021-3-26'
	t = TM.fromtimestamp(Path(path).stat().st_mtime)
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
def Trace ( message, e, DeBug= False ) :
	messa = sys._getframe().f_code.co_name
	str_t = TM.datetime.now()
	mx = 42
	print("+-"*mx)
	print(f'{message}\nError: {e}\nRepr: {repr(e)}' )
	print("-+"*mx)
	mx *= 2

	print ("Stack")
	print("="*mx,)
	stack = traceback.extract_stack()
	template = ' {filename:<26} | {lineno:5} | {funcname:<20} | {source:>12}'
	for filename, lineno, funcname, source in stack:
		if  funcname != '<module>':
			funcname = funcname + '()'
		print(template.format(
				filename=os.path.basename(filename),
				lineno=lineno,
				source=source,
				funcname=funcname)
				)
	print("="*mx,)

	print ("Sys Exec_Info")
	exc_type, exc_value, exc_traceback = sys.exc_info()
	print("-"*mx)
	for frm_ in traceback.extract_tb(exc_traceback):
#        print( (frm_comp) )
		print(f" {os.path.basename(frm_.filename):<26} | {frm_.lineno:5} | {frm_.name:20} | {frm_.line:12} " )
		print("-"*mx)
	print("="*mx)

	time.sleep(3)
##==============-------------------   End   -------------------==============##

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
#        input ("Delete?")
#        os.remove(src)
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
	Returns floating point resul for string (n/d) or val it's fp '.'
	'''
#    input ( val )
	r = 1
	if '/' in val:
		n, d = val.split('/')
		n = float(n)
		d = float(d)
		if n != 0 and d != 0 :
			r = n / d
	elif '.' in val:
		r = float(val)
	return round( r, 3)
##==============-------------------   End   -------------------==============##


def stmpd_rad_str(leng=13, head=''):
	_time = TM.datetime.now()
	rand_ = f"{_time:%y%j%H%M%S}"
	for char in random.sample( string.ascii_letters + string.hexdigits, leng):
		rand_ += char
	return head +rand_
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


def get_new_fname(file_name, new_ext='', strip=''):
	'''
	Returns a new filename derived from the Old File by adding and or removing
	'''
	fnm, ext = os.path.splitext(file_name)
	if len(strip):
		fnm = fnm.strip(strip)
	if new_ext == strip :
		return fnm +new_ext
	else:
		return fnm +ext +new_ext
##==============-------------------   End   -------------------==============##


def hm_sz( nbyte, type="B" ):
	'''
	Returns a human readable string from a number
	+ or -
	'''
	sufix = ['', 'K', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y']
	indx = 0

	if not nbyte:
		return '0 ' +type

	elif int(nbyte) < 0 :
		sign = '-'
	else:
		sign = ''

	valu = abs(float(nbyte))
	while valu >= 1024 and indx <= len(sufix) :
		valu /= 1024
		indx += 1

	return f'{sign}{round(valu,1)} {sufix[indx]}{type}'
##==============-------------------   End   -------------------==============##

def safe_options(strm, opts ):
	safe = {}
	# Only copy options that are expected and of correct type
	# (and do typecasting on them)
	for k, v in opts.items():
		if k in opts and v is not None:
			typ = opts[k]
			try:
				safe[k] = typ(v)
			except ValueError:
				pass
	return safe

def prs_frm_to(strm, dictio, DeBug=False ):
	resul = dictio
	try:
		for k in dictio.keys():
			item = strm.get(k, '_nvald_')
			if item == '_nvald_':
				resul[k] = '_nvald_'
				if DeBug : print ('_nvald_', k, '\n' )
			else:
				ty = type(item)
				dy = type(dictio[k])
				if   ty == str and dy == int:
					resul[k] = int(item)
				elif ty == str and dy == float:
					resul[k] = float(item)
				elif dy == dict:
					resul[k] = dict(item)
				else:
					resul[k] = item
	except Exception as e:
		messa = f'\n{len(strm)}\n{strm}\n{len(resul)}\n{resul}'
		print(messa)
		Trace(messa, e)
		input("All Fuked up")

	if len(dictio) > 1:
		return tuple(resul.values())
	else:
		return resul[k]
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
#            summary = traceback.StackSummary.extract( traceback.walk_stack(None) )
#            print("\n Err:",{err},"\n",''.join(summary.format()))
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
