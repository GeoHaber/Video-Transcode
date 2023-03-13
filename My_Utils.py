#!/usr/bin/python

import	os
import	re
import	sys
import	time
import	json
import	shutil
import	ctypes
import psutil
import	random
import	string
import	datetime as TM
import	platform
import	traceback
import	tracemalloc
import	logging

from	typing		import Union
from	functools	import wraps

#  DECORATORS
# XXX: color codes: https://en.wikipedia.org/wiki/ANSI_escape_code#Colors
def color_print(fg: int = 37, bg: int = 40):
	def decorator(func):
		def wrapper(text):
			print(f"\033[{fg}m\033[{bg}m{func(text)}\033[0m")
		return wrapper
	return decorator

def logit(logfile='out.log', de_bug=False):
	def logging_decorator(func):
		@wraps(func)
		def wrapper(*args, **kwargs):
			result = func(*args, **kwargs)
			with open(logfile, 'a') as f:
				if len(kwargs) > 0:
					f.write(f"\n{func.__name__}{args} {kwargs} = {result}\n")
				else:
					f.write(f"\n{func.__name__}{args} = {result}\n")
			if de_bug:
				if len(kwargs) > 0:
					print(f"{func.__name__}{args} {kwargs} = {result}")
				else:
					print(f"{func.__name__}{args} = {result}")
			return result
		return wrapper
	return logging_decorator
'''
@logit(logfile='mylog.log', de_bug=True)
def my_function(x, y):
	return x + y
'''

def handle_exception(func):
	"""Decorator to handle exceptions."""
	@wraps(func)
	def wrapper(*args, **kwargs):
		try:
			return func(*args, **kwargs)
		except Exception as e:
			print(f"Exception in {func.__name__}: {e}")
			logging.exception(f"Exception in {func.__name__}: {e}",exc_info=True, stack_info=True)
#			sys.exit(1)
		except TypeError :
			print(f"{func.__name__} wrong data types")
		except IOError:
			print("Could not write to file.")
		except :
			print("Someting Else?")
		else:
			print("No Exceptions")
		finally:
			logging.error("Error: ", exc_info=True)
			logging.error("uncaught exception: %s", traceback.format_exc())
	return wrapper

def measure_cpu_utilization(func):
	"""Measure CPU utilization, number of cores used, and their capacity."""
	@wraps(func)
	def wrapper(*args, **kwargs):
		cpu_count = psutil.cpu_count(logical=True)
		strt_time = time.monotonic()
		cpu_prcnt = psutil.cpu_percent(interval=0.1, percpu=True)
		result = func(*args, **kwargs)
		end_time = time.monotonic()
		cpu_percnt = sum(cpu_prcnt) / cpu_count
		return result, cpu_percnt, cpu_prcnt
	return wrapper

def log_exceptions(func):
	"""Log exceptions that occur within a function."""
	@wraps(func)
	def wrapper(*args, **kwargs):
		try:
			return func(*args, **kwargs)
		except Exception as e:
			print(f"Exception in {func.__name__}: {e}")
			logging.exception(f"Exception in {func.__name__}: {e}",exc_info=True, stack_info=True)
	return wrapper

def measure_execution_time(func):
	"""Measure the execution time of a function."""
	@wraps(func)
	def wrapper(*args, **kwargs):
		strt_time = time.perf_counter()
		result = func(*args, **kwargs)
		end_time = time.perf_counter()
		duration = end_time - strt_time
		print(f"{func.__name__}: Execution time: {duration:.5f} sec")
		return result
	return wrapper

def measure_memory_usage(func):
	"""Measure the memory usage of a function."""
	@wraps(func)
	def wrapper(*args, **kwargs):
		tracemalloc.start()
		result = func(*args, **kwargs)
		current, peak = tracemalloc.get_traced_memory()
		print(f"{func.__name__}: Mem usage: {current / 10**6:.6f} MB (avg), {peak / 10**6:.6f} MB (peak)")
		tracemalloc.stop()
		return result
	return wrapper

def performance_check(func):
	"""Measure performance of a function"""
	@log_exceptions
	@measure_execution_time
	@measure_memory_usage
	@measure_cpu_utilization
	@wraps(func)
	def wrapper(*args, **kwargs):
		return func(*args, **kwargs)
	return wrapper

def perf_monitor(func):
	""" Measure performance of a function """
	@wraps(func)
	def wrapper(*args, **kwargs):
		strt_time			= time.perf_counter()
		cpu_percent_prev	= psutil.cpu_percent(interval=0.05, percpu=False)
		tracemalloc.start()
		try:
			return func(*args, **kwargs)
		except Exception as e:
			logging.exception(f"Exception in {func.__name__}: {e}",exc_info=True, stack_info=True)
		finally:
			current, peak = tracemalloc.get_traced_memory()
			tracemalloc.stop()
			cpu_percent = psutil.cpu_percent(interval=None, percpu=False)
			cpu_percnt = cpu_percent - cpu_percent_prev
			end_time = time.perf_counter()
			duration = end_time - strt_time
			msj = f"{func.__name__} \t Used {abs(cpu_percnt):>5.1f} % CPU for {hm_time(duration)} \t Mem: [avrg:{hm_sz(current):>8}, max:{hm_sz(peak):>8}]\t({func.__doc__})"
			logging.info(msj)
			# update previous CPU usage for the next function call
	return wrapper


def measure_cpu_time(func):
	def wrapper(*args, **kwargs):
		start_time = time.time()
		cpu_percent = psutil.cpu_percent(interval=None, percpu=True)
		result = func(*args, **kwargs)
		elapsed_time = time.time() - start_time
		cpu_percent = [p - c for p, c in zip(psutil.cpu_percent(interval=None, percpu=True), cpu_percent)]
		print(f"Function {func.__name__} used {sum(cpu_percent)/len(cpu_percent)}% CPU over {elapsed_time:.2f} seconds")
		return result
	return wrapper

##>>============-------------------<  End  >------------------==============<<##

#  CLASES
# XXX: https://shallowsky.com/blog/programming/python-tee.html

class Tee:
	''' implement the Linux Tee funftion '''

	def __init__(self, *targets):
		self.targets = targets

	def __del__(self):
		for target in self.targets:
			if target not in (sys.stdout, sys.stderr):
				target.close()

	def write(self, obj):
		for target in self.targets:
			try:
				target.write(obj)
				target.flush()
			except Exception:
				pass

	def flush(self):
		pass

class RunningAverage:
	''' Compute the running averaga of a value '''

	def __init__(self):
		self.n = 0
		self.avg = 0

	def update(self, x):
		self.avg = (self.avg * self.n + x) / (self.n + 1)
		self.n += 1

	def get_avg(self):
		return self.avg

	def reset(self):
		self.n = 0
		self.avg = 0
##>>============-------------------<  End  >------------------==============<<##
## Functions
def hm_time(timez: float) -> str:
	'''Print time as years, months, weeks, days, hours, min, sec'''
	units = {'year': 31536000,
			 'month': 2592000,
			 'week': 604800,
			 'day': 86400,
			 'hour': 3600,
			 'min': 60,
			 'sec': 1,
			}
	if timez < 0:
		return "Error negative"
	elif timez == 0 :
		return "Zero"
	elif timez < 0.001:
		return f"{timez * 1000:.3f} ms"
	elif timez < 60:
		return f"{timez:>5.3f} sec{'s' if timez > 1 else ''}"
	else:
		frmt = []
		for unit, seconds_per_unit in units.items() :
			value = timez // seconds_per_unit
			if value != 0:
				frmt.append(f"{int(value)} {unit}{'s' if value > 1 else ''}")
			timez %= seconds_per_unit
		return ", ".join(frmt[:-1]) + " and " + frmt[-1] if len(frmt) > 1 else frmt[0] if len(frmt) == 1 else "0 sec"

def Trace(message: str, exception: Exception, debug: bool = False) -> None:
	"""Prints a traceback and debug info for a given exception"""
	max_chars = 42
	print("+-" * max_chars)
	print(f"Msg: {message}\nErr: {exception}\nRep: {repr(exception)}")
	print("-+" * max_chars)
	max_chars *= 2

	print("Stack")
	print("=" * max_chars)
	stack = traceback.extract_stack()
	template = " {filename:<26} | {lineno:5} | {funcname:<20} | {source:>12}"
	for filename, lineno, funcname, source in stack:
		if funcname != "<module>":
			funcname = funcname + "()"
		print(
			template.format(
				filename=os.path.basename(filename),
				lineno=lineno,
				source=source,
				funcname=funcname,
			)
		)

	print("=" * max_chars)
	print("Sys Exec_Info")
	exc_type, exc_value, exc_traceback = sys.exc_info()
	print("-" * max_chars)
	for frame in traceback.extract_tb(exc_traceback):
		print(
			f" {os.path.basename(frame.filename):<26} | {frame.lineno:5} | {frame.name:20} | {frame.line:12} "
		)
		print("-" * max_chars)
	print("=" * max_chars)
	logging.exception(f"Msg: {message}   Err: {exception}", exc_info=True, stack_info=True)
#	logging.error    (f"Msg: {message}   Err: {exception}", exc_info=True, stack_info=True))

	time.sleep(3)

##>>============-------------------<  End  >------------------==============<<##

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


def copy_move(src: str, dst: str, keep_it: bool = False) -> bool:
	"""Move or copy a file from src to dst, optionally keeping the original file.

	Returns True if the file was moved/copied successfully, False otherwise.
	"""
	if os.path.exists(dst) and os.path.samefile(src, dst):
		# Files are the same, do nothing
	#	print(f"{src} and {dst} are the same file, doing nothing.")
		time.sleep(3)
		return True

	if keep_it:
		# Copy file to destination and do not delete original
		try:
			shutil.copy2(src, dst)
	#		print(f"{src} copied to {dst}, not deleted.")
			return True
		except (PermissionError, IOError, OSError) as err:
			print(f"Error copying {src} to {dst}: {err}")
			return False

	# Move file to destination and delete original
	try:
		shutil.move(src, dst)
	#	print(f"{src} moved to {dst}")
		return True
	except (PermissionError, IOError, OSError) as err:
		print(f"Error moving {src} to {dst}: {err}")
		return False
##==============-------------------   End   -------------------==============##


def print_alighned(list: str) -> None :
	'''
	print a formated table with the {list} values provided
	'''
	lens = []
	for col in zip(*list):
		lens.append(max([len(v) for v in col]))
	format = "  ".join(["{:<" + str(l) + "}" for l in lens])
	for row in list:
		print(format.format(*row))
##==============-------------------   End   -------------------==============##


def divd_strn(val: str ) -> float:
	msj = sys._getframe().f_code.co_name
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


def test_filename(filename: str) -> None:
	legal_chars = '[A-Za-z0-9._-]+'
	if re.fullmatch(legal_chars, filename):
		print(f'{filename} is a legal filename.')
	else:
		print(f'{filename} is NOT a legal filename.')
		out_file =  re.sub(r'[^\w\s_-]+', '', filename).strip().replace(' ', '_')
		print ( f'{out_file} is rename it')

#test_filename("myfile.txt")
#test_filename("my file.txt")
##==============-------------------   End   -------------------==============##


def stmpd_rad_str(leng=13, head=''):
	_time = TM.datetime.now()
	rand_ = f"{_time:%M%S}"
	for char in random.sample( string.ascii_letters + string.hexdigits, leng):
		rand_ += char
	return head +rand_
##==============-------------------   End   -------------------==============##


def ordinal(num: str) -> str:
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


def hm_sz(numb: Union[str, int, float], type: str = "B") -> str:
	'''convert file size to human readable format'''
	numb = float(numb)
	try:
		if numb < 1024.0:
			return f"{numb} {type}"
		for unit in ['','K','M','G','T','P','E']:
			if numb < 1024.0:
				return f"{numb:.2f} {unit}{type}"
			numb /= 1024.0
		return f"{numb:.2f} {unit}{type}"
	except Exception as e:
		logging.exception(f"Error {e}", exc_info=True, stack_info=True)
		print (e)
#		traceback.print_exc()
##==============-------------------   End   -------------------==============##

def get_tree_size(path: str) -> int:
	"""Return total size of files in path and subdirs. If is_dir() or stat() fails, print an error message to stderr
	and assume zero size (for example, file has been deleted).
	"""
	total_size = 0
	for entry in os.scandir(path):
		try:
			if entry.is_file(follow_symlinks=False):
				total_size += entry.stat(follow_symlinks=False).st_size
			elif entry.is_dir(follow_symlinks=False):
				total_size += get_tree_size(entry.path)
		except (OSError, ValueError) as e:
			logging.error(f" {e}", exc_info=True)
			print(f"Error in {get_tree_size.__name__} when processing {entry.name}: {e}", file=sys.stderr)
	return total_size

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

def parse_from_to(strm, dictio, de_bug=True):
	msj = sys._getframe().f_code.co_name
	resul = {}
	try:
		resul = {k: (int(strm[k]) if type(dictio[k]) == int else
					 float(strm[k]) if type(dictio[k]) == float else
					 dict(strm[k]) if type(dictio[k]) == dict else
					 strm[k])
				 for k in dictio.keys() if k in strm}
	except Exception as e:
		logging.exception(f"Error {e}", exc_info=True, stack_info=True, extra=msj)
		msj = f'\n{len(strm)}\n{strm}\n{len(resul)}\n{resul}'
		print(msj)
		Traceback.print_exc()
		input("An error occurred.")

	if len(resul) > 1:
		return tuple(resul.values())
	elif len(resul) == 1:
		return next(iter(resul.values()))
	else:
		return None

##==============-------------------   End   -------------------==============##

def res_chk(folder='.'):
	msj = sys._getframe().f_code.co_name
	print("=" * 60)
	print(datetime.datetime.now().strftime('\n%a:%b:%Y %T %p'))
	print('\n:>', msj)
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
			print(msj, " ffMpeg Path Does not Exist:")
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
			print(msj, " Less that 30% Space on Disk")
			return False
		print("\nTotal : %s  Free %s %s %s"
			  % (hm_sz(total.value), hm_sz(free.value), round(free.value / total.value * 100), '%'))
	except Exception as e:
		logging.exception(f"Error {e}", exc_info=True, stack_info=True, extra=msj)
		msj += " WTF? Exception "
		Trace (msj, e)

	finally:
		print("\nResources OK\n")
		return True
##==============-------------------   End   -------------------==============##
