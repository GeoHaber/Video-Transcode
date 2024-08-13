#!/usr/bin/python

import	os
import	re
import	sys
import	time
import	json
import	shutil
import	ctypes
import  psutil
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
'''
\033[91m : Red
\033[92m : Green
\033[93m : Yellow
\033[94m : Blue
\033[95m : Purple
\033[96m : Cyan
\033[0m : Reset color
'''
# XXX: DECORATORS

def color_print(fg: int = 37, bg: int = 40):
	def decorator(func):
		def wrapper(text):
			print(f"\033[{fg}m\033[{bg}m{func(text)}\033[0m")
		return wrapper
	return decorator

def name_time(func):
	"""
	A decorator that prints the function's name, start time, end time,
	and the duration it took to execute the function.
	"""
	@wraps(func)
	def wrapper(*args, **kwargs):
		start_time = time.perf_counter()
		print(f"  +{func.__name__} Start: {time.strftime('%H:%M:%S')}")

		result = func(*args, **kwargs)

		end_time = time.perf_counter()
		duration = end_time - start_time
		hours, remainder = divmod(duration, 3600)
		minutes, seconds = divmod(remainder, 60)
#		print(f"  -Time: {int(hours)}h:{int(minutes)}m:{seconds:.2f}s")
		return result
	return wrapper

def debug(func):
	def wrapper(*args, **kwargs):
		calling_function = sys._getframe().f_back.f_code.co_name if sys._getframe().f_back is not None else "Top Level"
		try:
			result = func(*args, **kwargs)
			return result
		except Exception as e:
			traceback.print_exc()
			print(f"Exception in {calling_function}: {e}")
	return wrapper

def perf_monitor(func):
	@wraps(func)
	def wrapper(*args, **kwargs):
		calling_function = sys._getframe().f_back.f_code.co_name if sys._getframe().f_back is not None else "Top Level"
		strt_time = time.perf_counter()
		cpu_percent_prev = psutil.cpu_percent(interval=0.05, percpu=False)
		tracemalloc.start()
		try:
			return func(*args, **kwargs)
		except Exception as e:
			traceback.print_exc()
			print(f"Exception in {calling_function}: {e}")
		finally:
			current, peak = tracemalloc.get_traced_memory()
			tracemalloc.stop()
			cpu_percent = psutil.cpu_percent(interval=None, percpu=False)
			cpu_percnt = cpu_percent - cpu_percent_prev
			end_time = time.perf_counter()
			duration = end_time - strt_time
			msj = f"{calling_function}\t\tUsed {abs(cpu_percnt):>5.1f} % CPU: {duration:.2f} sec\t Mem: [avr:{current}, max:{peak}]\t({func.__doc__})"
			logging.info(msj)
	return wrapper

def perf_monitor_temp(func):
	""" Measure performance of a function """
	@wraps(func)
	def wrapper(*args, **kwargs):
		strt_time           = time.perf_counter()
		cpu_percent_prev    = psutil.cpu_percent(interval=0.05, percpu=False)
		tracemalloc.start()
		try:
			return func(*args, **kwargs)
		except Exception as e:
			logging.exception(f"Exception in {func.__name__}: {e}",exc_info=True, stack_info=True)
		finally:
			current, peak   = tracemalloc.get_traced_memory()
			tracemalloc.stop()
			cpu_percent     = psutil.cpu_percent(interval=None, percpu=False)
			cpu_percnt      = cpu_percent - cpu_percent_prev
			# New code to measure CPU temperature
			cpu_temp = psutil.sensors_temperatures().get('coretemp')[0].current
			print(f"CPU temperature: {cpu_temp}°C")
			end_time        = time.perf_counter()
			duration        = end_time - strt_time
			msj = f"{func.__name__}\t\tUsed {abs(cpu_percnt):>5.1f} % CPU: {hm_time(duration)}\t Mem: [avr:{hm_sz(current):>8}, max:{hm_sz(peak):>8}]\t({func.__doc__})"
			logging.info(msj)
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

def temperature ():
	sensors = psutil.sensors_temperatures()
	for name, entries in sensors.items():
		print(f"{name}:")
		for entry in entries:
			print(f"  {entry.label}: {entry.current}°C")

##>>============-------------------<  End  >------------------==============<<##

class Tee:
	def __init__(self, *files, error_on=False):
		self.files = files
		self.error_on = error_on
		self.original_stdout = sys.stdout
		self.original_stderr = sys.stderr

	def write(self, obj):
		if not isinstance(obj, str):
			obj = str(obj)

		for file in self.files:
			file.write(obj)
			file.flush()

	def flush(self):
		for file in self.files:
			file.flush()

	def close(self):
		for file in self.files:
			if file not in (self.original_stdout, self.original_stderr):
				file.close()

	def __enter__(self):
		sys.stdout = self
		if self.error_on:
			sys.stderr = self
		return self

	def __exit__(self, exc_type, exc_value, traceback):
		sys.stdout = self.original_stdout
		if self.error_on:
			sys.stderr = self.original_stderr
		self.close()
##>>============-------------------<  End  >------------------==============<<##

class Spinner:
	def __init__(self, spin_text="|/-o+\\", indent=0):
		self.spinner_count	= 0
		self.spin_text		= spin_text
		self.spin_length	= len(spin_text)
		self.prefix			= " " * indent  # Indentation string
	def print_spin(self, extra: str = "") -> None:
		"""
		Prints a spinner in the console to indicate progress.
		Args:
			extra (str): Additional text to display after the spinner.
		"""
		spin_char = self.spin_text[self.spinner_count % self.spin_length]
		sys.stderr.write(f"\r{self.prefix}| {spin_char} | {extra}")
		sys.stderr.flush()
		self.spinner_count += 1
class RunningAverage:
	''' Compute the running average of a value '''

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

class RunningStats:

	def __init__(self):
		self.total = 0.0
		self.count = 0
		self.min_val = float('inf')
		self.max_val = float('-inf')

	def update(self, num):
		self.total += num
		self.count += 1
		self.min_val = min(self.min_val, num)
		self.max_val = max(self.max_val, num)
		self.print_stats()

	def print_stats(self):
		running_average = self.total / self.count if self.count != 0 else 0.0
		print(f"Current number: {num}, Running average: {running_average}")
		print(f"Minimum value: {self.min_val}")
		print(f"Maximum value: {self.max_val}")
##>>============-------------------<  End  >------------------==============<<##

class Color:
	BLACK = "\033[30m"
	RED = "\033[31m"
	GREEN = "\033[32m"
	YELLOW = "\033[33m"
	BLUE = "\033[34m"
	MAGENTA = "\033[35m"
	CYAN = "\033[36m"
	WHITE = "\033[37m"
	RESET = "\033[0m"

	def __init__(self, color, bright=False):
		self.color = color
		self.bright = bright

	def __str__(self):
		return f"\033[{1 if self.bright else ''};{self.color}m" if self.bright else f"{self.color}m"

# Usage:
#print(f"{Color(Color.RED, bright=True)}This is bright red text!{Color(Color.RESET)}")
#print(f"{Color(Color.BLUE)}This is normal blue text!{Color(Color.RESET)}")

##	XXX: Functions :XXX
##==============-------------------  Start  -------------------==============##
def hm_sz(numb: Union[str, int, float], type: str = "B") -> str:
	"""
	Convert a file size to human-readable format.

	Parameters:
	- numb (str, int, float): File size.
	- type (str): Type of file size (default is "B" for bytes).

	Returns:
	- str: Human-readable file size with sign.
	"""
	if isinstance(numb, str):
		numb = float(numb)
	elif not isinstance(numb, (int, float)):
		raise ValueError("Invalid type for numb. Must be str, int, or float.")
	sign = '-' if numb < 0 else ''
	numb = abs(numb)  # Convert to absolute value for calculations
	units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB']

	for unit in units:
		if numb < 1024.0:
			return f"{sign}{numb:.2f} {unit}"
		numb /= 1024.0

	return f"{sign}{numb:.2f} {units[-1]}"
##==============-------------------   End   -------------------==============##

def hm_time(timez: float) -> str:
	'''Print time as years, months, weeks, days, hours, min, sec'''
	units = {
		'year': 31536000,
		'month': 2592000,
		'week': 604800,
		'day': 86400,
		'hour': 3600,
		'min': 60,
		'sec': 1,
	}

	if timez < 0.0:
		return "Error: time cannot be negative."
	elif timez == 0.0:
		return "Zero time."
	elif timez < 0.001:
		return f"{timez * 1000:.3f} ms"
	elif timez < 60:
		return f"{timez:>5.3f} sec{'s' if timez != 1 else ''}"
	else:
		frmt = []
		for unit, unit_of_time in units.items():
			value = timez // unit_of_time
			if value != 0:
				frmt.append(f"{int(value)} {unit}{'s' if value > 1 else ''}")
			timez %= unit_of_time
		return ", ".join(frmt[:-1]) + " and " + frmt[-1] if len(frmt) > 1 else frmt[0] if frmt else "0 sec"

##>>============-------------------<  End  >------------------==============<<##

def copy_move(src: str, dst: str, keep_original: bool = False, verbose: bool = False) -> bool:
	"""
	Copy or move a file.

	Parameters:
	- src (str): Source file path.
	- dst (str): Destination file path.
	- keep_original (bool, optional): If True, keep the original file. Defaults to False.
	- verbose (bool optional): If True print message. Defaults to False.
	"""
	if verbose:
		print(f"\ncopy_move called with src: {src}, dst: {dst}, keep_original: {keep_original}\n")

	if os.path.exists(dst) and os.path.samefile(src, dst):
		if not keep_original:
			os.remove(src)
			if verbose:
				print(f"Source {src} and destination {dst} are the same. Source has been deleted.")
		else:
			if verbose:
				print(f"{src} and {dst} are the same, nothing to do.")
		return True

	try:
		(action, transfer_func) = ("_Copied", shutil.copy2) if keep_original else ("_Moved", shutil.move)
		transfer_func(src, dst)
		if verbose:
			print(f"{action}: {src}\nTo    : {dst}")
		return True
	except (PermissionError, IOError, OSError) as err:
		print(f"\ncopy_move Error: {err}\n{action}: {src} to {dst}")
		return False
##==============-------------------   End   -------------------==============##


def flatten_list_of_lists(lst):
	"""Flatten a list of lists (or a mix of lists and other elements) into a single list."""
	result = []
	for item in lst:
		if isinstance(item, list):
			result.extend(item)
		elif isinstance(item, tuple):
			# If the item is a tuple, only extend the first element (which should be a list)
			result.extend(item[0])
		else:
			result.append(item)
	return result

##==============-------------------   End   -------------------==============##

def ordinal(n: int ) -> str :
	if 10 <= n % 100 <= 20:
		suffix = 'th'
	else:
		suffix = {1: 'st', 2: 'nd', 3: 'rd'}.get(n % 10, 'th')
	return f"{n}'{suffix}"
##==============-------------------   End   -------------------==============##


def divd_strn(val: str ) -> float:
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


def vis_compr(string1, string2, no_match_c='|', match_c='='):
	''' Visualy show diferences between sting1 graphx string2  '''
	str_t = datetime.datetime.now()
	message = sys._getframe().f_code.co_name + ':'
	print(f"     +{message}=: Start: {str_t:%T}")

	#	print (f"\n1: {string1}\n2: {string2}\n ??")
	# XXX: # TODO: location of differences , chunking
	graphx = ''
	n_diff = 0
	if len(string2) < len(string1):
		string1, string2 = string2, string1
	for c1, c2 in zip(string1, string2):
		if c1 == c2:
			graphx += match_c
		else:
			graphx += no_match_c
			n_diff += 1
	delta = len(string2) - len(string1)
	graphx += delta * no_match_c
	n_diff += delta
	if n_diff :
		print(f"{n_diff} Differences \n1: {string1}\n {graphx}\nMove: {string2}\n")
	return graphx, n_diff
#>=-------------------------------------------------------------------------=<#


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


def stmpd_rad_str(length=13, prefix=''):
	"""
	Generate a random string + prefix + timestamp.

	Parameters:
	length (int): The length of the random string to generate. Default is 13.
	prefix (str): A prefix to add to the generated string. Default is an empty string.

	Returns:
	str: The generated string with the timestamp prefix and random characters.
	"""
	# Get the current time and format it to include minutes and seconds
	current_time = TM.datetime.now()
	timestamp = current_time.strftime("_%M_%S_")

	# Generate random characters from ascii letters and hex digits
	random_chars = ''.join(random.sample(string.ascii_letters + string.digits, length))

	# Combine the prefix, timestamp, and random characters to form the final string
	random_string = prefix + timestamp + random_chars
	return random_string
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
##==============-------------------   End   -------------------==============##

def parse_from_to(strm, dictio, de_bug=True):
	msj = sys._getframe().f_code.co_name
	resul = {}
	try:
		resul = {k: (int(strm[k])	if type(dictio[k]) == int else
					float(strm[k])	if type(dictio[k]) == float else
					dict(strm[k])	if type(dictio[k]) == dict else
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

def calculate_total_bits(width, height, pixel_format):
	bits_per_pixel = {
		'yuv420p8':		12, #  8-bit YUV 4:2:0
		'p010le':		15, # 10-bit Packed YUV 4:2:0
		'yuv420p10le':	15, # 10-bit YUV 4:2:0
		'yuv422p8':		16, #  8-bit YUV 4:2:2
		'yuv422p10le':	20, # 10-bit YUV 4:2:2
		'yuv444p8':		24, #  8-bit YUV 4:4:4
		'yuv444p10le':	30, # 10-bit YUV 4:4:4
		# Add more pixel formats as needed
	}
	bpp = bits_per_pixel.get(pixel_format, None)
	if bpp is None:
		raise ValueError(f"Unsupported pixel format: {pixel_format}")
	total_bits = width * height * bpp
	print(f"Total bits for a frame with pixel format '{pixel_format}': {total_bits}")
	return total_bits
'''
import numpy as np
import matplotlib.pyplot as plt

# Define the x values (base) ranging from 1 to 10
x_values = np.linspace(1, 10, 500)

# Calculate the corresponding y values for each exponent (0.7, 0.5, 0.3)
y_values_07 = x_values ** 0.7
y_values_05 = x_values ** 0.5
y_values_03 = x_values ** 0.3

# Plot the graphs
plt.figure(figsize=(10, 6))
plt.plot(x_values, y_values_07, label='y = x^0.7', color='b')
plt.plot(x_values, y_values_05, label='y = x^0.5', color='g')
plt.plot(x_values, y_values_03, label='y = x^0.3', color='r')
plt.xlabel('x (Base)')
plt.ylabel('y (Result)')
plt.title('Graphs of y = x^0.7, y = x^0.5, and y = x^0.3')
plt.legend()
plt.grid(True)
plt.show()

# Evaluate which number is bigger
result_07 = 10 ** 0.7
result_05 = 10 ** 0.5
result_03 = 10 ** 0.3
(result_07, result_05, result_03)
'''
