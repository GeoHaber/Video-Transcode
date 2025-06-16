#!/usr/bin/python

import	os
import	re
import	sys
import	time
import	json
import	shutil
import	filecmp
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
	debug = False
	if not debug:
		# If running in optimized mode, return the original function
		return func

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
		cpu_prcnt = psutil.cpu_percent(interval=0.1, percpu=True)
		result = func(*args, **kwargs)
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

	def __init__(self, spin_text="|/-o+\\", indent=0, delay=0.1):
		self.spinner_count = 0
		self.spin_text = spin_text
		self.spin_length = len(spin_text)
		self.prefix = " " * indent  # Indentation string
		self.last_message_length = 0  # To keep track of the length of the last printed message
		self.cursor_hidden = False
		self.delay = delay  # Delay between spinner updates
		self.last_update_time = 0

	def hide_cursor(self):
		if not self.cursor_hidden:
			sys.stderr.write("\033[?25l")  # Hide cursor
			sys.stderr.flush()
			self.cursor_hidden = True

	def show_cursor(self):
		if self.cursor_hidden:
			sys.stderr.write("\033[?25h")  # Show cursor
			sys.stderr.flush()
			self.cursor_hidden = False

	def abbreviate_path(self, path: str, max_length: int) -> str:
		"""Abbreviates a path to fit within the terminal width."""
		if len(path) <= max_length:
			return path
		else:
			return f"{path[:max_length//2]}...{path[-max_length//2:]}"

	def print_spin(self, extra: str = "") -> None:
		"""Prints a spinner with optional extra text."""
		current_time = time.time()
		if current_time - self.last_update_time < self.delay:
			return  # Skip updating the spinner if it's too soon

		self.last_update_time = current_time  # Update the last update time

		# Hide the cursor
		self.hide_cursor()

		# Get terminal width
		terminal_width = shutil.get_terminal_size().columns

		# Abbreviate the extra text to fit the terminal
		extra = self.abbreviate_path(extra, terminal_width - 10)

		spin_char = self.spin_text[self.spinner_count % self.spin_length]
		message = f"\r{self.prefix}| {spin_char} | {extra}"

		# Calculate the number of spaces needed to clear the previous message
		clear_spaces = max(self.last_message_length - len(message), 0)

		# Print the spinner and the extra text, followed by enough spaces to clear any leftover characters
		sys.stderr.write(f"{message}{' ' * clear_spaces}")
		sys.stderr.flush()

		# Update the length of the last message
		self.last_message_length = len(message)

		self.spinner_count += 1

	def stop(self):
		"""Stops the spinner and shows the cursor."""
		# Show the cursor
		self.show_cursor()
		sys.stderr.write("\n")  # Move to the next line after stopping
		sys.stderr.flush()

'''
# Example usage:
	if __name__ == "__main__":
		# Testing with different indentation values
		spinner_no_indent = Spinner(indent=0)
		spinner_with_indent = Spinner(indent=4)

		for _ in range(100):  # Simulate a task with 10 iterations
			spinner_no_indent.print_spin(f" {_} Processing without indent...")
			time.sleep(0.1)  # Simulate work being done

		print("\n")

		for _ in range(100):  # Simulate a task with 10 iterations
			spinner_with_indent.print_spin(f" {_} Processing with indent...")
			time.sleep(0.1)  # Simulate work being done

		print("\nTask completed!")
'''
##>>============-------------------<  End  >------------------==============<<##

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

from typing import Union

def hm_sz(numb: Union[str, int, float], suffix: str = "B") -> str:
	"""
	Convert a size to human-readable format with a customizable suffix.

	Parameters:
	- numb (str, int, float): The size to convert.
	- suffix (str): The suffix to append to units (e.g., "B" for bytes, "g" for grams).

	Returns:
	- str: Human-readable size with sign and the given suffix.

	Example:
	- hm_sz(1024, "g") -> "1.00 Kg"
	"""
	# Handle different input types for numb
	if isinstance(numb, str):
		numb = float(numb)
	elif not isinstance(numb, (int, float)):
		raise ValueError("Invalid type for numb. Must be str, int, or float.")

	sign = '-' if numb < 0 else ''
	numb = abs(numb)  # Convert to absolute value for calculations

	units = ['', 'K', 'M', 'G', 'T', 'P', 'E']

	for unit in units:
		if numb < 1024.0:
			return f"{sign}{numb:.2f} {unit}{suffix}"
		numb /= 1024.0

	return f"{sign}{numb:.2f} {units[-1]}{suffix}"
##==============-------------------   End   -------------------==============##

def hm_time(timez) -> str:
	"""Converts time in seconds to a human-readable format."""

	# Handle invalid input types
	if not isinstance(timez, (int, float)):
		return f"Invalid input: {type(timez).__name__}"

	units = [
		('year', 31536000),
		('month', 2592000),
		('week',   604800),
		('day',     86400),
		('hour',     3600),
		('minute',     60),
		('second',      1)
	]

	# Handle edge cases
	if timez < 0:
		return "Error: time cannot be negative."
	if timez == 0:
		return "Zero time."
	if timez < 0.001:
		return f"{timez * 1000:.3f} ms"
	if timez < 60:
		return f"{timez:.3f} second{'s' if timez != 1 else ''}"

	result = []

	# Convert time into larger units
	for unit, seconds_in_unit in units:
		value = int(timez // seconds_in_unit)
		if value > 0:
			result.append(f"{value} {unit}{'s' if value > 1 else ''}")
			timez %= seconds_in_unit

	# Join the result with 'and' for the last unit
	if len(result) > 1:
		return ", ".join(result[:-1]) + " and " + result[-1]
	else:
		return result[0]
##==============-------------------   End   -------------------==============##



def copy_move(src: str, dst: str, keep_original: bool = True, verbose: bool = False) -> bool:
	"""
	Copies or moves a file, with a check and confirmation prompt before overwriting an existing file.

	Parameters:
	- src (str): Source file path.
	- dst (str): Destination file or directory path.
	- keep_original (bool, optional): If True, copies the file. If False, moves it. Defaults to True.
	- verbose (bool, optional): If True, prints detailed messages. Defaults to False.

	Returns:
	- bool: True if the operation was successful or successfully skipped, False on error.
	"""
	if not os.path.exists(src):
		print(f"Error: Source file not found at '{src}'")
		return False

	# If the destination is a directory, construct the full destination path
	if os.path.isdir(dst):
		dst = os.path.join(dst, os.path.basename(src))

	# --- OVERWRITE CHECK ADDED ---
	# First, check if the destination path already exists.
	if os.path.exists(dst):
		# If src and dst point to the exact same file, handle as before.
		if os.path.samefile(src, dst):
			if verbose:
				print(f"Source '{os.path.basename(src)}' and destination are the same file. No action needed.")
			# If it's a 'move' operation on the same file, it's effectively a delete of the source, which is confusing.
			# Best to just confirm nothing needs to be done.
			return True

		# The destination file exists but is a different file.
		# Compare the content of the source and destination files.
		if filecmp.cmp(src, dst, shallow=False):
			# Files are identical.
			prompt_msg = f"Destination file exists and is identical. Overwrite? (y/n): "
		else:
			# Files are different.
			prompt_msg = f"WARNING: Destination file exists with DIFFERENT content. Overwrite? (y/n): "

		if verbose:
			print(prompt_msg, end='')

		# Get user confirmation
		response = input().lower()

		# If the user does not respond with 'y', skip the operation.
		if response != 'y':
			if verbose:
				print("Skipping operation.")
			return True # Operation successfully skipped.

		if verbose:
			print("User approved overwrite.")

	# --- ORIGINAL LOGIC PROCEEDS FROM HERE ---
	try:
		# Determine the action and the shutil function to use.
		(action, transfer_func) = ("_Copy", shutil.copy2) if keep_original else ("_Move", shutil.move)

		# Perform the copy or move.
		transfer_func(src, dst)

		if verbose:
			print(f"{action}: {src}\n   To: {dst}")
		return True

	except (PermissionError, IOError, OSError) as err:
		print(f"\ncopy_move Error: {err}\n   Action: {action}: {src} to {dst}")
		return False

##==============-------------------   End   -------------------==============##


def ordinal(n: int ) -> str :
	if 10 <= n % 100 <= 20:
		suffix = 'th'
	else:
		suffix = {1: 'st', 2: 'nd', 3: 'rd'}.get(n % 10, 'th')
	return f"{n}'{suffix}"
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
	str_t = TM.datetime.now()
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
	print(TM.datetime.now().strftime('\n%a:%b:%Y %T %p'))
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
		print('\n', folder, " is a WTF?")

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
