# -*- coding: utf-8 -*-
import os
import re
import sys
import stat
import time
import math
import stat
import shutil
import psutil
import datetime
import itertools

from functools	import cmp_to_key
from typing		import List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

# External references from your code
from FFMpeg		import *
from My_Utils	import copy_move, hm_sz, hm_time, Tee

#from sklearn.cluster import DBSCAN

MultiThread = False		# XXX: It is set to True in the scan_folder # XXX:
de_bug = False

# Specify sorting keys and orders
sort_keys = [
			('size', True),	# Sort by size True=descending False=ascending
			('date', True),		# Then by date descending
			]

Log_File = f"__{os.path.basename(sys.argv[0]).strip('.py')}_{time.strftime('%Y_%j_%H-%M-%S')}.log"



''' Global Variables '''
glb_totfrms = 0
glb_vidolen = 0
vid_width = 0
aud_smplrt = 0
total_size = 0

##>>============-------------------< Start >------------------==============<<##
@perf_monitor
def metric(x, y, size_margin, leng_margin):
	''' used for clustering '''
	return (
		0 if (
			(1 - size_margin) * y[0] <= x[0] <= (1 + size_margin) * y[0] and
			(1 - leng_margin) * y[1] <= x[1] <= (1 + leng_margin) * y[1]
		) else 1
	)
##>>============-------------------<  End  >------------------==============<<##

@perf_monitor
def perform_clustering(data: List[List[float]], _lst: List[List]) -> None:
	size_margin = 0.01
	leng_margin = 0.01
	clustering = DBSCAN(eps=0.01, min_samples=2, metric=metric, metric_params={'size_margin': size_margin, 'leng_margin': leng_margin}).fit(data)
	labels = clustering.labels_

	clusters = {}
	for i, label in enumerate(labels):
		if label not in clusters:
			clusters[label] = []
		clusters[label].append(_lst[i])

	components = os.path.normpath(Root).split(os.sep)
	use = components[2]
	filename = f'_Zposiduble_{use}_.txt'

	with open(filename, "w", encoding='utf-8') as file:
		file.write(f"Posible Doubles in {Root}!\n")
		for label, cluster in clusters.items():
			sizes = [info[1] for info in cluster]
			lengths = [info[5] for info in cluster]
			min_size, max_size = min(sizes), max(sizes)
			min_length, max_length = min(lengths), max(lengths)

			print(f'\nCluster {label} [Files: {len(cluster)}) | Size = Min: {hm_sz(min_size)} Max: {hm_sz(max_size)} | Leng = Min:{int(min_length)} Max:{int(max_length)} ]')
			if 2 <= len(cluster) <= 4:
				file.write(f"\nCluster {label} => [Size Max: {hm_sz(max_size)} | Lenght: {int(min_length)} ]\n")
				for info in cluster:
					print(f"+{info[0]}")
					file.write(f"{info[0]}\n")
			else:
				for info in cluster:
					print(f'[Size: {hm_sz(info[1])}\t\tLen: {int(info[5])}] - {info[0]}')
##==============-------------------   End   -------------------==============##

@perf_monitor
def clean_up(input_file: str, output_file: str, skip_it: bool, debug: bool) -> int:
	msj = sys._getframe().f_code.co_name

	if skip_it:
		return 0

	if debug:
		print(f"  +{msj} Start: {time.strftime('%H:%M:%S')}" + " " * 80)

	try:
		if not os.path.exists(input_file):
			print(f"Input file '{input_file}' does not exist.")
			return -1
		input_file_size = os.path.getsize(input_file)
		os.chmod(input_file, stat.S_IWRITE)

		if not os.path.exists(output_file):
			print(f"Output file '{output_file}' does not exist.")
			return -1
		output_file_size = os.path.getsize(output_file)
		if output_file_size > 100 :
			os.chmod(output_file, stat.S_IWRITE)
		else :
			return 0

		ratio = round (100 * ((output_file_size - input_file_size) / input_file_size), 2)
		extra = "+Biger" if ratio > 0 else ("=Same" if (input_file_size - output_file_size) == 0 else "-Lost")
		sv_ms = f"    Size Was: {hm_sz(input_file_size)} Is: {hm_sz(output_file_size)} {extra}: {hm_sz(abs(input_file_size - output_file_size))} {ratio}%"

		final_output_file = input_file if input_file.endswith('.mp4') else input_file.rsplit('.', 1)[0] + '.mp4'

		random_chars = ''.join(random.sample(string.ascii_letters + string.digits, 4))
		temp_file = input_file + random_chars + "_Delete_.old"
		os.rename(input_file, temp_file)
		shutil.move(output_file, final_output_file)

#		debug = True
		if debug:
			confirm = input(f"  Are you sure you want to delete {temp_file}? (y/n): ")
			if confirm.lower() != "y":
				print("   Not Deleted.")
				return  input_file_size - output_file_size # Return here to stop further execution
			print(f"   File {temp_file} Deleted.")

		print(sv_ms)
		os.remove(temp_file)
		return input_file_size - output_file_size

	except FileNotFoundError as e:
		print(f"  {msj} File not found: {e}")
	except PermissionError as e:
		print(f"  {msj} Permission denied: {e}")
	except Exception as e:
		print(f"  {msj} An error occurred: {e}")
		input(f"? {msj} WTF ?")

	return -1

##>>============-------------------<  End  >------------------==============<<##

@perf_monitor
def process_file(file_info, cnt, fl_nmb ):
	msj = sys._getframe().f_code.co_name
	str_t = time.perf_counter()

	saved, procs, skipt, errod = 0, 0, 0, 0
	skip_it = False

	file_p =	file_info['path']
	file_s =	file_info['size']
	ext =		file_info['extension']
	jsn_ou =	file_info['metadata']

	# Is it a file ?
	if os.path.isfile(file_p):
#		print(f'\n{file_p}\n{hm_sz(file_s)}')
		print(f'\n{file_p}\n {ordinal(cnt)} of {fl_nmb}, {ext}, {hm_sz(file_s)}')
		if len(file_p) < 333:
			free_disk_space = shutil.disk_usage(Root).free
			free_temp_space = shutil.disk_usage(r"C:").free
			if free_disk_space < (3 * file_s) or free_temp_space < (3 * file_s):
				print(f'\nNot enough space on Drive: {hm_sz(free_disk_space)} free')
				input("Not enough space on Drive")
		else:
			input("File name too long > 333")

		try:
#			de_bug = True # DEBUG:
			all_good, skip_it = parse_finfo(file_p, jsn_ou, de_bug)
			if de_bug or ext != ".mp4":
				skip_it = False
#				print (f"\nDebug: {de_bug}  Ext: {ext}\n")
#				print (f"\nFile: {file_p}\nFfmpeg: {all_good}\n")
			if skip_it:
				print(f"\033[91m   .Skip: >|  {msj} |<\033[0m")
				skipt += 1

			all_good = ffmpeg_run(file_p, all_good, skip_it, ffmpeg, de_bug)
			if all_good :
				saved += clean_up(file_p, all_good, skip_it, de_bug) or 0
				procs += 1
			elif not skip_it:
				print(f"FFMPEG failed for {file_p}")
				if copy_move(file_p, Excepto, True, True):
					time.sleep(5)

		except ValueError as e:
			if e.args[0] == "Skip It":
				print(f"Go on: {e}")
				return saved, procs, skipt, errod
			errod += 1
			msj = f" +: ValueError: {e}\n{os.path.dirname(file_p)}\n\t{os.path.basename(file_p)}\t{hm_sz(file_s)}\nMoved"
			print(msj)
			if de_bug:
				input('Next')
			if copy_move(file_p, Excepto, True, True):
				print("Done")
			else:
				input("ValueError WTF")

		except Exception as e:
			errod += 1
			msj = f" +: Exception: {e}\n{os.path.dirname(file_p)}\n\t{os.path.basename(file_p)}\t{hm_sz(file_s)}\nCopied"
			print(msj)
			if de_bug :
				input('Next')
			if copy_move(file_p, Excepto, True, True):
				print("Done")
			else:
				input("Exception WTF")

		end_t = time.perf_counter()
		tot_t = end_t - str_t
		print(f"  -End: {time.strftime('%H:%M:%S')}\tTotal: {hm_time(tot_t)}")

	else:
		print(f' -Missing: {hm_sz(file_s)}\t{file_p}')
		errod += 1
		time.sleep(1)

	return saved, procs, skipt, errod
##==============-------------------   End   -------------------==============##
@perf_monitor
def scan_folder(root: str, xtnsio: List[str], sort_keys: Optional[List[Tuple[str, bool]]] = None, do_clustering: bool = False) -> Optional[List[Dict]]:
	"""Scans a directory for files with specified extensions, extracts information
	in parallel, and returns a sorted list of file information.

	Args:
		root (str): Path to the root directory.
		xtnsio (List[str]): List of file extensions (lowercase).
		sort_keys (Optional[List[Tuple[str, bool]]]): List of tuples containing sorting keys and their orders.
			Each tuple is in the form (key_name, descending_order), where descending_order is a boolean.
			Valid keys are 'name', 'size', 'extension', 'date', 'duration'.
		do_clustering (bool, optional): Enables clustering (implementation not provided). Defaults to False.

	Returns:
		Optional[List[Dict]]: A list of dictionaries containing file information.
		None on errors.
	"""
	str_t = time.perf_counter()
	msj = f"{sys._getframe().f_code.co_name} Start: {time.strftime('%H:%M:%S')}"
	print(f"Scan: {root}\tSize: {hm_sz(get_tree_size(root))}\n{msj}")
	spinner = Spinner(indent=0)
#	print(f"Extensions to scan: {xtnsio}")

	if not root or not isinstance(root, str) or not os.path.isdir(root):
		print(f"Invalid root directory: {root}")
		return []
	if not isinstance(xtnsio, (tuple, list)):
		print(f"Invalid extensions list: {xtnsio}")
		return []

	file_list = []

	def is_file_accessible(file_path):
		try:
			with open(file_path, 'rb') as f:
				f.read(1)
			return True
		except (IOError, OSError):
			return False

	def process_files(executor=None):
		futures = []
		for dirpath, _, files in os.walk(root):
			for one_file in files:
				f_path = os.path.join(dirpath, one_file)
				if not is_file_accessible(f_path):
					print(f"Skipping inaccessible file: {f_path}")
					continue
				_, ext = os.path.splitext(one_file)
				if ext.lower() in xtnsio:
					spinner.print_spin(f" {one_file} ")
					try:
						file_s = os.path.getsize(f_path)
						if not os.access(f_path, os.W_OK) or not (os.stat(f_path).st_mode & stat.S_IWUSR):
							print(f"Skip read-only file: {f_path}")
							continue
						if file_s < 10:
							print(f"Remove empty file: {f_path}")
							os.remove(f_path)
						elif file_s > 1000 and len(f_path) < 333:
							# Runn ffprobe to extract
							if executor:
								future = executor.submit(ffprobe_run, f_path)
								futures.append((f_path, file_s, ext, future))
							else:
								result = ffprobe_run(f_path)
								if result is not None:
									handle_result(f_path, file_s, ext, result)
					except Exception as e:
						print(f"Error getting size of file {f_path}: {e}")
						continue

		if executor:
			for f_path, file_s, ext, future in futures:
				try:
					jsn_ou = future.result()
					handle_result(f_path, file_s, ext, jsn_ou)
				except Exception as e:
					print(f"Error processing future for file {f_path}: {e}")
		spinner.stop()  # Ensure the spinner stops

	def handle_result(f_path, file_s, ext, jsn_ou):
		if jsn_ou:
			mod_time = os.path.getmtime(f_path)
			mod_datetime = datetime.datetime.fromtimestamp(mod_time)
			duration = float(jsn_ou.get('format', {}).get('duration', 0.0))
			file_info = {
				'path':		f_path,
				'name':		os.path.basename(f_path),
				'size':		file_s,
				'extension':ext.lower(),
				'date':		mod_datetime,
				'duration':	duration,
				'metadata':	jsn_ou,
			}
			file_list.append(file_info)
			if de_bug:
				print(f"Collected file info: {file_info}")

	MultiThread = True
	if MultiThread:
		with ThreadPoolExecutor() as executor:
			process_files(executor)
	else:
		process_files()

	if do_clustering:
		print(f" Start Clustering : {time.strftime('%H:%M:%S')}")
		duration = round(float(jsn_ou.get('format', {}).get('duration', 0.0)), 1)
		print(f"     Duration: {duration}")
		data.append([file_s, duration])
		perform_clustering(data, _lst)

	# Sorting based on the provided key # XXX: TBD
	valid_sort_keys = {
		'name':		lambda x: x['name'],
		'size':		lambda x: x['size'],
		'extension':lambda x: x['extension'],
		'date':		lambda x: x['date'],
		'duration':	lambda x: x['duration'],
	}
	# Sorting by the key specified (name, size, extension, date)
	if sort_keys:
		sort_keys = [(key, descending) for key, descending in sort_keys if key in valid_sort_keys]
		if not sort_keys:
			sort_keys = [('size', True)]
	else:
		sort_keys = [('size', True)]
	key_funcs = [valid_sort_keys[key] for key, _ in sort_keys]
	sort_orders = [descending for _, descending in sort_keys]
#	sorted_list = sorted(_lst, key=lambda item: item[1],        reverse=sort_order)
	def compare_items(a, b):
		for key_func, descending in zip(key_funcs, sort_orders):
			a_key = key_func(a)
			b_key = key_func(b)
			if a_key != b_key:
				if descending:
					return -1 if a_key > b_key else 1
				else:
					return -1 if a_key < b_key else 1
		return 0  # All keys are equal
	sorted_list = sorted(file_list, key=cmp_to_key(compare_items))

	# Prepare sorting order string for display
	order_str = ', '.join([f"{key} ({'Desc' if desc else 'Asc'})" for key, desc in sort_keys])

	end_t = time.perf_counter()
	print(f"\nSort by: {order_str}\nScan Done: {time.strftime('%H:%M:%S')}\tTotal: {hm_time(end_t - str_t)}\n")
	return sorted_list

##==============-------------------   End   -------------------==============##

def main():
	str_t = time.perf_counter()

	with Tee(sys.stdout, open(Log_File, 'w', encoding='utf-8')) as qa:
		print(f"{psutil.cpu_count()} CPU's\t ¯\\_(%)_/¯")
		print(f"Python version: {sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}")
		print(f"Script absolute path: {os.path.abspath(__file__)}")
		print(f'Main Start: {time.strftime("%Y-%m-%d %H:%M:%S")}')
		print("-" * 70)

		if not Root:
			print("Root directory not provided")
			return

		if not os.path.exists(Excepto):
			print(f"Creating dir: {Excepto}")
			os.mkdir(Excepto)

		saved = procs = skipt = errod = total_time = 0

		fl_lst = scan_folder(Root, File_extn, sort_keys=sort_keys, do_clustering=False)
		fl_nmb = len(fl_lst)
		counter = itertools.count(1)

		def process_all_files():
			nonlocal saved, procs, skipt, errod
			if MultiThread:
				with ThreadPoolExecutor(max_workers=4) as executor:
					future_to_file = {executor.submit(process_file, each, next(counter), fl_nmb): each for each in fl_lst}
					for future in as_completed(future_to_file):
						each = future_to_file[future]
						try:
							s, p, sk, e = future.result()
							saved += s
							procs += p
							skipt += sk
							errod += e
						except Exception as exc:
							print(f"Generated an exception: {exc}")
			else:
				for each in fl_lst:
					cnt = next(counter)
					s, p, sk, e = process_file(each, cnt, fl_nmb)
					saved += s
					procs += p
					skipt += sk
					errod += e

		process_all_files()

		end_t = time.perf_counter()
		total_time = end_t - str_t
		print(f"\n Done: {time.strftime('%H:%M:%S')}\t Total Time: {hm_time(total_time)}")
		print(f" Files: {fl_nmb}\tProcessed: {procs}\tSkipped : {skipt}\tErrors : {errod}\n Saved in Total: {hm_sz(saved)}\n")

	input('All Done :)')
	exit()
##==============-------------------   End   -------------------==============##

# Call main function
if __name__ == "__main__":
	main()

##>>============-------------------<  End  >------------------==============<<##
