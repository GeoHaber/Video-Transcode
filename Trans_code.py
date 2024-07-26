# -*- coding: utf-8 -*-
import os
import re
import sys
import time
import math
import stat
import shutil
import psutil
import itertools

from FFMpeg   import *
from typing   import List, Optional
from My_Utils import copy_move, hm_sz, hm_time, Tee
from concurrent.futures import ThreadPoolExecutor, as_completed

#from sklearn.cluster import DBSCAN

MultiThread = False		# XXX: It is set to True in the scan_folder # XXX:
de_bug = False

Sort_Order = False
Sort_Order = True

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
	function_name = sys._getframe().f_code.co_name

	if skip_it:
		return 0

	print(f"  +{function_name} Start: {time.strftime('%H:%M:%S')}")

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
		os.chmod(output_file, stat.S_IWRITE)

		ratio = round (100 * ((output_file_size - input_file_size) / input_file_size), 2)
		extra = "+Biger" if ratio > 0 else ("=Same" if (input_file_size - output_file_size) == 0 else "-Lost")
		msg = f"    Size Was: {hm_sz(input_file_size)} Is: {hm_sz(output_file_size)} {extra}: {hm_sz(input_file_size - output_file_size)} = {ratio}%"

		if ratio > 120:
			msg += " ! Much Bigger !"
		elif ratio < -100:
			msg += " ! Much Smaller !"

		final_output_file = input_file if input_file.endswith('.mp4') else input_file.rsplit('.', 1)[0] + '.mp4'
		temp_file = input_file + "_Delete_.old"
		os.rename(input_file, temp_file)
		shutil.move(output_file, final_output_file)

		if not debug:
			os.remove(temp_file)

		print(msg)

		return input_file_size - output_file_size

	except (FileNotFoundError, PermissionError) as e:
		print(f"Error accessing file: {e}")
	except Exception as e:
		print(f"An error occurred: {e}")
		input ("WTF")

	return -1

##>>============-------------------<  End  >------------------==============<<##

def process_file(file_info, cnt, fl_nmb ):
	saved, procs, skipt, errod = 0, 0, 0, 0
	str_t = time.perf_counter()
	file_p, file_s, ext, jsn_ou = file_info
	skip_it = False
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
			all_good, skip_it = zabrain_run(file_p, jsn_ou, de_bug)
#			debug = True # DEBUG:
			if debug or ext != ".mp4":
				skip_it = False
#				print (f"\nFile: {file_p}\nFfmpeg: {all_good}\n")
			if skip_it:
				print(f"\033[91m   | Skip ffmpeg_run |\033[0m")
				skipt += 1

			all_good = ffmpeg_run(file_p, all_good, skip_it, ffmpeg, de_bug)

			if not all_good and not skip_it:
				print("FFMPEG did not create anything good")
				time.sleep(5)

			saved += clean_up(file_p, all_good, skip_it, de_bug) or 0
			procs += 1

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
			if de_bug:
				input('Next')
			if copy_move(file_p, Excepto, True, True):
				print("Done")
			else:
				input("Exception WTF")

		end_t = time.perf_counter()
		tot_t = end_t - str_t
		print(f"  -End: {time.strftime('%H:%M:%S')}\tTotal: {hm_time(tot_t)}")

	else:
		print(f'Not Found-: {file_p}\t\t{hm_sz(file_s)}')
		errod += 1
		time.sleep(1)

	return saved, procs, skipt, errod
##==============-------------------   End   -------------------==============##


@perf_monitor
def scan_folder(root: str, xtnsio: List[str], sort_order: bool, do_clustering: bool = False) -> Optional[List[List]]:
	"""Scans a directory for files with specified extensions, extracts information
	in parallel, and returns a sorted list of file information.

	Args:
		root (str): Path to the root directory.
		xtnsio (List[str]): List of file extensions (lowercase).
		sort_order (bool): True for descending, False for ascending sort by size.
		do_clustering (bool, optional): Enables clustering (implementation not provided). Defaults to False.

	Returns:
		Optional[List[List]]: A list of lists containing file information (path, size, extension, video length).
		None on errors.
	"""
	msj = sys._getframe().f_code.co_name
	str_t = time.perf_counter()
	msj += f" Start: {time.strftime('%H:%M:%S')}"
	print(f"Scan: {root}\tSize: {hm_sz(get_tree_size(root))}\n{msj}")

	if not root or not isinstance(root, str) or not os.path.isdir(root):
		print(f"Invalid root directory: {root}")
		return []
	if not xtnsio or not isinstance(xtnsio, str):
		print(f"Invalid extensions list: {xtnsio}")
		return []

	_lst = []
	data = []

	def process_files(executor=None):
		futures = []
		for dirpath, _, files in os.walk(root):
			for one_file in files:
				_, ext = os.path.splitext(one_file.lower())
				if ext in xtnsio:
					f_path = os.path.join(dirpath, one_file)
					try:
						file_s = os.path.getsize(f_path)
						if not os.access(f_path, os.W_OK) or not (os.stat(f_path).st_mode & stat.S_IWUSR):
							print(f"Skip read-only file: {f_path}")
							input("Yes ?")
							continue
						if file_s < 10:
							print(f"Remove empty file: {f_path}")
							input("Yes ?")
							os.remove(f_path)
						elif file_s > 1000 and len(f_path) < 333:
							if executor:
								future = executor.submit(ffprobe_run, f_path)
								futures.append((f_path, file_s, ext, future))
							else:
								result = ffprobe_run(f_path)
								if result is not None:
									handle_result((f_path, file_s, ext, result))
					except Exception as e:
						print(f"Error getting size of file {f_path}: {e}")
						continue

		if executor:
			for f_path, file_s, ext, future in futures:
				try:
					jsn_ou = future.result()
					result = (f_path, file_s, ext, jsn_ou)
					handle_result(result)
				except Exception as e:
					print(f"Error processing future for file {f_path}: {e}")

	def handle_result(result):
		if result:
			if de_bug:
				print(f"{result}")
			_lst.append(result)

	# XXX: Set to True # XXX:
	MultiThread = 1
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

	order = "Descending >" if sort_order else "Ascending <"

	end_t = time.perf_counter()
	print(f"\n Scan: Done : {time.strftime('%H:%M:%S')}\tTotal: {hm_time(end_t - str_t)}")
	print(f" Sort: {order}")

	return sorted(_lst, key=lambda item: item[1], reverse=sort_order)
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

		fl_lst = scan_folder(Root, File_extn, sort_order=Sort_Order, do_clustering=False)
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
		print(f" Files: {fl_nmb}\tProces: {procs}\tSkip: {skipt}\tErr: {errod}\n Saved in Total: {hm_sz(saved)}\n")

	input('All Done :)')
	exit()
##==============-------------------   End   -------------------==============##

# Call main function
if __name__ == "__main__":
	main()

##>>============-------------------<  End  >------------------==============<<##
