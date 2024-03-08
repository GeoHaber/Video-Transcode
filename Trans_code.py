# -*- coding: utf-8 -*-

import os
import re
import sys
import math
import psutil

from FFMpeg		import *
from My_Utils	import get_new_fname, copy_move, hm_sz, hm_time, Tee

from concurrent.futures import ThreadPoolExecutor
#from sklearn.cluster import DBSCAN

#de_bug = True
Log_File = f"__{os.path.basename(sys.argv[0]).strip('.py')}_{time.strftime('%Y_%j_%H-%M-%S')}.log"

# XXX: Sort order True = Biggest first, False = Smallest first
Sort_Order = True

#
''' Global Variables '''
glb_totfrms = 0
glb_vidolen = 0
vid_width   = 0
aud_smplrt  = 0
total_size  = 0

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
	# Define the margin values
	size_margin = 0.01  # 5% margin for file size
	leng_margin = 0.01  # 3% margin for video length

	# Perform clustering using DBSCAN with the custom metric function
	clustering = DBSCAN(eps=0.01, min_samples=2, metric=metric, metric_params={'size_margin': size_margin, 'leng_margin': leng_margin}).fit(data)

	labels = clustering.labels_

	# Group files into clusters based on labels
	clusters = {}
	for i, label in enumerate(labels):
		if label not in clusters:
			clusters[label] = []
		clusters[label].append(_lst[i])

	# Creata a File , Split the path into drive letter and relative path
	components = os.path.normpath(Root).split(os.sep)
	use = components[2]
#	print (components, use)
	filename = f'_Zposiduble_{use}_.txt'

	with open(filename, "w", encoding='utf-8') as file:
		# Write content to the file
		file.write(f"Posible Doubles in {Root}!\n")
	# Output the clusters after the scan is complete
		for label, cluster in clusters.items():
			# Initialize lists to store sizes and lengths from the cluster
			sizes = []
			lengths = []
			# Extract sizes and lengths from the cluster
			for info in cluster:
				size = info[1]
				length = info[5]  # Assuming that the video length is stored in index 5 of the info list
				sizes.append(size)
				lengths.append(length)

			# Calculate min and max size and length
			min_size = min(sizes)
			max_size = max(sizes)
			min_length = min(lengths)
			max_length = max(lengths)

			# Print cluster information
			print(f'\nCluster {label} [Files: {len(cluster)}) | Size = Min: {hm_sz(min_size)} Max: {hm_sz(max_size)} | Leng = Min:{int(min_length)} Max:{int(max_length)} ]')
			if 2 <= len(cluster) <= 4:
				file.write(f"\nCluster {label} => [Size Max: {hm_sz(max_size)} | Lenght: {int(min_length)} ]\n")
				for info in cluster:
					print(    f"+{info[0]}")
					file.write(f"{info[0]}\n")
			else :
				for info in cluster:
					print(f'[Size: {hm_sz(info[1])}\t\tLen: {int(info[5])}] - {info[0]}')

# XXX:
@perf_monitor
def scan_folder(root: str, xtnsio: List[str], sort_order: bool, do_clustering: bool = False ) -> Optional[List[List]]:
	'''Create the list of Files from "root with xtnsio" to Process '''
	msj = sys._getframe().f_code.co_name
	str_t = time.perf_counter()

	msj += f" Start: {time.strftime('%H:%M:%S')}\tRoot: {root}\tSize: {hm_sz(get_tree_size(root))}"
	print (msj)

	# Validate inputs
	if not root or not isinstance(root, str) or not os.path.isdir(root):
		print (f"Invalid root directory: {root}")
		return None
	if not xtnsio or not isinstance(xtnsio, str):
		print (f"Invalid extensions list: {xtnsio}")
		return None

	# a Directory ?
	if os.path.isdir(root):
		_lst = []
		data = []
		# Create a thread pool for parallel processing
		with ThreadPoolExecutor() as executor:
			# Create a list to store the future results
			futures = []
			for root, dirs, files in os.walk(root):
				cur_dir = os.path.split(root)[-1]
				for one_file in files:
					_, ext = os.path.splitext(one_file.lower())
					# XXX: if Video File
					if ext in xtnsio:
						f_path = os.path.join(root, one_file)
						try:
							file_s = os.path.getsize(f_path)
							if file_s  < 10 :
								print(f"Remove Empty file: {f_path}")
								input (" Yes ?")
								os.remove(f_path)
							elif file_s > 1000 and len(f_path) < 333:
								# Submit the extract_file_info => defined in FFMpeg.py to the thread pool
								future = executor.submit(extract_file_info, f_path)
								# Store the future result along with the file information
								futures.append((f_path, file_s, ext, cur_dir, dirs, future))
							else:
								msj= f"\nSkip {f_path}\nSz:{file_s} path L {len(f_path)}"
								print (msj)
								copy_move(f_path, Excepto, False)
						except Exception as e:
							print (f"Error getting size of file {f_path}: {e}")
							continue

			# Wait for all the futures to complete to extract the executor results
			for f_path, file_s, ext, cur_dir, dirs, future in futures:
				video_length = future.result()
				info = [f_path, file_s, ext, cur_dir, dirs, video_length]
				if de_bug :
					print(f"{info}")
				_lst.append(info)
				# append Data fro Clustering
				data.append([file_s, video_length])

	# if File
	elif os.path.isfile(root):
		_, ext = os.path.splitext(root.lower())
		if ext in xtnsio:
			f_path = root  # XXX: # TODO: ? normpath ?
			file_s = os.path.getsize(root)
			# XXX  |[0] Full Path |[1] File Size |[3] ext | XXX
			return root
	else:
		print(f"\n{root} Not a Directory or a File ?\n")
		return None

	end_t = time.perf_counter()
	print(f"  Scan Done : {time.strftime('%H:%M:%S')}\tTotal: {hm_time(end_t - str_t)}")

	# Perform clustering if the flag is set
	if do_clustering:
		print(f"  Start Clustering : {time.strftime('%H:%M:%S')}")
		perform_clustering(data, _lst)

	# Sort Order reverse = True -> Descending Biggest first
	# XXX:
	order = "Descending" if sort_order else "Ascending"
	print(f"  Sort: {order}")

	end_t = time.perf_counter()
#	print(f' -End: {time.strftime('%H:%M:%S')}\tTotal: {hm_time(end_t - str_t)}')

	return sorted(_lst, key=lambda Item: Item[1], reverse=sort_order)

##>>============-------------------<  End  >------------------==============<<##

@perf_monitor
def clean_up(input_file: str, output_file: str, skip_it: str, debug: bool) -> int:
	"""Take care of renaming temp files etc.."""
	msj = sys._getframe().f_code.co_name

	if skip_it:
		return 0

	str_t = time.perf_counter()
	print(f"  +{msj} Start: {time.strftime('%H:%M:%S')}")

	if not input_file or not output_file:
		msj += (f" {msj} Inf: {input_file} Out: {output_file} Exit:")
		print(msj)
		return False

	try:
		inpf_sz = os.path.getsize(input_file)
		if not inpf_sz or inpf_sz == 0:
			raise Exception(f" In  file size {inpf_sz} Zero")
	except FileNotFoundError as e:
		msj += f" Input file: {input_file} does not exist !!\n"
		print(f"  ={msj}")
		return False

	try:
		outf_sz = os.path.getsize(output_file)
		if not outf_sz or outf_sz == 0:
			raise Exception(f" Out file size {outf_sz} Zero")
	except FileNotFoundError as e:
		msj += f" Output file: {output_file} does not exist !!\n   Cp: {input_file}\n   To: {Excepto}"
		print(f"  ={msj}\n\n")
		doit = copy_move(input_file, Excepto, True)
		if not doit:
			raise ValueError(msj)
		return False

	ratio = round(100 * ((outf_sz - inpf_sz) / inpf_sz), 2)
	extra = "+ Gain:" if ratio > 0 else "- Lost:"
	msg = f"    Size Was: {hm_sz(inpf_sz)} Is: {hm_sz(outf_sz)} {extra} {hm_sz(abs(inpf_sz - outf_sz))} {ratio:>8}%"

	if abs(ratio) > 98 :
		msg += " ! Huge difference !"
		print(msg)
		huge_diff = get_new_fname(input_file, "_Huge_Diff.mp4", TmpF_Ex)
		copy_move(output_file, huge_diff)
		return 0

	if debug:
		msj += "\t de_bug mode input file not Changed"
		print(msj)
		return 0

	_, xt = os.path.splitext(input_file)
	delte_me_fnam = get_new_fname(input_file, "_DeletMe.old", TmpF_Ex)
	copy_move(input_file, delte_me_fnam)
	new_done_fnam = get_new_fname(input_file, TmpF_Ex, xt)
	if os.path.exists(output_file):
		copy_move(output_file, new_done_fnam)

	print(msg)

	return (inpf_sz - outf_sz)

##>>============-------------------<  End  >------------------==============<<##

if __name__ == '__main__':
	str_t = time.perf_counter()

	with Tee(sys.stdout, open(Log_File, 'w', encoding='utf-8')) as qa:

		print(f"{psutil.cpu_count()} CPU's\t ¯\\_(%)_/¯" )
		print(f"Python version:  {sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}")
		print(f"Script absolute path: {os.path.abspath(__file__)}")

		print(f'Main Start: {TM.datetime.now()}')

		if not os.path.exists(Excepto): # XXX: Defined in .yml file
			print(f"Creating dir: {Excepto}")
			os.mkdir(Excepto)

		time.sleep(1)
		print("-" * 70)

		if not len (Root) :
			print("Root directory not provided")

		fl_lst = scan_folder(Root, File_extn, sort_order=Sort_Order, do_clustering=False)
		fl_nmb = len(fl_lst)

		saved = 0
		procs = 0
		skipt = 0
		errod = 0
		total_time = 0
		for cnt, each in enumerate(fl_lst):
			str_t = time.perf_counter()
			cnt += 1
			file_p  = each[0]
			file_s  = each[1]
			ext     = each[2]
			dirs    = each[3]
			skip_it = False

			if os.path.isfile(file_p)  :
				print(f'\n{file_p}\n{ordinal(cnt)} of {fl_nmb}  {hm_sz(file_s)}')
				if len(file_p) < 333 :
					free_disk_space = shutil.disk_usage(Root).free
					free_temp_space = shutil.disk_usage(r"C:").free
					if free_disk_space < (3 * file_s) or free_temp_space < (3 * file_s) :
						print('\n!!! ', file_p[0:2], hm_sz(free_disk_space), " Free Space" )
						input("Not enough space on Drive")
				else :
					input("File name too long > 333 ")
				try:
					all_good = ffprobe_run(file_p, ffprob, de_bug)
					all_good, skip_it = zabrain_run(file_p, all_good, de_bug)
					if skip_it:
						print(f"\033[91m   | Skip ffmpeg_run |\033[0m")
						skipt += 1
					all_good = ffmpeg_run(file_p, all_good, skip_it, ffmpeg, de_bug)
					if not all_good and not skip_it :
						print("FFMPEG did not create anything good")
						time.sleep(5)
					saved += clean_up(file_p, all_good, skip_it, de_bug) or 0
					procs += 1
					total_size += glb_vidolen

				except ValueError as e:
					if e.args[0] == "Skip It":
						print(f"Go on: {e}")
						continue   # Continue to the next iteration of a loop
					errod += 1
					msj = f" +: ValueError: {e}\n{os.path.dirname(file_p)}\n\t{os.path.basename(file_p)}\t{hm_sz(file_s)}\nMoved"
					print(msj)

					if de_bug:
						input('Next')
					if (copy_move(file_p, Excepto, False, True)):
						print("Done")
					else:
						input("ValuError WTF")

				except Exception as e:
					errod += 1
					msj = f" +: Exception: {e}\n{os.path.dirname(file_p)}\n\t{os.path.basename(file_p)}\t{hm_sz(file_s)}\nCopied"
					print(msj)
					if de_bug:
						input('Next')
					if (copy_move(file_p, Excepto, False, True)):
						print("Done")
					else:
						input("Exception WTF")

				end_t = time.perf_counter()
				tot_t = end_t - str_t
				total_time += tot_t
				print(f"  -End: {time.strftime('%H:%M:%S')}\tTotal: {hm_time(tot_t)}")
				print(f"  Total saved: {hm_sz(saved)}")

			else:
				mess = f'Not Found-: {file_p}\t\t{hm_sz(file_s)}'
				print(f"\n{mess}\n")
				errod += 1
				time.sleep(1)
				continue   # Continue to the next iteration of the loop

		end_t = time.perf_counter()
		print(f"\n Done: {time.strftime('%H:%M:%S')}\t Total Time: {hm_time(total_time)}")
		print(f" Files: {fl_nmb}\tProces: {procs}\tSkip: {skipt}\tErr: {errod}\n Saved in Total: {hm_sz(saved)}\n")

	input('All Done :)')
	exit()

##>>============-------------------<  End  >------------------==============<<##
