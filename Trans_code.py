# -*- coding: utf-8 -*-

import os
import re
import sys
import math
import psutil
import logging
import datetime as TM

from Yaml		import *
from FFMpeg		import *
from My_Utils	import get_new_fname, copy_move, hm_sz, hm_time

from concurrent.futures import ThreadPoolExecutor
from sklearn.cluster import DBSCAN

''' Global Variables '''
glb_totfrms = 0
glb_vidolen = 0
vid_width   = 0
aud_smplrt  = 0
total_size  = 0

#WFolder = r"F:\Media\TV"
#WFolder = r"F:\Media\Movie"
#WFolder = r"F:\BackUp\_Adlt"
#WFolder = r"F:\Media\MasterClass Collection"
#WFolder = r"C:\Users\Geo\Desktop\Except"
#WFolder = r"C:\Users\Geo\Desktop\Except\Retry"

#de_bug = True

ancr_time = f"{TM.datetime.now(): %Y %j %H-%M-%S }"
Log_File = sys.argv[0].strip('.py') + ancr_time + '_.log'

# Create a Tee instance that writes to both the original sys.stdout and a log file
# Save the original sys.stdout
qa = Tee(sys.stdout, open(Log_File, 'w', encoding=console_encoding))
#qa = Tee(sys.stdout, open(Log_File, 'w', encoding='utf-8'))
sys.stdout = qa

'''
log_filename = os.path.splitext(Log_File)[0]

log_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), "_logs")
log_path = os.path.join(log_dir, log_filename)

# Create a logs directory if it does not exist
if not os.path.exists(log_dir):
	print(f"Creating log directory: {log_dir}")  # Debugging line
	os.makedirs(log_dir)
else :
	print(f"Log directory (absolute): {os.path.abspath(log_dir)}")

# Configure the logger
handler = logging.FileHandler(log_path)
handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
logging.getLogger().addHandler(handler)
# Print the return value of the logging.FileHandler() function
print(f"handler = {handler}")

# Test logging message
logging.debug("This is a debug message.")
logging.exception("Test")
logging.info("Test log message. This is a test message from the logger.")
# Print message indicating that the logger configuration is complete
logging.basicConfig(filename="_loging.log", level=logging.INFO)
logging.info("This is a test message")
print("Logger configuration complete.")
'''

print(f"Python version: {sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}")
print(f"{psutil.cpu_count()} CPU's\t ¯\_(%)_/¯\nTime:\t\t {ancr_time}" )
print(f"Script absolute path: {os.path.abspath(__file__)}")


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
	components = os.path.normpath(WFolder).split(os.sep)
	use = components[2]
#	print (components, use)
	filename = f'_Zposiduble_{use}_.txt'

	with open(filename, "w", encoding='utf-8') as file:
		# Write content to the file
		file.write(f"Posible Doubles in {WFolder}!\n")
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

	msj += f" Start: {TM.datetime.now():%T}\tRoot: {root}\tSize: {hm_sz(get_tree_size(root))}"
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
						except Exception as e:
							print (f"Error getting size of file {f_path}: {e}")
							continue
						if file_s > 1000 and len(f_path) < 333:
							# Submit the extract_file_info => defined in FFMpeg.py to the thread pool
							future = executor.submit(extract_file_info, f_path)
							# Store the future result along with the file information
							futures.append((f_path, file_s, ext, cur_dir, dirs, future))
						else:
							msj= f"\nSkip {f_path}\nSz:{file_s} path L {len(f_path)}"
							logging.error(msj)
							print (msj)
							copy_move(f_path, Excepto, False)
			# Wait for all the futures to complete to extract the executor results
			for f_path, file_s, ext, cur_dir, dirs, future in futures:
				video_length = future.result()
				info = [f_path, file_s, ext, cur_dir, dirs, video_length]
				if de_bug :
					print(f"{info}")
				_lst.append(info)
				# append DAta fro Clustering
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
	print(f'  Scan Done : {TM.datetime.now():%T}\tTotal: {hm_time(end_t - str_t)}')

	# Perform clustering if the flag is set
	if do_clustering:
		print(f'  Start Clustering : {TM.datetime.now():%T}')
		perform_clustering(data, _lst)

	# Sort Order reverse = True -> Descending Biggest first
	# XXX:
	order = "Descending" if sort_order else "Ascending"
	print(f"   Sort order: {order}")

	end_t = time.perf_counter()
	print(f'  -End: {TM.datetime.now():%T}\tTotal: {hm_time(end_t - str_t)}')

	return sorted(_lst, key=lambda Item: Item[1], reverse=sort_order)

##>>============-------------------<  End  >------------------==============<<##

@perf_monitor
def clean_up(input_file: str, output_file: str, skip_it: str, debug: bool) -> int:
	"""Take care of renaming temp files etc.."""
	msj = sys._getframe().f_code.co_name

	if skip_it:
		logging.info(f"{msj} skip_it={skip_it}")
		return 0
	logging.info(f"{msj}")

	str_t = time.perf_counter()
	print(f"  +{msj} Start: {TM.datetime.now():%T}")

	if not input_file or not output_file:
		msj += (f" {msj} Inf: {input_file} Out: {output_file} Exit:")
		print(msj)
		logging.info(msj)
		return False

	try:
		inpf_sz = os.path.getsize(input_file)
		if not inpf_sz or inpf_sz == 0:
			raise Exception(f" In  file size {inpf_sz} Zero")
	except FileNotFoundError as e:
		msj += f" Input file: {input_file} does not exist !!\n"
		print(f"  ={msj}")
		logging.info(msj)
		return 0

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

	if ratio > 98:
		msg += " ! Huge diff !"
		print(msg)
		seems_to_small = get_new_fname(input_file, "_seems_small.mp4", TmpF_Ex)
		copy_move(output_file, seems_to_small)
		return 0

	# Check if input file size is 10% smaller than output file size
	if inpf_sz < 0.7 * outf_sz:
		print( msg)
		print("  Skip renaming !")
		time.sleep(5)
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

	end_t = time.perf_counter()
	print(f"  -End: {TM.datetime.now():%T}\tTotal: {hm_time(end_t - str_t)}")

	return (inpf_sz - outf_sz)

##>>============-------------------<  End  >------------------==============<<##


if __name__ == '__main__':
	str_t = time.perf_counter()

	print(f'Main Start: {TM.datetime.now()}')

	if not os.path.exists(Excepto): # XXX: DEfined in .yml file
		print (f"Creating dir: {Excepto}")
		os.mkdir(Excepto)

	time.sleep(1)
	print("-" * 70)

	if not len (WFolder) :
		print (" Wfolder needs to be defined and point to the root diredtory to be Proccesed")

	fl_lst = scan_folder( WFolder, File_extn , sort_order = True )
	fl_nmb = len(fl_lst)

	saved = 0
	procs = 0
	skipt = 0
	errod = 0
	for cnt, each in enumerate(fl_lst) :
		cnt += 1
		file_p	= each[0]
		file_s	= each[1]
		ext		= each[2]
		dirs	= each[3]

		if os.path.isfile(file_p)  :
			print(f'\n{file_p}\n{ordinal(cnt)} of {fl_nmb}, {ext}, {hm_sz(file_s)}.')
			if len (file_p) < 333 :
				free_disk_space = shutil.disk_usage( WFolder ).free
				free_temp_space = shutil.disk_usage( r"C:" ).free
				if free_disk_space < ( 3 * file_s ) or free_temp_space < ( 3 * file_s ) :
					print ('\n!!! ', file_p[0:2], hm_sz(free_disk_space), " Free Space" )
					input ("Not Enoug space on Drive")
			else :
				input (" File name too long > 333 ")
			try:
				logging.info(f"\nProc: {file_p}")
				all_good =          ffprobe_run( file_p, ffprob,   de_bug )
#				de_bug = True
				all_good, skip_it = zabrain_run( file_p, all_good, de_bug )
				if  skip_it  == True :
					print(f"\033[91m   | Skip ffmpeg_run |\033[0m")
					skipt += 1
				all_good = ffmpeg_run( file_p, all_good, skip_it, ffmpeg, de_bug )
				logging.info(f"FFMPEG out = {all_good}")
				if not all_good and not skip_it :
					print ( " FFMPEG did not create anything good")
					time.sleep(5)
		#		print(f"\033[91m   | Make 3x3 matrix |\033[0m")
		#		if matrix_it (file_p, ext='.png') and not all_good :
		#			continue
#				print (" Create 4x Speedup\n")
#				speed_up  (file_p, execu )
#				print ("\n Create a Short version\n")
#				short_ver (file_p, execu, de_bug )
#				video_diff( file_p, all_good )

				saved += clean_up( file_p, all_good, skip_it, de_bug ) or 0
				procs +=1
				total_size += glb_vidolen

			except ValueError as e :
				errod +=1
				msj = f" -:Except: {e}\n{file_p}\n\t{hm_sz(file_s)}\nMoved"
				print( msj )
				copy_move(file_p, Excepto, False)
				continue

			except Exception as e :
				errod +=1
				msj = f" +:Except: {e}\n{os.path.dirname(file_p)}\n\t{os.path.basename(file_p)}\t{hm_sz(file_s)}\nCopied"
				print( msj )
				copy_move(file_p, Excepto, True)
#				breakpoint(continue)
				continue

			if not skip_it:
				print(f"  Saved Total: {hm_sz(saved)}")

		else:
			mess = f'Not Found-: {file_p}\t\t{hm_sz(file_s)}'
			print(f"\n{mess}\n")
			errod +=1
			time.sleep(1)
			continue	# continue forces the loop to start the next iteration pass will continue through the remainder or the loop body

	end_t = time.perf_counter()
	print(f'  Done: {TM.datetime.now():%T}\tTotal: {hm_time(end_t - str_t)}\n')

	print(f"\n  Saved in Total: {hm_sz(saved)} Time: {hm_time(total_size)}\n  Files: {fl_nmb}\tProces: {procs}\tSkip: {skipt}\tErr: {errod}\n")

	sys.stdout.flush()
	input('All Done :)')
	exit()
##>>============-------------------<  End  >------------------==============<<##
