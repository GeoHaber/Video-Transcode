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

''' Global Variables '''
glb_totfrms = 0
glb_vidolen = 0
vid_width   = 0
aud_smplrt  = 0
total_size  = 0



#de_bug = True

ancr_time = f"{TM.datetime.now(): %Y %j %H-%M-%S }"
This_File = sys.argv[0].strip('.py') +ancr_time
Log_File  = This_File + 'all.log'

# Create a Tee instance that writes to both the original sys.stdout and a log file
# Save the original sys.stdout
original_stdout = sys.stdout
qa = Tee(sys.stdout, open(Log_File, 'w', encoding=console_encoding))
sys.stdout = qa

print(f"{psutil.cpu_count()} CPU's\t ¯\_(%)_/¯\nTime:\t\t {ancr_time}\nPyton Version:\t {sys.version}\n" )
print(f"Script absolute path: {os.path.abspath(__file__)}")


@perf_monitor
def scan_folder(root: str, xtnsio: list) -> dict:
	'''Create the list of Files from "root with xtnsio" to Process '''
	str_t = time.perf_counter()

	msj = sys._getframe().f_code.co_name
	msj += f" Start: {TM.datetime.now():%T}\tRoot: {root}\tSize: {hm_sz(get_tree_size(root))}"

	print (msj)
	logging.info(msj)

	# a Directory ?
	if os.path.isdir(root):
		_lst = []
		for root, dirs, files in os.walk(root):
			cur_dir = os.path.split(root)[-1]
			for one_file in files:
				_, ext = os.path.splitext(one_file.lower())
				if ext in xtnsio:
#					f_path = '\\\\?\\' + os.path.join(root, one_file)
					f_path = os.path.join(root, one_file)  # XXX: # TODO: ? normpath ?
					file_s = os.path.getsize(f_path)
					if file_s > 1000 and len( f_path) < 300 :
						# XXX  |[0] Full Path |[1] File Size |[2] ext [3] Directory [4] sudirs| XXX
						info = [f_path, file_s, ext, cur_dir, dirs]
						if de_bug : print (f"\n{info}\n")
						_lst.append( info )
					else :
						msj= f"\nSkip {f_path}\nSz:{file_s} path L {len(f_path)}"
						logging.error(msj)
						print (msj)
						copy_move(f_path, Excepto, False)

	elif os.path.isfile(root):
		_, ext = os.path.splitext(root.lower())
		if ext in xtnsio:
			f_path = root  # XXX: # TODO: ? normpath ?
			file_s = os.path.getsize(root)
			# XXX  |[0] Full Path |[1] File Size |[3] ext | XXX
			_lst.append([f_path, file_s, ext])

	else:
		print(f"\n{root} Is Not a Directory\n")
		return False

	end_t = time.perf_counter()
	print(f'  -End: {TM.datetime.now():%T}\tTotal: {hm_time(end_t - str_t)}')

	# XXX: Sort Order reverse = True -> Biggest first
	logging.info(f" Sort: {True}")
	return sorted(_lst, key=lambda Item: Item[1], reverse=True)

##>>============-------------------<  End  >------------------==============<<##

@perf_monitor
def clean_up(input_file: str, outpt_file: str, skip_it: str, debug: bool) -> int:
	''' Take care of renaming temp files etc.. '''
	msj = sys._getframe().f_code.co_name

	if skip_it :
		logging.info (f"{msj} skip_it={skip_it}")
		return 0
	logging.info(f"{msj} ")

	str_t = time.perf_counter()
	print(f"  +{msj} Start: {TM.datetime.now():%T}")

	if not input_file or not outpt_file :
		print (f" {msj} Inf: {input_file} Out: {outpt_file} Exit:")
		logging.info(f"Inf: {input_file}\nOutf: {outpt_file}")
		return False

# XXX: Check that in and out Files are okay
	try :
		inpf_sz = os.path.getsize(input_file)
		if not inpf_sz :
			msj += " Input size Error"

		outf_sz = os.path.getsize(outpt_file)
		if not outf_sz :
			msj += " Output size Error"

	except Exception as e:
		logging.exception(f"Exception in {msj}: {e}",exc_info=True, stack_info=True)
		raise Exception(msj)

	ratio = round ( 100*(( outf_sz - inpf_sz ) / inpf_sz), 2)
	extra = '+Gain:'

	if ratio == 0 :
		extra = "=Same:"
	if ratio < 0 :
		extra = "-Lost:"
		ratio = abs(ratio)
	if ratio > 98 :
		extra += " ! Huge diff !"
		print(f"Size Was: {hm_sz(inpf_sz)} Is: {hm_sz(outf_sz)} {extra} {hm_sz( inpf_sz - outf_sz )} {ratio:>8} %" )
		seems_to_small = get_new_fname(input_file, "_seems_small.mp4", TmpF_Ex)
		copy_move( outpt_file, seems_to_small )
		return 0

	if de_bug :
		msj += "\t de_bug mode input file not Changed "
		print (msj)
		return 0

	_, xt = os.path.splitext(input_file)
	delte_me_fnam = get_new_fname(input_file, "_DeletMe.old", TmpF_Ex)
	copy_move( input_file, delte_me_fnam )
	new_done_fnam = get_new_fname(input_file, TmpF_Ex, xt)
	copy_move( outpt_file, new_done_fnam )

	msj = f"    File: {os.path.basename(input_file)[:66]}.. Was: {hm_sz(inpf_sz)} Is: {hm_sz(outf_sz)} Diff: {hm_sz( inpf_sz - outf_sz )} "
	if   inpf_sz > outf_sz :
		msj += f"-Lost: {ratio} %"
	elif inpf_sz < outf_sz :
		msj += f"+Gain: {ratio} %"
	else :
		msj += "Same Size"
	print( msj )

	end_t = time.perf_counter()
	print(f'  -End: {TM.datetime.now():%T}\tTotal: {hm_time(end_t - str_t)}')

	return (inpf_sz - outf_sz)
##>>============-------------------<  End  >------------------==============<<##

if __name__ == '__main__':
	str_t = time.perf_counter()

	logging.info(f'Main Start: {TM.datetime.now()}')
	print(f'Main Start: {TM.datetime.now()}')
	ancr_time = f"{TM.datetime.now(): %Y %j %H-%M-%S }"
	This_File = sys.argv[0].strip('.py') +ancr_time
	print(f"{psutil.cpu_count()} CPU's\t ¯\_(%)_/¯\nTime:\t\t {ancr_time}\nPyton Version:\t {sys.version}\n" )

	# Create a logs directory if it does not exist
	log_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), "._logs")
	print(f"Log directory (absolute): {os.path.abspath(log_dir)}")  # Debugging line
	if not os.path.exists(log_dir):
	    print(f"Creating log directory: {log_dir}")  # Debugging line
	    os.makedirs(log_dir)

	# Get the filename of the current script and append the current date and time
	script_name = os.path.basename(__file__).split(".")[0]
	log_filename = f"{script_name}_{TM.datetime.now():%Y-%m-%d_%H-%M-%S}.log"
	log_path = os.path.join(log_dir, log_filename)

	# Configure the logger
	print(f"Configuring logger with log path (absolute): {os.path.abspath(log_path)}")  # Debugging line
	logging.basicConfig(filename=log_path, level=logging.DEBUG,
	                    format="%(asctime)s %(levelname)s %(message)s")

	# Test logging message
	logging.info("Test log message. This is a test message from the logger.")

	# Print message indicating that the logger configuration is complete
	print("Logger configuration complete.")

	msj = f"Logging to {log_path}"
	print (msj)
	logging.info(msj)

	if not os.path.exists(Excepto): # XXX: DEfined in .yml file
		print (f"Creating dir: {Excepto}")
		os.mkdir(Excepto)

	time.sleep(1)
	print("-" * 70)

	if not len (WFolder) :
		print (" Wfolder needs to be defined and point to the root diredtory to be Proccesed")

	fl_lst = scan_folder( WFolder, File_extn )
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
			if len (file_p) < 259 :
				disk_free_space = shutil.disk_usage( file_p )[2]
				temp_free_space = shutil.disk_usage( Log_File )[2]
				if disk_free_space < ( 3 * file_s ) or temp_free_space < ( 3 * file_s ) :
					print ('\n!!! ', file_p[0:2], hm_sz(disk_free_space), " Free Space" )
					input ("Not Enoug space on Drive")
			else :
				input (" File name too long > 259 ")
			try:
				logging.info(f"\nProc: {file_p}")
				all_good = ffprobe_run( file_p, ffprob,   de_bug )
				all_good, skip_it = zabrain_run( file_p, all_good, de_bug )
				if  skip_it  == True :
					print(f"\033[91m    | Skip ffmpeg_run |\033[0m")
				all_good = ffmpeg_run( file_p, all_good, skip_it, ffmpeg, de_bug )
				logging.info(f"FFMPEG out = {all_good}")
				if not all_good and not skip_it :
					print ( " FFMPEG did not create anything good")
					time.sleep(5)
	#			print(f"\033[91m   Create 3x3 matrix\033[0m")
	#			if matrix_it (file_p, ext='.png') and not all_good :
	#				continue
#				print (" Create 4x Speedup\n")
#				speed_up  (file_p, execu )
#				print ("\n Create a Short version\n")
#				short_ver (file_p, execu, de_bug )
#				video_diff( file_p, all_good )
				saved += clean_up( file_p, all_good, skip_it, de_bug ) or 0
				procs +=1
				print ('Video len =', glb_vidolen )
				total_size += glb_vidolen

			except ValueError as e :
				errod +=1
				print( e )
				logging.exception(f"ValueError {e}",exc_info=True, stack_info=True)
				dirc_name = os.path.dirname(file_p)
				file_name = os.path.basename(file_p)
				print(f'-: {dirc_name}\n\t\t{file_name}\t{hm_sz(file_s)}\n')
				copy_move(file_p, Excepto, False)
				continue

			except Exception as e :
				errod +=1
				print( e )
				logging.exception(f"Exception {e}",exc_info=True, stack_info=True)
				dirc_name = os.path.dirname(file_p)
				file_name = os.path.basename(file_p)
				print(f'-: {dirc_name}\n\t\t{file_name}\t{hm_sz(file_s)}\n')
				copy_move(file_p, Excepto, True)
				continue

			print(f"  Saved Total: {hm_sz(saved)}")

		else:
			mess = f'Not Found-: {file_p}\t\t{hm_sz(file_s)}'
			print(f"\nmess\n")
			errod +=1
			time.sleep(1)
			continue
# continue forces the loop to start the next iteration pass will continue through the remainder or the loop body
	sys.stdout.flush()

	end_t = time.perf_counter()
	print(f'  Done: {TM.datetime.now():%T}\tTotal: {hm_time(end_t - str_t)}')
	print (f"\n  Total time: {hm_time(total_size)} Files: {fl_nmb}\tProces: {procs}\tSkip: {skipt}\tErr: {errod}" )
	print(f"  {hm_sz(saved)} Saved in Total")

	input('All Done :)')
	exit()
##>>============-------------------<  End  >------------------==============<<##
