# -*- coding: utf-8 -*-

import os
import re
import sys
import psutil
import logging
import datetime as TM

from Yaml		import *
from FFMpeg		import *
from My_Utils	import *

console_encoding = 'utf-8'

#de_bug = True

## XXX:  https://docs.python.org/3.2/library/time.html
ancr_time = f"{TM.datetime.now(): %Y %j %H-%M-%S }"
This_File = sys.argv[0].strip('.py')

Log_File  = This_File + ancr_time + 'all.log'
sys.stdout = Tee(sys.stdout, open(Log_File, 'w', encoding=console_encoding))

ok_f_nam = This_File + ancr_time + 'oky.txt'
OKs_file = open(ok_f_nam, 'w', encoding=console_encoding)
OKs_file.write(f" Time: = {ancr_time}\n" )

err_f_nm = This_File + ancr_time + 'ERR.txt'
ERR_file = open(err_f_nm, 'w', encoding=console_encoding)
ERR_file.write(f" Time: = {ancr_time}\n" )

print(f" {psutil.cpu_count()} CPU's  ¯\_(%)_/¯" )
print(f" Pyton Version:  {sys.version}\n" )
print(f" Time:           {ancr_time}" )

''' Global Variables '''
glb_totfrms = 0
vid_width   = 0
aud_smplrt  = 0

@performance_check
def scan_folder (root: str, xtnsio: list) -> dict :
	'''Create the list of Files from "root with xtnsio" to Process '''
	mesaj = sys._getframe().f_code.co_name
	str_t = time.perf_counter()
	logging.info(f"{mesaj} ")

	print(f"  +{mesaj}= : Start: {TM.datetime.now():%T}")
	print(f'Dir: {root}\tSize: {hm_sz(get_tree_size(root))}')

	# a Directory ?
	if os.path.isdir(root):
		_lst = []
		for rot, dirs, files in os.walk(root):
			cur_dir = os.path.split(rot)[-1]
			for one_file in files:
				_, ext = os.path.splitext(one_file.lower())
				if ext in xtnsio:
					f_path = os.path.join(rot, one_file) # XXX: # TODO: ? normpath ?
					file_s = os.path.getsize(f_path)
					if file_s > 1000 and len( f_path) < 259 :
					# XXX  |[0] Full Path |[1] File Size |[3] ext | XXX
						_lst.append([f_path, file_s, ext, cur_dir, dirs])
					else :
						msj= f"\nSkip {f_path}\nSz:{file_s} path L {len(f_path)}"
						logging.error(msj)
						print (msj)
	elif os.path.isfile(root):
		_, ext = os.path.splitext(root.lower())
		if ext in xtnsio:
			f_path = root  # XXX: # TODO: ? normpath ?
			file_s = os.path.getsize(root)
			# XXX  |[0] Full Path |[1] File Size |[3] ext | XXX
			_lst.append([f_path, file_s, ext])
	# not Directory exit
	else:
		print(f"\n{root} Is Not a Directory\n")
		return False

	end_t = time.perf_counter()
	print(f'  -End: {TM.datetime.now():%T}\tTotal: {(end_t - str_t):.2f} sec')
	logging.info(f" Sort: {True}")
	# XXX: Sort Order reverse = True -> Biggest first

	return sorted(_lst, key=lambda Item: (Item[1], Item[2]), reverse=True )
##>>============-------------------<  End  >------------------==============<<##

@performance_check
def clean_up(input_file: str, outpt_file: str, debug: bool) -> int:
	''' Take care of renaming temp files etc.. '''
	str_t = time.perf_counter()
	mesaj = sys._getframe().f_code.co_name
	logging.info(f"{mesaj} ")
	print(f"  +{mesaj}=: Start: {TM.datetime.now():%T}")

	if not input_file or not outpt_file :
		print (f" {mesaj} Inf: {input_file} Out: {outpt_file} Exit:")
		logging.info(f"Inf_: {input_file}\nOut_: {outpt_file}")
		return False

# XXX: Check that in and out Files are okay
	try :
		inpf_sz = os.path.getsize(input_file)
		if not inpf_sz :
			mesaj += " Input size Error"

		outf_sz = os.path.getsize(outpt_file)
		if not outf_sz :
			mesaj += " Output size Error"
	except Exception as e:
		logging.error(f" {e}", exc_info=True)
		raise Exception(mesaj)

	ratio = round ( 100*(( outf_sz - inpf_sz ) / inpf_sz), 2)
	extra = '+Gain:'

	if ratio == 0 :
		extra = "=Same:"
	if ratio < 0 :
		extra = "-Lost:"
		ratio = abs(ratio)
	if ratio > 97 :
		extra += " ! Huge diff !"
		print(f"Size Was: {hm_sz(inpf_sz)} Is: {hm_sz(outf_sz)} {extra} {hm_sz( inpf_sz - outf_sz )} {ratio:>8} %" )
		seems_to_small = get_new_fname(input_file, "_seems_small.mp4", TmpF_Ex)
		copy_move( outpt_file, seems_to_small )
		return 0

	if de_bug :
		mesaj += "\t de_bug mode input file not Changed "
		print (mesaj)
		return 0

	_, xt = os.path.splitext(input_file)
	delte_me_fnam = get_new_fname(input_file, "_DeletMe.old", TmpF_Ex)
	copy_move( input_file, delte_me_fnam )
	new_done_fnam = get_new_fname(input_file, TmpF_Ex, xt)
	copy_move( outpt_file, new_done_fnam )

	mesaj = f"    File: {os.path.basename(input_file)[:66]}.. Was: {hm_sz(inpf_sz)} Is: {hm_sz(outf_sz)} {extra} {hm_sz( inpf_sz - outf_sz )} {ratio:>8} %"
	if   inpf_sz > outf_sz :
		mesaj += f"Save: {ratio} %"
	elif inpf_sz < outf_sz :
		mesaj += f"Lost: {ratio} %"
	else :
		mesaj += "Same Size"
	print( mesaj )

	end_t = time.perf_counter()
	print(f'   -End: {TM.datetime.now():%T}\tTotal: {(end_t - str_t):.2f} sec')

	return (inpf_sz - outf_sz)
##>>============-------------------<  End  >------------------==============<<##

if __name__ == '__main__':

	logging.basicConfig( level=logging.DEBUG,
			filename=('_1og.log'), filemode='w',
			format='%(asctime)s %(levelname)s %(message)s',
			datefmt='%d-%H:%M:%S')

	str_t = time.perf_counter()
	print(f'Main Start: {TM.datetime.now()}')

	if not os.path.exists(Excepto):
		print (f"Creating dir: {Excepto}")
		os.mkdir(Excepto)

	mesaj = __file__ + '-:'
	print(mesaj)
	time.sleep(1)

	print("-" * 70)

	if not len (WFolder) :
		print (" Wfolder needs to be defined and point to the root diredtory to be scaned / Proccesed")

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
				logging.info(f"Proc: {file_p}")
				all_good = ffprobe_run( file_p, ffprob,   de_bug )
				all_good, skip_it = zabrain_run( file_p, all_good, de_bug )
				if  skip_it  == True :
					print(f"\033[91m    | Skip ffmpeg_run |\033[0m")
					end_t = time.perf_counter()
					print(f'   -End: {TM.datetime.now():%T}\tTotal: {(end_t - str_t):.2f} sec')
				all_good = ffmpeg_run( file_p, all_good, skip_it, ffmpeg, de_bug )
				logging.info(f"Fm_out_fl: {all_good}")
				if not all_good and skip_it != True:
					print ( " FFMPEG did not create anything good")
					time.sleep(1)
			#	print(f"\033[91m   Create 3x3 matrix\033[0m")
			#	if matrix_it (file_p, ext='.png') and not all_good :
#				print (" Create 4x Speedup\n")
#				speed_up  (file_p, execu )
#				print ("\n Create a Short version\n")
#				short_ver (file_p, execu, de_bug )
#				video_diff( file_p, all_good )
				saved += clean_up( file_p, all_good, de_bug ) or 0 
				procs +=1


			except ValueError as e:
				logging.error(f" {e}", exc_info=True)
				mesaj = str( e )
				if Skip_key in mesaj:
					skipt +=1
					print(f"\033[33m   | Skip it |\033[0m")
#					print('   | Skip it |')
#					file_p = file_p.encode( console_encoding, errors='ignore')
					OKs_file.write(f'=: {file_p}\n')
				else:
					print('Not Taken', mesaj)
					errod +=1
					ERR_file.write(f'-: {file_p}\n')
				continue

			except Exception as e:
				logging.error(f" {e}", exc_info=True)
				print(' Exception', repr( e ), '\nCopy file to: Except' )
				file_name = os.path.basename(file_p)
				dirc_name = os.path.dirname(file_p)
				print(f'-: {dirc_name}\n\t\t{file_name}\t{hm_sz(file_s)}\n')
				errod +=1
				ERR_file.write(f'-: {file_p}\t{e}\n')
				copy_move(file_p, Excepto, True)
				Trace (Exception, e)
				continue

			else:
				OKs_file.write(f'+: {file_p}\n')
			print(f"  Saved Total: {hm_sz(saved)}")

		else:
			mess = f'Not Found-: {file_p}\t\t{hm_sz(file_s)}'
			logging.error(f" {mess}", exc_info=True)
			print(f"\nmess\n")
			errod +=1
			ERR_file.write(f'-: {file_p}\n')
			time.sleep(2)
			continue
# continue forces the loop to start the next iteration pass will continue through the remainder or the loop body
	OKs_file.close()
	ERR_file.close()
	sys.stdout.flush()

	end_t = time.perf_counter()
	print(f'  Done: {TM.datetime.now():%T}\tTotal: {(end_t - str_t):.2f} sec')
	print (f"\n  Total: {fl_nmb}\tProces: {procs}\tSkip: {skipt}\tErr: {errod}" )
	print(f"  {hm_sz(saved)} Saved in Total")

	input('All Done :)')
	exit()
##>>============-------------------<  End  >------------------==============<<##
