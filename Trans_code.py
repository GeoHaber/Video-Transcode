import os
import re
import sys
import logging
import datetime
import multiprocessing

from Yaml		import *
from FFMpeg		import *
from My_Utils	import *

#DeBug = True

## XXX:  https://docs.python.org/3.2/library/time.html
ancr_time = f"{datetime.datetime.now(): %Y %j %H-%M-%S }"
This_File = sys.argv[0].strip('.py')

Log_File  = This_File + ancr_time + 'all.log'
sys.stdout = Tee(sys.stdout, open(Log_File, 'w', encoding=console_encoding))

ok_f_nam = This_File + ancr_time + 'oky.txt'
OKs_file = open(ok_f_nam, 'w', encoding=console_encoding)
OKs_file.write(f" Time: = {ancr_time}\n" )

err_f_nm = This_File + ancr_time + 'ERR.txt'
ERR_file = open(err_f_nm, 'w', encoding=console_encoding)
ERR_file.write(f" Time: = {ancr_time}\n" )

print(f" {multiprocessing.cpu_count()} CPU's  ¯\_(%)_/¯" )
print(f" Pyton Version:  {sys.version}\n" )
print(f" Time:           {ancr_time}" )

''' Global Variables '''
glb_totfrms = 0
vid_width   = 0

@performance_check
def scan_folder(root, xtnsio):
	'''Create the list of Files from "root with xtnsio" to Proces '''
	print(f"  +{mesaj}=: Start: {TM.datetime.now():%T}")
	str_t = time.perf_counter_ns()

	print(f'Dir: {root}\tSize: {hm_sz(get_tree_size(root))}')
	_lst = []
	# a Directory ?
	if os.path.isdir(root):
		for rot, _, files in os.walk( root ):
			for one_file in files:
				_, ext = os.path.splitext(one_file.lower())
				if ext in xtnsio:
					f_path = os.path.join(rot, one_file) # XXX: # TODO: ? normpath ?
	#				file_p = file_p
					file_s = os.path.getsize(f_path)

					# XXX  |[0] Full Path |[1] File Size |[3] ext | XXX
					_lst.append([f_path, file_s, ext])
	else:
		print(f"\n{root} Is Not a Directory\n")
		return False

	end_t = time.perf_counter_ns()
	print(f'  -End: {TM.datetime.now():%T}\tTotal: {((end_t-str_t) >> 20)/1000} sec')
	# XXX: Sort Order reverse = True -> Biggest first

	return sorted(_lst, key=lambda Item: (Item[1], Item[2]), reverse=True )
##>>============-------------------<  End  >------------------==============<<##

@performance_check
def clean_up(input_file, outpt_file, DeBug):
	''' Take care of renaming temp files etc.. '''
	str_t = datetime.datetime.now()
	mesaj = sys._getframe().f_code.co_name
	print(f"  +{mesaj}=: Start: {str_t:%T}")

# XXX: Check that in and out Files are okay
	inpf_sz = os.path.getsize(input_file)
	outf_sz = os.path.getsize(outpt_file)
	if not inpf_sz :
		mesaj += " Input size Error"
		raise Exception(mesaj)

	ratio = round ( 100*((( outf_sz - inpf_sz ) / inpf_sz)), 2)
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

	DeBug = True
	if DeBug :
		mesaj += "\t DeBug mode input file not Changed "
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

	end_t = datetime.datetime.now()
	convert = time.strftime("%H:%M:%S", time.gmtime((end_t-str_t).total_seconds()))
	mesaj =f'  -End: {end_t:%T}\tTotal: {convert} sec'
	print (mesaj)

	return (inpf_sz - outf_sz)
##>>============-------------------<  End  >------------------==============<<##

if __name__ == '__main__':

	logging.basicConfig(filename=('_Log.log'),
            level=logging.INFO,
            format='%(asctime)s %(message)s',
            datefmt='%m/%d/%Y %I:%M:%S %p'
            )

	str_t = datetime.datetime.now()
	print(f'Main Start: {str_t:%T}')

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

		if os.path.isfile(file_p)  :
			print(f'\n{file_p}\n{ordinal(cnt)} of {fl_nmb}, {ext}, {hm_sz(file_s)}')
			if len (file_p) < 256 :
				disk_free_space = shutil.disk_usage( file_p )[2]
				temp_free_space = shutil.disk_usage( Log_File )[2]
				if disk_free_space < ( 3 * file_s ) or temp_free_space < ( 3 * file_s ) :
					print ('\n!!! ', file_p[0:2], hm_sz(disk_free_space), " Free Space" )
					input ("Not Enoug space on Drive")
			try:
				all_good = ffprobe_run( file_p, ffprob,   DeBug )
				all_good = zabrain_run( file_p, all_good, DeBug )
#				make_matrx( file_p )
# XXX: all_good is the encoded file Name
#				DeBug = True
				all_good = ffmpeg_run( file_p, all_good, ffmpeg, DeBug )
#
#				all_good = short_ver( file_p )
#				video_diff( file_p, all_good )
				if not all_good:
					print ( " FFMPEG did not create anything good")
#					input('WTF')
				saved += clean_up( file_p, all_good, DeBug )
				procs +=1

				file_p = file_p.encode( console_encoding, errors='ignore')

			except ValueError as err:
				mesaj = str( err )
				if Skip_key in mesaj:
					skipt +=1
					print('   | Skip it |')
#					file_p = file_p.encode( console_encoding, errors='ignore')
					OKs_file.write(f'=: {file_p}\n')
				else:
					print('Not Taken', mesaj)
					errod +=1
					ERR_file.write(f'-: {file_p}\n')
				continue

			except Exception as e:
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
			mess = f'\nNot Found-: {file_p}\t\t{hm_sz(file_s)}\n'
			print(mess)
			errod +=1
			ERR_file.write(f'-: {file_p}\n')
			time.sleep(2)
			continue
# continue forces the loop to start the next iteration pass will continue through the remainder or the loop body
	OKs_file.close()
	ERR_file.close()
	sys.stdout.flush()

	end_t = datetime.datetime.now()
	convert = time.strftime("%H:%M:%S", time.gmtime((end_t-str_t).total_seconds()))
	mesaj =f'  -End: {end_t:%T}\tTotal: {convert} sec'
	print( mesaj )
	print (f"\n  Total: {fl_nmb}\tProces: {procs}\tSkip: {skipt}\tErr: {errod}" )
	print(f"  {hm_sz(saved)} Saved in Total")

	input('All Done :)')
	exit()
##>>============-------------------<  End  >------------------==============<<##
