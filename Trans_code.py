import os
import re
import sys
import json
import datetime
import traceback
import multiprocessing

from Yaml		import *
from FFMpeg		import *
from My_Utils	import *

WFolder = r"F:\Media\TV"
#WFolder = r"F:\Media\Movie"
#WFolder = r"C:\Users\Geo\Desktop\Except"

## XXX:  https://docs.python.org/3.2/library/time.html
ancr_time = f"{datetime.datetime.now(): %Y %j %H-%M-%S }"
This_File = sys.argv[0].strip('.py')

Log_File  = This_File + ancr_time + 'all.log'
sys.stdout = Tee(sys.stdout, open(Log_File, 'w', encoding=console_encoding))

ok_f_name = This_File + ancr_time + 'oky.txt'
Success_File = open(ok_f_name, 'w', encoding=console_encoding)
err_f_nam = This_File + ancr_time + 'ERR.txt'
ERR_file = open(err_f_nam, 'w', encoding=console_encoding)

print(f" Time: {ancr_time}" )
print(f" Number of CPUs: {multiprocessing.cpu_count()}  ¯\_(%)_/¯" )
print(f" Pyton Version:  {sys.version}\n" )

##>>============-------------------<  End  >------------------==============<<##

def scan_folder(root, xtnsio):
	'''
	Create the list of Files from "root with xtnsio" to Proces
	'''
	str_t = datetime.datetime.now()
	messa = sys._getframe().f_code.co_name
	print(f"+{messa}=:\t{root}\tStart: {str_t:%T}")

	print(f'Dir: {root}\tSize: {hm_sz(get_tree_size(root))}')
	_lst = []
	# a Directory ?
	if os.path.isdir(root):
		for rot, dirs, files in os.walk( root ):
			for one_file in files:
				_, ext = os.path.splitext(one_file.lower())
				if ext in xtnsio:
					f_path = os.path.join(rot, one_file) # XXX: # TODO: ? normpath ?
					file_s = os.path.getsize(f_path)
					# XXX  |[0] Full Path |[1] File Size |[3] ext | XXX
					_lst.append([f_path, file_s, ext])
	else:
		print(f"\n{root} Is Not a Directory\n")
		return False

	end_t = datetime.datetime.now()
	messa = (f'{len(_lst):,} Files\nEnd: {end_t:%T}\tTotal: {round((end_t-str_t).total_seconds(),2)} sec')
	print( messa )

	# XXX: Sort Order reverse = True -> Biggest first
	return sorted(_lst, key=lambda Item: (Item[1], Item[2]), reverse=True )
##>>============-------------------<  End  >------------------==============<<##

def post_clean(input_file, outpt_file):
	str_t = datetime.datetime.now()
	messa = sys._getframe().f_code.co_name
	#print(f"  +{messa}=: Start: {str_t:%T}")

# XXX: Check that in and out Files are okay
	outf_sz = os.path.getsize(outpt_file)
	inpf_sz = os.path.getsize(input_file)

	rati = abs(100 * ((outf_sz - inpf_sz) / inpf_sz))
	if rati > 9:
		ratio = round(rati)
	else :
		ratio = round(rati, 1)

	if abs( ratio ) > 96 :
		seems_to_small = get_new_fname(input_file, "_Seems_Small.mp4", TmpF_Ex)
		copy_move( outpt_file, seems_to_small )
		messa = f"\n\t! Huge Difference was {hm_sz(inpf_sz)} is {hm_sz(outf_sz)}\n Out File: {seems_to_small}"
		print(messa)
		return 0

	_, xt = os.path.splitext(input_file)
	delte_me_fnam = get_new_fname(input_file, "_DeletMe.old", TmpF_Ex)
	copy_move( input_file, delte_me_fnam )
	new_done_fnam = get_new_fname(input_file, TmpF_Ex, xt)
	copy_move( outpt_file, new_done_fnam )

	messa = f"  File: {os.path.basename(input_file)}\n\t Was: {hm_sz(inpf_sz)}\t Is: {hm_sz(outf_sz)}\t"
	if   inpf_sz > outf_sz :
		messa += f"Saved : {ratio}%"
	elif inpf_sz < outf_sz :
		messa += f"Lost  : {ratio}%"
	else :
		messa += "Same Size"
	print( messa )

	end_t = datetime.datetime.now()
	#print (f'  -End: {end_t:%T}\tTotal: {round((end_t-str_t).total_seconds(),2)}' )

	return (inpf_sz - outf_sz)
##>>============-------------------<  End  >------------------==============<<##

if __name__ == '__main__':
	str_t = datetime.datetime.now()
	print(f'Main Start: {str_t:%T}')

	if not os.path.exists(Excepto):
		print (f"Creating dir: {Excepto}")
		os.mkdir(Excepto)

	messa = __file__ + '-:'
	print(messa)
	time.sleep(1)

	print("-" * 70)

	fl_lst = scan_folder( WFolder, File_extn )
	nu_fi = len(fl_lst)
	Save = 0
	for cnt, each in enumerate(fl_lst) :
		cnt += 1
		file_p	= each[0]
		file_s	= each[1]
		ext		= each[2]

		if os.path.isfile(file_p) and len (file_p) < 256 :
			print(f'\n{file_p}\n{ordinal(cnt)} of {nu_fi}, {ext}, {hm_sz(file_s)}')
			disk_free_space = shutil.disk_usage( file_p )[2]
			if disk_free_space < ( 3 * file_s ):
				print ('\n!!! ', file_p[0:2], hm_sz(disk_free_space), " Free Space" )
				input ("Not Enoug space on Drive")
			tmp_free_space = shutil.disk_usage( Log_File )[2]
			if tmp_free_space < ( 3 * file_s ):
				print ('\n!!! ', file_p[0:2], hm_sz(tmp_free_space), " Free Space" )
				input ("Not Enoug space on Drive")

			try:
				all_good = run_ffprob( file_p )
				all_good = thza_brain( file_p, all_good )
#				make_matrx( file_p )
# XXX: all_good is the encoded file Name
				all_good = ffmpeg_run( file_p, all_good )
#				all_good = short_ver( file_p )
#				video_diff( file_p, all_good )
				if not all_good:
					input('WTF')
				Save += post_clean( file_p, all_good )

				file_p = file_p.encode( console_encoding, errors='ignore')

			except ValueError as err:
				messa = str( err )
				if '| <¯\_(%)_/¯>  Skip |' in messa:
					print(' ',messa)
					file_p = file_p.encode( console_encoding, errors='ignore')
					Success_File.write(f'=: {file_p}\n')
				else:
					print(messa)
					sys.stdout.flush()
				continue

			except Exception as e:
				print(' Exception', repr( e ), '\nCopy file to: Except' )
				file_name = os.path.basename(file_p)
				dirc_name = os.path.dirname(file_p)
				mess = f'-: {dirc_name}\n\t\t{file_name}\t{hm_sz(file_s)}\n'
				print(mess)
				copy_move(file_p, Excepto, True)
				Trace (messa, e)
				sys.stdout.flush()
				continue

			else:
				Success_File.write(f'+: {file_p}\n')
				sys.stdout.flush()
			print(f"  Saved {hm_sz(Save)}")

		else:
			file_name = os.path.basename(file_p)
			dirc_name = os.path.dirname(file_p)
			mess = f'\nNot Found-: {dirc_name}\n\t\t{file_name}\t{hm_sz(file_s)}\nNext??'
			ERR_file.write(f'{file_p}\n')
			print(mess)
			sys.stdout.flush()
			time.sleep(3)
			continue
# continue forces the loop to start the next iteration pass will continue through the remainder or the loop body
	sys.stdout.flush()
	Success_File.close()

	end_t = datetime.datetime.now()
	print(f' -End: {end_t:%T}\tTotal: {round((end_t-str_t).total_seconds(),2)} sec')

	input('All Done')
	exit()
##>>============-------------------<  End  >------------------==============<<##
