import os
import re
import sys
import json
import cgitb
import datetime
import traceback

from FFMpeg import *
from My_Utils import *
from Yaml import *

# https://docs.python.org/3.2/library/time.html
ancr_time = f"{datetime.datetime.now(): %Y %j %H-%M-%S }"

This_File = sys.argv[0].strip('.py')

Log_File = This_File + ancr_time + ' all.log'
sys.stdout = Tee(sys.stdout, open(Log_File, 'w', encoding="utf-8"))

Oky_File = This_File + ancr_time + ' oky.txt'
Success_File = open(Oky_File, 'w', encoding="utf-8")

messa  = f"## Time: {ancr_time}\t{Oky_File}\n"
messa += f'    | ¯\_(%)_/¯\n'
print(messa)

##>>============-------------------<  End  >------------------==============<<##

def build_list(root, _exten):
	'''
	Create the list of Files from "root with _exten" to Proces
	'''
	str_t = datetime.datetime.now()
	messa = sys._getframe().f_code.co_name
	print(f" +{messa}=:\t{root}\tStart: {str_t:%T}")

	print(f'Dir: {root}\tSize: {hm_sz(get_tree_size(root))}')
	queue_list = []
	# a Directory ?
	if os.path.isdir(root):
		for roota, dirs, files in os.walk( root ):
			for one_file in files:
				x, ext = os.path.splitext(one_file.lower())
				if ext in _exten:
					f_path = os.path.normpath(os.path.join(roota, one_file))
					file_s = os.path.getsize(f_path)
					# XXX  |[0] Full Path |[1] File Size |[3] ext |[4] Year Made XXX
					f_dict = f_path, file_s, ext
					queue_list.append(f_dict)
	else:
		print(f"It's Not a Directory {root}")
		return False

	end_t = datetime.datetime.now()
	messa = (f' { len(queue_list):,} Files\n End  : {end_t:%T}\tTotal: {round((end_t-str_t).total_seconds(),1):,}')
	print( messa )
	return queue_list
##>>============-------------------<  End  >------------------==============<<##


def post_clean(input_file, output_file):

	str_t = datetime.datetime.now()
	messa = sys._getframe().f_code.co_name
	print(f"  +{messa}=: Start: {str_t:%T}")

# XXX: Check that in and out Files are okay
	outf_sz = os.path.getsize(output_file)
	inpf_sz = os.path.getsize(input_file)
	ratio = round(100 * ((outf_sz - inpf_sz) / inpf_sz),1)

	delte_me_fnam = get_new_fname(input_file, "_DeletMe.old", TmpF_Ex)
	copy_move( input_file, delte_me_fnam )

	f_name, xt = os.path.splitext(input_file)
	new_done_fnam = get_new_fname(input_file, TmpF_Ex, xt)
	copy_move( output_file, new_done_fnam )

	messa = f"  File: {os.path.basename(input_file)}\n    Was: { hm_sz(inpf_sz)}\t Is: {hm_sz(outf_sz)}\t Saved: {hm_sz(inpf_sz - outf_sz)} = {ratio} %"
	if abs( ratio ) > 90 :
		messa += "\n    ! Huge Difference !"
	print(messa)

	end_t = datetime.datetime.now()
	print( f'  -End  : {end_t:%T}\tTotal: {round((end_t-str_t).total_seconds(),1):,}')

	return (1 + abs(inpf_sz - outf_sz))
##>>============-------------------<  End  >------------------==============<<##


if __name__ == '__main__':

	cgitb.enable(format='text')

	str_t = datetime.datetime.now()
	print(f' +Start: {str_t:%T}')

	messa = __file__ + '-:'
	print(messa)
	time.sleep(1)

	print("-" * 70)

	fl_lst = build_list( WFolder, File_extn )
	# XXX: Sort Order True -> Biggest first
	fl_lst = sorted(fl_lst, key=lambda Item: (Item[1], Item[2]), reverse=False)

	cnt = len(fl_lst)
	Fnum = 0
	Save = 0
	for each in fl_lst:
		Fnum	+= 1
		file_p	= each[0]
		file_s	= each[1]
		ext		= each[2]
		messa = f'\n{file_p}\n{ordinal(Fnum)} of {cnt}, {ext}, {hm_sz(file_s)}'
		if os.path.isfile(file_p) and len (file_p) < 256 :
			print(messa)
			disk_free_space = shutil.disk_usage( file_p )[2]
			if disk_free_space < ( 3 * file_s ):
				print ('\n!!! ', file_p[0:2], hm_sz(disk_free_space), " Free Space" )
				input ("Not Enoug space on Drive")
#			print (repr( file_p ))
			try:
#				print ("Here I am"), time.sleep(2)
#
				all_good = ffprob_run( file_p )
# XXX:				make_matrx( file_p )
				all_good = thza_brain( file_p, all_good )

#				all_good = short_ver( file_p )
				all_good = ffmpeg_run( file_p, all_good )
#				video_diff( file_p, all_good )
				if not all_good:
					input('WTF')
				Save += post_clean( file_p, all_good )

			except ValueError as err:
				messa = str( err )
				if '| <¯\_(%)_/¯>  Skip' in messa:
					print(messa)
					Success_File.write(f'=: {file_p}\n')
				else:
					print(messa)
					sys.stdout.flush()
				continue

			except Exception :
				print("\n", "v" * 40)
				print(f"Exec: \n{traceback.print_exc  ( limit=32 )}\n")
				print(f"Stack:\n{traceback.print_stack( limit=32 )}\n")
				print("\n", "^" * 40)
#				input("Exception !!! Press Any Key to Continue")
				file_name = os.path.basename(file_p)
				dirc_name = os.path.dirname(file_p)
				mess = f'-: {dirc_name}\n\t\t{file_name}\t{hm_sz(file_s)}\n'
				print(mess)
				copy_move(file_p, Excepto, True)
				sys.stdout.flush()
				continue

			else:
				Success_File.write(f'+: {file_p}\n')
				sys.stdout.flush()
			print(f"  Saved {hm_sz(Save)}")

		else:
			messa += f'\nNot Found !!\nNext'
			print(messa)
			time.sleep(1)
			continue
# continue forces the loop to start at the next iteration
# pass will continue through the remainder or the loop body
	sys.stdout.flush()
	Success_File.close()

	end_t = datetime.datetime.now()
	print(f' -End  : {end_t:%T}\tTotal: {end_t-str_t}')
	input('All Done')
	exit()
##>>============-------------------<  End  >------------------==============<<##
