# -*- coding: utf-8 -*-
#!/usr/bin/python3
__author__ = 'GeoHaZen'
'''
@author: 	  GeoHaZen
# XXX KISS
# XXX:ToDo: multiple languages
			raise exception to safe specifi files
			pass comand line and atributes for behaiviour
'''

DeBug = True

import os
import re
import sys
import json
import cgitb
import shutil
import random
import datetime
import traceback
import subprocess

from FFMpeg import Run_FFMpego, Run_FFProbe, FFZa_Braino, Make_Matrix
from My_Utils import *
from Yaml import *

#WFolder = '.'
WFolder = r"C:\Users\Geo\Desktop\downloads"
#WFolder = r"C:\"
#WFolder = r"C:\Users\Geo\Videos"

# https://docs.python.org/3.2/library/time.html
_time = datetime.datetime.now()
#date_post = f"{_time:_%Y_%m_%d_%H_%M_%a}"
#date_post = f"{_time:_%Y_%m_%d_%H_%M}"
date_post = f"{_time:_%Y %b %d_%I %p_}"

This_File = sys.argv[0].strip('.py')

Log_File = This_File + date_post + 'all.log'
Bad_File = This_File + date_post + 'bad.txt'
Oky_File = This_File + date_post + 'oky.txt'
sys.stdout = Tee(sys.stdout, open(Log_File, 'w', encoding="utf-8"))

Success_File = open(Oky_File, 'w', encoding="utf-8")
Exeptin_File = open(Bad_File, 'w', encoding="utf-8")

# greating mesage inside the file
messa = f"## Time: {_time} {Oky_File}\n\n"
Success_File.write(messa)
Exeptin_File.write(messa)
print(messa)
##>>============-------------------<  End  >------------------==============<<##

def Build_List(Top_dir, Exten_type):
	'''
	Create the list of Files to Proces
	'''
#	DeBug = True

	str_t = datetime.datetime.now()
	messa = sys._getframe().f_code.co_name
	print(f'  Dir: {Top_dir}\tis: {HuSa(get_tree_size(Top_dir))}')
	print(f"  +{messa}=: Start: {str_t:%H:%M:%S}")
	queue_list = []

	# a Directory ?
	if os.path.isdir(Top_dir):
		messa += f"\n Directory Scan :{Top_dir}"
		print(messa)
		for root, dirs, files in os.walk(Top_dir):
			for one_file in files:
				x, ext = os.path.splitext(one_file.lower())
				if ext in Exten_type:
					File_path = os.path.normpath(os.path.join(root, one_file))
					F_sz = os.path.getsize(File_path)
					info = File_path, F_sz, ext
					queue_list.append(info)

	end_t = datetime.datetime.now()
	print(f'End  : {end_t:%H:%M:%S}\tTotal: {(end_t-str_t).total_seconds()}')
	return queue_list
##>>============-------------------<  End  >------------------==============<<##

def Sort_Parse ( queue_list, Srt_item=0, Srt_ordr=True) :
#	DeBug = True
	str_t = datetime.datetime.now()
	messa = sys._getframe().f_code.co_name


#	File_size = os.path.getsize(File_path)
#	File_stat = os.stat(File_path)
#	Pars_year = Parse_filename(File_path)

	queue_list = sorted(queue_list, key=lambda Item: Item[Srt_item], reverse=Srt_ordr)

# XXX  |[0] Extension |[1] Full Path |[2] File Size |[3] File Info |[4] Year Made XXX

	return queue_list
##>>============-------------------<  End  >------------------==============<<##

def Skip_Files(File_dscrp, MinF_sz, File_ext=TmpF_Ex):
	#	DeBug = True
	'''
	Returns True if lock file is NOT
	'''
	str_t = datetime.datetime.now()
	messa = sys._getframe().f_code.co_name
#	print(f"  +{messa}=: Start: {str_t:%H:%M:%S}")

# XXX  |[0] Extension |[1] Full Path |[2] File Size |[3] File Info |[4] Year Made XXX
	The_file  = File_dscrp[1]
	Fname, _  = os.path.splitext(The_file)
	F_sz = File_dscrp[2]

# XXX: File does not exist :(
	if not os.path.exists(The_file):
		messa += f"\n File Not Found {The_file}\n"
		print(messa)
		Exeptin_File.write(messa)
		sys.stdout.flush()

		return False

# XXX Big enough to be video ?? # 256K should be One Mega byte 1048576
	elif F_sz < MinF_sz:
		messa += f"\n To Small:| {HuSa(F_sz):9} | {The_file}\n"
		print(messa)
		Exeptin_File.write(messa)
		sys.stdout.flush()

		return False

	return Fname
##>>============-------------------<  End  >------------------==============<<##


def Parse_filename(FileName):
	str_t = datetime.datetime.now()
	messa = sys._getframe().f_code.co_name
#	print(f"  +{messa}=: Start: {str_t:%H:%M:%S}")

	try:
		yr = re.findall(r'[\[\(]?((?:19[4-9]|20[0-1])[0-9])[\]\)]?', FileName)
		if yr:
			va = sorted(yr, key=lambda pzd: int(pzd), reverse=True)
			za = int(va[0])
			if za > 2019 or za < 1930:
				za = 1954
		else:
			za = 1234
	except:
		za = 1
	return za
##>>============-------------------<  End  >------------------==============<<##


def Pars_Clenup(Inp_file, Out_file):
	#	DeBug	= True
	str_t = datetime.datetime.now()
	messa = sys._getframe().f_code.co_name
	print(f"   +{messa}=: Start: {str_t:%H:%M:%S}")

# XXX: Check that in and out Files are okay
	Ou_fi_sz = os.path.getsize(Out_file)
	In_fi_sz = os.path.getsize(Inp_file)
	Ratio = round(100 * ((Ou_fi_sz - In_fi_sz) / In_fi_sz))

	To_delete = New_File_Name(Inp_file, "_DeletMe.old", TmpF_Ex)
	shutil.move(Inp_file, To_delete )

# XXX: # TODO:  Create New Name
#		New_Beut_Name = magic on ( f_name )
#		Title = 'title=\" ' + Sh_fil_name + " x265 Encoded " + __author__ + " Master "
#		ff_head = [Execute, '-i', Fmpg_in_file, '-hide_banner']
#		ff_tail = ['-metadata', Title, '-y', New_Beut_Name]

	f_name, xt = os.path.splitext(Inp_file)
	New_out_File = New_File_Name(Inp_file, TmpF_Ex, xt)

	Move_or_Copy( Out_file, New_out_File )

	messa = f"File: {os.path.basename(Inp_file)}\nWas: { HuSa(In_fi_sz)}\tIs: {HuSa(Ou_fi_sz)}\tSaved: {HuSa(In_fi_sz - Ou_fi_sz)} = {Ratio} % \n"
	if abs( Ratio ) > 90 :
		messa += " Huge Diference ?"
	print(messa)

	end_t = datetime.datetime.now()
	print( f'  -End  : {end_t:%H:%M:%S}\tTotal: {(end_t-str_t).total_seconds()}')

	return (1 + abs(In_fi_sz - Ou_fi_sz))
##>>============-------------------<  End  >------------------==============<<##


if __name__ == '__main__':
	#	DeBug = True

	cgitb.enable(format='text')

	str_t = datetime.datetime.now()
	print(f' +Start: {str_t:%H:%M:%S}')

	messa = __file__ + '-:'
	print(messa)
	time.sleep(1)


	'''
	print("-" * 70)
	if DeBug:
		print(f'    | ¯\_(%)_/¯ DeBug\n')
	File_Dict = Build_List(WFolder, File_extn)
	File_Dict = Sort_Parse(File_Dict, Srt_item=1, Srt_ordr=True)
	cnt = len (File_Dict)
	Fnum = 0
	Save = 0
	for each in File_Dict :
		Fnum += 1
		The_file  = each[0]
		F_sz      = each[1]
		ext       = each[2]

		messa = f'\n{The_file}\n{ordinal(Fnum)} of {cnt}, {ext}, {HuSa(F_sz)}'
		print(messa)

		try:
			all_good = Run_FFProbe( The_file )
			all_good = FFZa_Braino( The_file, all_good )
			all_good = Run_FFMpego( The_file, all_good )	# TODO: Parse the New name
			Save    -= Make_Matrix( The_file )
			Save    += Pars_Clenup( The_file, all_good )

# continue forces the loop to start at the next iteration
# pass will continue through the remainder or the loop body

		except ValueError as err:
			messa = f"{err}"
			if '| =>  _Skip_it' in messa :
				print(messa)
				Success_File.write(f'-: {The_file}\n')
				Save    -= Make_Matrix( The_file )
				pass
			else:
				print(messa)
				Exeptin_File.write(messa)
				continue

			print (f"  Saved {HuSa(Save)}")

		except Exception as err:
			messa += f'\n Exception {err.args}\nCopy & Delete {The_file}\n'
			print(messa)
			Move_or_Copy(The_file, Excepto, False)
			if DeBug :
#				print(f"Stack:\n{traceback.print_stack( limit=6 )}\n\n")
				print(f"Exec: \n{traceback.print_exc  ( limit=6 )}\n")
				print("\n", "=" * 40)
				input("Press Any Key to Continue")
			Exeptin_File.write(messa)
			continue

		else:
			Success_File.write(f'-: {The_file}\n')
			sys.stdout.flush()
		print (f"  Saved {HuSa(Save)}")
#	Do_it(File_Dict)

	Exeptin_File.close()
	Success_File.close()
	sys.stdout.flush()

	end_t = datetime.datetime.now()
	print(f' -End  : {end_t:%H:%M:%S}\tTotal: {end_t-str_t}')
	input('All Done')
	exit()
##>>============-------------------<  End  >------------------==============<<##
