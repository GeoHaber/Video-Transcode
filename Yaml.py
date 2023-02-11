# -*- coding: utf-8 -*-

import sys
import yaml

de_bug = False
yaml_f_loc = 'Trans_code.yml'

console_encoding = sys.getfilesystemencoding() # or 'utf-8'
print ("Console encoding = ",console_encoding)

# XXX read the yaml_f_loc file
try:
	with open(yaml_f_loc, 'r') as Yml_file :
		Yml_Data = yaml.safe_load(Yml_file)
		if de_bug :
			#	print('\n'.join(Yml_Data))
			for key, value in Yml_Data.items():
				#print ( len(value))
				print(f"\n{key}:")
				for ky, va in value.items() :
					print(f"\t{ky:<18}= {va}" )
except yaml.YAMLError as e:
	message = f' Yaml read error {e}'
	input(message)

try:
	Not_valid = Yml_Data['Glob']['junk_nm']
	Excepto   = Yml_Data['Path']['Excepto']
	WFolder   = Yml_Data['Path']['WFolder']
	TmpF_Ex   = Yml_Data['Path']['Tmp_exte']
	MinF_sz   = Yml_Data['Path']['Min_fsize']

	Skip_typ  = Yml_Data['Action']['Skip_typ']
	Skip_key  = Yml_Data['Action']['Skip_key']

	File_extn = Yml_Data['Video']['Extensi']
	Max_v_btr = Yml_Data['Video']['Max_v_btr']
	Max_frm_r = Yml_Data['Video']['Max_frm_r']
	Bl_and_Wh = Yml_Data['Video']['Bl_and_Wh']
	Video_crf = Yml_Data['Video']['crf-25']

#	Met_titil = Yml_Data['Metadata']['title']
#	Met_copyr = Yml_Data['Metadata']['copyright']
#	Met_comnt = Yml_Data['Metadata']['comment']
#	Met_authr = Yml_Data['Metadata']['author']

	Max_a_btr = Yml_Data['Audio']['Max_a_btr']

	Keep_langua = Yml_Data['Language']['Keep']
	Default_lng = Yml_Data['Language']['Default']

except Exception as ex:
	message = f'{yaml_f_loc} Ecception {ex} '
	input(message)
	raise Exception

print (f'File: {yaml_f_loc} Parsed')
