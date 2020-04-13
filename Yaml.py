# -*- coding: utf-8 -*-
#!/usr/bin/python3
__author__ = 'GeoHaZen'
'''
@author: 	  GeoHaZen
# XXX KISS
# XXX:ToDo: multiple languages
'''
import yaml

DeBug = False

# read Configurations
try:
	Yml_data = open("Trans_code_plus.yml", 'r')
	Yml_stru = yaml.safe_load(Yml_data)

except yaml.YAMLError as exc:
	message = f' Yaml read error {exc}'
	input(message)

else:
	if DeBug:
		print(Yml_stru)

	try:
		Excepto  = Yml_stru['Path']['Excepto']
		WFolder  = Yml_stru['Path']['WFolder']
		TmpF_Ex  = Yml_stru['Path']['Tmp_exte']
		MinF_sz  = Yml_stru['Path']['Min_fsize']

		Skip_typ = Yml_stru['Action']['Skip_type']

		Max_v_btr = Yml_stru['Video']['Max_v_btr']
		Max_frm_r = Yml_stru['Video']['Max_frm_r']
		Bl_and_Wh = Yml_stru['Video']['Bl_and_Wh']

		Max_a_btr = Yml_stru['Audio']['Max_a_btr']

		File_extn = Yml_stru['Extensi']

	except Exception as exc:
		message = f'Yaml read error '
		input(message)
	else:
		message = f'Yaml Done '
		print (message)
