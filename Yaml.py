# -*- coding: utf-8 -*-
#!/usr/bin/python3
__author__ = 'GeHab'
'''
@author: 	  GeHab
# XXX KISS
# XXX:ToDo: multiple languages
'''
import yaml

DeBug = False

Config = 'Trans_code.yml'
# XXX read the Config file
try:
	Yml_file = open(Config, 'r')
	Yml_Data = yaml.safe_load(Yml_file)
	if DeBug:
		print(Yml_Data)

except yaml.YAMLError as exc:
	message = f' Yaml read error {exc}'
	input(message)
try:
	Excepto  = Yml_Data['Path']['Excepto']
	WFolder  = Yml_Data['Path']['WFolder']
	TmpF_Ex  = Yml_Data['Path']['Tmp_exte']
	MinF_sz  = Yml_Data['Path']['Min_fsize']

	Skip_typ = Yml_Data['Action']['Skip_type']

	Max_v_btr = Yml_Data['Video']['Max_v_btr']
	Max_frm_r = Yml_Data['Video']['Max_frm_r']
	Bl_and_Wh = Yml_Data['Video']['Bl_and_Wh']

	Max_a_btr = Yml_Data['Audio']['Max_a_btr']

	File_extn = Yml_Data['Extensi']

except Exception as ex:
	message = f'{Config} Ecception {ex.args} '
	input(message)
	raise Exception

message = f'{Config} Parsed'
print (message)
