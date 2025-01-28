# -*- coding: utf-8 -*-

import sys
import yaml

debug = False  # Changed variable name for clarity
yaml_f_loc = 'Trans_code.yml'

console_encoding = sys.getfilesystemencoding() or 'utf-8'
print("Console encoding = ", console_encoding)

# XXX read the yaml_f_loc file
try:
	with open(yaml_f_loc, 'r', encoding='utf-8') as Yml_file:  # Added encoding explicitly
		Yml_Data = yaml.safe_load(Yml_file)

		if debug:
			for key, value in Yml_Data.items():
				print(f"\n{key}:")
				for ky, va in value.items():
					print(f"\t{ky:<18}= {va}")
except yaml.YAMLError as e:
	message = f' Yaml read error {e}'
	input(message)

# Use .get method to access values with default values
Glob =		Yml_Data.get('Glob', {})
Path =		Yml_Data.get('Path', {})
Action =	Yml_Data.get('Action', {})
Video =		Yml_Data.get('Video', {})
Audio =		Yml_Data.get('Audio', {})
Language =	Yml_Data.get('Language', {})

Not_valid =	Glob.get('junk_nm', None)

Excepto =	Path.get('Excepto', None)
Root =		Path.get('Root', None)
TmpF_Ex =	Path.get('Tmp_exte', None)
MinF_sz =	Path.get('Min_fsize', None)

Skip_typ =	Action.get('Skip_typ', None)
Skip_key =	Action.get('Skip_key', None)

File_extn =	Video.get('Extensions', None)  # Changed to 'Extensions'
Max_v_btr =	Video.get('Max_v_btr', None)
Max_frm_r =	Video.get('Max_frm_r', None)
Bl_and_Wh =	Video.get('Bl_and_Wh', None)
Video_crf =	Video.get('crf-25', None)

Max_a_btr =	Audio.get('Max_a_btr', None)

Keep_langua = Language.get('Keep', None)
Default_lng = Language.get('Default', None)

print(f'Parsed File: {yaml_f_loc}')
