# -*- coding: utf-8 -*-

import sys
import yaml

de_bug = False
yaml_f_loc = 'Trans_code.yml'

console_encoding = sys.getfilesystemencoding() or 'utf-8'
print ("Console encoding = ",console_encoding)

# XXX read the yaml_f_loc file
try:
    with open(yaml_f_loc, 'r') as Yml_file:
        Yml_Data = yaml.safe_load(Yml_file)

        if de_bug:
            for key, value in Yml_Data.items():
                print(f"\n{key}:")
                for ky, va in value.items():
                    print(f"\t{ky:<18}= {va}")
except yaml.YAMLError as e:
    message = f' Yaml read error {e}'
    input(message)

# Use .get method to access values with default values
Glob = Yml_Data.get('Glob', {})
Path = Yml_Data.get('Path', {})
Action = Yml_Data.get('Action', {})
Video = Yml_Data.get('Video', {})
Audio = Yml_Data.get('Audio', {})
Language = Yml_Data.get('Language', {})

Not_valid = Glob.get('junk_nm', None)

Excepto = Path.get('Excepto', None)
WFolder = Path.get('WFolder', None)
TmpF_Ex = Path.get('Tmp_exte', None)
MinF_sz = Path.get('Min_fsize', None)

Skip_typ = Action.get('Skip_typ', None)
Skip_key = Action.get('Skip_key', None)

File_extn = Video.get('Extensi', None)
Max_v_btr = Video.get('Max_v_btr', None)
Max_frm_r = Video.get('Max_frm_r', None)
Bl_and_Wh = Video.get('Bl_and_Wh', None)
Video_crf = Video.get('crf-25', None)

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

print (f'Parsed File: {yaml_f_loc}')

'''
import sys
import yaml

de_bug = False
yaml_f_loc = 'Trans_code.yml'


# XXX read the yaml_f_loc file
try:
    with open(yaml_f_loc, 'r') as Yml_file:
        Yml_Data = yaml.safe_load(Yml_file)

        if de_bug:
            for key, value in Yml_Data.items():
                print(f"\n{key}:")
                for ky, va in value.items():
                    print(f"\t{ky:<18}= {va}")
except yaml.YAMLError as e:
    message = f' Yaml read error {e}'
    input(message)

# Use .get method to access values with default values
Glob = Yml_Data.get('Glob', {})
Path = Yml_Data.get('Path', {})
Action = Yml_Data.get('Action', {})
Video = Yml_Data.get('Video', {})
Audio = Yml_Data.get('Audio', {})
Language = Yml_Data.get('Language', {})

Not_valid = Glob.get('junk_nm', None)
Excepto = Path.get('Excepto', None)
WFolder = Path.get('WFolder', None)
TmpF_Ex = Path.get('Tmp_exte', None)
MinF_sz = Path.get('Min_fsize', None)

Skip_typ = Action.get('Skip_typ', None)
Skip_key = Action.get('Skip_key', None)

File_extn = Video.get('Extensi', None)
Max_v_btr = Video.get('Max_v_btr', None)
Max_frm_r = Video.get('Max_frm_r', None)
Bl_and_Wh = Video.get('Bl_and_Wh', None)
Video_crf = Video.get('crf-25', None)

Max_a_btr = Audio.get('Max_a_btr', None)

Keep_langua = Language.get('Keep', None)
Default_lng = Language.get('Default', None)

print(f'Parsed File: {yaml_f_loc}')


import yaml
import json

# Define the file paths for the YAML and JSON files
yaml_file_path = 'Trans_code.yml'

json_file_path = '_Trans_code.json'

# Read the YAML file
with open(yaml_file_path, 'r') as yaml_file:
    yaml_data = yaml.safe_load(yaml_file)

# Convert the YAML data to JSON and write it to the JSON file
with open(json_file_path, 'w') as json_file:
    json.dump(yaml_data, json_file, indent=4)

print(f'YAML file "{yaml_file_path}" has been converted to JSON file "{json_file_path}".')
'''
