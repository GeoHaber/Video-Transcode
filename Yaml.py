# -*- coding: utf-8 -*-
import sys
import yaml

# It's better to avoid a global 'debug' variable here if FFMpeg.py has its own APP_CONFIG.debug_mode
# This debug flag would only control printing within Yaml.py during its own execution.
YAML_LOAD_DEBUG = False # Use a distinct name
YAML_FILE_LOCATION = 'Trans_code.yml'

console_encoding = sys.getfilesystemencoding() or 'utf-8'
print(f"Console encoding = {console_encoding}") # This prints when Yaml.py is imported

Yml_Data = {} # Initialize to an empty dict in case the file is missing or empty
try:
    with open(YAML_FILE_LOCATION, 'r', encoding='utf-8') as yml_file_stream:
        loaded_data = yaml.safe_load(yml_file_stream)
        if isinstance(loaded_data, dict): # Ensure we loaded a dictionary
            Yml_Data = loaded_data
        else:
            print(f"Warning: YAML file '{YAML_FILE_LOCATION}' is empty or not a dictionary. Using empty defaults.")

    if YAML_LOAD_DEBUG and Yml_Data: # Check if Yml_Data is not empty
        print(f"\n--- Content of '{YAML_FILE_LOCATION}' ---")
        for top_key, section_value in Yml_Data.items():
            print(f"\n[{top_key}]:")
            if isinstance(section_value, dict):
                for sub_key, val in section_value.items():
                    print(f"  {sub_key:<25} = {val}")
            else:
                print(f"  {section_value}")
        print("--- End of YAML Content ---")

except FileNotFoundError:
    print(f"Warning: YAML configuration file '{YAML_FILE_LOCATION}' not found. Script will use defaults for YAML-sourced values.")
except yaml.YAMLError as e:
    print(f"Warning: Error parsing YAML file '{YAML_FILE_LOCATION}': {e}. Script will use defaults for YAML-sourced values.")

# Define global variables for each top-level section from YAML
# FFMpeg.py will import these section dictionaries.
Glob = Yml_Data.get('Glob', {})
Path_section = Yml_Data.get('Path', {}) # Renamed to avoid conflict with pathlib.Path
Action = Yml_Data.get('Action', {})
Video = Yml_Data.get('Video', {})
Audio = Yml_Data.get('Audio', {})
Language = Yml_Data.get('Language', {})
Metadata = Yml_Data.get('Metadata', {})
FfMpegCom = Yml_Data.get('FfMpegCom', {}) # Exporting this in case it's needed later
GitHub = Yml_Data.get('GitHub', {})     # Exporting this in case it's needed later

# Define specific commonly used values as globals for convenience if your FFMpeg.py's
# load_script_config directly imports them.
# The refactored FFMpeg.py's load_script_config will primarily use the section dictionaries above.
Root =      Path_section.get('Root')
Excepto =   Path_section.get('Excepto')
TmpF_Ex =   Path_section.get('Tmp_exte')
MinF_sz =   Path_section.get('Min_fsize')
ffmpg_bin = Path_section.get('ffmpg_bin') # If you define this in your YAML Path section

Skip_typ =  Action.get('Skip_typ')
Skip_key =  Action.get('Skip_key')

File_extn = Video.get('Extensions')
Max_v_btr = Video.get('Max_v_btr')
Max_frm_r = Video.get('Max_frm_r')
Bl_and_Wh = Video.get('Bl_and_Wh')
Video_crf = Video.get('crf-25') # Key from your YAML

Audio_codec_name_from_yaml = Audio.get('codec_name') # Specific for audio codec, distinct name
Max_a_btr = Audio.get('Max_a_btr')

Keep_langua = Language.get('Keep')
Default_lng = Language.get('Default')

print(f"Parsed File: {YAML_FILE_LOCATION}")
