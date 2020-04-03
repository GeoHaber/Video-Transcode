# -*- coding: utf-8 -*-
#!/usr/bin/python3
__author__ = 'GeoHaZen'
'''
@author: 	  GeoHaZen
# XXX KISS
'''
import os
import re
import sys
import yaml
import json
import cgitb
import shutil
import random
import datetime
import traceback
import subprocess
from My_Utils import *

DeBug = False

Vi_Dur = '30:00'

This_File = sys.argv[0].strip('.py')
Log_File = This_File + '_run.log'
Bad_Files = This_File + '_bad.txt'
Good_Files = This_File + '_good.txt'

try:
    stream = open("config.yml", 'r')
    cfg = yaml.safe_load(stream)
except yaml.YAMLError as exc:
    message = f' Yaml read error {exc}'
    input(message)

else:
    try:
        path = cfg['Path']['fmpg_bin']
        ffmpeg = os.path.join(path, "ffmpeg.exe")
        ffprobe = os.path.join(path, "ffprobe.exe")

        Vdo_ext = cfg['Ext']
        Max_v_btr = cfg['Path']['Max_v_btr']
        Max_a_btr = cfg['Path']['Max_a_btr']
        Max_frm_rt = cfg['Path']['Max_frm_rt']
#		Tmp_F_Ext	= cfg['Path']['Tmp_F_Ext']
        Excepto = cfg['Path']['Excepto']
        Folder = cfg['Path']['Folder']
        Min_fsize = cfg['Path']['Min_fsize']
    except Exception as exc:
        message = f' Yaml read error {exc}'
        input(message)

Tmp_F_Ext = '.mp4'
Folder		= 'C:\\Users\\Geo\\Desktop\\downloads'
Folder		= 'E:\\Media\\TV'
Folder = 'C:\\Users\\Geo\\Desktop\\_2Conv'

##>>============-------------------<  End  >------------------==============<<##


def Move_Del_File(src, dst, DeBug=False):
    '''
    If Debug then files are NOT deleted, only copied
    '''
    message = sys._getframe().f_code.co_name + '-:'
    try:
        if os.path.isdir(src) and os.path.isdir(dst):
            shutil.copytree(src, dst)
        else:
            shutil.copy2(src, dst)
            if not DeBug:
                os.remove(src)
            else:
                print(f" ! Placebo did NOT delete: {src}")
                time.sleep(1)
    except OSError as e:  # if failed, report it back to the user ##
        message += f"\n!Error: Src: {src} -> Dst: {dst}\n{e}"
        if DeBug:
            input(message)
        raise Exception(message)
    else:
        return True
##>>============-------------------<  End  >------------------==============<<##


def Create_File(dst, msge='', times=1, DeBug=False):
    message = sys._getframe().f_code.co_name + '-:'
    try:
        Cre_lock = open(dst, "w", encoding="utf-8")
        if Cre_lock:
            Cre_lock.write(msge * times)
            Cre_lock.flush()
    except OSError as e:  # if failed, report it back to the user ##
        message += f"\n!Error: {dst}\n{e.filename}\n{e.strerror}\n"
        if DeBug:
            input(message)
        raise Exception(message)
    return True
##>>============-------------------<  End  >------------------==============<<##


def Sanitize_file(root, one_file, extens):
    message = sys._getframe().f_code.co_name + '-:'

    fi_path = os.path.normpath(os.path.join(root, one_file))
    fi_size = os.path.getsize(fi_path)
    fi_info = os.stat(fi_path)
    year_made = Parse_year(fi_path)
    clean = re.sub('[_]', ' ', one_file)  # XXX TBD More clenup

# XXX  |[0] Extension |[1] Full Path |[2] File Size |[3] File Info |[4] Year Made XXX
    return extens, fi_path, fi_size, fi_info, year_made
##>>============-------------------<  End  >------------------==============<<##


def Parse_year(FileName):
    #	DeBug = True
    message = sys._getframe().f_code.co_name + '-:'
    if DeBug:
        print(message)
    try:
        yr = re.findall(r'[\[\(]?((?:19[4-9]|20[0-1])[0-9])[\]\)]?', FileName)
        if yr:
            va = sorted(yr, key=lambda pzd: int(pzd), reverse=True)
            za = int(va[0])
            if za > 2019 or za < 1930:
                za = 1954
            if DeBug:
                print(FileName, yr, len(yr), va, za)
        else:
            za = 1234
    except:
        za = 1
    return za
##>>============-------------------<  End  >------------------==============<<##


""" ======================= The real McCoy =================== """
# XXX: Sort_ord=True (Big First) Sort_loc = 2 => File Size: =4 => year_made


def Build_List(Top_dir, Ext_types, Sort_loc=2, Sort_ord=True):
    '''
    Create the list of Files to be proccesed
    '''
#	DeBug = True
    message = sys._getframe().f_code.co_name + '-:'

    cnt = 0
    queue_list = []

    print("=" * 60)
    start_time = datetime.datetime.now()
    value = HuSa(get_tree_size(Top_dir))
    print(f'Dir: {Top_dir}\tis: {value}')
    print(f'Start: {start_time:%H:%M:%S}')

    # a Directory ?
    if os.path.isdir(Top_dir):
        message += "\n Directory Scan :{}".format(Top_dir)
        print(message)
        for root, dirs, files in os.walk(Top_dir):
            for one_file in files:
                x, extens = os.path.splitext(one_file.lower())
                if extens in Ext_types:
                    Save_items = Sanitize_file(root, one_file, extens)
                    queue_list += [Save_items]
    # a File ?
    elif os.path.isfile(Top_dir):
        message += f" -> Single File Not a Directory: {Top_dir}"
        print(message)
        x,  extens = os.path.splitext(Top_dir.lower())
        if extens in Ext_types:
            Save_items = Sanitize_file(root, one_file, extens)
            queue_list += [Save_items]

# XXX: Sort based in item [2] = filesize defined by Sort_loc :)
    # XXX: sort defined by caller
    queue_list = sorted(
        queue_list, key=lambda Item: Item[Sort_loc], reverse=Sort_ord)
# XXX: https://wiki.python.org/moin/HowTo/Sorting

    end_time = datetime.datetime.now()
    Tot_time = end_time - start_time
    Tot_time = Tot_time.total_seconds()
    print(f'End  : {end_time:%H:%M:%S}\tTotal: {Tot_time}')
    return queue_list
##>>============-------------------<  End  >------------------==============<<##


def Skip_Files(File_dscrp, Min_fsize):
    #	DeBug = True
    '''
    Returns True if lock file is NOT
    '''
    message = sys._getframe().f_code.co_name + '-:'
# XXX  |[0] Extension |[1] Full Path |[2] File Size |[3] File Info |[4] Year Made XXX
    The_file = File_dscrp[1]
    Fname, ex = os.path.splitext(The_file)
    fi_size = File_dscrp[2]

# XXX: File does not exist :(
    if not os.path.exists(The_file):
        message += f"\n File Not Found {The_file}\n"
        print(message)
        Exeptions_File.write(message)
        Exeptions_File.flush()
        sys.stdout.flush()
        if DeBug:
            input("? Skip it ?")
        return False

# XXX Big enough to be video ?? # 256K should be One Mega byte 1048576
    elif fi_size < Min_fsize:
        message += f"\n To Small:| {HuSa(fi_size):9} | {The_file}\n"
        print(message)
        Exeptions_File.write(message)
        Exeptions_File.flush()
        sys.stdout.flush()
        if DeBug:
            input("? Skip it ?")
        return False

# XXX:  Ignore files that have been Locked (Procesed before)
    Lock_File = The_file + ".lock"

    if os.path.exists(Lock_File):
        message = f" < > _Skip_it : All is well file is Locked "
        Succesful_File.write(message)
        Succesful_File.flush()
        sys.stdout.flush()
        raise ValueError(message)

    return Lock_File
##>>============-------------------<  End  >------------------==============<<##


def Do_it(List_of_files, Excluded=''):
    #	DeBug = True

    message = sys._getframe().f_code.co_name + '-:'
    print("=" * 60)
    print(message)
    print(' Total of {} Files to Procces'. format(len(List_of_files)))

    if DeBug:
        print(f"Proccesing {List_of_files} one by one")

    if not List_of_files:
        raise ValueError(message, 'No files to procces')

    elif len(Excluded):
        message += " Excluding" + len(Excluded) + Excluded
        raise ValueError(message, 'Not Implemented yet :( ')
# XXX: TBD Skip those in the List

    queue_list = []
    Fnum = 0
    cnt = len(List_of_files)
    Saving = 0
    for File_dscrp in List_of_files:
        print("-" * 20)
        Fnum += 1
# XXX  |[0] Extension |[1] Full Path |[2] File Size |[3] File Info |[4] Year Made XXX
        extens = File_dscrp[0]
        The_file = File_dscrp[1]
        fi_size = File_dscrp[2]
        year_made = File_dscrp[4]
        message = f': {ordinal(Fnum)} of {cnt}, {HuSa(fi_size)}, {extens}, Year {year_made}\n: {The_file}'
        print(message)
        start_time = datetime.datetime.now()
        print(f' Start: {start_time:%H:%M:%S}')
        try:
            Lock_File = Skip_Files(File_dscrp, Min_fsize)
            if Lock_File:
                all_good = FFProbe_run(The_file)
                if all_good:
                    all_good = FFZa_Brain(The_file, all_good)
                    if all_good:
                        all_good = FFMpeg_run(The_file, all_good)
                        if all_good:
                            all_good = FFClean_up(The_file, all_good)
                            if all_good:
                                Saving += all_good
                                if DeBug:
                                    print("\nDo> Create_File Log ")
                                all_good = Create_File(Lock_File, message)
                                if all_good:
                                    cnt -= 1
                                    queue_list += [The_file]
                                    Succesful_File.write(The_file)
                                    Succesful_File.flush()
                                    if DeBug:
                                        # XXX should be One_descr after it was Modifyed XXX
                                        print('\nThe List ... \n{}'.format(
                                            json.dumps(queue_list, indent=2)))
                                    print("  Total Saved {}".format(
                                        HuSa(Saving)))
# XXX: Someting is Fish :O
                                else:
                                    print('Lock File Not Created')
                            else:
                                message += f'\n FFClean_up  ErRor :( = Copy & Del\n{The_file}'
                                print(message)
                                Exeptions_File.write(message)
    #							Create_File   ( Lock_File, message, 10, DeBug=True )
                                Move_Del_File(The_file, Excepto, DeBug=True)
                                sys.stdout.flush()
                        else:
                            message += f'\n FFMpeg_run  ErRor :( = Copy & Del\n{The_file}'
                            print(message)
                            Exeptions_File.write(message)
    #						Create_File   ( Lock_File, message, 10, DeBug=True )
                            Move_Del_File(The_file, Excepto, DeBug=True)
                            sys.stdout.flush()
                    else:
                        message += f'\n FFZa_Brain  ErRor :( = Copy & Del\n{The_file}'
                        print(message)
                        Exeptions_File.write(message)
    #					Create_File   ( Lock_File, message, 10, DeBug=True )
                        Move_Del_File(The_file, Excepto, DeBug=True)
                        sys.stdout.flush()
                else:
                    message += f'\n FFProb  ErRor :( = Copy & Del\n{The_file}'
                    print(message)
                    Exeptions_File.write(message)
    #				Create_File   ( Lock_File, message, 10 )
                    Move_Del_File(The_file, Excepto, DeBug=True)
                    sys.stdout.flush()
            else:
                pass
        except ValueError as err:
            message += f"\n\n ValueError Exception {err.args}"
            if '_Skip_it :' in message:
                #				print('_Skip_it :')
                #				time.sleep(3)
                Succesful_File.write(message)
#				Create_File ( Lock_File, message )
            else:
                Exeptions_File.write(message)
                message += f'\n Copy & Delete {The_file}'
                print(message)
                print(f"Stack:\n{traceback.print_stack( limit=5 )}\n")
                print(f"Exec:\n{ traceback.print_exc( limit=5 ) }\n")
                print("\n", "=" * 40)
#				Create_File   ( Lock_File, message, 100, DeBug=True )
                Move_Del_File(The_file, Excepto, DeBug=True)
                sys.stdout.flush()
        except Exception as e:
            message += f" WTF? General Exception {e}"
            print("\n", "-+" * 20)
            print(message)
            print(f"Stack:\n{traceback.print_stack( limit=5 )}\n")
            print(f"Exec:\n{ traceback.print_exc( limit=5 ) }\n")
            print("\n", "=" * 40)
#			Create_File   ( Lock_File, message, 100, DeBug=True )
            Move_Del_File(The_file, Excepto, DeBug=True)
            Exeptions_File.flush()
            Succesful_File.flush()
            sys.stdout.flush()
#			input ("## Bad Error :")

        Exeptions_File.flush()
        Succesful_File.flush()
        sys.stdout.flush()
        cnt -= 1
        queue_list += [The_file]

        end_time = datetime.datetime.now()
        Tot_time = end_time - start_time
        Tot_time = Tot_time.total_seconds()
        print(f' End  : {end_time:%H:%M:%S}\tTotal: {Tot_time}')
        print('=' * 20)
    return queue_list
##>>============-------------------<  End  >------------------==============<<##


def FFProbe_run(File_in, Execute=ffprobe):
    #	DeBug = True

    start_time = datetime.datetime.now()
    message = sys._getframe().f_code.co_name + '|:'
    print(f"  {message}\t\tStart: {start_time:%H:%M:%S}")

    if os.path.exists(File_in):
        file_size = os.path.getsize(File_in)
        message = f"\n{File_in}\t{HuSa(file_size)}\n"
        if DeBug:
            print(message)
    else:
        message += f"No Input file:( \n {File_in}"
        print(message)
        if DeBug:
            input('Now WTF?')
        return False

    Comand = [Execute,
              '-analyzeduration', '2000000000',
              '-probesize',       '2000000000',
              '-i', File_in,
              '-v', 'verbose',		# XXX quiet, panic, fatal, error, warning, info, verbose, debug, trace
              '-of', 'json',		# XXX default, csv, xml, flat, ini
              '-hide_banner',
              '-show_format',
              '-show_error',
              '-show_streams']

    try:
        ff_out = subprocess.run(Comand,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                universal_newlines=True,
                                encoding='utf-8')
    except subprocess.CalledProcessError as err:  # XXX: TBD Fix error in some rare cases
        message += f" FFProbe: CalledProcessError {err}"
        if DeBug:
            print(message), input('Next')
        raise Exception(message)
    else:
        out = ff_out.stdout
        err = ff_out.stderr
        bad = ff_out.returncode
        if bad:
            message += f"Oy vey {bad}\nIst mir {err}\n"
            if DeBug:
                print(message), input("Bad")
            raise ValueError(message)
        else:
            jlist = json.loads(out)
            if len(jlist) < 2:
                message += f"Json out to small\n{File_in}\n{jlist}"
                if DeBug:
                    print(message), input(" Jlist to small ")
                raise Exception(message)
        end_time = datetime.datetime.now()
        Tot_time = end_time - start_time
        Tot_time = Tot_time.total_seconds()
        print(f'   End  : {end_time:%H:%M:%S}\tTotal: {Tot_time}')
        return jlist
##>>============-------------------<  End  >------------------==============<<##


def FFZa_Brain(Ini_file, Meta_dta, verbose=False):
    global Vi_Dur  # make it global so we can reuse for fmpeg check ...
    global Tot_Frms
#	global DeBug
#	DeBug = True

    start_time = datetime.datetime.now()
    message = sys._getframe().f_code.co_name + '/:'
    print(f"  {message}\t\tStart: {start_time:%H:%M:%S}")

    Local_f_name = os.path.basename(Ini_file)

    Prst_all = []
    Au_strms = []
    Vi_strms = []
    Su_strms = []
    Dt_strms = []
    Xt_strms = []

# TODO Parse Format
    try:
        _mtdta = dict( nb_streams=int(0),
                       duration=float(0),
                       bit_rate=int(0),
                       size=float(0))
        Parse_from_to(Meta_dta['format'], _mtdta)
        Prst_all.append(_mtdta)
        if 'Pu_la' in _mtdta.values():
            message += f":O: Meta_dta has Pu_la\n"
            if 'Pu_la' in _mtdta['duration']:
                _mtdta['duration'] = '55.51'
            elif 'Pu_la' in _mtdta['bit_rate']:
                _mtdta['bit_rate'] = '50099'
            else:
                print(message)
                input('Meta WTF')
                raise ValueError(json.dumps(_mtdta, indent=2))
        _kdat = dict(	codec_type=''	)

        for rg in range(_mtdta['nb_streams']):
            Strm_X = Meta_dta['streams'][rg]
            key = Parse_from_to(Strm_X, _kdat)
# XXX: Is Video
            if key == 'video' 		:
                _vdata = dict(index=int(0),
                              codec_name='',
                              width=int(0),
                              height=int(0),
                              coded_width=int(0),
                              coded_height=int(0),
                              bit_rate=int(0),
                              avg_frame_rate='')
                Parse_from_to(Strm_X, _vdata)
                Prst_all.append(_vdata)
                Vi_strms.append(_vdata)  # procces all before building command
# XXX: Is Audio
            elif key == 'audio' 	:
                _adata = dict(	index=int(0),
                               codec_name='',
                               channels=int(0),
                               sample_rate=float(0),
                               bit_rate=int(0),
                               disposition={},
                               tags={})
                Parse_from_to(Strm_X, _adata)
                Prst_all.append(_adata)
                Au_strms.append(_adata)  # procces all before building command
# XXX: Is subtitle
            elif key == 'subtitle'	:
                _sdata = dict(	index=int(0),
                               codec_name='',
                               codec_type='',
                               duration=float(0),
                               disposition={},
                               tags={})
                Parse_from_to(Strm_X, _sdata)
                Prst_all.append(_sdata)
                Su_strms.append(_sdata)
# XXX: Is Data
            elif key == 'data'		:
                _ddata = dict(	index=int(0))
                Parse_from_to(Strm_X, _ddata)
                Prst_all.append(_ddata)
                Dt_strms.append(_ddata)
# XXX: Is attachment
            elif key == 'attachment'	:
                _atach = dict(	index=int(0))
                Parse_from_to(Strm_X, _atach)
                Prst_all.append(_atach)
                Xt_strms.append(_atach)
# XXX: Is WTF ?
            else:
                print("Key:\n",	  json.dumps(
                    key,      indent=2, sort_keys=False))
                print("Strm_X:\n",	  json.dumps(
                    Strm_X,   indent=2, sort_keys=False))
                print("Meta_dta:\n", json.dumps(
                    Meta_dta, indent=2, sort_keys=False))
                message += f' Cant Parse Streams WTF? \n{Local_f_name}\n'
                print(message)
                input('Next ')
                raise ValueError(message)

# XXX: Check it :)
        if len(Vi_strms) == 0:
            message = f'File \n{Local_f_name}\n!! Has no Video => Can\'t convert\n'
            if DeBug:
                print(message), input('Next ?')
            raise ValueError(message)
        if len(Au_strms) == 0:
            message = f'File:\n{Local_f_name}\n!! Has no Audio\n'
            if DeBug:
                print(message), input('Next ?')
#			raise  ValueError( message )
        if len(Su_strms) == 0:
            message = f'File:\n{Local_f_name}\n!! Has no Subtitle\n'
            if DeBug:
                print(message), input('Next ?')
#			raise  ValueError( message )
    #			print ( f" T: {repr( type( abu))}\t I= {i} D= {d}" )
        mins,  secs = divmod(int(_mtdta['duration']), 60)
        hours, mins = divmod(mins, 60)
        Vi_Dur = f'{hours:02d}:{mins:02d}:{secs:02d}'
        Vi_D = f'{hours:02d}h {mins:02d}m'  # {secs:02d}'
        frm_rate = float(Util_str_calc(_vdata['avg_frame_rate']))
        Tot_Frms = round(frm_rate * int(_mtdta['duration']))

# XXX: Container
# XXX: Print Banner
        message = f"    |< CT >|{Vi_D}| {_vdata['width']:^4}x{_vdata['height']:^4} |Tfr: {HuSa(Tot_Frms):6}|Vid: {len(Vi_strms)}|Aud: {len(Au_strms)}|Sub: {len(Su_strms)}|"
        print(message)

# XXX: Video
        if DeBug:
            input("VID !!")
        extra = ''
        ff_video = []
        if len(Vi_strms) == 0:
            print('    |<V:No>| Video Missing')
        else:
            for _vid in Vi_strms:
                if DeBug:
                    print(_vid.items()), time.sleep(2)
                if _vid['bit_rate'] == 'Pu_la':
                    # XXX approximation 80% video
                    _vi_btrt = int(float(_mtdta['bit_rate']) * 0.82)
                    extra += ' BitRate Estimate Pu_la'
                    if DeBug:
                        print(f"Pu_la Corection { _mtdta['bit_rate']}")
                else:
                    _vi_btrt = int(_vid['bit_rate'])
                if 'Pu_la' in _vid.values():
                    if DeBug > 1:
                        print(json.dumps(_vid, indent=2,
                                         sort_keys=False)), input('ZZ')

# XXX: Print Banner
                message = f"    |<V:{_vid['index']:2}>| {_vid['codec_name']:5} |Br: {HuSa(_vi_btrt):>7}|Fps: {frm_rate:>5}| {extra}"
                print(message)
                message = ''

                if _vid['codec_name'] == 'mjpeg':
                    continue

                zzz = '0:' + str(_vid['index'])
                ff_video.extend(['-map', zzz])

                if _vid['height'] > 1440:  # Scale to 1080p
                    ff_video.extend(
                        ['-vf', 'scale = -1:1440', '-c:v', 'libx265', '-crf', '25', '-preset', 'slow'])
                elif _vid['codec_name'] == 'hevc':
                    if _vi_btrt <= Max_v_btr * 1.1:  # XXX: 10% grace :D
                        ff_video.extend(['-c:v', 'copy'])
                    else:
                        print(f'{_vi_btrt} {Max_v_btr}')
                        ff_video.extend(
                            ['-c:v', 'libx265', '-preset', 'medium',   '-b:v', str(Max_v_btr)])
                else:
                    if _vid['height'] > 620:
                        ff_video.extend(
                            ['-c:v', 'libx265', '-crf', '25', '-preset', 'medium'])
                    elif _vid['height'] > 260:
                        ff_video.extend(
                            ['-c:v', 'libx265', '-crf', '27', '-preset', 'medium'])
                    else:
                        ff_video.extend(['-c:v', 'libx265',
                                         '-preset', 'fast'])

                if frm_rate > Max_frm_rt:
                    #					ff_video.extend( = [ '-r', '25' ] )
                    message = f"    ! FYI Frame rate convert {frm_rate} to 25"
                    print(message)
            if DeBug:
                print(ff_video)

# XXX: audio
        if DeBug:
            input("AUD !!")
        extra = ''
        ff_audio = []
        if len(Au_strms) == 0:
            print('    |<A:No>| Audio Missing')
            _au_code = 'nofucinkaudio'
            _au_btrt = 0
            if DeBug:
                input('Next ?')
        else:
            if DeBug > 1:
                print(json.dumps(Au_strms, indent=3, sort_keys=False))
            _disp = dict(	default=int(0),
                          dub=int(0),
                          lyrics=0,
                          karaoke=0,
                          forced=int(0),
                          clean_effects=0)
            for _aud in Au_strms:
                if DeBug:
                    print(_aud.items()),	time.sleep(2)
                _au_code = _aud['codec_name']
                if _aud['bit_rate'] == 'Pu_la':
                    # XXX:  aproximation
                    _au_btrt = int(_mtdta['bit_rate'] *
                                   0.05 * _aud['channels'])
                    extra += ' BitRate Estimate Pu_la'
                    if DeBug:
                        print(f"Pu_la Corection { _mtdta['bit_rate']}")
                else:
                    _au_btrt = int(_aud['bit_rate']) / int(_aud['channels'])
                Parse_from_to(_aud['disposition'], _disp)
                if 'Pu_la' in _aud.values():
                    if DeBug > 1:
                        print(json.dumps(_aud, indent=2,
                                         sort_keys=False)), input('ZZ')
                    extra = 'has Pu_la'
                _lng = dict(language='')
                if _aud['tags'] == 'Pu_la':
                    _lng['language'] = 'wtf'
                else:
                    Parse_from_to(_aud['tags'], _lng)
# XXX: Print Banner
                message = f"    |<A:{_aud['index']:2}>| {_au_code:5} |Br: {HuSa(_au_btrt):>7}|Fq:   {HuSa(_aud['sample_rate']):>6}|Ch: {_aud['channels']}|{_lng['language']}|{_disp['default']}| {extra}"

                zzz = '0:' + str(_aud['index'])
                ff_audio.extend(['-map', zzz])
                zzz = '-c:a:' + str(_aud['index'])

                if _aud['codec_name'] in ('aac', 'opus', 'vorbis'):
                    if _au_btrt <= Max_a_btr:  # and _aud['channels'] < 3 :
                        ff_audio.extend([zzz, 'copy'])
                    else:
                        ff_audio.extend([zzz, 'libvorbis', '-q:a', '6'])
                else:
                    ff_audio.extend([zzz, 'libvorbis', '-q:a', '7'])
            if _lng['language'] == 'eng' and _disp['default'] == 1:
                message += " * Yey *"
            print(message)
            if DeBug:
                print(ff_audio)

# XXX subtitle
        if DeBug:
            input("SUB !!")
        ff_subtl = []
        if len(Su_strms) == 0:
            print('    |<S:No>| Subtitle Missing')
            if DeBug:
                input('Next ?')
        else:
            extra = ''
            for _sub in Su_strms:
                if DeBug > 1:
                    print(f'Sub : {_sub}')
                if 'Pu_la' in _sub.values():
                    if DeBug > 1:
                        print(json.dumps(_sub, indent=2,
                                         sort_keys=False)), input('ZZ')
                    extra = 'has Pu_la'

                _lng = dict(language='')
                Parse_from_to(_sub['tags'], _lng)
                if 'Pu_la' in _lng['language']:
                    _lng['language'] = 'wtf'

                zzz = '0:' + str(_sub['index'])
                ff_subtl.extend(['-map', zzz])
                zzz = '-c:s:' + str(_sub['index'])

                if Tmp_F_Ext == '.mp4':
                    Sub_fi_name = Ini_file + '_' + \
                        str(_lng['language']) + '_' + \
                        str(_sub['index']) + '.srt'
                    extra = '* Xtract *'
                    ff_subtl.extend([zzz, 'mov_text', Sub_fi_name])
# XXX: Print Banner
                message = f"    |<S:{_sub['index']:2}>|{_sub['codec_name']:5}|{_sub['codec_type']:^10}|{_lng['language']:3}| {extra}"

                if _sub['codec_name'] in ('hdmv_pgs_subtitle', 'dvd_subtitle'):
                    #					message += f" : Skip : {_sub['codec_name']}"
                    #					print (message)
                    if DeBug:
                        input('Next Sub ?')
                        continue
                else:
                    #					if 'eng' or 'rum' or 'fre' or 'wtf' in _lng['language'] or True :
                    if _lng['language'] in ('eng', 'rum', 'fre', 'wtf'):
                        print(message)
                        ff_subtl.extend( [ zzz, 'mov_text' ] )
                    else:
                        message += f"Skipo :( {_sub['codec_name']}"
                        print(message)
                        if DeBug:
                            input('Next Sub ?')
                print(message)
                if DeBug:
                    print(ff_subtl)
                    #-c:s mov_text -metadata:s:s:0 language=eng
            ff_subtl = ['-map 0:s?', '-c:s', 'mov_text']

        if DeBug:
            for pu in Prst_all:
                if 'Pu_la' in pu.values():
                    print(f'    | ¯\_(%)_/¯ some Pu_la\n{pu}')

    except Exception as e:
        message = f"FFZa_Brain: Exception => {e}\n"
        print(message)
        time.sleep(1)
        input(e)
        raise Exception(message)
    else:
        FFM_cmnd = ff_video + ff_audio + ff_subtl

        x, extens = os.path.splitext(Local_f_name)
        if extens.lower() in Tmp_F_Ext.lower() and _vid['codec_name'] == 'hevc' and _au_code in ('aac', 'opus', 'vorbis', 'nofucinkaudio'):
            # XXX: 10% Grace
            if _vi_btrt <= Max_v_btr * 1.1 and _vid['height'] <= 1440 and _au_btrt <= Max_a_btr * 1.1:
                message = f"     < V= {_vid['codec_name']} A= {_au_code} > _Skip_it : All is well "
                print(message)
#                raise ValueError(message)
        '''
		# XXX: Not the case

		if _vid['codec_name'] != 'hevc'  :
				message = f"   <| Vid= {_vid['codec_name']} |Aud= {_au_code} | Should Convert {Local_f_name}\n"
				print( message )
				time.sleep(1)
				raise ValueError( message )
		'''

        end_time = datetime.datetime.now()
        Tot_time = end_time - start_time
        Tot_time = Tot_time.total_seconds()
        print(f'   End  : {end_time:%H:%M:%S}\tTotal: {Tot_time}')
        if DeBug:
            print(FFM_cmnd)
        return FFM_cmnd
##>>============-------------------<  End  >------------------==============<<##


def FFMpeg_run(Fmpg_in_file, Za_br_com, Execute=ffmpeg):
    #	DeBug = True
    global Tot_Frms

    start_time = datetime.datetime.now()
    message = sys._getframe().f_code.co_name + '-:'
    print(f"  {message}\t\tStart: {start_time:%H:%M:%S}")

# XXX FileName for the Title ...
    Sh_fil_name = os.path.basename(Fmpg_in_file).title()
    Sh_fil_name, xt = os.path.splitext(Sh_fil_name)
    Sh_fil_name += Tmp_F_Ext

    Fmpg_ou_file = '_' + Random_String(11) + Tmp_F_Ext

    Title = 'title=\" ' + Sh_fil_name + " x265 Encoded By: " + __author__ + " Master "

    ff_head = [Execute, '-i', Fmpg_in_file, '-hide_banner']
    ff_tail = ['-metadata', Title, '-movflags', '+faststart',
               '-fflags', 'genpts', '-y', Fmpg_ou_file]

    Cmd = ff_head + Za_br_com + ff_tail

    loc = 0
    symbs = '|/-+\\'
    try:
        if DeBug or True :
            print("    |>-", Cmd)
            input("Ready to Do it? ")
            ff_out = subprocess.run(Cmd,
                                    universal_newlines=True,
                                    encoding='utf-8')
            errcode = ff_out.returncode
            if errcode:
                message += f" ErRor: ErRorde {errcode}"
                print(message)
                input('Next')
                raise Exception('$hit ', message)
            input("Are we Done?")
            return Fmpg_ou_file
        else:
            print("    |>=", Cmd[4:-8])  # XXX:  Skip First 4 and Last 6
            ff_out = subprocess.Popen(Cmd,
                                      stdout=subprocess.PIPE,
                                      stderr=subprocess.STDOUT,
                                      universal_newlines=True,
                                      encoding='utf-8')
    except subprocess.CalledProcessError as err:
        ff_out.kill()
        message += f" ErRor: {err} CalledProcessError :"
        if DeBug:
            print(message), input('Next')
        if os.path.exists(Fmpg_ou_file):
            os.remove(Fmpg_ou_file)
        raise Exception('$hit ', message)
    except Exception as e:
        ff_out.kill()
        message += f" ErRor: Exception {e}:"
        if DeBug:
            print(message), input('Next')
        if os.path.exists(Fmpg_ou_file):
            os.remove(Fmpg_ou_file)
        raise Exception('$hit ', message)
    else:
        while ff_out.poll() is None:
            lineo = ff_out.stdout.readline()
            stderri = ff_out.stderr
            if DeBug:
                print(f"<{lineo}>")
            errcode = ff_out.returncode
            if errcode:
                message += f" ErRor: ErRorde {errcode} stderr {stderri}:"
                print(message)
                raise ValueError('$hit ', message)
            elif 'frame=' in lineo:
                Prog_cal(lineo, symbs[loc])
                loc += 1
                if loc == len(symbs):
                    loc = 0
            elif 'global headers:' and "muxing overhead:" in lineo:
                print(f'\n|>+<| {lineo}')

    end_time = datetime.datetime.now()
    Tot_time = end_time - start_time
    Tot_time = Tot_time.total_seconds()
    print(f'   End  : {end_time:%H:%M:%S}\tTotal: {Tot_time}')

    if not os.path.exists(Fmpg_ou_file):
        message += ' No Out File Error '
        print(message)
        raise Exception('$hit ', message)
    elif os.path.getsize(Fmpg_ou_file) < Min_fsize:
        message += ' File Size Error'
        print(message)
        os.remove(Fmpg_ou_file)
        raise Exception('$hit ', message)
    else:
        message += "   FFMpeg Done !!"

    print(message)
    return Fmpg_ou_file
##>>============-------------------<  End  >------------------==============<<##


if __name__=='__main__':
#	DeBug = True

    cgitb.enable(format='text')

    message = __file__ + '-:'
    print(message)

    start_time = datetime.datetime.now()
    print(f' Start: {start_time:%H:%M:%S}')

    sys.stdout = Tee(sys.stdout,	open(Log_File,   'w', encoding="utf-8"))
    Exeptions_File = open(Bad_Files,  'w', encoding="utf-8")
    Succesful_File = open(Good_Files, 'w', encoding="utf-8")

    if not Resource_Check(Folder):
        print("Aborting Not Enough resources")
        exit()

# XXX  |[0] Extension |[1] Full Path |[2] File Size |[3] File Info |[4] Year Made Sort = True => Largest First XXX
    Qlist_of_Files = Build_List(Folder, Vdo_ext, Sort_loc=2, Sort_ord=False)
    if DeBug:
        print(Qlist_of_Files)

    Do_it(Qlist_of_Files)

    Exeptions_File.close()
    Succesful_File.close()
    sys.stdout.flush()

    end_time = datetime.datetime.now()
    print(f' \tEnd  : {end_time:%H:%M:%S}\tTotal: {end_time-start_time}')
    input('All Done')
    exit()
##>>============-------------------<  End  >------------------==============<<##
