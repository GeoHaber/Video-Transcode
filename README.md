#Transcoding
#H264 #H265 #HEVC #MPEG #transcode

The processing is done in a few steps:

1) Scan a directory pointed to by "Folder" to build a list of files, while extracting some basic Info like size extension, date, etc ....

While there are unprocessed files do :

  2) Check if there is a lock file on that file or folder

  3) Use Ffparse to parse the file, extract all Info about Video, Audio, Subtitle, etc.

  4) Use the parsed results to figure out what needs to be done by FFZa_Brain to generate the proper Ffmpeg commands

  5) Use the Ffmpeg commands generated to transcode both Audio and Video while copying or extracting the subtitles

  6) Cleanup

 Report results
