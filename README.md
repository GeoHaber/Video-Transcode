# Transcoding
#H264 #H265 #HEVC #MPEG transcode

The processing is done in a few steps:

1) Scan a directory pointed to by "Folder."

While there are unprocessed files

  2) Check if there is a lock file on that file or folder

  3) Use FFparse to parse the file extract all info about Video, Audio, Subtitle etc.

  4) Use the parsed results to figure out what needs to be done FFZa_Brain to generate FFMpeg commands

  5) Use the commands generated to FFmpeg Transcode both Audio and Video while extracting the subtitles

  6) Cleanup

 Report results
