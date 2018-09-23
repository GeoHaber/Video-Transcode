# Transcoding
#H264 #H265 #HEVC #MPEG transcoding

The proccesing is done in a few steps

1) Scan a directory pointed to by "Folder"

While there are files unproccesed
  2) Check if there is a lock file on that folder
  3) Use FFparse to parse the vide file
  4) Use the parse results to figure out what needs to be done
  5) Use the comands generated to FFmpeg Transcode both Audi and Video while extracting the subtitels
  6) Cleanup
 Report results
 

