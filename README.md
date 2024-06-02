Transcoding Update

Supported almoust all video Formats: H.264, H.265/HEVC, MPEG, avi etc ...

The transcoding process involves several steps:

  Directory Scanning: Scan the directory specified by "WFolder" to create a list of files and gather basic information such as size, extension, and date.

  File Parsing: Utilize Ffparse to parse each file and extract detailed information about the video, audio, and subtitles.

  Conversion Check: Determine if the file requires conversion.

  FFMPEG Command Generation: Use the parsed data to guide FFZa_Brain in generating the appropriate Ffmpeg commands for processing.

  Transcoding: Execute the Ffmpeg commands to transcode audio and video, while copying or extracting subtitles as needed.

  Cleanup: Perform necessary cleanup tasks after transcoding.

  Reporting: Generate a report of the results.

Enjoy
