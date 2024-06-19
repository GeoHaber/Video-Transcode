# Transcoding Update

This update supports almost all video formats including H.264, H.265/HEVC, MPEG, AVI, and more.

## Overview

The transcoding process involves several steps to ensure efficient and high-quality conversion of media files. Below is a detailed breakdown of each step:

### 1. Directory Scanning

- **Description:** Scans the directory specified by "WFolder" to create a list of files.
- **Information Gathered:** Size, extension, and date of each file.

### 2. File Parsing

- **Description:** Utilizes Ffparse to parse each file.
- **Information Extracted:** Detailed information about video, audio, and subtitles.

### 3. Conversion Check

- **Description:** Determines if the file requires conversion based on the extracted information.

### 4. FFMPEG Command Generation

- **Description:** Uses the parsed data to guide FFZa_Brain in generating the appropriate Ffmpeg commands for processing.
- **Output:** Proper Ffmpeg commands for audio and video transcoding, subtitle copying, or extraction.

### 5. Transcoding

- **Description:** Executes the Ffmpeg commands to transcode audio and video, while copying or extracting subtitles as needed.

### 6. Cleanup

- **Description:** Performs necessary cleanup tasks after the transcoding process is completed.

### 7. Reporting

- **Description:** Generates a report of the results, providing details about the transcoding process and its outcome.

## Enjoy

Enjoy the enhanced capabilities and streamlined process for transcoding your media files with this update.

---

*Feel free to contribute or provide feedback on the update. Your input helps us improve the transcoding process even further.*
