import os
import re
import sys
import json
import time # <-- Import the time module for the delay
import random
import shutil
import subprocess
import subliminal

from pathlib import Path
from babelfish import Language
from subliminal.exceptions import GuessingError

# -------------------------------------------------------------------
# CONFIGURATION - EDIT THESE VALUES
# -------------------------------------------------------------------
Scan_Dirs = [r"F:\Media\TV"]
# Scan_Dirs = [r"F:\Media\Movie"]

LANGUAGES_TO_FIND = ['eng', 'rom', 'fra', 'heb']

EXCLUDE_KEYWORDS = [
	'interview', 'making of', 'extra', 'blooper', 'deleted scene',
	'alternate take', 'anatomy of', 'teaser', 'trailer', 'featurette',
	'behind the scenes', 'gallery', 'vfx reel', 'compilation',
	'inside the episode', 'menu art', 'soundtrack', 'profile', 'faq'
]
# -------------------------------------------------------------------

FFPROBE_PATH = shutil.which("ffprobe")

def get_embedded_subtitles(video_path: Path) -> set[str]:
	"""Uses ffprobe to check for embedded subtitle streams."""
	if not FFPROBE_PATH:
		return set()
	command = [
		FFPROBE_PATH, "-v", "error", "-print_format", "json",
		"-show_streams", "-select_streams", "s", str(video_path)
	]
	try:
		result = subprocess.run(command, capture_output=True, text=True, check=True, encoding='utf-8')
		data = json.loads(result.stdout)
		return {stream.get("tags", {}).get("language", "und") for stream in data.get("streams", [])}
	except Exception:
		return set()

# --- NEW FUNCTION TO FIND LOCAL SUBTITLE FILES ---
def get_external_subtitles(video_path: Path) -> set[str]:
	"""Scans for external subtitle files (e.g., .srt, .ass) in the same directory."""
	found_languages = set()
	# Search for any file that starts with the video's name and ends with a common subtitle extension
	for sub_path in video_path.parent.glob(f'{video_path.stem}*'):
		if sub_path.suffix.lower() in ['.srt', '.ass', '.sub']:
			# The language code is usually between the first and last dot
			parts = sub_path.stem.split('.')
			if len(parts) > 1:
				lang_code = parts[-1]
				# Use babelfish to validate and normalize the language code (handles 'en' vs 'eng')
				try:
					lang = Language.fromietf(lang_code)
					found_languages.add(lang.alpha3)
				except ValueError:
					continue # Ignore files with non-language codes like 'forced'
	return found_languages

def filter_and_scan_videos(root_dirs: list[str]) -> list:
	# ... This function remains the same as the previous version ...
	video_extensions = {".mp4", ".mkv", ".avi", ".mov", ".wmv", ".mpeg", ".mpg", ".flv"}
	video_objects = []
	skipped_count = 0
	total_files = 0
	spinner_chars = ['|', '/', '-', '\\']
	i = 0
	try:
		terminal_width = os.get_terminal_size().columns
	except OSError:
		terminal_width = 80
	print("Phase 1: Scanning for videos...")
	for root_dir_str in root_dirs:
		root_dir = Path(root_dir_str)
		if not root_dir.is_dir():
			print(f"\nWARNING: Directory not found, skipping: {root_dir_str}")
			continue
		for root, _, files in os.walk(root_dir):
			total_files += len(files)
			spinner_char = spinner_chars[i % len(spinner_chars)]
			relative_path = str(Path(root).relative_to(root_dir.parent))
			prefix = f' {spinner_char} Scanning: .\\'
			max_path_len = terminal_width - len(prefix) - 1
			if len(relative_path) > max_path_len:
				relative_path = "..." + relative_path[-(max_path_len - 3):]
			line_content = f'{prefix}{relative_path}'
			sys.stdout.write(' ' * (terminal_width - 1) + '\r')
			sys.stdout.write(f'{line_content}\r')
			sys.stdout.flush()
			i += 1
			for filename in files:
				file_path = Path(root) / filename
				if file_path.suffix.lower() in video_extensions:
					filename_lower = filename.lower()
					if any(keyword in filename_lower for keyword in EXCLUDE_KEYWORDS):
						skipped_count += 1
						continue
					try:
						video = subliminal.Video.fromname(str(file_path))
						video_objects.append(video)
					except (GuessingError, OSError):
						skipped_count += 1
						pass
	print(' ' * (terminal_width - 1) + '\r')
	print("Scan complete.")
	print(f"Found {total_files} total files, skipped {skipped_count} (extras/unparsable).")
	print(f"Ready to process {len(video_objects)} standard movies/episodes.\n")
	return video_objects

def main():
	"""Main function to scan and download subtitles."""
	print("🎬 Subtitle Fetcher Initialized")
	print("-" * 30)

	if not FFPROBE_PATH:
		print("WARNING: ffprobe not found. Cannot check for existing embedded subtitles.")

	desired_languages = {Language(lang) for lang in LANGUAGES_TO_FIND}
	print(f"Seeking subtitles for languages: {', '.join(LANGUAGES_TO_FIND)}")

	videos = filter_and_scan_videos(Scan_Dirs)
	total_videos = len(videos)
	if total_videos > 0:
		print("--- Phase 2: Processing Files ---")

	for i, video in enumerate(videos, 1):
		print(f"[{i}/{total_videos}] Processing: {video.name}")
		video_path = Path(video.name)

		# --- MODIFIED LOGIC: Check both embedded AND external subtitles ---
		embedded_langs = get_embedded_subtitles(video_path)
		external_langs = get_external_subtitles(video_path)

		all_existing_langs = embedded_langs.union(external_langs)

		if all_existing_langs:
			print(f"  - Found existing subtitles: {', '.join(all_existing_langs)}")

		missing_languages = {lang for lang in desired_languages if lang.alpha3 not in all_existing_langs}
		# --- END OF MODIFICATION ---

		if not missing_languages:
			print("  - ✅ All desired languages are present. Skipping.\n")
			# Add the rate-limiting delay here as well
			delay = random.uniform(0.3, 1.0)
			time.sleep(delay)
			continue

		print(f"  - 💬 Searching for missing languages: {', '.join(l.alpha3 for l in missing_languages)}")
		try:
			downloaded_subtitles = subliminal.download_best_subtitles([video], missing_languages)

			if downloaded_subtitles.get(video):
				try:
					subliminal.save_subtitles(video, downloaded_subtitles[video])
					for sub in downloaded_subtitles[video]:
						extension = getattr(sub, 'extension', 'srt')
						subtitle_path = Path(video.name).with_suffix(f'.{sub.language.alpha2}.{extension}')
						if subtitle_path.exists():
							print(f"  - ✅ Verified: Saved {sub.language.alpha3} sub from [{sub.provider_name}] to: {subtitle_path.name}")
						else:
							print(f"  - 🔥 Failed: {sub.language.alpha3} sub from [{sub.provider_name}] reported success but did not save.")
				except Exception as e:
					print(f"  - 💥 Failed to save subtitles: {e}")
			else:
				print("  - ❌ No suitable subtitles were found online.")

		except Exception as e:
			print(f"  - 💥 An error occurred during download: {e}")

		# --- MODIFICATION: Generate and print the actual random delay ---
		delay = random.uniform(0.5, 3.0)
		print(f"  - Pausing for {delay:.1f} seconds to avoid rate-limiting...")
		time.sleep(delay)


		print("") # Newline for readability

	print("-" * 30)
	print("All processing complete.")

if __name__ == "__main__":
	main()
