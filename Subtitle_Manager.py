# -*- coding: utf-8 -*-
"""
Subtitle_Manager.py Rev 2.1
The Ultimate Subtitle Utility.

Features:
1. CLEAN: Detects and removes duplicate subtitles (using smart content hashing).
2. ENSURE-ENG: If English is missing, translates the best available foreign sub to English.
3. ADD-LANGS: Translates English (or source) to specified target languages.

Usage:
  Run from editor. Configure options in the "USER CONFIGURATION" section below.
"""

import os
import sys
import re
import json
import hashlib
import shutil
import subprocess as sp
from pathlib import Path
from types import SimpleNamespace
from typing import List, Dict, Tuple, Set, Optional
from collections import defaultdict
from dataclasses import dataclass

# Import project utilities
import Utils
from Utils import (
    WORK_DIR, RUN_TMP, FFMPEG, FFPROBE,
    REQUIRED_LIBS, ensure_libs,
    safe_print, print_lock,
    LLM_AVAILABLE, llm_query,
)

# Ensure standard required libraries
ensure_libs(REQUIRED_LIBS)

# Try importing AI libraries
try:
    import torch
    from transformers import MarianMTModel, MarianTokenizer
    AI_AVAILABLE = True
except ImportError:
    AI_AVAILABLE = False

# LLM availability is provided by Utils.LLM_AVAILABLE

try:
    from tkinter import Tk
    from tkinter.filedialog import askdirectory
    HAS_TKINTER = True
except ImportError:
    HAS_TKINTER = False

# =============================================================================
# USER CONFIGURATION
# =============================================================================

# Default Path to Process (File or Folder)
# If this path does not exist, the folder picker will open automatically.
DEFAULT_SCAN_DIR = str(WORK_DIR)

# Options
OPT_CLEAN        = False      # Remove duplicate subtitle streams
OPT_ENSURE_ENG   = True       # Translate to English if missing
OPT_ADD_LANGS    = ""         # Comma-separated list of langs to add (e.g. "fra,spa")
OPT_DRY_RUN      = False      # True = Don't modify files, just show what would happen

# AI / LLM Settings
OPT_USE_LLM      = True       # True = Use Local LLM; False = Use Helsinki-NLP (HuggingFace)

# =============================================================================
# Configuration & Constants
# =============================================================================

MODEL_CACHE_DIR = WORK_DIR / "AI_Models"
MODEL_CACHE_DIR.mkdir(exist_ok=True)

# Map 3-letter ISO codes (ffmpeg) to 2-letter ISO codes (Helsinki-NLP)
LANG_MAP = {
    "eng": "en", "fra": "fr", "spa": "es", "deu": "de", "ita": "it",
    "por": "pt", "rus": "ru", "jpn": "ja", "chi": "zh", "zho": "zh",
    "nld": "nl", "pol": "pl", "tur": "tr", "ara": "ar", "hin": "hi",
    "swe": "sv", "fin": "fi", "dan": "da", "nor": "no", "kor": "ko"
}

# Inverse map for converting back to 3-letter codes
ISO2_TO_ISO3 = {v: k for k, v in LANG_MAP.items()}


# =============================================================================
# Helper Classes
# =============================================================================

@dataclass
class SubtitleItem:
    index: int
    timestamp: str
    content: str

class SubtitleContent:
    """Helper to parse and normalize subtitle text."""
    
    @staticmethod
    def parse_srt(text: str) -> List[SubtitleItem]:
        items = []
        blocks = re.split(r'\n\s*\n', text.strip())
        for block in blocks:
            lines = block.strip().splitlines()
            if len(lines) >= 3:
                try:
                    idx = int(lines[0].strip())
                    time_str = lines[1].strip()
                    content = "\n".join(lines[2:])
                    items.append(SubtitleItem(idx, time_str, content))
                except Exception: continue
        return items

    @staticmethod
    def normalize(raw_text: str) -> str:
        """
        Strips timestamps, tags, indices, and formatting to get pure dialogue.
        This aggressive normalization ensures that duplicates are detected even
        when they differ only in styling, HTML tags, or formatting.
        """
        clean_lines = []
        for line in raw_text.splitlines():
            line = line.strip()
            if not line: continue
            if line.isdigit(): continue  # Skip subtitle indices
            if "-->" in line: continue   # Skip timestamps
            
            # Remove HTML/XML tags (e.g., <i>, <b>, <font color="red">)
            line = re.sub(r'<[^>]+>', '', line)
            
            # Remove styling tags (e.g., {\an8}, {\i1})
            line = re.sub(r'\{[^}]+\}', '', line)
            
            # Remove common subtitle artifacts [Music], (SIGHS), etc.
            line = re.sub(r'\[.*?\]', '', line)
            line = re.sub(r'\(.*?\)', '', line)
            
            # Remove special characters and normalize punctuation
            line = re.sub(r'[^\w\s]', '', line)
            
            # Normalize whitespace (multiple spaces -> single space)
            line = re.sub(r'\s+', ' ', line).strip()
            
            # Convert to lowercase for case-insensitive comparison
            line = line.lower()
            
            if line:  # Only add non-empty lines
                clean_lines.append(line)
        
        return "\n".join(clean_lines)

# =============================================================================
# AI Translation Engine
# =============================================================================

class Translator:
    def __init__(self, src_lang: str, tgt_lang: str):
        if not AI_AVAILABLE:
            raise ImportError("AI libraries not installed. Run: pip install transformers torch sentencepiece")
            
        self.src = LANG_MAP.get(src_lang, src_lang)
        self.tgt = LANG_MAP.get(tgt_lang, tgt_lang)
        self.model_name = f"Helsinki-NLP/opus-mt-{self.src}-{self.tgt}"
        
        print(f"  [AI] Loading model: {self.model_name} ...")
        try:
            self.tokenizer = MarianTokenizer.from_pretrained(self.model_name, cache_dir=MODEL_CACHE_DIR)
            self.model = MarianMTModel.from_pretrained(self.model_name, cache_dir=MODEL_CACHE_DIR)
        except Exception as e:
            raise ValueError(f"Could not load model for {self.src}->{self.tgt}. Error: {e}")

        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.model.to(self.device)

    def translate_batch(self, texts: List[str]) -> List[str]:
        encoded = self.tokenizer(texts, return_tensors="pt", padding=True, truncation=True).to(self.device)
        with torch.no_grad():
            generated_tokens = self.model.generate(**encoded)
        return self.tokenizer.batch_decode(generated_tokens, skip_special_tokens=True)


class LLMTranslator:
    """Translation engine using Local_LLM (in-memory GGUF inference)."""
    def __init__(self, src_lang: str, tgt_lang: str, **kwargs):
        if not LLM_AVAILABLE:
            raise ImportError("Local_LLM library not installed. Cannot use LLM translation.")

        self.src = LANG_MAP.get(src_lang, src_lang)
        self.tgt = LANG_MAP.get(tgt_lang, tgt_lang)

        print(f"  [LLM] Using Local_LLM for {self.src} -> {self.tgt} translation ...")

    def translate_batch(self, texts: List[str]) -> List[str]:
        translated = []
        system = (
            f"You are a professional subtitle translator. "
            f"Translate the following video subtitle from '{self.src}' to '{self.tgt}'. "
            f"Maintain the same meaning and tone. Output ONLY the translated text, no explanations."
        )
        for text in texts:
            try:
                result = llm_query(text, system=system, max_tokens=256)
                if result is not None:
                    translated.append(result.strip())
                else:
                    print(f"    [LLM-ERROR] Engine returned None, keeping original.")
                    translated.append(text)
            except Exception as e:
                print(f"    [LLM-ERROR] Translation failed: {type(e).__name__}")
                translated.append(text)  # Fallback to original
        return translated

# =============================================================================
# Core Logic
# =============================================================================

class SubtitleManager:
    def __init__(self, file_path: str, dry_run: bool = False):
        self.file_path = file_path
        self.dry_run = dry_run
        self.streams = []
        self.format = {}
        self.temp_files = []

    def probe(self) -> bool:
        """Probes the file to get stream info."""
        cmd = [FFPROBE, "-v", "quiet", "-print_format", "json", "-show_format", "-show_streams", self.file_path]
        try:
            # Force UTF-8 decoding
            res = sp.run(cmd, capture_output=True, text=True, encoding='utf-8', errors='replace')
            if res.returncode != 0: return False
            data = json.loads(res.stdout)
            self.format = data.get("format", {})
            self.streams = data.get("streams", [])
            return True
        except Exception: return False

    def get_subtitle_streams(self) -> List[Dict]:
        return [s for s in self.streams if s.get("codec_type") == "subtitle"]

    def extract_srt_content(self, stream_idx: int) -> Tuple[str, str]:
        """Returns (raw_srt_text, normalized_hash)."""
        cmd = [FFMPEG, "-hide_banner", "-loglevel", "error", "-i", self.file_path, 
               "-map", f"0:{stream_idx}", "-f", "srt", "-"]
        try:
            res = sp.run(cmd, capture_output=True, timeout=60)
            raw_text = res.stdout.decode("utf-8", errors="ignore")
            norm_text = SubtitleContent.normalize(raw_text)
            if not norm_text: return raw_text, "empty"
            return raw_text, hashlib.md5(norm_text.encode("utf-8")).hexdigest()
        except Exception: return "", "error"

    # -------------------------------------------------------------------------
    # ACTION: Clean Duplicates
    # -------------------------------------------------------------------------
    def find_duplicates(self) -> List[int]:
        """Returns a list of stream indices to KEEP."""
        subs = self.get_subtitle_streams()
        if not subs: return []

        lang_groups = defaultdict(list)
        for s in subs:
            lang = s.get("tags", {}).get("language", "und")
            lang_groups[lang].append(s['index'])

        to_keep = []
        print(f"  [CLEAN] Analyzing {len(subs)} streams...")

        for lang, indices in lang_groups.items():
            if len(indices) == 1:
                to_keep.append(indices[0])
            else:
                print(f"    Checking {len(indices)} '{lang}' streams...")
                seen_hashes = set()
                for idx in indices:
                    _, content_hash = self.extract_srt_content(idx)
                    if content_hash == "empty" or content_hash == "error":
                        to_keep.append(idx) # Keep errors to be safe
                    elif content_hash in seen_hashes:
                        print(f"      Stream {idx}: Duplicate content. Dropping.")
                    else:
                        seen_hashes.add(content_hash)
                        to_keep.append(idx)
        
        return sorted(to_keep)

    # -------------------------------------------------------------------------
    # ACTION: Translate
    # -------------------------------------------------------------------------
    def translate_stream(self, src_idx: int, src_lang: str, tgt_lang: str, use_llm: bool = False, llm_model: str = "") -> Optional[str]:
        """Translates a stream and returns path to new .srt file."""
        engine = "LLM" if use_llm else "AI"
        print(f"  [{engine}] Translating Stream {src_idx} ({src_lang}) -> {tgt_lang}...")
        
        raw_srt, _ = self.extract_srt_content(src_idx)
        if not raw_srt: return None

        items = SubtitleContent.parse_srt(raw_srt)
        if not items: return None

        try:
            if use_llm:
                translator = LLMTranslator(src_lang, tgt_lang)
                batch_size = 5 # Smaller batches for LLM to avoid context/token limits
            else:
                translator = Translator(src_lang, tgt_lang)
                batch_size = 16
            
            texts = [i.content.replace("\n", " ") for i in items]
            
            translated = []
            total = len(texts)
            
            for i in range(0, total, batch_size):
                batch = texts[i:i+batch_size]
                res = translator.translate_batch(batch)
                translated.extend(res)
                print(f"    {min(i+batch_size, total)}/{total}", end="\r")
            
            print("") # Newline
            
            # Update content
            for i, item in enumerate(items):
                item.content = translated[i]

            # Save
            out_path = str(RUN_TMP / f"{Path(self.file_path).stem}.{tgt_lang}.srt")
            with open(out_path, "w", encoding="utf-8") as f:
                for item in items:
                    f.write(f"{item.index}\n{item.timestamp}\n{item.content}\n\n")
            
            self.temp_files.append(out_path)
            return out_path

        except Exception as e:
            print(f"    [ERROR] Translation failed: {e}")
            return None

    # -------------------------------------------------------------------------
    # EXECUTION: Remux
    # -------------------------------------------------------------------------
    def apply_changes(self, keep_indices: List[int], new_subs: List[Tuple[str, str, str]]):
        """
        Re-muxes the file with:
        - Kept original streams (keep_indices)
        - New subtitle files (new_subs: [(path, lang, title), ...])
        """
        if self.dry_run:
            print("  [DRY-RUN] Would remux file with:")
            print(f"    - Keeping streams: {keep_indices}")
            print(f"    - Adding {len(new_subs)} new subtitles")
            return

        temp_out = str(RUN_TMP / f"{Path(self.file_path).stem}_new.mp4")
        cmd = [FFMPEG, "-y", "-hide_banner", "-loglevel", "error", "-i", self.file_path]

        # Add input for each new subtitle
        for path, _, _ in new_subs:
            cmd.extend(["-i", path])

        # Map Video & Audio (Copy if present)
        has_video = any(s.get("codec_type") == "video" for s in self.streams)
        has_audio = any(s.get("codec_type") == "audio" for s in self.streams)
        
        if has_video:
            cmd.extend(["-map", "0:v", "-c:v", "copy"])
        if has_audio:
            cmd.extend(["-map", "0:a", "-c:a", "copy"])

        # Map Kept Subtitles
        for idx in keep_indices:
            cmd.extend(["-map", f"0:{idx}", "-c:s", "copy"])

        # Map New Subtitles
        # They start at input index 1, 2, 3...
        for i, (_, lang, title) in enumerate(new_subs):
            input_idx = i + 1
            cmd.extend([
                "-map", f"{input_idx}:0", 
                "-c:s", "mov_text",
                f"-metadata:s:s:{len(keep_indices)+i}", f"language={lang}",
                f"-metadata:s:s:{len(keep_indices)+i}", f"title={title}"
            ])

        cmd.append(temp_out)
        
        print(f"  [EXEC] Remuxing...")
        if sp.run(cmd).returncode == 0:
            # Replace original
            shutil.move(temp_out, self.file_path)
            print("  [SUCCESS] File updated.")
        else:
            print("  [ERROR] Remux failed.")

    def cleanup(self):
        for p in self.temp_files:
            try: os.remove(p)
            except Exception: pass

# =============================================================================
# Main Entry Point
# =============================================================================

def process_path(path: str, args):
    sm = SubtitleManager(path, args.dry_run)
    if not sm.probe():
        print(f"[SKIP] Could not probe {path}")
        return

    print(f"\nProcessing: {Path(path).name}")
    
    # 1. CLEAN
    keep_indices = []
    if args.clean:
        keep_indices = sm.find_duplicates()
    else:
        # Keep all existing subtitles if not cleaning
        keep_indices = [s['index'] for s in sm.get_subtitle_streams()]

    # 2. Identify available languages
    existing_langs = set()
    eng_stream_idx = None
    best_foreign_idx = None
    
    for idx in keep_indices:
        # Find the stream object for this index
        stream = next(s for s in sm.streams if s['index'] == idx)
        lang = stream.get("tags", {}).get("language", "und")
        existing_langs.add(lang)
        
        if lang == "eng" and eng_stream_idx is None:
            eng_stream_idx = idx
        if lang != "eng" and best_foreign_idx is None:
            best_foreign_idx = idx

    new_subs = [] # List of (path, lang, title)

    # 3. ENSURE ENGLISH
    if args.ensure_eng and "eng" not in existing_langs:
        if best_foreign_idx is not None:
            # Translate foreign -> eng
            src_lang = next(s for s in sm.streams if s['index'] == best_foreign_idx).get("tags", {}).get("language", "und")
            print(f"  [INFO] Missing English. Translating from {src_lang}...")
            srt = sm.translate_stream(best_foreign_idx, src_lang, "eng", use_llm=args.llm, llm_model=args.model)
            if srt:
                new_subs.append((srt, "eng", f"Translated from {src_lang}"))
                existing_langs.add("eng") # Mark as present so we can use it for others
                # If we just created English, we can't easily use it as source immediately 
                # without reloading, so we'll skip add-langs based on English for this run
        else:
            print("  [WARN] No subtitles available to translate to English.")

    # 4. ADD LANGUAGES
    if args.add_langs:
        targets = args.add_langs.split(",")
        # Prefer English as source, otherwise use best foreign
        src_idx = eng_stream_idx if eng_stream_idx is not None else best_foreign_idx
        
        if src_idx is not None:
            src_lang = next(s for s in sm.streams if s['index'] == src_idx).get("tags", {}).get("language", "und")
            
            for tgt in targets:
                tgt = tgt.strip()
                if tgt in existing_langs:
                    print(f"  [SKIP] {tgt} already exists.")
                    continue
                
                srt = sm.translate_stream(src_idx, src_lang, tgt, use_llm=args.llm, llm_model=args.model)
                if srt:
                    new_subs.append((srt, tgt, f"Translated from {src_lang}"))
        else:
            print("  [WARN] No source subtitle available for translation.")

    # 5. APPLY
    # Only remux if we actually changed something (removed duplicates or added new)
    total_original = len(sm.get_subtitle_streams())
    if len(keep_indices) < total_original or len(new_subs) > 0:
        sm.apply_changes(keep_indices, new_subs)
    else:
        print("  [INFO] No changes needed.")
    
    sm.cleanup()


def pick_directory(title: str = "Path not found. Please select a folder.", initial_dir: Optional[str] = None) -> Optional[str]:
	"""Open a GUI directory picker dialog."""
	if not HAS_TKINTER:
		return None
	try:
		print("📁 Opening directory picker...")
		# Validate initial_dir - if it doesn't exist, Tkinter might behave unpredictably (e.g. last used)
		if initial_dir and not os.path.exists(initial_dir):
			initial_dir = None
			
		root_tk = Tk()
		root_tk.withdraw()  # Hide main window
		root_tk.attributes('-topmost', True)  # Bring to front
		root_tk.lift()
		root_tk.focus_force()
		
		# Robust path handling (verified fix)
		if initial_dir: initial_dir = os.path.abspath(initial_dir)
		
		selected = askdirectory(parent=root_tk, title=title, initialdir=initial_dir)
		root_tk.destroy()
		return selected if selected else None
	except Exception as e:
		print(f"⚠️  Failed to open GUI directory picker: {e}")
		return None

def validate_path(path_arg: str) -> str:
	"""Creates a robust loop to validate or pick a path."""
	current_path = path_arg
	
	while True:
		if current_path and os.path.exists(current_path):
			return current_path
		
		if current_path:
			print(f"\n[WARN] Path not found: '{current_path}'")
		
		print("       Opening folder picker (check taskbar)...")
		
		# Try to use parent of missing path as start, or default
		start_dir = os.path.dirname(current_path) if current_path else None
		
		new_path = pick_directory(title=f"Path not found. Select media folder.", initial_dir=start_dir)
		
		if new_path and os.path.exists(new_path):
			print(f"       ✅ Selected: '{new_path}'")
			return new_path
			
		# If user cancelled or picked invalid
		print("       [CANCEL] No valid folder selected.")
		retry = input("       Retry? (Y/n): ").strip().lower()
		if retry == 'n':
			print("       Exiting.")
			sys.exit(0)
		# Loop continues to pick_directory again

if __name__ == "__main__":
    # Remove argparse, use top-level config
    
    # 1. Create args object from config
    args = SimpleNamespace(
        path       = DEFAULT_SCAN_DIR,
        clean      = OPT_CLEAN,
        ensure_eng = OPT_ENSURE_ENG,
        add_langs  = OPT_ADD_LANGS,
        llm        = OPT_USE_LLM,
        model      = "",
        dry_run    = OPT_DRY_RUN
    )
    
    # 2. Add files from sys.argv if dropped onto script (ignoring flags since we removed argparse)
    #    This allows "Drag & Drop" support to still work which is handy.
    if len(sys.argv) > 1:
        # Check if first arg is a path
        potential_path = sys.argv[1]
        if os.path.exists(potential_path):
            args.path = potential_path

    # 3. Validation & Execution
    target_path = validate_path(args.path)
    
    if os.path.isfile(target_path):
        process_path(target_path, args)
    elif os.path.isdir(target_path):
        for root, _, files in os.walk(target_path):
            for f in files:
                if Path(f).suffix.lower() in File_extn:
                    process_path(os.path.join(root, f), args)
    else:
        print("Invalid path logic.") # Should be caught by validate_path

    if PAUSE_ON_EXIT:
        input("\nAll Done :) Press Enter to exit.")
