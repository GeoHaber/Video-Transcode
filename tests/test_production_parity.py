import sys
from pathlib import Path

import FFMpeg
import Utils
import Trans_code
import Subtitle_Manager

def test_production_symbols_presence():
    """Ensures all critical architectural components are present."""
    
    expectations = {
        "FFMpeg": [
            "MediaFile", "detect_hardware_encoder", "post_encode_artifacts",
            "PROC_MGR", "matrix_it", "speed_up", "short_ver",
            "add_subtl_from_file", "parse_video", "parse_audio", "parse_subtl"
        ],
        "Utils": [
            "safe_print", "print_lock", "progress_lock", "ensure_libs"
        ],
        "Subtitle_Manager": [
            "SubtitleManager", "process_path"
        ],
        "Trans_code": [
            "process_file", "scan_folder", "main"
        ]
    }
    
    modules = {
        "FFMpeg": FFMpeg,
        "Utils": Utils,
        "Subtitle_Manager": Subtitle_Manager,
        "Trans_code": Trans_code
    }
    
    for mod_name, symbols in expectations.items():
        mod = modules[mod_name]
        for sym in symbols:
            assert hasattr(mod, sym), f"Symbol {sym} missing from {mod_name}"
