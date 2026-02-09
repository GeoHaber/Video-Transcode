import sys
import json
from pathlib import Path
from unittest.mock import patch, MagicMock
import pytest

import FFMpeg
import Utils

def test_parse_video_exhaustive():
    """Exhaustively tests parse_video branches by varying input streams."""
    mf = FFMpeg.MediaFile(Path("test.mp4"))
    
    # Test matrix: (stream_subset, expected_fragments_in_cmd)
    test_cases = [
        # 1. 10-bit HEVC (Should map directly or add main10 profile)
        ({"codec_name": "hevc", "pix_fmt": "yuv420p10le"}, ["-c:v", "copy"]),
        
        # 2. H264 (Should re-encode)
        ({"codec_name": "h264", "bit_rate": "1000000", "width": 1920, "height": 1080}, ["libx265", "-preset"]),
        
        # 3. HDR10 (BT.2020 + PQ)
        ({
            "codec_name": "hevc", "pix_fmt": "yuv420p10le", 
            "color_space": "bt2020nc", "color_transfer": "smpte2084"
        }, ["-c:v", "copy"]), # Already HEVC 10-bit, should copy
        
        # 4. HDR10 h264 (re-encode with HDR metadata passthrough, no tone mapping)
        ({
            "codec_name": "h264", "pix_fmt": "yuv420p10le",
            "color_space": "bt2020nc", "color_transfer": "smpte2084"
        }, ["libx265", "smpte2084"]),
        
        # 5. Interlaced (code logs it but does not add yadif; just re-encodes)
        ({"field_order": "tt", "width": 1920, "height": 1080}, ["libx265"]),
        
        # 6. VFR (code logs it as VFR; just re-encodes)
        ({"avg_frame_rate": "30/1", "r_frame_rate": "60/1", "width": 1920, "height": 1080}, ["libx265"]),
        
        # 7. Low resolution (code does not upscale)
        ({"width": 640, "height": 480}, ["libx265"]),
        
        # 8. Unknown bitrate
        ({"width": 1920, "height": 1080, "bit_rate": "0"}, ["libx265"]),
        
        # 9. Hardware: NVENC + 10bit
        ({"width": 1920, "height": 1080, "codec_name": "h264"}, ["hevc_nvenc", "main10"], "hevc_nvenc")
    ]
    
    for case in test_cases:
        stream_data = case[0]
        expected_matches = case[1]
        forced_enc = case[2] if len(case) > 2 else "libx265"
        
        # Fill defaults for stream
        full_stream = {
            "index": 0, "codec_type": "video", "codec_name": "h264", 
            "width": 1920, "height": 1080, "bit_rate": "5000000",
            "avg_frame_rate": "24/1", "r_frame_rate": "24/1",
            "pix_fmt": "yuv420p"
        }
        full_stream.update(stream_data)
        
        with patch("FFMpeg.CURRENT_ENCODER", forced_enc):
            mf.streams = [full_stream]
            cmd, skip, logs = FFMpeg.parse_video(mf)
            cmd_str = " ".join(cmd)
            for match in expected_matches:
                assert match in cmd_str or any(match in l for l in logs), f"Failed scenario {stream_data}: {match} not in {cmd_str}"

def test_parse_audio_exhaustive():
    """Exhaustively tests audio selection logic."""
    mf = FFMpeg.MediaFile(Path("test.mp4"))
    
    # 1. Prefer Default_lng (eng)
    streams = [
        {"index": 0, "codec_type": "video"}, # Padding
        {"index": 1, "codec_type": "audio", "tags": {"language": "fra"}},
        {"index": 2, "codec_type": "audio", "tags": {"language": "eng"}}
    ]
    mf.streams = streams
    with patch("Utils.Default_lng", "eng"):
        cmd, skip, logs = FFMpeg.parse_audio(mf)
        assert "0:2" in " ".join(cmd)
    
    # 2. Re-encode non-AC3/AAC (e.g. DTS)
    mf.streams = [{"index": 1, "codec_type": "audio", "codec_name": "dts", "channels": 6}]
    cmd, skip, logs = FFMpeg.parse_audio(mf)
    assert any("-c:a" in arg for arg in cmd)
    assert "aac" in " ".join(cmd)
    
    # 3. High bitrate audio -> Re-encode
    mf.streams = [{"index": 1, "codec_type": "audio", "codec_name": "ac3", "bit_rate": "1500000", "channels": 6}]
    cmd, skip, logs = FFMpeg.parse_audio(mf)
    assert any("-c:a" in arg for arg in cmd) or any("-b:a" in arg for arg in cmd)
