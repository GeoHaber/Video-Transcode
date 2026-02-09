import sys
import json
from pathlib import Path
from unittest.mock import patch, MagicMock
import pytest

import FFMpeg
import Utils

def test_parse_video_comprehensive():
    """Hits as many branches in parse_video as possible."""
    mf = FFMpeg.MediaFile(Path("movie.mp4"))
    
    # 1. Test every hardware encoder
    encoders = ["hevc_amf", "hevc_nvenc", "hevc_qsv", "libx265", "libx264"]
    for enc in encoders:
        with patch("FFMpeg.CURRENT_ENCODER", enc):
            # Test different resolutions
            resolutions = [(1920, 1080), (3840, 2160), (640, 480), (720, 480), (0, 0)]
            for w, h in resolutions:
                mf.streams = [{"index": 0, "codec_type": "video", "codec_name": "h264", "width": w, "height": h, "bit_rate": "10000000"}]
                mf.estimated_video_bitrate = 10000000
                cmd, skip, logs = FFMpeg.parse_video(mf)
                assert any("-c:v" in arg for arg in cmd)

    # 2. Test HDR and Color Profiles
    hdr_configs = [
        {"color_transfer": "smpte2084", "color_primaries": "bt2020", "color_space": "bt2020nc", "pix_fmt": "yuv420p10le"},
        {"color_transfer": "arib-std-b67", "color_primaries": "bt2020", "color_space": "bt2020nc", "pix_fmt": "yuv420p10le"}, # HLG
        {"color_transfer": "bt709", "color_primaries": "bt709", "color_space": "bt709", "pix_fmt": "yuv420p"}
    ]
    for cfg in hdr_configs:
        mf.streams = [{"index": 0, "codec_type": "video", "codec_name": "hevc", "width": 3840, "height": 2160}]
        mf.streams[0].update(cfg)
        mf.estimated_video_bitrate = 50000000
        cmd, skip, logs = FFMpeg.parse_video(mf)
        assert any("-c:v" in arg for arg in cmd)

    # 3. Test Red Fix / Hidden HDR
    # Hidden HDR requires: HEVC + 10-bit + SDR tags (unknown primaries) + high saturation
    with patch("FFMpeg.MediaFile.get_pixel_stats", return_value=(0, 0, 150.0)):
        mf.streams = [{"index": 0, "codec_type": "video", "codec_name": "hevc",
                       "pix_fmt": "yuv420p10le", "color_primaries": "unknown",
                       "color_transfer": "unknown", "color_space": "unknown",
                       "width": 1920, "height": 1080, "bit_rate": "5000000"}]
        mf.estimated_video_bitrate = 5000000
        cmd, skip, logs = FFMpeg.parse_video(mf)
        assert any("zscale" in str(c) for c in cmd)

def test_parse_audio_comprehensive():
    """Hits as many branches in parse_audio as possible."""
    mf = FFMpeg.MediaFile(Path("movie.mp4"))
    
    # 1. No audio streams
    mf.streams = [{"index": 0, "codec_type": "video"}]
    cmd, skip, logs = FFMpeg.parse_audio(mf)
    assert skip is True
    
    # 2. Multiple languages, selecting non-default
    mf.streams = [
        {"index": 1, "codec_type": "audio", "tags": {"language": "spa"}, "codec_name": "ac3"},
        {"index": 2, "codec_type": "audio", "tags": {"language": "fra"}, "codec_name": "aac"}
    ]
    with patch("Utils.Default_lng", "eng"):
        cmd, skip, logs = FFMpeg.parse_audio(mf)
        # Should select first if none are eng
        assert "0:1" in " ".join(cmd)

    # 3. Limited bitrate — use non-AAC to trigger re-encode
    mf.streams = [{"index": 1, "codec_type": "audio", "codec_name": "ac3", "channels": 2, "bit_rate": "320000"}]
    cmd, skip, logs = FFMpeg.parse_audio(mf)
    assert any("-b:a" in arg for arg in cmd)

def test_helper_functions():
    """Tests small helper functions in FFMpeg.py."""
    assert FFMpeg._get_aspect_ratio(1920, 1080) == "16:9"
    assert FFMpeg._get_aspect_ratio(1280, 720) == "16:9"
    assert FFMpeg._get_aspect_ratio(4, 3) == "4:3"
    assert FFMpeg._get_aspect_ratio(100, 100) == "100:100"
    
    assert FFMpeg.hm_sz(1024) == "1.0 KB"
    assert FFMpeg.hm_sz(1024*1024) == "1.0 MB"
    
    assert FFMpeg.hm_tm(60) == "1.0 min"
    assert FFMpeg.hm_tm(3600) == "1.00 hr"
