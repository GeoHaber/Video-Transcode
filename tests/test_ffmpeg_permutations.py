import sys
from pathlib import Path
from unittest.mock import patch, MagicMock
import pytest

import FFMpeg
import Utils

def test_plan_permutations():
    """Systematically tests MediaFile.plan with many stream configurations."""
    
    scenarios = [
        # Scenario: Low bitrate H264 -> HEVC (Should encode)
        {
            "streams": [{"codec_type": "video", "codec_name": "h264", "bit_rate": "1000000", "width": 1920, "height": 1080}],
            "expect_skip": False,
            "expect_encode": True
        },
        # Scenario: High bitrate HEVC (Should encode/resize)
        {
            "streams": [{"codec_type": "video", "codec_name": "hevc", "bit_rate": "50000000", "width": 3840, "height": 2160}],
            "expect_skip": False,
            "expect_encode": True
        },
        # Scenario: Already efficient HEVC (Should skip or remux)
        {
            "streams": [{"codec_type": "video", "codec_name": "hevc", "bit_rate": "2000000", "width": 1920, "height": 1080}],
            "expect_skip": False,
            "expect_encode": False # Should remux
        }
    ]
    
    for scene in scenarios:
        mf = FFMpeg.MediaFile(Path("test.mp4"))
        mf.streams = scene["streams"]
        mf.duration = 100
        mf.bitrate = int(scene["streams"][0].get("bit_rate", 0))
        mf.width = scene["streams"][0]["width"]
        mf.height = scene["streams"][0]["height"]
        mf.codec_v = scene["streams"][0]["codec_name"]
        
        # Mock the parsing functions to avoid real logic here, focusing on the plan() aggregation
        with patch("FFMpeg.parse_video", return_value=(["-c:v", "libx265"], False, ["logs"])), \
             patch("FFMpeg.parse_audio", return_value=(["-c:a", "copy"], True, ["logs"])), \
             patch("FFMpeg.parse_subtl", return_value=(["-map", "0:s"], False, ["logs"], [], "eng", "default")):
            
            cmd, skip, logs = mf.plan()
            assert skip == scene["expect_skip"]
            if not skip:
                cmd_str = " ".join(cmd)
                # Ensure plan included parts from all mocks
                assert "-c:v libx265" in cmd_str
                assert "-c:a copy" in cmd_str
                assert "-map 0:s" in cmd_str

def test_add_subtl_from_file_mocked():
    """Tests the logic that injects external SRT files via add_subtl_from_file."""
    mf = FFMpeg.MediaFile(Path("movie.mp4"))
    ext_srt = Path("movie.srt")
    
    # Mocking iterdir to find a candidate and open to read it
    mock_files = [ext_srt]
    
    with patch("pathlib.Path.iterdir", return_value=mock_files), \
         patch("pathlib.Path.is_file", return_value=True), \
         patch("FFMpeg._detect_encoding", return_value="utf-8"), \
         patch("builtins.open", MagicMock()):
        
        cmd_part, skip, logs, lang, disp = FFMpeg.add_subtl_from_file(str(mf.path), mf.streams)
        # Should find 'movie.srt' and add it
        assert not skip
        assert "-i" in cmd_part
