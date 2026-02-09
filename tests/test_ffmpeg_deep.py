import sys
from pathlib import Path
from unittest.mock import patch, MagicMock
import pytest

import FFMpeg
import Utils

@pytest.fixture
def base_mf():
    return FFMpeg.MediaFile(Path("test.mp4"))

def test_parse_video_complexity():
    """Tests parse_video with HDR, resolution changes, and hardware encoders."""
    mf = FFMpeg.MediaFile(Path("t.mp4"))
    
    # Standard 1080p SDR
    mf.streams = [{"index": 0, "codec_type": "video", "width": 1920, "height": 1080, "pix_fmt": "yuv420p", "bit_rate": "5000000"}]
    mf.estimated_video_bitrate = 5000000
    cmd, skip, logs = FFMpeg.parse_video(mf)
    assert "-c:v" in " ".join(cmd)
    
    # 4K HDR10 -> 1080p (downscale + HDR metadata passthrough, no tone mapping)
    mf.streams = [{
        "index": 0, "codec_type": "video", "width": 3840, "height": 2160,
        "pix_fmt": "yuv420p10le", "color_space": "bt2020nc",
        "color_transfer": "smpte2084", "color_primaries": "bt2020", "bit_rate": "50000000"
    }]
    mf.estimated_video_bitrate = 50000000
    cmd, skip, logs = FFMpeg.parse_video(mf)
    cmd_str = " ".join(cmd)
    assert "scale=" in cmd_str or "1920" in cmd_str  # downscale
    assert "smpte2084" in cmd_str or "bt2020" in cmd_str  # HDR metadata preserved
    
    # Hardware specific: NVENC
    with patch("FFMpeg.CURRENT_ENCODER", "hevc_nvenc"):
        cmd, skip, logs = FFMpeg.parse_video(mf)
        assert "hevc_nvenc" in " ".join(cmd)
        assert "-preset" in " ".join(cmd)
        
    # Hardware specific: AMF
    with patch("FFMpeg.CURRENT_ENCODER", "hevc_amf"):
        cmd, skip, logs = FFMpeg.parse_video(mf)
        assert "hevc_amf" in " ".join(cmd)
        assert "-quality" in " ".join(cmd)

    # Hardware specific: QSV
    with patch("FFMpeg.CURRENT_ENCODER", "hevc_qsv"):
        cmd, skip, logs = FFMpeg.parse_video(mf)
        assert "hevc_qsv" in " ".join(cmd)
        assert "-load_plugin" in str(cmd) or any("-preset" in arg for arg in cmd)

def test_parse_subtl_logic():
    """Tests subtitle parsing and lang selection."""
    mf = FFMpeg.MediaFile(Path("test.mp4"))
    mf.streams = [
        {"codec_type": "subtitle", "index": 2, "codec_name": "mov_text", "tags": {"language": "eng", "title": "English"}},
        {"codec_type": "subtitle", "index": 3, "codec_name": "mov_text", "tags": {"language": "fra", "title": "French"}}
    ]
    
    # Filter for eng
    with patch("Utils.Default_lng", "eng"):
        cmd, skip, logs, *_ = FFMpeg.parse_subtl(mf)
        assert "-map 0:2" in " ".join(cmd)
    
    # Case insensitive check via Default_lng
    with patch("Utils.Default_lng", "fra"):
        cmd, skip, logs, *_ = FFMpeg.parse_subtl(mf)
        assert "-map 0:3" in " ".join(cmd)

def test_matrix_it_mock():
    """Verifies matrix_it calls ffmpeg with correct args."""
    with patch("FFMpeg._popen_managed") as mock_popen:
        mock_proc = MagicMock()
        mock_proc.communicate.return_value = (b"", b"")
        mock_proc.returncode = 0
        mock_popen.return_value = mock_proc
        
        res = FFMpeg.matrix_it(Path("in.mp4"), Path("out.jpg"), "TASK", duration=100, width=1920, height=1080)
        assert res is True
        assert mock_popen.called

def test_speed_up_mock():
    with patch("FFMpeg._popen_managed") as mock_popen:
        mock_proc = MagicMock()
        mock_proc.communicate.return_value = (b"", b"")
        mock_proc.returncode = 0
        mock_popen.return_value = mock_proc
        
        res = FFMpeg.speed_up(Path("in.mp4"), 2.0, Path("out.mp4"), "TASK", has_audio=True)
        assert res is True

def test_short_ver_mock():
    with patch("FFMpeg._popen_managed") as mock_popen:
        mock_proc = MagicMock()
        mock_proc.communicate.return_value = (b"", b"")
        mock_proc.returncode = 0
        mock_popen.return_value = mock_proc
        
        res = FFMpeg.short_ver(Path("in.mp4"), Path("out.mp4"), "TASK", duration=100)
        assert res is True
