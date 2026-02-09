import sys
from unittest.mock import patch, MagicMock
from pathlib import Path

import FFMpeg

def test_hardware_detection_priority():
    """Verifies the priority order of hardware detection."""
    # Priority: NVIDIA -> AMD -> Intel QSV -> Software
    
    # 1. Test NVIDIA preference
    with patch("FFMpeg._test_encoder") as mock_test:
        mock_test.side_effect = lambda e: e == "hevc_nvenc"
        result = FFMpeg.detect_hardware_encoder()
        assert result == "hevc_nvenc"
        
    # 2. Test AMD fallback
    with patch("FFMpeg._test_encoder") as mock_test:
        mock_test.side_effect = lambda e: e == "hevc_amf"
        result = FFMpeg.detect_hardware_encoder()
        assert result == "hevc_amf"
        
    # 3. Test QSV fallback
    with patch("FFMpeg._test_encoder") as mock_test:
        mock_test.side_effect = lambda e: e == "hevc_qsv"
        result = FFMpeg.detect_hardware_encoder()
        assert result == "hevc_qsv"
        
    # 4. Test Software fallback
    with patch("FFMpeg._test_encoder") as mock_test:
        mock_test.return_value = False
        result = FFMpeg.detect_hardware_encoder()
        assert result == "libx265"

def test_test_encoder_logic():
    """Verifies that _test_encoder calls ffmpeg correctly."""
    with patch("subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0)
        assert FFMpeg._test_encoder("some_enc") is True
        
        mock_run.return_value = MagicMock(returncode=1)
        assert FFMpeg._test_encoder("bad_enc") is False
        
        mock_run.side_effect = Exception("Crash")
        assert FFMpeg._test_encoder("crash_enc") is False
