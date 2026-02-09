import sys
import json
from pathlib import Path
from unittest.mock import patch, MagicMock
import pytest

import FFMpeg
import Utils

def test_media_file_full_lifecycle():
    """Simulates probe -> plan cycle safely (avoids run/cleanup threading issues)."""
    mf = FFMpeg.MediaFile(Path("movie.mkv"))

    mock_probe_out = json.dumps({
        "format": {"duration": "60.0", "size": "1000000", "bit_rate": "133333"},
        "streams": [{"index": 0, "codec_type": "video", "codec_name": "h264",
                      "width": 1920, "height": 1080, "avg_frame_rate": "24/1",
                      "r_frame_rate": "24/1", "pix_fmt": "yuv420p"}]
    })

    with patch("FFMpeg._popen_managed") as mock_popen:
        mock_proc = MagicMock()
        mock_proc.communicate.return_value = (mock_probe_out, b"")
        mock_proc.returncode = 0
        mock_popen.return_value = mock_proc

        # 1. Probe
        mf.probe()
        assert mf.width == 1920

        # 2. Plan
        cmd, skip, logs = mf.plan()
        assert not skip  # h264 should trigger re-encode

def test_transcode_main_args():
    """Verifies main() returns 1 when ffmpeg is not found."""
    import Trans_code
    with patch("shutil.which", return_value=None):
        result = Trans_code.main()
        assert result == 1
