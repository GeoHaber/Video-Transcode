import sys
from pathlib import Path
from unittest.mock import patch, MagicMock
import pytest
import shutil

import Trans_code
import Utils

def test_pick_directory_mock():
    with patch("Trans_code.HAS_TKINTER", True), \
         patch("Trans_code.Tk") as mock_tk, \
         patch("Trans_code.askdirectory") as mock_ask:
        mock_ask.return_value = "/mock/dir"
        res = Trans_code.pick_directory()
        assert res == "/mock/dir"

def test_handle_bad_file():
    import tempfile
    with tempfile.TemporaryDirectory() as tmpdir:
        bad_file = Path(tmpdir) / "bad.mp4"
        bad_file.write_text("not a video")
        # handle_bad_file(file_path, reason, root_dir=None, move=True)
        Trans_code.handle_bad_file(str(bad_file), "Broken", root_dir=tmpdir, move=True)

def test_process_file_skip_logic():
    """Test process_file returns SKIPPED when plan says to skip."""
    mock_mf = MagicMock()
    mock_mf.plan.return_value = ([], True, ["Skipping..."])
    mock_mf.path = Path("test.mp4")
    mock_mf.streams = []
    mock_mf.logs = ["Skipping..."]

    file_info = {"path": "test.mp4", "name": "test.mp4", "size": 1000000, "streams": []}

    with patch("FFMpeg.MediaFile.from_dict", return_value=mock_mf), \
         patch("FFMpeg.post_encode_artifacts"), \
         patch("Utils.safe_print"):
        res = Trans_code.process_file(file_info, 1, 1, "TASK")
        assert res["status"] == "SKIPPED"
        assert res["skipt"] == 1

def test_process_file_success_logic():
    """Test process_file returns PROCESSED on successful encode."""
    mock_mf = MagicMock()
    mock_mf.plan.return_value = (["ffmpeg_cmd"], False, ["Planning..."])
    mock_mf.run.return_value = "temp_out.mp4"
    mock_mf.cleanup.return_value = 1000000
    mock_mf.path = Path("test.mp4")
    mock_mf.streams = []
    mock_mf.logs = ["Planning..."]

    file_info = {"path": "test.mp4", "name": "test.mp4", "size": 1000000, "streams": []}

    with patch("FFMpeg.MediaFile.from_dict", return_value=mock_mf), \
         patch("FFMpeg.post_encode_artifacts"), \
         patch("Subtitle_Manager.SubtitleManager") as mock_sm, \
         patch("Utils.safe_print"), \
         patch("Utils.SMART_RENAME", False), \
         patch("shutil.move"):
        res = Trans_code.process_file(file_info, 1, 1, "TASK")
        assert res["status"] == "PROCESSED"
        assert res["saved"] == 1000000
