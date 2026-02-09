import sys
from pathlib import Path
from unittest.mock import patch, MagicMock
import pytest
import os

import Utils

def test_safe_print_does_not_crash():
    Utils.safe_print("Simple message")
    Utils.safe_print(f"Complex message with {Path.cwd()}")

def test_ensure_libs_mocks():
    # ensure_libs expects a dict: {"import_name": "pip_package_name"}
    # Patch sp.check_call on Utils FIRST, then mock importlib inside
    with patch.object(Utils, 'sp') as mock_sp:
        with patch("importlib.import_module", side_effect=ImportError):
            Utils.ensure_libs({"nonexistent_lib": "nonexistent-lib"})
            assert mock_sp.check_call.called

def test_copy_move_logic():
    import tempfile, os
    # copy_move(src, dst_dir, move) — needs a real directory or good patches
    with tempfile.TemporaryDirectory() as tmpdir:
        src_file = Path(tmpdir) / "src.txt"
        src_file.write_text("hello")
        dst_dir = Path(tmpdir) / "dst"
        dst_dir.mkdir()

        # Test copy mode
        result = Utils.copy_move(str(src_file), str(dst_dir), move=False)
        assert result is True
        assert (dst_dir / "src.txt").exists()
        assert src_file.exists()  # original still there

def test_errlog_block():
    import tempfile
    with tempfile.TemporaryDirectory() as tmpdir:
        with patch.object(Utils, "WORK_DIR", Path(tmpdir)):
            Utils.errlog_block("file.mp4", "Error Title", "Error Body")
