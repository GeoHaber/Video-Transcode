import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

import FFMpeg
import Utils

def test_post_encode_artifacts_guards():
    """Tests the guard logic in post_encode_artifacts."""
    # 1. Missing file
    with patch("Utils.safe_print") as mock_print:
        FFMpeg.post_encode_artifacts(Path("non_existent.mp4"), {}, "TASK")
        assert "final file missing" in mock_print.call_args[0][0]

    # 2. Missing info
    target = Path(__file__) # Existing file
    with patch("Utils.safe_print") as mock_print:
        FFMpeg.post_encode_artifacts(target, {}, "TASK")
        assert "missing output info" in mock_print.call_args[0][0]

def test_artifact_engine_toggles():
    """Verifies that artifact engine respects ADD_ADDITIONAL toggle."""
    target = Path(__file__)
    info = {"dur_out": 10, "w_enc": 1920, "h_enc": 1080}
    
    with patch("Utils.ADD_ADDITIONAL", False):
        with patch("Utils.safe_print") as mock_print:
            FFMpeg.post_encode_artifacts(target, info, "TASK", main_was_skipped=False)
            assert "Artifacts disabled" in mock_print.call_args[0][0]
