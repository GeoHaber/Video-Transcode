import sys
import json
from pathlib import Path
from unittest.mock import patch, MagicMock
from types import SimpleNamespace

import Subtitle_Manager

def test_subtitle_dupe_detection():
    """Tests the core duplicate detection logic in SubtitleManager."""
    mock_probe_data = {
        "format": {},
        "streams": [
            {"index": 0, "codec_type": "video"},
            {"index": 1, "codec_type": "subtitle", "tags": {"language": "eng"}},
            {"index": 2, "codec_type": "subtitle", "tags": {"language": "eng"}}
        ]
    }

    with patch("subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0, stdout=json.dumps(mock_probe_data))

        # Mock extract_srt_content to return identical content for dedup
        with patch.object(Subtitle_Manager.SubtitleManager, "extract_srt_content") as mock_extract:
            mock_extract.return_value = ("1\n00:00:01 --> 00:00:02\nTest content\n", "hash123")

            sm = Subtitle_Manager.SubtitleManager("test.mp4")
            sm.probe()
            keep = sm.find_duplicates()

            # Should keep index 1, drop index 2 (duplicate of 1)
            assert 1 in keep
            assert 2 not in keep

def test_remux_logic_with_missing_audio():
    """Verifies that remux logic handles missing streams correctly."""
    mock_probe_data = {
        "streams": [
            {"index": 0, "codec_type": "video"} # No audio
        ]
    }
    
    with patch("subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0, stdout=json.dumps(mock_probe_data))
        
        sm = Subtitle_Manager.SubtitleManager("test.mp4")
        sm.probe()
        
        with patch("shutil.move"):
            # Should not crash and should NOT include -map 0:a
            sm.apply_changes([0], [])
            
            # Verify ffmpeg command doesn't have -map 0:a
            called_cmd = mock_run.call_args[0][0]
            assert "-map" in called_cmd
            assert "0:v" in called_cmd
            assert "0:a" not in called_cmd

def test_subtitle_normalization():
    """Verifies that normalization strips tags and extra noise correctly."""
    raw = "<i>(SIGHS)</i> <font color=\"red\">[Music]</font> Hello! {\an8}World"
    clean = Subtitle_Manager.SubtitleContent.normalize(raw)
    # Lowercase, no tags, no music, no sighs, no punctuation
    assert "hello world" in clean
    assert "i" not in clean # <i> tag stripped
    assert "sighs" not in clean
    assert "music" not in clean

def test_translation_batch_mock():
    """Verifies that LLMTranslator uses Local_LLM for translation."""
    with patch("Subtitle_Manager.llm_query") as mock_llm:
        mock_llm.return_value = "Translated!"
        with patch("Subtitle_Manager.LLM_AVAILABLE", True):
            tr = Subtitle_Manager.LLMTranslator("eng", "fra")
            res = tr.translate_batch(["Hello"])
            assert res == ["Translated!"]
            assert mock_llm.called
