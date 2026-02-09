# Video_Transcode — Automated Batch Video Transcoding Pipeline

Automated batch video transcoding using **FFmpeg** — scans directories, analyzes streams, and transcodes to **HEVC/H.265** with AI-powered subtitle and audio features.

## Features

- **Batch processing** — directory scanning with caching and parallel workers
- **Full FFmpeg wrapper** — probe, transcode, remux, corruption detection
- **HEVC/H.265 encoding** — configurable BPP, CRF, 10-bit, hardware acceleration
- **Smart file management** — auto-renaming, size-guard logic (prevents oversized output)
- **Subtitle management** — multi-language extraction, OCR via Tesseract, AI-generated subs
- **AI integration** — optional LLM queries and Whisper-based audio transcription
- **Artifact generation** — matrix thumbnails, speed previews, short clips
- **YAML configuration** — all settings via `config.yaml`
- **Resume support** — concise resume/report at exit

## Quick Start

```bash
python Trans_code.py
```

Launches a **folder picker** (or uses config root) → scans for video files → processes through FFmpeg.

## Files

| File | Description |
|------|-------------|
| `Trans_code.py` | Main orchestrator — scan, analyze, transcode |
| `FFMpeg.py` | Full FFmpeg wrapper (2100+ lines) |
| `Subtitle_Manager.py` | Multi-language subtitle extraction and management |
| `Utils.py` | Shared utilities |
| `config.yaml` | YAML configuration |
| `config_loader.py` | Configuration loader |
| `pip_spy_max.py` | Dependency analyzer |

## Dependencies

- **FFmpeg / FFprobe** (system) — video processing
- `faster-whisper` — AI audio transcription
- `psutil` — system monitoring
- `rich` — terminal output
- `torch`, `transformers` — AI features
- `tqdm` — progress bars
- `pytest` — testing

## License

MIT
