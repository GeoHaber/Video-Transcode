# -*- coding: utf-8 -*-
"""Configuration management for FFmpeg transcoding pipeline."""

from __future__ import annotations
import os
import yaml
from pathlib import Path
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field, asdict

@dataclass
class ScanConfig:
    """Scanning configuration."""
    root_directories: List[str] = field(default_factory=list)
    file_extensions: List[str] = field(default_factory=lambda: [
        ".avi", ".flv", ".av1", ".m4v", ".mkv", ".mov", ".mp4",
        ".ts", ".mts", ".webm", ".wmv"
    ])
    parallel_scan: bool = True
    max_scan_workers: int = 8
    scan_cache_enabled: bool = True

@dataclass
class EncodingConfig:
    """Encoding configuration."""
    target_codec: str = "hevc"
    bits_per_pixel: float = 0.045
    target_crf: int = 23
    hardware_encoder_priority: List[str] = field(default_factory=lambda: ["hevc_nvenc", "hevc_qsv", "hevc_amf", "libx265"])
    force_10bit: bool = True
    size_ok_margin: float = 1.2
    tag_hevc_as_hvc1: bool = True

@dataclass
class LanguageConfig:
    """Language handling configuration."""
    default_language: str = "eng"
    keep_languages: List[str] = field(default_factory=lambda: ["eng", "fre", "ger", "heb", "hun", "ita", "jpn", "rum", "rus", "spa"])
    ensure_english_subtitle: bool = True
    ai_subtitle_languages: List[str] = field(default_factory=list)
    ai_audio_languages: List[str] = field(default_factory=list)

@dataclass
class ArtifactConfig:
    """Artifact generation configuration."""
    enabled: bool = False
    force_on_skip: bool = True
    enable_matrix: bool = True
    enable_speed_up: bool = True
    enable_short_clip: bool = True
    matrix_columns: int = 4
    matrix_rows: int = 3
    matrix_width: int = 320
    matrix_start_time: float = 30.0
    short_duration: float = 33.0
    speed_factor: float = 2.0
    quality_crf: int = 24

@dataclass
class ProcessingConfig:
    """Processing behavior configuration."""
    parallel_processing: bool = False
    max_workers: int = 1
    pause_on_exit: bool = True
    error_logs_enabled: bool = True
    smart_rename: bool = False
    check_corruption: bool = False

@dataclass
class LLMConfig:
    """LLM integration configuration."""
    enabled: bool = True
    use_local_llm: bool = True

@dataclass
class OutputConfig:
    """Output paths configuration."""
    work_directory: Optional[str] = None
    exceptions_directory: Optional[str] = None

@dataclass
class Config:
    """Main configuration container."""
    scanning: ScanConfig = field(default_factory=ScanConfig)
    encoding: EncodingConfig = field(default_factory=EncodingConfig)
    languages: LanguageConfig = field(default_factory=LanguageConfig)
    artifacts: ArtifactConfig = field(default_factory=ArtifactConfig)
    processing: ProcessingConfig = field(default_factory=ProcessingConfig)
    llm: LLMConfig = field(default_factory=LLMConfig)
    output: OutputConfig = field(default_factory=OutputConfig)

    @classmethod
    def from_yaml(cls, path: Path) -> Config:
        """Load configuration from YAML file."""
        if not path.exists():
            return cls()  # Return defaults

        with open(path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f) or {}

        def _safe_init(klass, section_data):
            """Init dataclass using only keys it actually declares."""
            valid = {f.name for f in klass.__dataclass_fields__.values()}
            return klass(**{k: v for k, v in section_data.items() if k in valid})

        return cls(
            scanning=_safe_init(ScanConfig, data.get('scanning', {})),
            encoding=_safe_init(EncodingConfig, data.get('encoding', {})),
            languages=_safe_init(LanguageConfig, data.get('languages', {})),
            artifacts=_safe_init(ArtifactConfig, data.get('artifacts', {})),
            processing=_safe_init(ProcessingConfig, data.get('processing', {})),
            llm=_safe_init(LLMConfig, data.get('llm', {})),
            output=_safe_init(OutputConfig, data.get('output', {})),
        )

    def to_yaml(self, path: Path) -> None:
        """Save configuration to YAML file."""
        data = {
            'scanning': asdict(self.scanning),
            'encoding': asdict(self.encoding),
            'languages': asdict(self.languages),
            'artifacts': asdict(self.artifacts),
            'processing': asdict(self.processing),
            'llm': asdict(self.llm),
            'output': asdict(self.output),
        }

        with open(path, 'w', encoding='utf-8') as f:
            yaml.dump(data, f, default_flow_style=False, indent=2, sort_keys=False)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            'scanning': asdict(self.scanning),
            'encoding': asdict(self.encoding),
            'languages': asdict(self.languages),
            'artifacts': asdict(self.artifacts),
            'processing': asdict(self.processing),
            'llm': asdict(self.llm),
            'output': asdict(self.output),
        }

    def validate(self) -> List[str]:
        """Validate configuration and return list of issues."""
        issues = []

        # Validate root directories exist
        for dir_path in self.scanning.root_directories:
            if not Path(dir_path).exists():
                issues.append(f"Root directory does not exist: {dir_path}")

        # Validate numeric ranges
        if not 0.0 < self.encoding.bits_per_pixel < 1.0:
            issues.append(f"bits_per_pixel should be between 0 and 1, got {self.encoding.bits_per_pixel}")

        if not 0 < self.encoding.target_crf < 51:
            issues.append(f"target_crf should be between 0 and 51, got {self.encoding.target_crf}")

        if self.processing.max_workers < 1:
            issues.append(f"max_workers must be >= 1, got {self.processing.max_workers}")

        return issues

# Global config instance
_config: Optional[Config] = None

def load_config(config_path: Optional[Path] = None) -> Config:
    """Load configuration from file or create default."""
    global _config
    
    if config_path is None:
        # Look for config.yaml in script directory
        script_dir = Path(__file__).parent
        config_path = script_dir / "config.yaml"
    
    _config = Config.from_yaml(config_path)
    
    # Validate
    issues = _config.validate()
    if issues:
        print("⚠️  Configuration warnings:")
        for issue in issues:
            print(f"  - {issue}")
    
    return _config

def get_config() -> Config:
    """Get the current configuration."""
    global _config
    if _config is None:
        load_config()
    return _config
