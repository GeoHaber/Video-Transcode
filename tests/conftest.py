# -*- coding: utf-8 -*-
"""
Shared test fixtures and path setup for Video_Transcode tests.
"""
import sys
from pathlib import Path

# Add project root to sys.path once — all tests import from here
PROJECT_DIR = Path(__file__).resolve().parent.parent
if str(PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_DIR))
