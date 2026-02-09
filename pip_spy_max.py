#!/usr/bin/env python3
# pip_spy_max.py  —  Universal Python dependency analyzer & requirements generator
# Drop into any project root and run:  python pip_spy_max.py
# Zero external dependencies — uses only the Python standard library.
"""
Features:
  * Scans all .py files in a directory tree via AST (no regex hacks)
  * Detects every import with file + line location
  * Automatically excludes the project's own modules & packages
  * Resolves import names -> PyPI package names (130+ mappings)
  * Pins installed versions; omits pin if not installed
  * Annotates each line in requirements.txt with a short description
  * --clean-pycache   removes all __pycache__ folders before scanning
  * --find-orphans    reports .py files unreachable from entrypoints
  * --verbose         detailed per-import locations & frequency
  * --graph           Graphviz .dot dependency graph
  * --path <dir>      scan a project other than the script's own directory
  * --interactive     review/remove packages before writing
  * --merge <file>    merge with an existing requirements.txt
  * --ci              fail if unapproved packages detected (approved.txt)
"""
from __future__ import annotations

import ast
import argparse
import importlib.metadata
import os
import shutil
import sys
from collections import Counter, defaultdict
from pathlib import Path

__version__ = "3.0.0"

# --- stdlib detection --------------------------------------------------------
# Python 3.10+ exposes sys.stdlib_module_names.  For older runtimes we keep a
# comprehensive static fallback so the tool stays self-contained.

_STATIC_STDLIB = frozenset({
    "__future__", "_thread", "abc", "aifc", "argparse", "array", "ast",
    "asynchat", "asyncio", "asyncore", "atexit", "audioop", "base64",
    "bdb", "binascii", "binhex", "bisect", "builtins", "bz2", "calendar",
    "cgi", "cgitb", "chunk", "cmath", "cmd", "code", "codecs", "codeop",
    "collections", "colorsys", "compileall", "concurrent", "configparser",
    "contextlib", "contextvars", "copy", "copyreg", "cProfile", "crypt",
    "csv", "ctypes", "curses", "dataclasses", "datetime", "dbm", "decimal",
    "difflib", "dis", "distutils", "doctest", "email", "encodings",
    "enum", "errno", "faulthandler", "fcntl", "filecmp", "fileinput",
    "fnmatch", "fractions", "ftplib", "functools", "gc", "getopt",
    "getpass", "gettext", "glob", "graphlib", "grp", "gzip", "hashlib",
    "heapq", "hmac", "html", "http", "idlelib", "imaplib", "imghdr",
    "imp", "importlib", "inspect", "io", "ipaddress", "itertools", "json",
    "keyword", "lib2to3", "linecache", "locale", "logging", "lzma",
    "mailbox", "mailcap", "marshal", "math", "mimetypes", "mmap",
    "modulefinder", "multiprocessing", "netrc", "nis", "nntplib",
    "numbers", "operator", "optparse", "os", "ossaudiodev", "pathlib",
    "pdb", "pickle", "pickletools", "pipes", "pkgutil", "platform",
    "plistlib", "poplib", "posix", "posixpath", "pprint", "profile",
    "pstats", "pty", "pwd", "py_compile", "pyclbr", "pydoc", "queue",
    "quopri", "random", "re", "readline", "reprlib", "resource", "rlcompleter",
    "runpy", "sched", "secrets", "select", "selectors", "shelve", "shlex",
    "shutil", "signal", "site", "smtpd", "smtplib", "sndhdr", "socket",
    "socketserver", "spwd", "sqlite3", "sre_compile", "sre_constants",
    "sre_parse", "ssl", "stat", "statistics", "string", "stringprep",
    "struct", "subprocess", "sunau", "symtable", "sys", "sysconfig",
    "syslog", "tabnanny", "tarfile", "telnetlib", "tempfile", "termios",
    "test", "textwrap", "threading", "time", "timeit", "tkinter", "token",
    "tokenize", "tomllib", "trace", "traceback", "tracemalloc", "tty",
    "turtle", "turtledemo", "types", "typing", "unicodedata", "unittest",
    "urllib", "uu", "uuid", "venv", "warnings", "wave", "weakref",
    "webbrowser", "winreg", "winsound", "wsgiref", "xdrlib", "xml",
    "xmlrpc", "zipapp", "zipfile", "zipimport", "zlib", "zoneinfo",
    # internal / private
    "_abc", "_collections_abc", "_io", "_thread", "_bootsubprocess",
})

def _get_stdlib_names() -> frozenset:
    """Return the set of top-level stdlib module names."""
    if hasattr(sys, "stdlib_module_names"):          # Python 3.10+
        return sys.stdlib_module_names | _STATIC_STDLIB
    base = set(sys.builtin_module_names) | _STATIC_STDLIB
    return frozenset(base)

STDLIB_NAMES = _get_stdlib_names()

# --- import-name -> PyPI package mapping -------------------------------------
# Only entries where the import name != the pip install name.
PYPI_MAP = {
    # Imaging / Vision
    "cv2":                   "opencv-python",
    "PIL":                   "Pillow",
    "Image":                 "Pillow",
    "skimage":               "scikit-image",
    # ML / AI
    "sklearn":               "scikit-learn",
    "xgboost":               "xgboost",
    "lightgbm":              "lightgbm",
    "catboost":              "catboost",
    "tf":                    "tensorflow",
    "tensorflow":            "tensorflow",
    "torch":                 "torch",
    "torchvision":           "torchvision",
    "transformers":          "transformers",
    "sentence_transformers": "sentence-transformers",
    "huggingface_hub":       "huggingface-hub",
    "datasets":              "datasets",
    "tokenizers":            "tokenizers",
    "accelerate":            "accelerate",
    "diffusers":             "diffusers",
    "safetensors":           "safetensors",
    "peft":                  "peft",
    "bitsandbytes":          "bitsandbytes",
    "auto_gptq":             "auto-gptq",
    "faiss":                 "faiss-cpu",
    "faster_whisper":        "faster-whisper",
    "whisper":               "openai-whisper",
    "hf_transfer":           "hf-transfer",
    "openai":                "openai",
    "anthropic":             "anthropic",
    "langchain":             "langchain",
    "langchain_core":        "langchain-core",
    "langchain_community":   "langchain-community",
    "langchain_openai":      "langchain-openai",
    "llama_cpp":             "llama-cpp-python",
    "ctransformers":         "ctransformers",
    "chromadb":              "chromadb",
    "pinecone":              "pinecone-client",
    "weaviate":              "weaviate-client",
    "rank_bm25":             "rank-bm25",
    # Data / Serialization
    "yaml":                  "PyYAML",
    "toml":                  "toml",
    "tomli":                 "tomli",
    "ujson":                 "ujson",
    "orjson":                "orjson",
    "msgpack":               "msgpack",
    # Web scraping / parsing
    "bs4":                   "beautifulsoup4",
    "lxml":                  "lxml",
    "scrapy":                "scrapy",
    "selenium":              "selenium",
    "playwright":            "playwright",
    # Web frameworks
    "flask":                 "Flask",
    "django":                "Django",
    "fastapi":               "fastapi",
    "starlette":             "starlette",
    "uvicorn":               "uvicorn",
    "gunicorn":              "gunicorn",
    "sanic":                 "sanic",
    "bottle":                "bottle",
    "tornado":               "tornado",
    "nicegui":               "nicegui",
    # HTTP / networking
    "httpx":                 "httpx",
    "aiohttp":               "aiohttp",
    "requests":              "requests",
    "urllib3":               "urllib3",
    "websocket":             "websocket-client",
    "websockets":            "websockets",
    "paramiko":              "paramiko",
    "grpc":                  "grpcio",
    "zmq":                   "pyzmq",
    # Database / storage
    "pymongo":               "pymongo",
    "motor":                 "motor",
    "redis":                 "redis",
    "psycopg2":              "psycopg2-binary",
    "MySQLdb":               "mysqlclient",
    "mysql":                 "mysql-connector-python",
    "sqlalchemy":            "SQLAlchemy",
    "alembic":               "alembic",
    "peewee":                "peewee",
    "qdrant_client":         "qdrant-client",
    "qdrant":                "qdrant-client",
    # PDF / document
    "pymupdf":               "PyMuPDF",
    "fitz":                  "PyMuPDF",
    "pypdf":                 "pypdf",
    "pypdf2":                "PyPDF2",
    "PyPDF2":                "PyPDF2",
    "pdfplumber":            "pdfplumber",
    "docx":                  "python-docx",
    "openpyxl":              "openpyxl",
    "xlrd":                  "xlrd",
    "pytesseract":           "pytesseract",
    # Crypto / auth
    "Crypto":                "pycryptodome",
    "nacl":                  "PyNaCl",
    "jose":                  "python-jose",
    "jwt":                   "PyJWT",
    # CLI / UI / display
    "tqdm":                  "tqdm",
    "rich":                  "rich",
    "click":                 "click",
    "typer":                 "typer",
    "colorama":              "colorama",
    # Plotting
    "matplotlib":            "matplotlib",
    "seaborn":               "seaborn",
    "plotly":                "plotly",
    "bokeh":                 "bokeh",
    "altair":                "altair",
    # Data / science
    "numpy":                 "numpy",
    "np":                    "numpy",
    "pandas":                "pandas",
    "pd":                    "pandas",
    "scipy":                 "scipy",
    "sympy":                 "sympy",
    "statsmodels":           "statsmodels",
    "polars":                "polars",
    "pyarrow":               "pyarrow",
    "dask":                  "dask",
    # Audio / video
    "sounddevice":           "sounddevice",
    "soundfile":             "soundfile",
    "pyttsx3":               "pyttsx3",
    "pydub":                 "pydub",
    "librosa":               "librosa",
    "moviepy":               "moviepy",
    "ffmpeg":                "ffmpeg-python",
    # System / DevOps
    "psutil":                "psutil",
    "watchdog":              "watchdog",
    "dotenv":                "python-dotenv",
    "decouple":              "python-decouple",
    # Testing
    "pytest":                "pytest",
    "respx":                 "respx",
    "hypothesis":            "hypothesis",
    "freezegun":             "freezegun",
    "factory":               "factory-boy",
    "faker":                 "Faker",
    # Streamlit / Gradio
    "streamlit":             "streamlit",
    "gradio":                "gradio",
    # Misc
    "pydantic":              "pydantic",
    "attrs":                 "attrs",
    "marshmallow":           "marshmallow",
    "loguru":                "loguru",
    "arrow":                 "arrow",
    "pendulum":              "pendulum",
    "dateutil":              "python-dateutil",
    "regex":                 "regex",
    "chardet":               "chardet",
    "charset_normalizer":    "charset-normalizer",
    "certifi":               "certifi",
    "idna":                  "idna",
    "wrapt":                 "wrapt",
    "deprecated":            "Deprecated",
    "appdirs":               "appdirs",
    "platformdirs":          "platformdirs",
    "packaging":             "packaging",
    "setuptools":            "setuptools",
    "pip":                   "pip",
    "wheel":                 "wheel",
    "Cython":                "Cython",
    "numba":                 "numba",
    "joblib":                "joblib",
    "dill":                  "dill",
    "cloudpickle":           "cloudpickle",
    # cross-encoder
    "cross_encoder":         "sentence-transformers",
    # Telegram / messaging
    "telegram":              "python-telegram-bot",
    "telebot":               "pyTelegramBotAPI",
    "twilio":                "twilio",
    "slack_sdk":             "slack-sdk",
    "discord":               "discord.py",
    # Object detection
    "ultralytics":           "ultralytics",
    # RTF processing
    "striprtf":              "striprtf",
    # More web/doc
    "docutils":              "docutils",
    "sphinx":                "Sphinx",
    "mako":                  "Mako",
    "jinja2":                "Jinja2",
    "markupsafe":            "MarkupSafe",
    # ORM / validation
    "tortoise":              "tortoise-orm",
    "beanie":                "beanie",
}

# --- Short descriptions for common packages ----------------------------------
LIB_DESCRIPTIONS = {
    "opencv-python":         "Computer vision & image processing",
    "Pillow":                "Image manipulation",
    "scikit-image":          "Image processing algorithms",
    "scikit-learn":          "Machine learning toolkit",
    "xgboost":               "Gradient boosting framework",
    "tensorflow":            "Deep learning framework",
    "torch":                 "PyTorch deep learning",
    "torchvision":           "Vision models for PyTorch",
    "transformers":          "Hugging Face NLP models",
    "sentence-transformers": "Sentence & text embeddings",
    "huggingface-hub":       "Hugging Face model hub client",
    "datasets":              "Hugging Face datasets library",
    "faiss-cpu":             "Similarity search / vector index",
    "faster-whisper":        "Fast speech-to-text (CTranslate2)",
    "openai-whisper":        "OpenAI Whisper speech-to-text",
    "openai":                "OpenAI API client",
    "anthropic":             "Anthropic Claude API client",
    "langchain":             "LLM application framework",
    "langchain-core":        "LangChain core abstractions",
    "langchain-community":   "LangChain community integrations",
    "llama-cpp-python":      "llama.cpp Python bindings",
    "chromadb":              "Chroma vector database",
    "pinecone-client":       "Pinecone vector database client",
    "rank-bm25":             "BM25 ranking for information retrieval",
    "PyYAML":                "YAML parser & emitter",
    "beautifulsoup4":        "HTML/XML parsing",
    "lxml":                  "Fast XML/HTML processing",
    "Flask":                 "Lightweight web framework",
    "Django":                "Full-stack web framework",
    "fastapi":               "Modern async API framework",
    "uvicorn":               "ASGI server",
    "starlette":             "ASGI toolkit",
    "nicegui":               "Web-based GUI framework",
    "httpx":                 "Async-capable HTTP client",
    "aiohttp":               "Async HTTP client/server",
    "requests":              "HTTP client",
    "websocket-client":      "WebSocket client",
    "websockets":            "WebSocket client & server",
    "pymongo":               "MongoDB driver",
    "redis":                 "Redis client",
    "psycopg2-binary":       "PostgreSQL adapter",
    "SQLAlchemy":            "SQL toolkit & ORM",
    "qdrant-client":         "Qdrant vector database client",
    "PyMuPDF":               "PDF/document processing (MuPDF)",
    "pypdf":                 "Pure-Python PDF library",
    "PyPDF2":                "PDF processing (legacy)",
    "pdfplumber":            "PDF text/table extraction",
    "python-docx":           "Word document processing",
    "openpyxl":              "Excel file processing",
    "pytesseract":           "OCR (Tesseract wrapper)",
    "pycryptodome":          "Cryptographic primitives",
    "PyJWT":                 "JSON Web Tokens",
    "tqdm":                  "Progress bars",
    "rich":                  "Rich terminal formatting",
    "click":                 "CLI framework",
    "typer":                 "Modern CLI framework",
    "colorama":              "Cross-platform colored terminal text",
    "matplotlib":            "Plotting & visualization",
    "seaborn":               "Statistical data visualization",
    "plotly":                "Interactive plots",
    "numpy":                 "Numerical computing",
    "pandas":                "Data analysis & manipulation",
    "scipy":                 "Scientific computing",
    "polars":                "Fast DataFrame library",
    "pyarrow":               "Apache Arrow for columnar data",
    "sounddevice":           "Audio I/O",
    "pyttsx3":               "Offline text-to-speech",
    "pydub":                 "Audio manipulation",
    "librosa":               "Audio analysis",
    "psutil":                "System & process utilities",
    "watchdog":              "Filesystem event monitoring",
    "python-dotenv":         "Load .env files",
    "python-decouple":       "Settings management",
    "pytest":                "Testing framework",
    "respx":                 "HTTPX mock transport for tests",
    "hypothesis":            "Property-based testing",
    "Faker":                 "Fake data generation",
    "streamlit":             "Data app framework",
    "gradio":                "ML demo UI framework",
    "pydantic":              "Data validation with type hints",
    "loguru":                "Simplified logging",
    "python-dateutil":       "Date/time utilities",
    "regex":                 "Advanced regular expressions",
    "chardet":               "Character encoding detection",
    "packaging":             "Python packaging utilities",
    "setuptools":            "Build system / package tools",
    "joblib":                "Lightweight pipelining / caching",
    "hf-transfer":           "Fast Hugging Face downloads",
    "numba":                 "JIT compiler for numerical code",
    "python-telegram-bot":   "Telegram Bot API client",
    "pyTelegramBotAPI":      "Telegram Bot API (telebot)",
    "twilio":                "Twilio SMS/voice API client",
    "slack-sdk":             "Slack API client",
    "discord.py":            "Discord bot framework",
    "ultralytics":           "YOLOv8+ object detection",
    "striprtf":              "RTF to plain text conversion",
    "Jinja2":                "Template engine",
    "MarkupSafe":            "Safe string markup",
    "Sphinx":                "Documentation generator",
    "tortoise-orm":          "Async ORM for Python",
    "pip":                   "Python package installer",
}


# =============================================================================
#  Core logic
# =============================================================================

def parse_args():
    p = argparse.ArgumentParser(
        prog="pip_spy_max",
        description="Universal Python dependency analyzer & requirements generator.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Examples:\n"
               "  python pip_spy_max.py                       # scan cwd, write requirements.txt\n"
               "  python pip_spy_max.py --path ../MyProject   # scan another project\n"
               "  python pip_spy_max.py --verbose              # show every import location\n"
               "  python pip_spy_max.py --clean-pycache        # delete __pycache__ first\n",
    )
    p.add_argument("--path",           type=str, default=None,
                   help="Project root to scan (default: directory containing this script).")
    p.add_argument("--output", "-o",   type=str, default="requirements.txt",
                   help="Output filename (default: requirements.txt).")
    p.add_argument("--summary",        action="store_true", default=True,
                   help="Show summary of detected packages (default).")
    p.add_argument("--verbose", "-v",  action="store_true",
                   help="Show per-import file:line details & frequency.")
    p.add_argument("--exclude",        nargs="*", default=[],
                   help="Folder prefixes to skip (space-separated).")
    p.add_argument("--include",        nargs="*", default=[],
                   help="Only scan these folder prefixes.")
    p.add_argument("--merge",          type=str, default=None,
                   help="Merge with an existing requirements file (preserves comments/pins).")
    p.add_argument("--interactive",    action="store_true",
                   help="Interactively review packages before writing.")
    p.add_argument("--graph",          action="store_true",
                   help="Write import_graph.dot (Graphviz).")
    p.add_argument("--stats",          action="store_true",
                   help="Print usage-frequency statistics.")
    p.add_argument("--clean-pycache",  action="store_true",
                   help="Delete all __pycache__ folders before scanning.")
    p.add_argument("--find-orphans",   action="store_true",
                   help="Report .py files unreachable from detected entrypoints.")
    p.add_argument("--entrypoints",    nargs="*", default=None,
                   help="Entrypoint filenames for orphan detection (default: auto-detect).")
    p.add_argument("--ci",             action="store_true",
                   help="Fail (exit 1) if unapproved packages found (needs approved.txt).")
    p.add_argument("--no-pin",         action="store_true",
                   help="Do not pin versions in the output.")
    p.add_argument("--no-descriptions", action="store_true",
                   help="Do not add # description comments in the output.")
    # ── Project health & structure ──
    p.add_argument("--health",         action="store_true",
                   help="Run full project health check (structure, junk, essentials, misplaced files).")
    p.add_argument("--check-structure", action="store_true",
                   help="Analyze project directory structure and suggest improvements.")
    p.add_argument("--check-junk",     action="store_true",
                   help="Detect junk, backup, and temp files.")
    p.add_argument("--check-essentials", action="store_true",
                   help="Check for missing essential files (README, .gitignore, LICENSE, etc.).")
    p.add_argument("--check-misplaced", action="store_true",
                   help="Detect files that are likely in the wrong directory.")
    p.add_argument("--fix-gitignore",  action="store_true",
                   help="Generate or update .gitignore with Python best practices.")
    p.add_argument("--version",        action="version", version=f"%(prog)s {__version__}")
    return p.parse_args()


# --- File collection ----------------------------------------------------------

_ALWAYS_SKIP = frozenset({
    ".git", ".hg", ".svn", "__pycache__", ".mypy_cache", ".pytest_cache",
    ".tox", ".nox", ".eggs", "node_modules",
    "venv", ".venv", "env", ".env",
    "site-packages", "dist-packages",
    "_archive", "_Old", "_old", "_bin",
})

def collect_py_files(root, args):
    """Walk *root* and return .py files respecting include/exclude rules."""
    results = []

    def _skip_dir(name, rel):
        if name in _ALWAYS_SKIP or name.endswith(".egg-info"):
            return True
        if args.include and not any(rel.startswith(p) for p in args.include):
            return True
        if args.exclude and any(rel.startswith(p) for p in args.exclude):
            return True
        return False

    for dirpath, dirnames, filenames in os.walk(root):
        rel_dir = os.path.relpath(dirpath, root)
        # prune dirs in-place
        dirnames[:] = [
            d for d in dirnames
            if not _skip_dir(d, os.path.join(rel_dir, d) if rel_dir != "." else d)
        ]
        for fn in filenames:
            if fn.endswith(".py") and not fn.startswith("pip_spy"):
                results.append(Path(dirpath) / fn)
    return results


# --- Import parsing -----------------------------------------------------------

def parse_imports(py_files):
    """Return {top_level_module: [(filepath, lineno), ...]}."""
    locs = defaultdict(list)
    total = len(py_files)
    for i, fpath in enumerate(py_files, 1):
        print(f"  [{i}/{total}] {fpath.name}", end="\r")
        try:
            source = fpath.read_text(encoding="utf-8", errors="ignore")
            tree = ast.parse(source, filename=str(fpath))
        except (SyntaxError, ValueError):
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    mod = alias.name.split(".")[0]
                    locs[mod].append((str(fpath), node.lineno))
            elif isinstance(node, ast.ImportFrom) and node.module:
                mod = node.module.split(".")[0]
                locs[mod].append((str(fpath), node.lineno))
    print()  # clear \r line
    return dict(locs)


# --- Local module detection ---------------------------------------------------

def find_local_modules(root, py_files):
    """Detect names that are local project modules/packages, not third-party.
    
    Covers:
    - Top-level .py files (their stem is a local module)
    - Directories with __init__.py (Python packages)
    - Directories containing .py files (implicit namespace packages / source trees)
    - Any directory name that appears as rel_path parts[0] of scanned .py files
    - Sub-module names: any .py file stem or directory name anywhere in the tree
      that appears as an import (catches 'from controllers import ...' inside
      sub-packages like API/)
    """
    local = set()
    
    # 1. Top-level .py files and directories
    for item in root.iterdir():
        if item.is_file() and item.suffix == ".py":
            name = item.stem
            if name.isidentifier() and not name.startswith("_"):
                local.add(name)
        elif item.is_dir():
            name = item.name
            if name.isidentifier() and not name.startswith("."):
                # If the directory has ANY .py file or __init__.py, it's local
                has_python = (item / "__init__.py").exists() or any(item.rglob("*.py"))
                if has_python:
                    local.add(name)
    
    # 2. Any top-level directory name that scanned .py files live under
    for f in py_files:
        try:
            rel = f.relative_to(root)
        except ValueError:
            continue
        if len(rel.parts) > 1:
            top = rel.parts[0]
            if top.isidentifier() and not top.startswith("."):
                local.add(top)
    
    # 3. ALL directory names and .py file stems in the entire project tree.
    #    If someone does 'from models import Foo' and models/ exists anywhere
    #    in the project, it's a local module — not a PyPI package.
    #    Exception: names that are known PyPI packages (in PYPI_MAP values or
    #    have an installed distribution) are kept as third-party.
    known_pypi = set(PYPI_MAP.keys()) | set(PYPI_MAP.values())
    for dirpath, dirnames, filenames in os.walk(root):
        # Skip irrelevant directories
        rel_dir = os.path.relpath(dirpath, root)
        base_dir = rel_dir.split(os.sep)[0] if rel_dir != "." else ""
        if base_dir in _ALWAYS_SKIP:
            dirnames.clear()
            continue
        for d in dirnames:
            if d.isidentifier() and not d.startswith(".") and d not in _ALWAYS_SKIP:
                if d in known_pypi:
                    continue    # don't shadow a real PyPI package
                # Only add if the directory has python files
                dpath = Path(dirpath) / d
                if (dpath / "__init__.py").exists() or any(dpath.glob("*.py")):
                    local.add(d)
        for fn in filenames:
            if fn.endswith(".py"):
                stem = fn[:-3]
                if stem.isidentifier() and not stem.startswith("_"):
                    if stem not in known_pypi:
                        local.add(stem)
    
    return frozenset(local)


# --- Filtering ----------------------------------------------------------------

def filter_third_party(import_locs, local_modules):
    """Keep only genuine third-party imports.
    
    A module is considered third-party if:
    - It's not in stdlib
    - It's not a detected local module (case-insensitive)
    - AND at least one of:
      (a) It has a known PyPI mapping (PYPI_MAP)
      (b) It's installed locally (importlib.metadata can find it)
    Otherwise it's likely a dead/missing local import and is skipped.
    """
    local_lower = {m.lower() for m in local_modules}
    result = {}
    for mod, locs in import_locs.items():
        if mod in STDLIB_NAMES:
            continue
        if mod in local_modules or mod.lower() in local_lower:
            continue
        # Skip obviously-wrong names
        if not mod.isidentifier() or mod.startswith("_"):
            continue
        # Must be either a known PyPI package or actually installed
        pkg = resolve_name(mod)
        if mod not in PYPI_MAP and get_installed_version(pkg) is None:
            continue   # not a real third-party package
        result[mod] = locs
    return result


# --- Resolution & versioning --------------------------------------------------

def resolve_name(mod):
    """Map an import name to its PyPI package name."""
    return PYPI_MAP.get(mod, mod)


def get_installed_version(pkg):
    """Return installed version or None."""
    # Try the resolved name first, then common case variations
    for candidate in (pkg, pkg.lower(), pkg.replace("-", "_"), pkg.replace("_", "-")):
        try:
            return importlib.metadata.version(candidate)
        except importlib.metadata.PackageNotFoundError:
            continue
    return None


def format_requirement(pkg, pin=True):
    """Return a requirements line like 'package==1.2.3' or just 'package'."""
    if pin:
        ver = get_installed_version(pkg)
        if ver:
            return f"{pkg}=={ver}"
    return pkg


# --- Merge --------------------------------------------------------------------

def merge_with_existing(new_lines, merge_path):
    """Merge new requirements with an existing file, preserving comments & pins."""
    existing_pkgs = {}          # lowercase-name -> original line
    if os.path.exists(merge_path):
        with open(merge_path, "r", encoding="utf-8") as f:
            for raw in f:
                line = raw.strip()
                if not line or line.startswith("#"):
                    continue
                name = line.split("==")[0].split(">=")[0].split("<=")[0].split("~=")[0].split("!=")[0].strip()
                existing_pkgs[name.lower()] = raw.rstrip("\n")

    # Build merged list: existing pins win, new packages are appended
    seen = set()
    merged = []
    for raw_line in new_lines:
        line = raw_line.strip()
        if line.startswith("#") or not line:
            merged.append(line)
            continue
        name = line.split("==")[0].split(">=")[0].strip()
        key = name.lower()
        if key in seen:
            continue
        seen.add(key)
        if key in existing_pkgs:
            merged.append(existing_pkgs[key])       # keep existing pin
        else:
            merged.append(line)
    # Add anything from the old file that we didn't emit
    for key, old_line in existing_pkgs.items():
        if key not in seen:
            merged.append(old_line)
            seen.add(key)
    return sorted(merged, key=lambda l: l.lstrip("#").strip().lower())


# --- Interactive review -------------------------------------------------------

def interactive_review(pkgs):
    """Let the user remove packages interactively."""
    while True:
        print("\n  Packages to include:")
        for i, p in enumerate(pkgs, 1):
            print(f"    {i:3d}. {p}")
        choice = input("\n  Remove # (or Enter to accept): ").strip()
        if not choice:
            break
        try:
            idx = int(choice) - 1
            if 0 <= idx < len(pkgs):
                removed = pkgs.pop(idx)
                print(f"  x Removed {removed}")
        except ValueError:
            print("  Invalid input -- enter a number or press Enter.")
    return pkgs


# --- Pycache cleanup ----------------------------------------------------------

def clean_pycache(root):
    count = 0
    for dirpath, dirnames, _ in os.walk(root):
        for d in list(dirnames):
            if d == "__pycache__":
                target = Path(dirpath) / d
                try:
                    shutil.rmtree(target)
                    count += 1
                except OSError:
                    pass
    return count


# --- Orphan file detection ----------------------------------------------------

_DEFAULT_ENTRYPOINTS = frozenset({
    "main.py", "__main__.py", "app.py", "app_new.py", "manage.py",
    "wsgi.py", "asgi.py", "server.py", "run.py", "start.py",
    "setup.py", "conftest.py",
})

def find_orphans(root, py_files, import_locs, custom_entrypoints=None):
    """Return .py files not reachable from any entrypoint via import graph."""
    ep_names = set(custom_entrypoints) if custom_entrypoints else set(_DEFAULT_ENTRYPOINTS)

    file_to_module = {}
    module_to_files = defaultdict(set)
    for f in py_files:
        try:
            rel = f.relative_to(root)
        except ValueError:
            continue
        top = rel.parts[0].replace(".py", "") if len(rel.parts) == 1 else rel.parts[0]
        file_to_module[f] = top
        module_to_files[top].add(f)

    # Build adjacency: file -> set of files it imports
    file_imports = defaultdict(set)
    for mod, locs in import_locs.items():
        targets = module_to_files.get(mod, set())
        for src_path_str, _ in locs:
            src = Path(src_path_str)
            file_imports[src].update(targets)

    # BFS from entrypoints
    entrypoints = [f for f in py_files if f.name in ep_names]
    reachable = set()
    queue = list(entrypoints)
    while queue:
        current = queue.pop(0)
        if current in reachable:
            continue
        reachable.add(current)
        # Also mark all files in the same package as reachable
        mod = file_to_module.get(current)
        if mod:
            for peer in module_to_files.get(mod, set()):
                if peer not in reachable:
                    queue.append(peer)
        for dep in file_imports.get(current, set()):
            if dep not in reachable:
                queue.append(dep)

    return sorted(f for f in py_files if f not in reachable)


# --- Graph output -------------------------------------------------------------

def write_graph(third_party, root):
    dot_path = root / "import_graph.dot"
    with open(dot_path, "w", encoding="utf-8") as f:
        f.write("digraph imports {\n  rankdir=LR;\n  node [shape=box];\n")
        for mod, locs in third_party.items():
            pkg = resolve_name(mod)
            seen_files = set()
            for fpath, _ in locs:
                short = os.path.relpath(fpath, root)
                if short not in seen_files:
                    seen_files.add(short)
                    f.write(f'  "{short}" -> "{pkg}";\n')
        f.write("}\n")
    print(f"  > Graph written to {dot_path}")


# =============================================================================
#  Project health & structure analysis
# =============================================================================

# ── Junk / backup / temp file detection ──────────────────────────────────────

_JUNK_EXTENSIONS = frozenset({
    ".pyc", ".pyo", ".bak", ".swp", ".swo", ".tmp", ".temp",
    ".orig", ".rej", ".log", ".DS_Store",
})

_JUNK_PATTERNS = (
    # backup naming patterns
    lambda n: n.endswith("~"),                    # file~ (editor backup)
    lambda n: ".~" in n,                          # file.~py
    lambda n: n.startswith("~$"),                 # ~$file (Office lock)
    lambda n: n == "Thumbs.db",
    lambda n: n == ".DS_Store",
    lambda n: n == "desktop.ini",
    lambda n: n.endswith(".pyc"),
    lambda n: n.startswith(".#"),                  # Emacs lock files
    lambda n: n == "New Text Document.txt",        # Windows default
)

_JUNK_DIR_NAMES = frozenset({
    "__pycache__", ".mypy_cache", ".pytest_cache", ".tox", ".nox",
    ".eggs", "*.egg-info", ".ipynb_checkpoints",
})

def find_junk_files(root):
    """Find junk, backup, temp, and cache files/directories in the project."""
    junk_files = []
    junk_dirs = []
    
    for dirpath, dirnames, filenames in os.walk(root):
        rel = os.path.relpath(dirpath, root)
        # Skip .git internals
        if ".git" in rel.split(os.sep):
            dirnames.clear()
            continue
        
        # Check directory names
        for d in list(dirnames):
            if d in _JUNK_DIR_NAMES or d.endswith(".egg-info"):
                junk_dirs.append(os.path.join(rel, d) if rel != "." else d)
        
        # Check files
        for fn in filenames:
            _, ext = os.path.splitext(fn)
            is_junk = ext.lower() in _JUNK_EXTENSIONS
            if not is_junk:
                is_junk = any(p(fn) for p in _JUNK_PATTERNS)
            if is_junk:
                junk_files.append(os.path.join(rel, fn) if rel != "." else fn)
    
    return junk_files, junk_dirs


# ── Missing essentials check ─────────────────────────────────────────────────

_ESSENTIAL_FILES = {
    "README.md": {
        "alts": ["README.rst", "README.txt", "README"],
        "why":  "Project overview for GitHub & newcomers",
        "priority": "HIGH",
    },
    ".gitignore": {
        "alts": [],
        "why":  "Prevents committing pycache, venv, secrets, etc.",
        "priority": "HIGH",
    },
    "requirements.txt": {
        "alts": ["pyproject.toml", "setup.cfg", "Pipfile"],
        "why":  "Declares project dependencies",
        "priority": "HIGH",
    },
    "LICENSE": {
        "alts": ["LICENSE.md", "LICENSE.txt", "LICENCE"],
        "why":  "Legal terms for open-source distribution",
        "priority": "MEDIUM",
    },
    ".streamlit/config.toml": {
        "alts": [],
        "why":  "Streamlit app configuration",
        "priority": "LOW",
        "condition": "streamlit",  # only check if streamlit is a dependency
    },
    "pytest.ini": {
        "alts": ["pyproject.toml", "setup.cfg", "tox.ini"],
        "why":  "Test configuration",
        "priority": "LOW",
        "condition": "pytest",
    },
}

def check_essentials(root, third_party_names=None):
    """Check for missing essential project files."""
    missing = []
    present = []
    third_party_names = third_party_names or set()
    
    for name, info in _ESSENTIAL_FILES.items():
        # Check condition (only check if the related package is used)
        cond = info.get("condition")
        if cond and cond not in third_party_names:
            continue
        
        found = False
        found_as = name
        
        # Check primary name
        if (root / name).exists():
            found = True
        else:
            # Check alternatives
            for alt in info["alts"]:
                if (root / alt).exists():
                    found = True
                    found_as = alt
                    break
        
        if found:
            present.append((name, found_as))
        else:
            missing.append((name, info["why"], info["priority"]))
    
    return present, missing


# ── Misplaced files detection ────────────────────────────────────────────────

# Rules: (file pattern check, expected location, description)
_PLACEMENT_RULES = [
    # Test files should be in tests/
    {
        "check": lambda rel, fn: fn.startswith("test_") and fn.endswith(".py"),
        "expected_dirs": {"tests", "test", "Tests"},
        "description": "Test file should be in tests/ directory",
    },
    # conftest.py can be at root or in tests/
    {
        "check": lambda rel, fn: fn == "conftest.py",
        "expected_dirs": {".", "tests", "test", "Tests"},
        "description": "conftest.py should be at root or in tests/",
    },
    # Documentation files
    {
        "check": lambda rel, fn: fn.endswith(".md") and fn not in {"README.md", "CHANGELOG.md", "CONTRIBUTING.md", "CODE_OF_CONDUCT.md"},
        "expected_dirs": {"docs", "Docs", "doc", "."},
        "description": "Documentation should be in docs/ directory",
    },
    # Shell scripts / batch files at root or scripts/
    {
        "check": lambda rel, fn: fn.endswith((".sh", ".bat", ".ps1")) and not fn.startswith("."),
        "expected_dirs": {".", "scripts", "bin", "tools"},
        "description": "Scripts should be at root or in scripts/ directory",
    },
    # Config/settings files
    {
        "check": lambda rel, fn: fn in {"config.yaml", "config.yml", "config.json", "config.toml", "settings.yaml", "settings.json"},
        "expected_dirs": {".", "config", "conf", ".streamlit"},
        "description": "Config files should be at root or in config/ directory",
    },
]

def find_misplaced_files(root):
    """Detect files that appear to be in the wrong directory."""
    misplaced = []
    
    for dirpath, dirnames, filenames in os.walk(root):
        rel_dir = os.path.relpath(dirpath, root)
        # Skip hidden/internal dirs
        parts = rel_dir.split(os.sep) if rel_dir != "." else []
        if any(p.startswith(".") or p.startswith("_") for p in parts):
            continue
        
        top_dir = parts[0] if parts else "."
        
        for fn in filenames:
            for rule in _PLACEMENT_RULES:
                if rule["check"](rel_dir, fn):
                    if top_dir not in rule["expected_dirs"]:
                        rel_file = os.path.join(rel_dir, fn) if rel_dir != "." else fn
                        expected = " or ".join(sorted(rule["expected_dirs"] - {"."})) or "project root"
                        misplaced.append((rel_file, rule["description"], expected))
                    break  # only match first rule per file
    
    return misplaced


# ── Project structure analysis ───────────────────────────────────────────────

_STANDARD_DIRS = {
    "tests":    "Unit & integration tests",
    "docs":     "Documentation",
    "scripts":  "Helper/maintenance scripts",
    "config":   "Configuration files",
    "ui":       "UI components (Streamlit, etc.)",
    "i18n":     "Internationalization / locales",
}

def analyze_structure(root, py_files, third_party):
    """Analyze overall project structure and return a health report."""
    report = {
        "score": 100,
        "issues": [],
        "good": [],
        "stats": {},
    }
    
    # Count files by top-level directory
    dir_counts = Counter()
    root_file_count = 0
    total_lines = 0
    largest_files = []
    
    for f in py_files:
        try:
            rel = f.relative_to(root)
        except ValueError:
            continue
        
        top = str(rel.parts[0]) if len(rel.parts) > 1 else "."
        dir_counts[top] += 1
        
        if len(rel.parts) == 1:
            root_file_count += 1
        
        # Count lines and find largest files
        try:
            lines = f.read_text(encoding="utf-8", errors="ignore").count("\n") + 1
            total_lines += lines
            largest_files.append((str(rel), lines))
        except OSError:
            pass
    
    largest_files.sort(key=lambda x: x[1], reverse=True)
    
    report["stats"] = {
        "total_py_files": len(py_files),
        "total_lines": total_lines,
        "root_files": root_file_count,
        "directories": dict(dir_counts),
        "largest_files": largest_files[:10],
        "third_party_count": len(third_party),
    }
    
    # ── Check: too many files at root ──
    if root_file_count > 10:
        report["issues"].append(
            f"ROOT CLUTTER: {root_file_count} Python files at project root. "
            f"Consider moving source code into a src/ or core/ package."
        )
        report["score"] -= min(15, root_file_count - 10)
    elif root_file_count <= 5:
        report["good"].append(f"Clean root: only {root_file_count} Python files at project root.")
    
    # ── Check: tests directory exists ──
    tests_dir = any(d in dir_counts for d in ("tests", "test", "Tests"))
    if tests_dir:
        test_count = sum(dir_counts.get(d, 0) for d in ("tests", "test", "Tests"))
        report["good"].append(f"Tests directory present with {test_count} test file(s).")
    else:
        # Check for test files elsewhere
        scattered_tests = [f for f in py_files if f.name.startswith("test_")]
        if scattered_tests:
            report["issues"].append(
                f"SCATTERED TESTS: {len(scattered_tests)} test file(s) found outside tests/ directory."
            )
            report["score"] -= 10
        else:
            report["issues"].append("NO TESTS: No test files or tests/ directory found.")
            report["score"] -= 15
    
    # ── Check: docs directory ──
    docs_exists = (root / "docs").exists() or (root / "Docs").exists() or (root / "doc").exists()
    if docs_exists:
        report["good"].append("Documentation directory present.")
    else:
        report["issues"].append("NO DOCS: Consider adding a docs/ directory for project documentation.")
        report["score"] -= 5
    
    # ── Check: very large files ──
    for fpath, lines in largest_files[:5]:
        if lines > 1000:
            report["issues"].append(
                f"LARGE FILE: {fpath} has {lines:,} lines. Consider splitting into modules."
            )
            report["score"] -= 3
    
    # ── Check: __init__.py presence in packages ──
    for dirpath, dirnames, filenames in os.walk(root):
        rel = os.path.relpath(dirpath, root)
        if rel == ".":
            continue
        parts = rel.split(os.sep)
        if any(p.startswith(".") or p in _ALWAYS_SKIP for p in parts):
            continue
        py_in_dir = [f for f in filenames if f.endswith(".py")]
        if py_in_dir and "__init__.py" not in filenames:
            # Only flag if it looks like a Python package (not scripts/)
            if parts[0] not in ("scripts", "bin", "tools", "docs", "Docs"):
                report["issues"].append(
                    f"MISSING __init__.py: {rel}/ has {len(py_in_dir)} Python files but no __init__.py"
                )
                report["score"] -= 2
    
    report["score"] = max(0, report["score"])
    return report


# ── .gitignore generator ─────────────────────────────────────────────────────

_GITIGNORE_PYTHON = """\
# === Python ===
__pycache__/
*.py[cod]
*$py.class
*.so
*.egg-info/
*.egg
dist/
build/
eggs/
*.whl

# === Virtual environments ===
venv/
.venv/
env/
ENV/

# === IDE / Editor ===
.vscode/
.idea/
*.swp
*.swo
*~
.#*

# === OS ===
.DS_Store
Thumbs.db
desktop.ini

# === Testing ===
.pytest_cache/
.mypy_cache/
.coverage
htmlcov/
.tox/
.nox/

# === Project-specific ===
*.log
logs/
cache/
uploads/
*.tmp
*.bak
*.old

# === Secrets (NEVER commit) ===
.env
*.key
*.pem
secrets/
"""

def generate_gitignore(root):
    """Generate or update .gitignore with Python best practices."""
    gitignore_path = root / ".gitignore"
    
    if gitignore_path.exists():
        existing = gitignore_path.read_text(encoding="utf-8")
        # Find lines in template that are missing from existing
        existing_lines = {l.strip() for l in existing.splitlines() if l.strip() and not l.startswith("#")}
        template_lines = [l for l in _GITIGNORE_PYTHON.splitlines()]
        
        additions = []
        for line in template_lines:
            stripped = line.strip()
            if stripped and not stripped.startswith("#") and stripped not in existing_lines:
                additions.append(line)
        
        if additions:
            with open(gitignore_path, "a", encoding="utf-8", newline="\n") as f:
                f.write("\n# === Added by pip_spy_max ===\n")
                for line in additions:
                    f.write(line + "\n")
            return "updated", len(additions)
        else:
            return "complete", 0
    else:
        with open(gitignore_path, "w", encoding="utf-8", newline="\n") as f:
            f.write(_GITIGNORE_PYTHON)
        return "created", _GITIGNORE_PYTHON.count("\n")


# ── Print health report ──────────────────────────────────────────────────────

def print_health_report(root, py_files, third_party, third_party_names):
    """Run all health checks and print a comprehensive report."""
    
    print(f"\n  {'='*56}")
    print(f"    PROJECT HEALTH REPORT")
    print(f"  {'='*56}\n")
    
    # 1. Structure analysis
    report = analyze_structure(root, py_files, third_party)
    stats = report["stats"]
    
    score = report["score"]
    if score >= 90:
        grade = "A  (Excellent)"
    elif score >= 75:
        grade = "B  (Good)"
    elif score >= 60:
        grade = "C  (Needs work)"
    elif score >= 40:
        grade = "D  (Poor)"
    else:
        grade = "F  (Critical)"
    
    print(f"    Health Score:  {score}/100  {grade}")
    print(f"    {'─'*50}")
    
    # Stats
    print(f"\n    PROJECT STATS")
    print(f"      Python files:      {stats['total_py_files']:>6,}")
    print(f"      Lines of code:     {stats['total_lines']:>6,}")
    print(f"      Root-level files:  {stats['root_files']:>6}")
    print(f"      Third-party pkgs:  {stats['third_party_count']:>6}")
    
    # Directory breakdown
    print(f"\n    DIRECTORY BREAKDOWN")
    for d, count in sorted(stats["directories"].items(), key=lambda x: -x[1]):
        label = f"      {d + '/':<25s}" if d != "." else f"      {'(root)':<25s}"
        print(f"{label} {count:>4} files")
    
    # Largest files
    if stats["largest_files"]:
        print(f"\n    LARGEST FILES")
        for fpath, lines in stats["largest_files"][:8]:
            marker = " (!)" if lines > 1000 else ""
            print(f"      {fpath:<40s} {lines:>6,} lines{marker}")
    
    # Good things
    if report["good"]:
        print(f"\n    GOOD")
        for g in report["good"]:
            print(f"      [+] {g}")
    
    # Issues
    if report["issues"]:
        print(f"\n    ISSUES")
        for issue in report["issues"]:
            print(f"      [-] {issue}")
    
    # 2. Essential files
    present, missing = check_essentials(root, third_party_names)
    print(f"\n    ESSENTIAL FILES")
    for name, found_as in present:
        label = f" (as {found_as})" if found_as != name else ""
        print(f"      [+] {name}{label}")
    for name, why, priority in missing:
        print(f"      [-] {name}  [{priority}]  {why}")
        if priority == "HIGH":
            report["score"] = max(0, report["score"] - 5)
    
    # 3. Misplaced files
    misplaced = find_misplaced_files(root)
    if misplaced:
        print(f"\n    MISPLACED FILES")
        for fpath, desc, expected in misplaced[:20]:
            print(f"      [-] {fpath}")
            print(f"           -> {desc}  (expected in: {expected})")
    else:
        print(f"\n    MISPLACED FILES")
        print(f"      [+] All files appear to be in appropriate directories.")
    
    # 4. Junk files
    junk_files, junk_dirs = find_junk_files(root)
    if junk_files or junk_dirs:
        print(f"\n    JUNK / TEMP FILES")
        if junk_dirs:
            print(f"      Cached directories ({len(junk_dirs)}):")
            for d in junk_dirs[:10]:
                print(f"        {d}/")
            if len(junk_dirs) > 10:
                print(f"        ... and {len(junk_dirs) - 10} more")
        if junk_files:
            print(f"      Temp/backup files ({len(junk_files)}):")
            for f_name in junk_files[:15]:
                print(f"        {f_name}")
            if len(junk_files) > 15:
                print(f"        ... and {len(junk_files) - 15} more")
    else:
        print(f"\n    JUNK / TEMP FILES")
        print(f"      [+] No junk files detected. Clean project!")
    
    # Final score (recalc)
    print(f"\n    {'─'*50}")
    print(f"    Final Score:  {report['score']}/100  {grade}")
    print(f"  {'='*56}\n")
    
    return report


# =============================================================================
#  Main
# =============================================================================

def main():
    args = parse_args()
    root = Path(args.path).resolve() if args.path else Path(__file__).parent.resolve()

    if not root.is_dir():
        print(f"ERROR: {root} is not a directory.")
        sys.exit(1)

    print(f"\n{'='*60}")
    print(f"  pip_spy_max v{__version__} -- scanning {root.name}/")
    print(f"{'='*60}\n")

    # -- Step 0: clean __pycache__ --
    if args.clean_pycache:
        n = clean_pycache(root)
        print(f"  Cleaned {n} __pycache__ folder(s)\n")

    # -- Step 1: collect files --
    py_files = collect_py_files(root, args)
    print(f"  Found {len(py_files)} Python files\n")
    if not py_files:
        print("  Nothing to scan.")
        return

    # -- Step 2: parse imports --
    print("  Parsing imports...")
    import_locs = parse_imports(py_files)

    # -- Step 3: detect local modules & filter --
    local_modules = find_local_modules(root, py_files)
    third_party = filter_third_party(import_locs, local_modules)
    counter = Counter({mod: len(locs) for mod, locs in third_party.items()})
    unique = sorted(third_party.keys(), key=lambda m: resolve_name(m).lower())

    print(f"  {len(unique)} third-party packages detected")
    if local_modules:
        display = sorted(local_modules)[:15]
        print(f"  {len(local_modules)} local modules excluded: "
              f"{', '.join(display)}"
              f"{'...' if len(local_modules) > 15 else ''}")

    # -- Step 4: build output lines --
    output_lines = [
        f"# Auto-generated by pip_spy_max v{__version__}",
        f"# Project: {root.name}",
        f"# {len(unique)} third-party packages detected",
        "",
    ]
    req_only = []                    # for interactive / merge
    not_installed = []

    for mod in unique:
        pkg = resolve_name(mod)
        line = format_requirement(pkg, pin=not args.no_pin)
        desc = LIB_DESCRIPTIONS.get(pkg)

        if not args.no_descriptions and desc:
            output_lines.append(f"# {desc}")
        output_lines.append(line)
        req_only.append(line)

        if "==" not in line and not args.no_pin:
            not_installed.append(pkg)

    # -- Step 5: merge / interactive --
    if args.merge:
        merged = merge_with_existing(output_lines, args.merge)
        output_lines = merged

    if args.interactive:
        req_only = interactive_review(req_only)
        # Rebuild output_lines from surviving packages
        output_lines = [
            f"# Auto-generated by pip_spy_max v{__version__}",
            f"# Project: {root.name}", "",
        ]
        for line in req_only:
            pkg = line.split("==")[0].strip()
            desc = LIB_DESCRIPTIONS.get(pkg)
            if not args.no_descriptions and desc:
                output_lines.append(f"# {desc}")
            output_lines.append(line)

    # -- Step 6: write output --
    out_path = Path(args.output)
    if not out_path.is_absolute():
        out_path = root / args.output
    with open(out_path, "w", encoding="utf-8", newline="\n") as f:
        for line in output_lines:
            f.write(line + "\n")
    print(f"\n  Written -> {out_path}  ({len(unique)} packages)")

    if not_installed:
        print(f"  WARNING: {len(not_installed)} package(s) not installed locally (no version pin):")
        for p in not_installed:
            print(f"       - {p}")

    # -- Optional outputs --
    if args.graph:
        write_graph(third_party, root)

    if args.stats:
        print("\n  Package usage frequency:")
        for mod, count in counter.most_common():
            print(f"       {resolve_name(mod):30s} {count:4d} imports")

    if args.verbose:
        print(f"\n  Detailed import report ({len(unique)} packages):")
        for mod in unique:
            pkg = resolve_name(mod)
            print(f"\n  {pkg}  ({counter[mod]} import{'s' if counter[mod]>1 else ''}):")
            for fpath, lineno in third_party[mod]:
                short = os.path.relpath(fpath, root)
                print(f"    {short}:{lineno}")
    elif args.summary:
        print(f"\n  Third-party packages:")
        for mod in unique:
            pkg = resolve_name(mod)
            ver = get_installed_version(pkg)
            desc = LIB_DESCRIPTIONS.get(pkg, "")
            ver_str = f"  v{ver}" if ver else "  (not installed)"
            desc_str = f"  -- {desc}" if desc else ""
            print(f"       {pkg:30s}{ver_str}{desc_str}")

    # -- Orphan detection --
    if args.find_orphans:
        orphans = find_orphans(root, py_files, import_locs, args.entrypoints)
        if orphans:
            print(f"\n  {len(orphans)} potentially orphaned file(s):")
            for o in orphans:
                short = os.path.relpath(o, root)
                print(f"       {short}")
        else:
            print("\n  No orphaned files detected.")

    # -- CI mode --
    if args.ci:
        approved_path = root / "approved.txt"
        approved = set()
        if approved_path.exists():
            approved = {
                l.strip().lower()
                for l in approved_path.read_text(encoding="utf-8").splitlines()
                if l.strip() and not l.startswith("#")
            }
        detected = {resolve_name(m).lower() for m in unique}
        unapproved = detected - approved
        if unapproved:
            print(f"\n  CI FAIL -- {len(unapproved)} unapproved package(s):")
            for p in sorted(unapproved):
                print(f"       {p}")
            sys.exit(1)
        else:
            print("\n  CI PASS -- all packages approved.")

    print(f"\n{'='*60}\n")


if __name__ == "__main__":
    main()
