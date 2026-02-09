# Local_LLM Architecture FAQ — Multi-Project Concerns

**Date:** 2026-02-09  
**Scope:** Answers to 4 critical architecture questions about sharing Local_LLM across projects.

---

## Current Project Landscape

| Project | UI Framework | LLM Integration Style | In-Process? |
|---------|-------------|----------------------|-------------|
| **RAG_RAT** | Streamlit | Re-implements FIFO worker locally, uses `LocalLLMManager` for discovery UI | Yes (own worker) |
| **Video_Transcode** | tkinter | `Utils.llm_query()` sync wrapper over `FIFOLlamaCppInference` | Yes |
| **MARKET_AI** | Streamlit | HTTP `LocalLLMAdapter` → `http://127.0.0.1:8001` | No — external server |
| **ZEN_AI_RAG** | Streamlit + NiceGUI | Direct imports of 8+ Local_LLM modules at module level | Yes |

---

## Question 1: Overload / Confusion — "Who asked, who gets the answer?"

### Short Answer: Not a problem today, but a ticking time bomb.

### How It Works

The loaded GGUF model is a **process-wide singleton** (`FIFOLlamaCppInference._shared_llm`). Load/unload is protected by a `threading.Lock`.

```
Process A (RAG_RAT)          Process B (Video_Transcode)        Process C (MARKET_AI)
┌─────────────────────┐     ┌─────────────────────┐           ┌──────────────────┐
│ Own copy of model   │     │ Own copy of model   │           │ HTTP client      │
│ in own VRAM/RAM     │     │ in own VRAM/RAM     │           │ → port 8001      │
│                     │     │                     │           │ (llama-server)   │
│ _shared_llm = Llama │     │ _shared_llm = Llama │           │ No model loaded  │
└─────────────────────┘     └─────────────────────┘           └──────────────────┘
         ↑ Isolated                  ↑ Isolated                     ↑ Separate
```

### Why It's Safe Today

- **Each project runs in its own Python process** (Streamlit = own process, Video_Transcode = own process, etc.). They never collide.
- **Within a single process**, if two threads call `llm_query()` simultaneously, they both hit the same loaded model. The C++ layer (`llama.cpp`) internally serializes inference — results never get mixed up, but the second caller blocks waiting for the first to finish.
- **MARKET_AI** talks to an external `llama-server` on port 8001, so it's truly decoupled.

### Where The Risk Lives

- If you ever refactor projects to share a single process (e.g., a unified dashboard), concurrent `query()` calls would serialize at the C++ level, causing slowdowns.
- The FIFO buffer in Local_LLM is for **metrics/observability only** — it does NOT serialize or queue requests.
- **Two projects loading different models simultaneously** would each consume full VRAM/RAM for their model. On a GPU with 8GB VRAM, two 4GB models = OOM.

### Recommendation

For now: **no action needed**. If you consolidate into fewer processes later, add a proper request queue (`asyncio.Queue` feeding a single inference worker) to serialize access.

---

## Question 2: How Do We Select the Proper LLM for Each Project?

### Short Answer: The machinery exists in Local_LLM but nobody uses it properly yet.

### What Local_LLM Provides

```python
# Model Discovery
registry = ModelRegistry(model_dir=Path("C:/AI/Models"))
registry.discover()

# Category-based selection
fast_models     = registry.get_cards_by_category(ModelCategory.FAST)
balanced_models = registry.get_cards_by_category(ModelCategory.BALANCED)
large_models    = registry.get_cards_by_category(ModelCategory.LARGE)

# Use-case recommendations
recs = registry.get_recommendations('coding')    # → List[ModelCard]
recs = registry.get_recommendations('reasoning')  # → List[ModelCard]
recs = registry.get_recommendations('fast')        # → List[ModelCard]

# Explicit model loading
engine = FIFOLlamaCppInference(model_path="C:/AI/Models/specific-model.gguf")

# Runtime model switching (thread-safe)
engine.switch_model("C:/AI/Models/different-model.gguf")
```

### What Actually Happens Today

| Project | Current Behavior | Problem |
|---------|-----------------|---------|
| **Video_Transcode** | Auto-discovers first small model (<5GB) | May pick a coding model for subtitle translation |
| **RAG_RAT** | Re-implements FIFO locally, uses `LocalLLMManager` only for UI | Doesn't use recommendations API |
| **ZEN_AI_RAG** | Hardcoded module-level load | No selection logic at all |
| **MARKET_AI** | Talks to hardcoded `localhost:8001` | Can't switch models |

### What Each Project Actually Needs

| Project | Ideal `use_case` | Why |
|---------|-----------------|-----|
| **RAG_RAT** | `'reasoning'` or `'quality'` | Deep document analysis, evidence chains, long context |
| **Video_Transcode** | `'fast'` | Quick filename/error prompts, low latency, simple tasks |
| **MARKET_AI** | `'reasoning'` | Financial analysis, multi-agent coordination |
| **ZEN_AI_RAG** | `'balanced'` | General conversational chat |

### How to Fix

Add a `model_preference` field to each project's `config.yaml`:

```yaml
# Video_Transcode/config.yaml
llm:
  enabled: true
  use_local_llm: true
  model_preference: fast        # ← NEW

# RAG_RAT/config/config.yaml
llm:
  model_preference: reasoning   # ← NEW
```

Then in each project's LLM initialization:

```python
# Instead of:
engine = FIFOLlamaCppInference()  # auto-discovers anything

# Do:
from local_llm import ModelRegistry, ModelCategory
registry = ModelRegistry()
registry.discover()
recs = registry.get_recommendations(config.llm.model_preference)
if recs:
    engine = FIFOLlamaCppInference(model_path=str(recs[0].path))
else:
    engine = FIFOLlamaCppInference()  # fallback to auto
```

---

## Question 3: Creating Executables — Will They Include Local_LLM?

### Short Answer: No. This is currently broken and needs fixing.

### The Problem

Every project imports Local_LLM via **`sys.path` hacks**:

```python
# What happens today (Video_Transcode/Utils.py, RAG_RAT/ui/state.py, etc.)
sys.path.insert(0, r"C:\Users\dvdze\Documents\_Python\Local_LLM")
from local_llm import FIFOLlamaCppInference  # filesystem import
```

PyInstaller/cx_Freeze **cannot follow `sys.path` hacks**. They trace static `import` statements. Your executable would launch and immediately crash with `ModuleNotFoundError: No module named 'local_llm'`.

### What Needs to Ship in an Executable

```
my_app/
├── my_app.exe
├── _internal/                    # PyInstaller's bundled dependencies
│   ├── local_llm/                # ← Must be included
│   │   ├── __init__.py
│   │   ├── Core/
│   │   │   └── services/
│   │   │       ├── inference_engine.py
│   │   │       ├── local_llm_manager.py
│   │   │       └── ...
│   │   └── ...
│   ├── llama_cpp/                # ← C++ bindings (platform-specific)
│   │   ├── libllama.dll          # ← Compiled binary
│   │   └── ...
│   └── ... other deps ...
└── models/                       # ← NOT bundled (2-8GB each!)
    └── phi-3-mini-Q4_K_M.gguf   # User provides separately
```

### Issues to Fix

| Issue | Detail | Fix |
|-------|--------|-----|
| **sys.path imports invisible to bundlers** | PyInstaller won't find `from local_llm import X` if it's only reachable via runtime `sys.path` | Make Local_LLM pip-installable |
| **pyproject.toml is broken** | `packages.find` only includes `Core*`, not the top-level `__init__.py` | Fix to include root package |
| **GGUF model files are 2-8 GB** | Cannot bundle inside an .exe | Ship separately, config points to `models/` dir next to .exe |
| **llama-cpp-python needs C++ binaries** | `libllama.dll` is platform-specific | PyInstaller `--collect-all llama_cpp` + test on target platform |
| **No dependency declared** | No project's `requirements.txt` references `local_llm` properly | Add path dependency |

### Step-by-Step Fix

**Step 1: Make Local_LLM properly pip-installable**

Fix `Local_LLM/pyproject.toml`:
```toml
[tool.setuptools.packages.find]
where = ["."]
include = ["Core*", "local_llm*"]  # ← Was missing top-level

# Or better: restructure to have a proper local_llm/ subdirectory
```

**Step 2: Install as editable in each project's venv**
```bash
cd Video_Transcode
pip install -e ../../../Local_LLM
```

**Step 3: Add PyInstaller hook** (`hook-local_llm.py`):
```python
from PyInstaller.utils.hooks import collect_all
datas, binaries, hiddenimports = collect_all('local_llm')
hiddenimports += ['llama_cpp']
```

**Step 4: Configure model path for portability**
```yaml
# config.yaml
llm:
  model_dir: "./models"  # relative to exe location
```

**Step 5: Build**
```bash
pyinstaller --additional-hooks-dir=hooks my_app.py
```

---

## Question 4: PyQt vs Streamlit vs NiceGUI — Is That a Problem?

### Short Answer: No. Zero conflict.

### Why It Doesn't Matter

Local_LLM's `FIFOLlamaCppInference` is a **pure Python/asyncio backend** with zero UI dependencies. The `query()` method returns an `AsyncGenerator[str, None]`. Each project wraps that however fits their UI:

```
┌──────────────────────────────────────────────────────┐
│                    Local_LLM                         │
│  FIFOLlamaCppInference.query(prompt) → AsyncGen[str] │
└──────────────────┬───────────────────────────────────┘
                   │
         ┌─────────┼──────────┬─────────────┐
         ▼         ▼          ▼             ▼
    Streamlit    tkinter    NiceGUI       PyQt
    st.write_    blocking   await         QThread +
    stream()     asyncio    natively      signal/slot
                 .run()
```

### How Each UI Consumes LLM Output

| UI Framework | Pattern | Example |
|-------------|---------|---------|
| **Streamlit** | `st.write_stream(generator)` | Streams tokens into a Streamlit text element |
| **tkinter** | `llm_query()` blocks synchronously via `asyncio.run()` | Video_Transcode does this today |
| **NiceGUI** | `async for chunk in engine.query(...)` | NiceGUI is natively async |
| **PyQt** | `QThread` runs `llm_query()`, emits `signal` per chunk | Main thread stays responsive |

### The One Gotcha: Streamlit Re-runs

Streamlit re-runs the **entire script** on every user interaction. If a Streamlit app creates a new `FIFOLlamaCppInference()` on every rerun, it would try to re-initialize the model each time.

**Fix:** Use `@st.cache_resource`:
```python
@st.cache_resource
def get_llm_engine():
    return FIFOLlamaCppInference()

engine = get_llm_engine()  # Created once, reused across reruns
```

RAG_RAT already does this correctly.

### Can You Mix UI Frameworks in One Project?

Yes, but you shouldn't. Each framework has its own event loop:
- Streamlit: runs in its own web server process
- tkinter: `mainloop()` blocks the thread
- NiceGUI: `uvicorn` async server
- PyQt: `QApplication.exec()` blocks the thread

They don't share state or conflict, but running two in one process would require threading or multiprocessing. Better to pick one per project.

---

## Summary Table

| Question | Verdict | Action Needed |
|----------|---------|---------------|
| **1. Overload/confusion** | Safe today (separate processes). Not serialized within a process. | None now. Add request queue if consolidating. |
| **2. Model selection** | Infrastructure exists but unused. | Add `model_preference` to each project's config.yaml |
| **3. Executables** | Broken. sys.path hacks invisible to bundlers. | Fix pyproject.toml, pip install -e, add PyInstaller hooks |
| **4. UI frameworks** | Zero conflict. Local_LLM is UI-agnostic. | None. Keep using `@st.cache_resource` in Streamlit apps. |

---

## Current Import Styles Across Projects

Two conventions exist — should be unified:

| Style | Used By | Example |
|-------|---------|---------|
| `from local_llm import ...` (lowercase) | RAG_RAT, Video_Transcode | `from local_llm import FIFOLlamaCppInference` |
| `from Local_LLM.xxx import ...` (uppercase) | ZEN_AI_RAG, archived code | `from Local_LLM.metrics import FIFO_Memory` |

**Recommendation:** Standardize on lowercase `from local_llm import ...` everywhere. This matches Python package naming conventions and will work correctly when Local_LLM is pip-installed.

---

*Analysis based on actual codebase inspection of Local_LLM, RAG_RAT, Video_Transcode, MARKET_AI, and ZEN_AI_RAG source code.*
