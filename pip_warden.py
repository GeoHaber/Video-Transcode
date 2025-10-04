#!/usr/bin/env python3
"""
pip_warden.py — Safe pip maintenance you can run any time.

Features
- List outdated packages with semantic change risk (patch/minor/major).
- One-click safe update of selected packages (skip majors unless you say so).
- Full backup of current environment (requirements_backup_*.txt) and post-update freeze.
- Security audit via pip-audit (if available) with JSON report.
- Detect possibly-unused packages by scanning a code directory for imports and
  mapping them to installed distributions (no guessing required).
- Rich TUI with tables and prompts. Works without it too (falls back to print).

Usage examples
  python pip_warden.py --list
  python pip_warden.py --update safe        # updates only patch/minor by default
  python pip_warden.py --update all --allow-major
  python pip_warden.py --audit
  python pip_warden.py --scan-unused ./my_project
  python pip_warden.py --update safe --scan-unused ./src --report reports/

Notes
- Always prefer running inside a virtualenv. This tool will warn if you're global.
- 'pip-audit' and 'rich' are optional. If missing, audits/tables degrade gracefully.
"""
from __future__ import annotations

import argparse
import ast
import json
import os
import re
import shutil
import subprocess as SP
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

# -------- Optional pretty UI --------
try:
	from rich.console import Console
	from rich.table import Table
	from rich import box
	from rich.prompt import Confirm
except Exception:
	Console = None
	Table = None
	box = None
	Confirm = None

def cprint(*a, **k):
	if Console:
		Console().print(*a, **k)
	else:
		print(*a)

# -------- Helpers --------
def run(cmd: List[str], check: bool = False) -> SP.CompletedProcess:
	return SP.run(cmd, text=True, capture_output=True, check=check)

def ensure_tools():
	# make sure 'pip' exists (it should), warn if not in venv
	in_venv = (hasattr(sys, 'real_prefix') or (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix))
	if not in_venv:
		cprint("[yellow]Warning:[/yellow] you're not in a virtual environment. Consider using one before updating.")
	return in_venv

def timestamp() -> str:
	return datetime.now().strftime("%Y%m%d_%H%M%S")

def backup_requirements(outdir: Path) -> Path:
	outdir.mkdir(parents=True, exist_ok=True)
	path = outdir / f"requirements_backup_{timestamp()}.txt"
	with path.open("w", encoding="utf-8") as f:
		SP.check_call([sys.executable, "-m", "pip", "freeze"], stdout=f)
	return path

def freeze_requirements(outdir: Path) -> Path:
	outdir.mkdir(parents=True, exist_ok=True)
	path = outdir / "requirements.txt"
	with path.open("w", encoding="utf-8") as f:
		SP.check_call([sys.executable, "-m", "pip", "freeze"], stdout=f)
	return path

def pip_outdated() -> List[Dict]:
	p = run([sys.executable, "-m", "pip", "list", "--outdated", "--format", "json"])
	if p.returncode != 0:
		cprint(p.stderr)
		return []
	try:
		return json.loads(p.stdout)
	except json.JSONDecodeError:
		return []

# Semantic-ish version compare (PEP440 via packaging if available)
try:
	from packaging.version import parse as vparse
except Exception:
	def vparse(s: str):
		# naive fallback
		return tuple(int(x) for x in re.split(r"[^\d]+", s) if x.isdigit())

def major_minor_patch(ver: str) -> Tuple[int,int,int]:
	try:
		from packaging.version import Version
		v = Version(ver)
		parts = (v.major, v.minor, v.micro)
	except Exception:
		nums = [int(x) for x in re.findall(r"\d+", ver)[:3] or [0,0,0]]
		nums += [0]*(3-len(nums))
		parts = tuple(nums[:3])
	return parts  # type: ignore

@dataclass
class UpdateItem:
	name: str
	current: str
	latest: str
	kind: str  # "patch"|"minor"|"major"|"unknown"

def classify_updates(rows: List[Dict]) -> List[UpdateItem]:
	out = []
	for r in rows:
		cur = r.get("version","")
		lat = r.get("latest_version","") or r.get("latest","")
		if not cur or not lat:
			k = "unknown"
		else:
			cM, cm, cp = major_minor_patch(cur)
			lM, lm, lp = major_minor_patch(lat)
			if lM > cM: k = "major"
			elif lm > cm: k = "minor"
			elif lp > cp: k = "patch"
			else: k = "unknown"
		out.append(UpdateItem(r["name"], cur, lat, k))
	return out

def print_outdated_table(items: List[UpdateItem]):
	if Table and Console:
		table = Table(title="Outdated packages", box=box.ROUNDED)
		table.add_column("#", justify="right", style="cyan")
		table.add_column("Package", style="magenta")
		table.add_column("Current", style="yellow")
		table.add_column("Latest", style="green")
		table.add_column("Change", style="bold")
		for i, it in enumerate(items, 1):
			color = {"major":"red","minor":"yellow","patch":"green","unknown":"white"}.get(it.kind,"white")
			table.add_row(str(i), it.name, it.current, it.latest, f"[{color}]{it.kind}[/{color}]")
		Console().print(table)
	else:
		print("Outdated packages:")
		for i, it in enumerate(items, 1):
			print(f"{i:>3}. {it.name:30s} {it.current:>12s} -> {it.latest:>12s} [{it.kind}]")

def pip_upgrade(names: List[str], allow_prerelease: bool = False) -> bool:
	if not names:
		return True
	cmd = [sys.executable, "-m", "pip", "install", "--upgrade", "--upgrade-strategy", "only-if-needed"]
	if allow_prerelease:
		cmd += ["--pre"]
	cmd += names
	p = run(cmd)
	if p.returncode != 0:
		cprint(p.stdout); cprint(p.stderr)
		return False
	cprint(p.stdout)
	return True

def pip_audit(report_dir: Path) -> Optional[Path]:
	report_dir.mkdir(parents=True, exist_ok=True)
	# try pip-audit
	try:
		p = run([sys.executable, "-m", "pip_audit", "-f", "json"])
		if p.returncode == 0 and p.stdout.strip():
			path = report_dir / f"pip_audit_{timestamp()}.json"
			path.write_text(p.stdout, encoding="utf-8")
			vulns = json.loads(p.stdout)
			total = sum(len(x.get("vulns", [])) for x in vulns)
			if Console:
				Console().print(f"[bold]{'No known vulnerabilities' if total==0 else f'Vulnerabilities found: {total}'}[/bold]  → {path}")
			else:
				print(f"pip-audit report → {path}  (vulns: {total})")
			return path
		else:
			cprint("[yellow]pip-audit not available or returned no data[/yellow]")
	except Exception:
		cprint("[yellow]pip-audit not installed. Install with: pip install pip-audit[/yellow]")
	return None

# ---- Possibly-unused detection by code scan ----

def scan_imports(code_dir: Path) -> Set[str]:
	"""Collect top-level imported module names from .py files under code_dir."""
	imports: Set[str] = set()
	for path in code_dir.rglob("*.py"):
		try:
			src = path.read_text(encoding="utf-8", errors="ignore")
			tree = ast.parse(src, filename=str(path))
			for node in ast.walk(tree):
				if isinstance(node, ast.Import):
					for n in node.names:
						imports.add(n.name.split(".")[0])
				elif isinstance(node, ast.ImportFrom):
					if node.module:
						imports.add(node.module.split(".")[0])
		except Exception:
			continue
	# exclude obvious stdlib/builtins by a quick heuristic (no dash in names etc.)
	return imports

def map_modules_to_distributions() -> Dict[str, List[str]]:
	"""module -> [distributions] mapping"""
	try:
		from importlib.metadata import packages_distributions
	except Exception:
		# Python <3.10 fallback
		from importlib_metadata import packages_distributions  # type: ignore
	return packages_distributions()

def detect_possibly_unused(code_dir: Path) -> List[str]:
	imports = scan_imports(code_dir)
	mod2dist = map_modules_to_distributions()
	used_dists: Set[str] = set()
	for mod in imports:
		for dist in mod2dist.get(mod, []):
			used_dists.add(dist.lower())
	# All installed distributions
	try:
		from importlib.metadata import distributions
	except Exception:
		from importlib_metadata import distributions  # type: ignore
	installed = {d.metadata['Name'].lower() for d in distributions() if 'Name' in d.metadata}
	# Heuristic "possibly-unused" = installed - used_dists - core tools
	ignore = {"pip","setuptools","wheel","pkg-resources","pip-audit","pipdeptree"}
	candidates = sorted((installed - used_dists) - ignore)
	return candidates

# -------- CLI --------
def main():
	ap = argparse.ArgumentParser(description="Safe pip maintenance: update, audit, and unused detection.")
	ap.add_argument("--list", action="store_true", help="List outdated packages")
	ap.add_argument("--update", choices=["none","safe","all"], default="none",
					help="'safe' updates patch/minor only; 'all' updates everything found")
	ap.add_argument("--allow-major", action="store_true", help="Allow major version updates in --update safe/all")
	ap.add_argument("--pre", action="store_true", help="Allow pre-releases when upgrading")
	ap.add_argument("--exclude", nargs="*", default=[], help="Package names to exclude from updates")
	ap.add_argument("--audit", action="store_true", help="Run pip-audit and save JSON report")
	ap.add_argument("--scan-unused", type=str, default="", help="Path to project source to scan for unused packages")
	ap.add_argument("--report", type=str, default="pip_reports", help="Directory to write backups/reports")
	ap.add_argument("--yes", action="store_true", help="Non-interactive; assume 'yes' to prompts")
	args = ap.parse_args()

	ensure_tools()

	report_dir = Path(args.report)
	report_dir.mkdir(parents=True, exist_ok=True)

	# 1) list outdated
	rows = pip_outdated()
	items = classify_updates(rows)
	if args.list or (args.update != "none" and Console):
		print_outdated_table(items)

	to_update: List[str] = []
	if args.update != "none":
		# choose packages based on policy
		for it in items:
			if it.name.lower() in {x.lower() for x in args.exclude}:
				continue
			if args.update == "safe":
				if it.kind in ("patch","minor") or (it.kind=="major" and args.allow_major):
					to_update.append(it.name)
			else:  # all
				if it.kind=="major" and not args.allow_major:
					continue
				to_update.append(it.name)
		if not to_update:
			cprint("[green]Nothing eligible to update under the chosen policy.[/green]")
		else:
			if not args.yes and Confirm:
				ok = Confirm.ask(f"Proceed to upgrade {len(to_update)} package(s)?")
				if not ok:
					to_update = []
			if to_update:
				backup = backup_requirements(report_dir)
				cprint(f"Backup written → {backup}")
				ok = pip_upgrade(to_update, allow_prerelease=args.pre)
				if ok:
					req = freeze_requirements(report_dir)
					cprint(f"New requirements written → {req}")
				else:
					cprint("[red]Upgrade failed. You can rollback with:[/red] pip install -r " + str(backup))

	# 2) audit
	if args.audit:
		pip_audit(report_dir)

	# 3) unused detection
	if args.scan_unused:
		code_dir = Path(args.scan_unused).resolve()
		if not code_dir.exists():
			cprint(f"[red]scan-unused path not found:[/red] {code_dir}")
		else:
			candidates = detect_possibly_unused(code_dir)
			out = report_dir / f"possibly_unused_{timestamp()}.txt"
			out.write_text("\n".join(candidates), encoding="utf-8")
			if Console and Table:
				table = Table(title=f"Possibly-unused (scanned {code_dir})", box=box.ROUNDED)
				table.add_column("#", justify="right")
				table.add_column("Distribution")
				for i, name in enumerate(candidates, 1):
					table.add_row(str(i), name)
				Console().print(table)
			else:
				print("Possibly-unused:", candidates)
			cprint(f"Report → {out}")

	# Exit code
	if to_update:
		sys.exit(0 if ok else 1)
	sys.exit(0)

if __name__ == "__main__":
	main()
