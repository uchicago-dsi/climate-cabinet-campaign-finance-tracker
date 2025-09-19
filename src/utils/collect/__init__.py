"""Modules for scraping campgian related data from US States."""

import importlib
from pathlib import Path

for subdir in Path(__file__).parent.iterdir():
    if subdir.is_dir() and subdir.stem != "__pycache__":
        for p in subdir.glob("*.py"):
            if p.stem not in ["__init__"]:
                importlib.import_module(f"{__name__}.{subdir.stem}.{p.stem}")
