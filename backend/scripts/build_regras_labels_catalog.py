#!/usr/bin/env python3
"""Gera regras_labels_catalog.json para API (src/utils) e frontend."""

from __future__ import annotations

import json
import sys
from pathlib import Path

_SCRIPT_DIR = Path(__file__).resolve().parent
_BACKEND = _SCRIPT_DIR.parent
sys.path.insert(0, str(_BACKEND))

from src.utils.regras_labels import build_regras_labels_catalog  # noqa: E402

OUT_PATHS = [
    _BACKEND / "src" / "utils" / "regras_labels_catalog.json",
    _BACKEND.parent / "frontend" / "regras_labels_catalog.json",
]


def main() -> None:
    catalog = build_regras_labels_catalog()
    payload = json.dumps(catalog, ensure_ascii=False, indent=2)
    for path in OUT_PATHS:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(payload + "\n", encoding="utf-8")
        print(f"Wrote {path} ({len(catalog)} regras)")


if __name__ == "__main__":
    main()
