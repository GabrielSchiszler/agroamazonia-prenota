#!/usr/bin/env python3
"""
Upload de teste: 1 PDF com texto visível (fixture) para o ambiente dev.

O arquivo ``fixtures/local_visible_sample.pdf`` contém linhas de texto; abra-o
localmente antes de rodar o upload para confirmar que o conteúdo aparece no leitor.

Reutiliza ``simulate_duplicate_uploads_dev.py`` (OAuth / headers / presigned + PUT).

Uso (recomendado a partir de ``backend/``):

  cd backend
  python3 scripts/local_upload_visible_pdf_dev.py

Só pedir presigned + listar ``file_key`` (sem PUT no S3):

  python3 scripts/local_upload_visible_pdf_dev.py --dry-presigned-only

Demais flags são repassadas ao script de simulação (ex.: ``--api-url ...``).
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

_FIXTURE = Path(__file__).resolve().parent / "fixtures" / "local_visible_sample.pdf"


def main() -> int:
    if not _FIXTURE.is_file():
        print("Fixture ausente:", _FIXTURE, file=sys.stderr)
        return 2
    simulate = Path(__file__).resolve().parent / "simulate_duplicate_uploads_dev.py"
    cmd = [
        sys.executable,
        str(simulate),
        "--file",
        str(_FIXTURE),
        "--count",
        "1",
    ]
    cmd.extend(sys.argv[1:])
    print("PDF local (abra no visualizador para ver o texto):", flush=True)
    print(" ", _FIXTURE.resolve(), flush=True)
    print(flush=True)
    print("Executando:", " ".join(cmd), flush=True)
    print(flush=True)
    return int(subprocess.call(cmd))


if __name__ == "__main__":
    raise SystemExit(main())
