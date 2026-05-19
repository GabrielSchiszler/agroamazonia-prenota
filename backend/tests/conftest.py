"""Pytest: path e env para importar Lambdas (ex.: update_metrics.handler)."""

import os
import sys

_backend_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_src_root = os.path.join(_backend_root, "src")
_lambdas_root = os.path.join(_backend_root, "lambdas")

for _p in (_backend_root, _src_root, _lambdas_root):
    if _p not in sys.path:
        sys.path.insert(0, _p)

os.environ.setdefault("TABLE_NAME", "test-table")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
