"""
manifest.py
===========
Intake gate for a landing run. A run folder contains data files plus a
``_READY_<run_id>.json`` manifest that is written LAST by the extract process.
Nothing is loaded unless the manifest says READY and every file matches its
declared checksum, row count and column list.

Pure Python - no Spark dependency.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class ValidationResult:
    ok: bool = True
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def error(self, msg: str) -> None:
        self.ok = False
        self.errors.append(msg)


def find_manifest(run_dir: str | Path) -> Path:
    matches = sorted(Path(run_dir).glob("_READY_*.json"))
    if not matches:
        raise FileNotFoundError(f"No _READY_*.json manifest in {run_dir}")
    return matches[0]


def load_manifest(run_dir: str | Path) -> dict[str, Any]:
    with open(find_manifest(run_dir), "r", encoding="utf-8") as fh:
        return json.load(fh)


def sha256_file(path: str | Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def count_tsv_rows(path: str | Path) -> int:
    """Data rows = non-empty lines minus the header."""
    n = 0
    with open(path, "rb") as fh:
        for line in fh:
            if line.strip():
                n += 1
    return max(n - 1, 0)


def read_tsv_header(path: str | Path) -> list[str]:
    with open(path, "r", encoding="utf-8-sig", newline="") as fh:
        return fh.readline().rstrip("\r\n").split("\t")


def validate_run(
    run_dir: str | Path,
    manifest: dict[str, Any] | None = None,
    check_checksums: bool = True,
) -> ValidationResult:
    """Validate every file in the manifest. Returns a ValidationResult."""
    res = ValidationResult()
    run_dir = Path(run_dir)
    try:
        manifest = manifest or load_manifest(run_dir)
    except (FileNotFoundError, ValueError) as exc:
        res.error(f"manifest unreadable: {exc}")
        return res

    if manifest.get("status") != "READY":
        res.error(f"manifest status is {manifest.get('status')!r}, expected 'READY'")

    files = manifest.get("files", [])
    if manifest.get("file_count") is not None and manifest["file_count"] != len(files):
        res.error(f"file_count={manifest['file_count']} but {len(files)} files listed")

    total_rows = 0
    total_bytes = 0
    for f in files:
        name = f["file_name"]
        path = run_dir / name
        if not path.exists():
            res.error(f"{name}: missing")
            continue

        size = path.stat().st_size
        total_bytes += size
        if f.get("file_size_bytes") is not None and f["file_size_bytes"] != size:
            res.error(f"{name}: size {size} != manifest {f['file_size_bytes']}")

        if check_checksums and f.get("checksum"):
            if sha256_file(path) != f["checksum"]:
                res.error(f"{name}: checksum mismatch")

        if f.get("format") == "TSV":
            rows = count_tsv_rows(path)
            if f.get("columns") and read_tsv_header(path) != f["columns"]:
                res.error(f"{name}: header columns differ from manifest")
        elif f.get("format") == "JSON":
            with open(path, "r", encoding="utf-8") as fh:
                rows = len(json.load(fh).get(f.get("root_key", "hierarchies"), []))
        else:
            res.warnings.append(f"{name}: unknown format {f.get('format')!r}, row count skipped")
            continue

        total_rows += rows
        if f.get("row_count") is not None and f["row_count"] != rows:
            res.error(f"{name}: row_count {rows} != manifest {f['row_count']}")

    if manifest.get("total_row_count") is not None and manifest["total_row_count"] != total_rows:
        res.error(f"total_row_count {total_rows} != manifest {manifest['total_row_count']}")
    if manifest.get("total_size_bytes") is not None and manifest["total_size_bytes"] != total_bytes:
        res.warnings.append(f"total_size_bytes {total_bytes} != manifest {manifest['total_size_bytes']}")
    return res
