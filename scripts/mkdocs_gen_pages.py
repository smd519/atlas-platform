"""MkDocs hook mirroring root and module Markdown into the docs site at build time.

Module READMEs and ROADMAP.md live outside ``docs/``. Mirroring them virtually (instead of
pointing ``docs_dir`` at the repository root) keeps the sources of truth in place and avoids
publishing unrelated repository files such as ``.env.*`` or ``.venv``.
"""

from __future__ import annotations

from pathlib import Path

import mkdocs_gen_files

ROOT = Path(__file__).resolve().parents[1]


def _mirror(source: Path, target: str) -> None:
    with mkdocs_gen_files.open(target, "w") as handle:
        handle.write(source.read_text(encoding="utf-8"))


_mirror(ROOT / "ROADMAP.md", "roadmap.md")
_mirror(ROOT / "HDROADMAP.md", "HDROADMAP.md")

for readme in sorted(ROOT.glob("modules/*/README.md")):
    _mirror(readme, f"modules/{readme.parent.name}.md")

