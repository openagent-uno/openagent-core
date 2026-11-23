"""Enumerate an explicitly approved Markdown corpus from a sealed Git vault."""

from collections import Counter
from hashlib import sha256
from pathlib import Path

from openagent_core.contracts import canonical_json
from openagent_core.memory.vault.documents import note_path
from .migration import MANIFEST, _digest, _relative, verify_snapshot
from .preflight import _git_read, _vault_inventory


class VaultImportMismatch(ValueError):
    """An aggregate or source assertion failed. Messages contain no note data."""


class VaultSource:
    def __init__(self, snapshot, vault, paths, *, working_tree_only=False, absent_vault=False):
        self.root = Path(snapshot).resolve()
        self.receipt = _digest(self.root / MANIFEST)
        self.manifest = verify_snapshot(self.root)
        if not self.manifest.get("fence_id"):
            raise VaultImportMismatch("Host writer fence required")
        self.relative = str(_relative(vault))
        self.vault = self.root / self.relative
        self.git = self.vault / ".git"
        if type(working_tree_only) is not bool or type(absent_vault) is not bool or (absent_vault and working_tree_only):
            raise TypeError("Expected an explicit source history mode")
        self.absent_vault = absent_vault
        self.working_tree_only = working_tree_only or absent_vault
        if working_tree_only and self.git.exists():
            raise VaultImportMismatch("Working-tree-only custody cannot exclude existing Git history")
        if (self.git / "info/grafts").exists():
            raise VaultImportMismatch(
                "Source history has an external ancestry override"
            )
        if isinstance(paths, str):
            raise TypeError("Explicit note paths are required")
        self.paths = frozenset(note_path(p, allow_generated=True) for p in paths)

        self.corpus = sha256(canonical_json(sorted(self.paths)).encode()).hexdigest()
        self.inventory = _vault_inventory(self.root, self.manifest, self.relative,
                                          not working_tree_only, history_only=absent_vault)
        if absent_vault and self.paths:
            raise VaultImportMismatch("A sealed absent vault cannot supply document revisions")
        if not self.paths and not absent_vault and (not working_tree_only or self.inventory["markdown_files"]):
            raise ValueError("An explicit historical corpus is required")
        self.verify_receipt()

    def verify_receipt(self):
        if _digest(self.root / MANIFEST) != self.receipt:
            raise VaultImportMismatch("Snapshot manifest changed")

    def verify_files(self):
        self.verify_receipt()
        verify_snapshot(self.root)
        self.verify_receipt()

    def _git(self, *args):
        return _git_read(self.git, *args)

    def _content(self, data):
        if len(data) > 2 << 20 or b"\x00" in data:
            raise VaultImportMismatch(
                "A selected revision exceeds the document contract"
            )
        return data.decode("utf-8")

    def _facts(self, commit, parents):
        raw = self._git("cat-file", "commit", commit)
        header, _, message = raw.partition(b"\n\n")
        author = next(
            line[7:] for line in header.splitlines() if line.startswith(b"author ")
        )
        # Original signed commit bytes, DAG, trees and full message remain in
        # the host archive. Only recognized provenance fields enter this ACL domain.
        allowed = {"origin", "session", "workflow", "task", "run", "tool", "user"}
        trailers = []
        for line in reversed(message.rstrip(b"\n").splitlines()):
            key, colon, value = line.partition(b":")
            if not colon:
                break
            if key.decode("ascii", errors="ignore").lower() in allowed:
                trailers.append([key.decode("ascii"), value.lstrip().decode("utf-8")])
        return dict(
            source="git",
            commit=commit,
            parents=parents,
            author=author.decode("utf-8"),
            origin=list(reversed(trailers)),
        )

    def revisions(self):
        counts = Counter()
        self.graph = {}
        nearest = {}
        commits = self._git(
            "rev-list", "--topo-order", "--reverse", "--parents", "--all", "--reflog"
        ).splitlines() if not self.working_tree_only else []
        for line in commits:
            commit, *parents = line.decode("ascii").split()
            parent = nearest.get(parents[0]) if parents else None
            nearest[commit] = parent
            revisions = (parents[0], commit) if parents else ("--root", commit)
            raw = self._git(
                "diff-tree",
                "--no-commit-id",
                "--no-ext-diff",
                "--no-textconv",
                "--no-abbrev",
                "-r",
                "-z",
                "-M",
                *revisions,
            )
            items = iter(raw.rstrip(b"\x00").split(b"\x00") if raw else ())
            facts = None
            for entry in items:
                oldmode, mode, oldoid, oid, change = entry.decode("ascii").split()
                first = next(items).decode("utf-8", errors="surrogateescape")
                rename = change.startswith("R")
                new = (
                    next(items).decode("utf-8", errors="surrogateescape")
                    if rename
                    else first
                )
                if first not in self.paths and new not in self.paths:
                    continue
                if mode not in ("000000", "100644", "100755") or oldmode[1:] not in (
                    "000000",
                    "100644",
                    "100755",
                ):
                    raise VaultImportMismatch(
                        "Historical path has unsupported Git type"
                    )
                if facts is None:
                    facts = self._facts(commit, parents)
                    nearest[commit] = commit
                    self.graph[commit] = (
                        parent,
                        {**facts, "order": len(self.graph) + 1},
                    )
                if rename and first in self.paths:
                    counts[first] += 1
                    yield (
                        first,
                        counts[first],
                        None,
                        {
                            **facts,
                            "effect": "renamed_from",
                            "peer": new if new in self.paths else None,
                        },
                    )
                if new in self.paths:
                    content = None
                    if mode != "000000":
                        size = int(self._git("cat-file", "-s", oid))
                        if size > 2 << 20:
                            raise VaultImportMismatch(
                                "A selected revision exceeds the document contract"
                            )
                        content = self._content(self._git("cat-file", "blob", oid))
                    counts[new] += 1
                    yield (
                        new,
                        counts[new],
                        content,
                        {
                            **facts,
                            "effect": change,
                            "blob": oid if content is not None else None,
                            "peer": first if rename and first in self.paths else None,
                        },
                    )
        current = (
            self._git("rev-parse", "--verify", "HEAD").decode().strip()
            if commits
            else None
        )
        self.active_parent = nearest.get(current)
        for path in sorted(self.paths):
            entry = self.manifest["files"].get(f"{self.relative}/{path}")
            if entry is None and not counts[path]:
                raise VaultImportMismatch(
                    "Approved path has no historical or working evidence"
                )
            content = None
            if entry:
                if entry["bytes"] > 2 << 20:
                    raise VaultImportMismatch(
                        "Working document exceeds the document contract"
                    )
                raw = (self.vault / path).read_bytes()
                if sha256(raw).hexdigest() != entry["sha256"]:
                    raise VaultImportMismatch(
                        "Working document differs from the sealed manifest"
                    )
                content = self._content(raw)
            counts[path] += 1
            yield (
                path,
                counts[path],
                content,
                {
                    "source": "working_tree",
                    "sha256": entry["sha256"] if entry else None,
                },
            )
        self.verify_receipt()
