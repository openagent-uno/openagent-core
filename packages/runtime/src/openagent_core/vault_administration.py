"""Authorized vault administration, shared by product HTTP and embedded hosts.

An instance represents one host-authorized corpus. Note frontmatter never grants
access. Responses go only to the verified management caller; conversational use
continues through the runtime memory/catalog publication contracts.
"""
from __future__ import annotations

import asyncio
from contextlib import nullcontext
from dataclasses import replace
from pathlib import Path, PurePosixPath
import re
import os

from .administration import ManagementContext
from .contracts import ResourceRef, require_authorized

_READ = frozenset({'notes', 'read', 'graph', 'search', 'search/files', 'search/in-file', 'gate', 'stats', 'history', 'commit'})
_WRITE = frozenset({'write', 'delete', 'restore', 'reset', 'doctor', 'derived', 'move', 'init', 'index/sync'})


def _json(value):
    if isinstance(value, dict): return {k: _json(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)): return [_json(v) for v in value]
    if hasattr(value, 'isoformat'): return value.isoformat()
    return value


def _boolean(value=False):
    if isinstance(value, bool): return value
    if value in ('true', '1', 'yes', 'on'): return True
    if value in ('false', '0', 'no', 'off', ''): return False
    raise ValueError('Invalid boolean')


def _limit(value, default, maximum):
    if value is None: return default
    if isinstance(value, bool): raise ValueError('Invalid limit')
    result = int(value)
    if not 1 <= result <= maximum: raise ValueError('Limit outside allowed range')
    return result


class VaultAdministration:
    def __init__(self, vault_root, authorizer, *, index_path=None, runtime=None,
                 validate_on_write=True, max_note_bytes=2 << 20):
        self.root = Path(vault_root).absolute()
        self.authorizer, self.runtime = authorizer, runtime
        self.index_path = index_path
        self.validate_on_write = validate_on_write
        self.max_note_bytes = max_note_bytes
        self._owned_service = None

    def scope(self):
        from .runtime import runtime_scope
        return runtime_scope(self.runtime) if self.runtime is not None else nullcontext()

    def _service(self):
        from .memory.vault.service import VaultService, get_service
        if self.runtime is not None:
            return get_service(self.root)
        if self._owned_service is None:
            if self.index_path is None: raise ValueError('An explicit index path is required without a runtime')
            self._owned_service = VaultService(self.root, index_path=self.index_path)
        return self._owned_service

    async def close(self):
        if self._owned_service is not None:
            await self._owned_service.close()
            self._owned_service = None

    async def authorize(self, context, action):
        if not isinstance(context, ManagementContext): raise TypeError('Verified management context required')
        await require_authorized(self.authorizer, context, action,
            ResourceRef('agent', context.tenant_id, context.agent_id))

    def _path(self, value, *, directory=False):
        if not isinstance(value, str) or not value or '\\' in value or '\x00' in value:
            raise ValueError('Invalid vault path')
        rel = PurePosixPath(value)
        if rel.is_absolute() or any(part in ('', '.', '..') or part.startswith('.') for part in value.split('/')):
            raise ValueError('Invalid vault path')
        if not directory and rel.suffix.lower() != '.md': raise ValueError('A Markdown note is required')
        full = self.root
        if full.is_symlink(): raise ValueError('Vault root cannot be a symlink')
        for part in rel.parts:
            full = full / part
            if full.is_symlink(): raise ValueError('Vault paths cannot contain symlinks')
        try: full.resolve().relative_to(self.root.resolve())
        except (ValueError, OSError) as exc: raise ValueError('Invalid vault path') from exc
        return rel.as_posix(), full

    def _read(self, value):
        from .memory.vault.parser import split_frontmatter, load_frontmatter_yaml, FrontmatterSyntaxError, extract_wikilinks
        rel, full = self._path(value)
        if not full.is_file(): raise LookupError('Note not found')
        if full.stat().st_size > self.max_note_bytes: raise ValueError('Note exceeds configured limit')
        content = full.read_text(errors='replace')
        raw, body = split_frontmatter(content)
        try: meta = load_frontmatter_yaml(raw) if raw is not None else {}
        except FrontmatterSyntaxError: meta = {}
        if not isinstance(meta, dict): meta = {}
        return {'path': rel, 'content': content, 'frontmatter': _json(meta),
                'body': body.strip() if raw is not None else content,
                'links': [target for target, _ in extract_wikilinks(content)],
                'modified': full.stat().st_mtime}

    @staticmethod
    def _reference(value):
        if not isinstance(value, str) or not re.fullmatch(r'[a-fA-F0-9]{7,64}', value):
            raise ValueError('An exact commit hash is required')
        return value

    async def execute(self, context, operation, *, path=None, query=None, body=None):
        if operation not in _READ | _WRITE: raise ValueError('Unknown vault operation')
        if query is None: query = {}
        if body is None: body = {}
        if not isinstance(query, dict) or not isinstance(body, dict): raise ValueError('Expected object')
        allowed_query = {'search': {'q', 'limit'}, 'search/files': {'q', 'limit'},
            'search/in-file': {'q', 'path', 'regex'}, 'gate': {'strict', 'limit'},
            'history': {'path', 'limit'}, 'commit': {'hash'}, 'doctor': {'apply'}, 'index/sync': {'force'}}
        allowed_body = {'write': {'content'}, 'restore': {'hash'}, 'reset': {'hash', 'confirm'}, 'move': {'from', 'to'}}
        if set(query) - allowed_query.get(operation, set()) or set(body) - allowed_body.get(operation, set()):
            raise ValueError('Unknown vault fields')
        if path is not None and operation not in {'read', 'write', 'delete'}: raise ValueError('Unexpected note path')
        action = 'vault.read' if operation in _READ else 'vault.write'
        await self.authorize(context, action)
        if self.root.is_symlink(): raise ValueError('Vault root cannot be a symlink')
        if operation in {'restore', 'reset', 'derived', 'init', 'move'} or (operation == 'doctor' and _boolean(query.get('apply', False))):
            # Broad mutations must not follow links inserted by another editor.
            # Ordinary reads ignore them through the shared index walker.
            def check_tree():
                for directory, folders, files in os.walk(self.root):
                    if any((Path(directory) / name).is_symlink() for name in folders + files):
                        raise ValueError('Broad vault mutations cannot traverse symlinks')
            await asyncio.to_thread(check_tree)
        with self.scope():
            result = await self._execute(context, operation, path, query, body)
        # Revocation while a read/index/Git operation is pending must also prevent
        # publication. A completed mutation is never repeated automatically.
        await self.authorize(context, action)
        return _json(result)

    async def _execute(self, context, operation, path, query, body):
        service = self._service()
        origin = {'kind': 'management', 'user': context.authority.key, 'agent': context.agent_id}
        if operation in {'read', 'write', 'delete'}:
            rel, full = self._path(path)
            if operation == 'read': return await asyncio.to_thread(self._read, rel)
            if operation == 'delete':
                if not full.is_file(): raise LookupError('Note not found')
                result = await service.delete_note(rel, origin)
                return {'ok': True, 'commit': result['commit']}
            content = body.get('content')
            if not isinstance(content, str) or len(content.encode()) > self.max_note_bytes:
                raise ValueError('Invalid note content')
            result = await service.write_note(rel, content, origin, validate=self.validate_on_write)
            return {**result, 'path': rel}
        if operation in {'notes', 'graph'}:
            if not self.root.exists(): return {'notes': []} if operation == 'notes' else {'nodes': [], 'edges': []}
            index = await service._ensure_index()
            await asyncio.to_thread(index.sync, False)
            def collect():
                notes = []
                for note in index.all_notes():
                    try: self._path(note.path)
                    except ValueError: continue
                    notes.append(note)
                if operation == 'notes':
                    return {'notes': [{'path': n.path, 'title': n.title or n.stem, 'tags': n.tags,
                        'type': self._read(n.path)['frontmatter'].get('type', ''), 'modified': n.mtime, 'size': n.byte_size} for n in notes]}
                visible = {n.path for n in notes}; edges = []
                for note in notes:
                    targets = {index.resolve_link(raw) for raw in note.outlinks}
                    edges.extend({'source': note.path, 'target': t} for t in sorted(targets - {None, note.path}) if t in visible)
                return {'nodes': [{'id': n.path, 'label': n.title or n.stem, 'tags': n.tags} for n in notes], 'edges': edges}
            return await asyncio.to_thread(collect)
        if operation in {'search', 'search/files'}:
            text = query.get('q', '').strip()
            limit = _limit(query.get('limit'), 50, 500)
            if not text: return {'results': []}
            rows = await (service.search if operation == 'search' else service.search_files)(text, limit=limit)
            visible = []
            for row in rows:
                try: self._path(row['path'])
                except ValueError: continue
                visible.append(row)
            return {'results': visible}
        if operation == 'search/in-file':
            document = await asyncio.to_thread(self._read, query.get('path'))
            text = query.get('q')
            if not isinstance(text, str) or not text or len(text) > 4096: raise ValueError('A bounded query is required')
            regex = _boolean(query.get('regex', False))
            matches = await _search_lines(document['content'], text, regex)
            return {'path': document['path'], 'query': text, 'regex': regex, 'matches': matches, 'count': len(matches)}
        if operation == 'gate':
            config = replace(service.config, strict=True) if _boolean(query.get('strict', False)) else service.config
            report = (await service.gate(config=config)).to_dict()
            limit = _limit(query.get('limit'), 500, 10000)
            if len(report['violations']) > limit:
                report['violations'] = report['violations'][:limit]; report['violations_truncated'] = True
            return report
        if operation == 'stats': return await service.stats()
        if operation == 'doctor': return await service.doctor(apply=_boolean(query.get('apply', False)), origin=origin)
        if operation == 'derived': return await service.regenerate_derived(origin=origin)
        if operation == 'init': return await service.init_taxonomy(origin=origin)
        if operation == 'index/sync': return await service.sync(force=_boolean(query.get('force', False)))
        if operation == 'history':
            rel = self._path(query['path'], directory=True)[0] if query.get('path') else None
            return {'commits': await service.git_log(_limit(query.get('limit'), 50, 500), rel), 'path': rel}
        if operation == 'commit':
            result = await service.git_show(self._reference(query.get('hash')))
            if result is None: raise LookupError('Commit not found')
            return result
        if operation in {'restore', 'reset'}:
            ref = self._reference(body.get('hash'))
            if operation == 'reset' and body.get('confirm') is not True: raise ValueError('Reset requires explicit confirmation')
            return await (service.restore_to if operation == 'restore' else service.reset_to)(ref, origin)
        if operation == 'move':
            old, _ = self._path(body.get('from'), directory=True)
            new, _ = self._path(body.get('to'), directory=True)
            if new == old or new.startswith(old + '/'):
                raise ValueError('A folder cannot be moved inside itself')
            return await service.move(old, new, origin=origin)
        raise AssertionError(operation)


async def _search_lines(content, query, regex):
    def search():
        import time
        if regex:
            import regex as patterns
            try: pattern = patterns.compile(query)
            except patterns.error as exc: raise ValueError('Invalid regex') from exc
        matches = []
        deadline = time.monotonic() + 2
        try:
            for number, line in enumerate(content.split('\n'), 1):
                if regex:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0: raise TimeoutError()
                    positions = (match.start() for match in pattern.finditer(line, timeout=remaining))
                else:
                    position = line.lower().find(query.lower())
                    positions = () if position < 0 else (position,)
                for position in positions:
                    item = {'line': number, 'col': position + 1, 'text': line[:4096]}
                    if len(line) > 4096: item['text_truncated'] = True
                    matches.append(item)
                    if len(matches) >= 1000: return matches
        except TimeoutError as exc:
            raise ValueError('Regex execution deadline exceeded') from exc
        return matches
    return await asyncio.to_thread(search)
