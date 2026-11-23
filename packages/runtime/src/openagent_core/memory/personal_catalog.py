"""A personal catalog spanning the private and shared spaces selected by the host."""
import base64
from dataclasses import dataclass, replace
import json


@dataclass(frozen=True)
class PersonalMemorySpace:
    space_id: str
    visibility: str

    def __post_init__(self):
        if (not isinstance(self.space_id, str) or not 1 <= len(self.space_id.encode()) <= 200
                or self.visibility not in {"personal", "shared"}):
            raise ValueError("Invalid memory space")


def _cursor(space, path):
    return base64.urlsafe_b64encode(json.dumps([space, path]).encode()).decode()


def _resume(value):
    if value is None:
        return None, None
    try:
        if not isinstance(value, str) or len(value) > 2400:
            raise ValueError
        decoded = json.loads(base64.b64decode(value, altchars=b"-_", validate=True))
        if not isinstance(decoded, list) or len(decoded) != 2:
            raise ValueError
        space, path = decoded
        if not isinstance(space, str) or not isinstance(path, str):
            raise ValueError
        return space, path
    except (UnicodeError, TypeError, ValueError) as error:
        raise ValueError("Invalid personal memory cursor") from error


class PersonalMemoryCatalog:
    """page_spaces returns sorted grants with ID > after; select revalidates exactly one grant."""
    def __init__(self, page_spaces, select):
        self.page_spaces, self.select = page_spaces, select

    async def _open(self, vault, context, space_id):
        space, selected = await self.select(context, space_id)
        if (not isinstance(space, PersonalMemorySpace) or space.space_id != space_id
                or replace(selected, scopes=context.scopes) != context):
            raise PermissionError("The selected memory grant changed the request identity")
        actual = await vault._scope(selected, "memory.read")
        if actual["space"] != space_id:
            raise PermissionError("The selected memory space changed")
        return space, selected

    async def detail(self, vault, context, space_id, path):
        if not space_id:
            raise ValueError("Choose the space of the requested note")
        space, selected = await self._open(vault, context, space_id)
        row = await vault.read(selected, path)
        current, _ = await self._open(vault, context, space_id)
        return {**row, "space": space_id, "visibility": current.visibility}

    async def browse(self, vault, context, *, query, cursor, limit, space_id=None):
        # The collection sorts first by logical space ID, then by note path.
        last_space, last_path = _resume(cursor)
        if space_id and last_space and space_id != last_space:
            raise ValueError("Personal memory cursor belongs to another space")
        field = "hits" if query is not None else "notes"
        rows, published = [], {}
        async def collect(ident, after):
            space, selected = await self._open(vault, context, ident)
            count = min(100, limit + 1 - len(rows))
            if query is None:
                page = await vault.list_notes(selected, after=after, limit=count)
            else:
                page = await vault.search_page(selected, query=query, after=after, limit=count)
            items = [dict(row, space=ident, visibility=space.visibility) for row in page[field]]
            if items:
                published[ident] = space
                rows.extend(items)
            return bool(page["next_cursor"])
        more = False
        if space_id:
            more = await collect(space_id, last_path)
        else:
            if last_space:
                try:
                    more = await collect(last_space, last_path)
                except (LookupError, PermissionError):
                    pass
            while not more and len(rows) <= limit:
                batch = await self.page_spaces(context, after=last_space, limit=16)
                if not isinstance(batch, (tuple, list)) or len(batch) > 16:
                    raise ValueError("Invalid memory space page")
                for ident in batch:
                    if not isinstance(ident, str) or not ident or (last_space and ident <= last_space):
                        raise ValueError("Memory spaces must advance in ID order")
                    last_space = ident
                    try:
                        more = await collect(ident, None)
                    except (PermissionError, LookupError):
                        continue
                    if more or len(rows) > limit:
                        break
                if len(batch) < 16:
                    break
        for ident, original in published.items():
            current, _ = await self._open(vault, context, ident)
            if current != original:
                raise PermissionError("Memory space visibility changed")
        visible = rows[:limit]
        next_cursor = _cursor(visible[-1]["space"], visible[-1]["path"]) if visible and (more or len(rows) > limit) else None
        return {field: visible, "next_cursor": next_cursor}
