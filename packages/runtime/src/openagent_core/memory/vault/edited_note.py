"""Validate Markdown edits with the same rules across document backends."""
import datetime

from .doctor import _FIXABLE_RULES, fix_note_content
from .parser import FrontmatterSyntaxError, load_frontmatter_yaml, parse_note_text, split_frontmatter


def validate_edited_note(rel, content, *, is_new, config, journal_root=None):
    """Apply deterministic write repairs and return blocking issues before commit."""
    note = parse_note_text(rel, content, journal_root=journal_root or config.journal_root)
    today = datetime.date.today().isoformat()
    fixed, applied = fix_note_content(
        content, note, set(_FIXABLE_RULES), today)
    errors: list = []
    warnings: list = []
    # Repair first and validate the exact bytes eligible for publication.
    raw_fm, _ = split_frontmatter(fixed)
    if raw_fm is not None and raw_fm.strip():
        try:
            load_frontmatter_yaml(raw_fm)
        except FrontmatterSyntaxError as e:
            errors.append({"rule": "frontmatter_yaml", "severity": "error",
                           "message": f"frontmatter is not valid YAML: {e}"})
    note2 = parse_note_text(rel, fixed, journal_root=journal_root or config.journal_root)
    if is_new and note2.line_count > config.max_lines:
        errors.append({
            "rule": "atomicity", "severity": "error",
            "message": (f"note is {note2.line_count} body lines "
                        f"(> {config.max_lines}); split it into "
                        "atomic notes, one idea each, and link them")})
    if not note2.summary:
        warnings.append({
            "rule": "frontmatter", "severity": "warn",
            "message": ("missing 'summary' — add a one-sentence "
                        "summary so the note is self-describing")})
    return fixed, errors, warnings, applied
