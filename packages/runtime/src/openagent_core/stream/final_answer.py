"""Project an attested final message using already authorized output media."""
from .content_parts import parse_response_content
from ..memory.artifacts import public_attachment_ref


def canonical_final_answer(text, attachments=(), *, unavailable=False):
    """Format only already-selected, freshly authorized final references."""
    text = parse_response_content(text, allow_inline_ui=False).text
    refs = [public_attachment_ref(ref) for ref in attachments]
    if unavailable:
        note = "An attachment is unavailable: no authorized file publisher could deliver it."
        text += ("\n\n" if text else "") + note
    media = [{"kind": "attachment", "attachment": ref} for ref in refs]
    return {"text": text, "parts": ([{"kind": "text", "text": text}] if text else []) + media,
            "attachments": refs}


def project_final_answer(answer, *, parts=(), attachments=(), attachment_bindings=()):
    # Legacy aggregate arguments remain accepted for internal call
    # compatibility; they are not final-selection authority.
    if not isinstance(answer, dict) or not isinstance(answer.get("text"), str):
        return None
    parsed = parse_response_content(answer["text"], allow_inline_ui=False)
    text = parsed.text
    media = []
    unavailable = False
    # Full transcript media can include input previews and intermediate tool
    # output. Authorization to read those bytes is not an instruction to send
    # them again. Only an exact marker in the attested assistant boundary may
    # select its corresponding normally-resolved canonical reference.
    for marker in parsed.attachments:
        ref = next((ref for source, ref in attachment_bindings
                    if source.get("type") == marker.get("type")
                    and source.get("path") == marker.get("path")), None)
        if ref is None:
            unavailable = True
            continue
        clean = public_attachment_ref(ref)
        if not clean:
            unavailable = True
        elif not any(p["attachment"] == clean for p in media):
            media.append({"kind": "attachment", "attachment": clean})
    # Only existing canonical authorized references cross this boundary. A
    # marker is never a new permission grant or an implicit filesystem read.
    refs = [dict(p["attachment"]) for p in media]
    return canonical_final_answer(text, refs, unavailable=unavailable)
