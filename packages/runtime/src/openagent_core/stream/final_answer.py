"""Project an attested final message using already authorized output media."""
from .content_parts import parse_response_content
from ..memory.artifacts import public_attachment_ref


def project_final_answer(answer, *, parts=(), attachments=()):
    if not isinstance(answer, dict) or not isinstance(answer.get("text"), str):
        return None
    parsed = parse_response_content(answer["text"], allow_inline_ui=False)
    text = parsed.text
    media = [{"kind": "attachment", "attachment": public_attachment_ref(p["attachment"])}
             for p in parts if isinstance(p, dict) and p.get("kind") == "attachment"
             and isinstance(p.get("attachment"), dict)]
    for ref in attachments:
        clean = public_attachment_ref(ref)
        if not any(p["attachment"] == clean for p in media):
            media.append({"kind": "attachment", "attachment": clean})
    # Only existing canonical authorized references cross this boundary. A
    # marker is never a new permission grant or an implicit filesystem read.
    refs = [dict(p["attachment"]) for p in media]
    if parsed.attachments and not refs:
        note = "An attachment is unavailable: no authorized file publisher could deliver it."
        text += ("\n\n" if text else "") + note
    final_parts = ([{"kind": "text", "text": text}] if text else []) + media
    return {"text": text, "parts": final_parts, "attachments": refs}
