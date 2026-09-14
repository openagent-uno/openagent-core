"""Symptom -> diagnostic-category routing: the four ways it captured the wrong subsystem."""
from __future__ import annotations

from ._framework import TestContext, test
from src.core import local_support_controller as c


# Byte-for-byte the shape the eSound web form appends to every report: a
# blank line, `---`, then one `key: value` per line.
_FORM_TRAILER = (
    "\n\n---\naccount_email: user@example.com\n"
    "account_user_id: 7f99d3b67b4ce85868a8a123dbbf4fa5\napp_version: 5.2.6\n"
    "device: samsung SM-S918B\nguest: no\ninstall_source: com.android.vending\n"
    "native_version: 5.2.6\nos: Android 14\npackage: com.esound\n"
    "platform: android\npremium: yes\nruntime: native\ntablet: no"
)
# The store-review importer uses the same separator with its own fields.
_REVIEW_TRAILER = (
    "\n\n---\napp_version: 5.2.6\napp_version_code: 834\nos: Android 11\n"
    "device: Samsung a30 (Galaxy A30)\nram_mb: 3746\nreviewer_language: en"
)


@test("support_diagnostic_routing", "device metadata never decides the capture category")
async def t_metadata_does_not_route(_ctx: TestContext):
    symptom = "le canzoni si bloccavano dopo qualche secondo dall inizio dei brani"
    # `premium: yes` rides in the trailer of EVERY report from a paying user.
    # Routing on it captured the billing subsystem for a playback fault.
    assert c._diagnostic_category(symptom + _FORM_TRAILER) == "playback"
    assert c._diagnostic_category(symptom) == "playback"
    # A store review's trailer carries reviewer_language, not premium.
    assert c._diagnostic_category("songs don't load" + _REVIEW_TRAILER) == "playback"
    # The trailer is removed, not merely ignored by the router.
    assert c._strip_report_metadata(symptom + _FORM_TRAILER) == symptom
    # A channel that flattened the trailer onto one line is still a trailer.
    assert c._strip_report_metadata("ciao --- app_version: 5.2.6 premium: yes") == "ciao"
    # `---` the customer typed is NOT a trailer: no known field follows it, so
    # every word they wrote survives.
    for prose in (
        "primo punto\n---\nsecondo punto: importante",
        "il piano premium --- quello annuale --- non si attiva",
    ):
        assert c._strip_report_metadata(prose) == prose, prose


@test("support_diagnostic_routing", "stems match at a word boundary, not mid-word")
async def t_no_midword_matches(_ctx: TestContext):
    # "ad " lived inside "downloa|d |": every download report captured `ads`.
    assert c._diagnostic_category("I can't download my songs anymore") == "playback"
    assert c._diagnostic_category("the app won't load anything") == "general"
    # Italian `ad` is the euphonic preposition, not an advert.
    assert c._diagnostic_category("non riesco ad accedere al mio account") == "auth"
    # Advertising still routes.
    for text in ("I see too many ads", "advertisement everywhere", "troppa pubblicita"):
        assert c._diagnostic_category(text) == "ads", text


@test("support_diagnostic_routing", "specific routes win over the ones that contain them")
async def t_route_order(_ctx: TestContext):
    # `play` is a prefix of `playlist`; with the generic route first, no
    # playlist report could ever reach the playlists capture.
    assert c._diagnostic_category("my playlist is empty") == "playlists"
    assert c._diagnostic_category("la playlist non si apre") == "playlists"
    assert c._diagnostic_category("the player stops") == "playback"


@test("support_diagnostic_routing", "the phrasings customers actually use reach playback")
async def t_playback_vocabulary(_ctx: TestContext):
    # None of these carried one of the four English words the old list had, so
    # they fell to `general` -- which skips the capture entirely.
    for text in (
        "le canzoni si bloccano",
        "la riproduzione si interrompe",
        "songs keep stopping while I listen",
        "a musica para sozinha",
        "la cancion se corta",
        "le son se coupe pendant l ecoute",
    ):
        assert c._diagnostic_category(text) == "playback", text


@test("support_diagnostic_routing", "a playback capture does not send the customer to reproduce on demand")
async def t_playback_bundle_and_instruction(_ctx: TestContext):
    available = {"ads", "general", "network", "playback", "player", "purchases"}
    # The bundle explains the primary category instead of slicing it.
    assert c._diagnostic_bundle("playback", available) == [
        "playback", "player", "network", "general",
    ]
    assert c._diagnostic_bundle("purchases", available) == [
        "purchases", "network", "general",
    ]
    assert len(c._diagnostic_bundle("playback", available)) <= c._MAX_DIAGNOSTIC_CATEGORIES


@test("support_diagnostic_routing", "accented Spanish and Portuguese reach their category")
async def t_accent_folding(_ctx: TestContext):
    # Written unaccented in the stem list; without folding these matched
    # nothing and fell to `general`, which skips the capture outright.
    for text in (
        "no me carga la música",
        "la reproducción se detiene sola",
        "a reprodução para sozinha",
        "la canción se corta",
        "le son se coupe pendant l'écoute",
    ):
        assert c._diagnostic_category(text) == "playback", text
    assert c._fold_accents("reprodução") == "reproducao"


@test("support_diagnostic_routing", "an explicit billing word outranks the word account")
async def t_auth_vs_purchases(_ctx: TestContext):
    # "account" is the commonest word in BOTH kinds of report, so it must not
    # pull a refund thread away from purchases.
    assert c._diagnostic_category("voglio un rimborso, il mio account premium") == "purchases"
    assert c._diagnostic_category("quiero cancelar mi suscripcion") == "purchases"
    # ...and a plain "I can't get back into my account" is authentication,
    # even when the customer mentions their music in the same breath.
    assert c._diagnostic_category(
        "Non riesco a rientrare dentro il mio account di eSound. Ho tutta la musica."
    ) == "auth"
    assert c._diagnostic_category("no puedo entrar en mi cuenta") == "auth"
