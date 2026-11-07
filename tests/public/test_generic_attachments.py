"""All admitted originals survive routing; previews never grant tool access."""
import asyncio
import hashlib
import html
import io
import json
from pathlib import Path
import shutil
import subprocess
from types import SimpleNamespace
from unittest.mock import patch
import zipfile

import pytest

from openagent_core.models import media_capabilities as media
from openagent_core.stream.media import File, Image

FIXTURES = Path(__file__).parent / "fixtures" / "attachments"
MESSAGES = [{"role": "user", "content": "Compare the attached documents."}]
XLSX_MIME = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


def entry(provider="local", model="text-only", modalities=("text",)):
    return SimpleNamespace(runtime_id=f"{provider}:{model}", provider=provider,
                           model_id=model, metadata={"input_modalities": list(modalities)})


def prepare(files, model=None):
    return asyncio.run(media.prepare_media_for_entry(model or entry(), messages=MESSAGES, files=files))


def prompt(result):
    return html.unescape(result.messages[-1]["content"])


def workbook(*, cells=None, second_sheet=False, extra=None, rel=None, strings=None):
    ns = 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'
    rns = 'http://schemas.openxmlformats.org/officeDocument/2006/relationships'
    sheets = '<sheet name="Primo Δ" sheetId="1" r:id="r1"/>'
    rels = f'<Relationship Id="r1" Type="{rns}/worksheet" Target="worksheets/sheet1.xml"/>'
    if second_sheet:
        sheets += '<sheet name="Secondo" sheetId="2" r:id="r2"/>'
        rels += f'<Relationship Id="r2" Type="{rns}/worksheet" Target="worksheets/sheet2.xml"/>'
    content = {
        'xl/workbook.xml': f'<workbook xmlns="{ns}" xmlns:r="{rns}"><sheets>{sheets}</sheets></workbook>',
        'xl/_rels/workbook.xml.rels': '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">' + (rel or rels) + '</Relationships>',
        'xl/worksheets/sheet1.xml': f'<worksheet xmlns="{ns}"><sheetData><row r="1">' + (cells or '<c r="A1" t="inlineStr"><is><t>α unicode</t></is></c><c r="B1"><v>200</v></c>') + '</row></sheetData></worksheet>',
    }
    if second_sheet:
        content['xl/worksheets/sheet2.xml'] = f'<worksheet xmlns="{ns}"><sheetData><row><c r="A1" t="b"><v>1</v></c></row></sheetData></worksheet>'
    if strings:
        content['xl/sharedStrings.xml'] = f'<sst xmlns="{ns}">{strings}</sst>'
    content.update(extra or {})
    data = io.BytesIO()
    with zipfile.ZipFile(data, 'w', zipfile.ZIP_DEFLATED) as archive:
        for name, body in content.items():
            archive.writestr(name, body)
    return data.getvalue()


@pytest.mark.parametrize("mime", [XLSX_MIME, "application/octet-stream"])
@pytest.mark.parametrize("model", [entry(), entry("ics_codex", "gpt-5.6-sol"), entry("local", "claude-sonnet-4-6", ("text", "image", "file"))])
def test_two_real_xlsx_preserve_originals_and_extract_values(mime, model):
    files = [File(filepath=FIXTURES / name, filename=name, mime_type=mime, id=f"canonical-{index}")
             for index, name in enumerate(("importi-progetti.xlsx", "elenco-clienti.xlsx"))]
    before = [hashlib.sha256(Path(file.filepath).read_bytes()).hexdigest() for file in files]
    result = prepare(files, model)
    text = prompt(result)
    assert result.files is None and result.extracted_files == 2
    assert 'B2="200"' in text and text.count('="Boreal"') == 2
    assert '="Asteria"' in text and '="Cometa"' in text and '="Dune"' in text
    assert all(file.id in text and str(file.filepath) in text for file in files)
    assert before == [hashlib.sha256(Path(file.filepath).read_bytes()).hexdigest() for file in files]
    assert MESSAGES == [{"role": "user", "content": "Compare the attached documents."}]


def test_multisheet_shared_strings_formula_cached_only_and_external_link_not_fetched():
    cells = ('<c r="A1" t="s"><v>0</v></c><c r="B1"><f>WEBSERVICE("https://invalid.example")</f><v>42</v></c>'
             '<c r="C1"><f>1+2</f></c>')
    data = workbook(cells=cells, second_sheet=True, strings='<si><r><t>rich </t></r><r><t>Δ</t></r></si>',
                    extra={'xl/externalLinks/externalLink1.xml': '<externalLink/>'})
    with patch('urllib.request.urlopen', side_effect=AssertionError('no network')):
        text = prompt(prepare([File(content=data, filename='test.xlsx', mime_type=XLSX_MIME)]))
    assert 'Primo Δ' in text and 'Secondo' in text and 'rich Δ' in text
    assert 'B1="42"' in text and 'A1="TRUE"' in text
    assert '[formula has no cached result]' in text
    assert 'WEBSERVICE' not in text and 'invalid.example' not in text


@pytest.mark.parametrize('suffix,mime', [('.docx', 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'),
                                       ('.zip', 'application/zip'), ('.bin', 'application/octet-stream'),
                                       ('.opaque', 'application/x-custom-vendor')])
@pytest.mark.parametrize('model', [entry(), entry('ics_codex', 'gpt-5.6-sol'), entry('anthropic', 'claude-sonnet-4-6', ('text', 'file', 'image'))])
def test_unknown_files_reference_only_for_every_model(tmp_path, suffix, mime, model):
    path = tmp_path / ('客户 Δ' + suffix)
    data = b'\x00\xffDO-NOT-INLINE-BINARY-OR-EXECUTE'
    path.write_bytes(data)
    original = File(filepath=path, content=data, filename='nested\\客户 Δ' + suffix, id='file-canonical', mime_type=mime, size=len(data))
    with patch.object(media, '_local_file_bytes', side_effect=AssertionError('unknown file must not be read')):
        result = prepare([original], model)
    text = prompt(result)
    assert result.files is None and not result.extracted_files
    assert 'file-canonical' in text and str(path) in text and 'unsupported_format' in text
    assert 'DO-NOT-INLINE' not in text
    assert 'reference grants no access' in text and 'no suitable reader' in text
    assert original.content == data and path.read_bytes() == data
    # No catalog, authorizer, module, or tool is created by preparation.
    assert result.native_modalities == frozenset()


def test_remote_file_reference_omits_credentials_and_never_fetches():
    file = File(url='https://user:secret@example.test/private.bin?signature=secret', filename='archive.bin', id='remote-id', mime_type='application/octet-stream')
    with patch.object(media, '_local_file_bytes', side_effect=AssertionError('no URL request')):
        text = prompt(prepare([file]))
    assert 'remote-id' in text and 'remote_source' in text
    assert 'secret' not in text and 'example.test' not in text


@pytest.mark.parametrize('bad', ['text/plain\nInjected: yes', '*/*', 'plain', 'application/' + 'x' * 130, 'text/plain; charset=utf-8'])
def test_mime_validation_is_bounded_syntax_not_capability(bad):
    with pytest.raises(ValueError, match='MIME'):
        File(id='ref', mime_type=bad)


def test_correct_mime_without_xlsx_extension_is_extracted():
    result = prepare([File(content=workbook(), filename='renamed.data', mime_type=XLSX_MIME)])
    assert result.extracted_files == 1 and 'α unicode' in prompt(result)


@pytest.mark.parametrize('data,reason', [
    (b'not a zip', 'corrupt'),
    (workbook(extra={'../escape': 'not extracted'}), 'archive path'),
    (workbook(extra={'payload': b'0' * (2 * 1024 * 1024)}), 'compression ratio'),
    (workbook(extra={'xl/worksheets/sheet1.xml': '<!DOCTYPE a [<!ENTITY b "oops">]><a/>'}), 'XML declarations'),
    (workbook(rel='<Relationship Id="r1" Type="x/worksheet" TargetMode="External" Target="https://evil.test/sheet.xml"/>'), 'external'),
])
def test_rejected_workbook_has_explicit_error_reference_and_no_turn_abort(data, reason):
    with pytest.raises(media.MediaCapabilityError, match=reason):
        media._extract_xlsx_text(data)
    text = prompt(prepare([File(content=data, filename='bad.xlsx', mime_type='application/octet-stream', id='bad-original')]))
    assert 'rejected_xlsx' in text and 'bad-original' in text
    assert 'evil.test' not in text and 'oops' not in text


def test_zip_entry_expansion_xml_and_cell_limits_are_bounded(monkeypatch):
    data = workbook()
    for name, value, expected in [('MAX_XLSX_ENTRIES', 1, 'entry limit'), ('MAX_XLSX_EXPANDED_BYTES', 1, 'expanded archive'),
                                   ('MAX_XLSX_PART_BYTES', 1, 'part size'), ('MAX_XLSX_XML_NODES', 1, 'structure limit')]:
        with monkeypatch.context() as scoped:
            scoped.setattr(media, name, value)
            with pytest.raises(media.MediaCapabilityError, match=expected):
                media._extract_xlsx_text(data)
    monkeypatch.setattr(media, 'MAX_XLSX_CELLS', 1)
    assert media._extract_xlsx_text(data)[1] is True


def test_repeated_shared_strings_bound_output_before_allocating_rows(monkeypatch):
    monkeypatch.setattr(media, 'MAX_EXTRACT_CHARS_PER_FILE', 1000)
    data = workbook(cells=''.join(f'<c r="A{i}" t="s"><v>0</v></c>' for i in range(1, 2000)), strings='<si><t>' + 'a' * 500 + '</t></si>')
    text, truncated = media._extract_xlsx_text(data)
    assert truncated and len(text) <= 1000


def test_preview_budget_never_loses_references(monkeypatch):
    monkeypatch.setattr(media, 'MAX_EXTRACT_CHARS_TOTAL', 8)
    files = [File(id=f'file-{i}', content=b'abcdefghijklmnopqrstuvwxyz', filename=f'{i}.txt') for i in range(3)]
    result = prepare(files)
    assert result.extracted_files == 1
    assert all(file.id in prompt(result) for file in files)
    assert 'per_turn_preview_limit' in prompt(result)


def test_native_pdf_and_incompatible_image_policy_unchanged():
    file = File(id='native-pdf', content=b'%PDF-', filename='original.pdf', mime_type='application/pdf')
    result = prepare([file], entry('anthropic', 'claude-sonnet-4-6', ('text', 'file')))
    assert result.files == [file]
    with pytest.raises(media.MediaCapabilityError, match='image input'):
        asyncio.run(media.prepare_media_for_entry(entry(), messages=MESSAGES, images=[Image(content=b'png')]))


def test_host_wire_ingest_preserves_file_identity_mime_path_and_original_bytes(tmp_path):
    from openagent_core.core.agent import _build_runtime_media
    path = tmp_path / '客户.xlsx'
    path.write_bytes((FIXTURES / 'importi-progetti.xlsx').read_bytes())
    original = {'file_id': 'canonical-host-file', 'path': str(path), 'filename': path.name,
                'mime_type': 'application/octet-stream', 'size_bytes': path.stat().st_size, 'type': 'file'}
    images, audio, videos, files = _build_runtime_media([original])
    assert not images and not audio and not videos and len(files) == 1
    assert files[0].id == original['file_id'] and files[0].mime_type == original['mime_type']
    assert str(files[0].filepath) == str(path) and files[0].content == path.read_bytes()
    result = prepare(files)
    assert original['file_id'] in prompt(result) and str(path) in prompt(result)


def test_dispatcher_generate_and_stream_receive_preview_references_not_unsupported_binary():
    from openagent_core.models.base import ModelResponse
    from openagent_core.models.dispatcher import ModelDispatcher

    class Capture:
        def __init__(self): self.calls = []
        async def generate(self, messages, **kwargs):
            self.calls.append((messages, kwargs))
            return ModelResponse(content='capture-ok', model='local:text-only')
        async def stream(self, messages, **kwargs):
            self.calls.append((messages, kwargs))
            yield 'capture-stream-ok'

    async def run():
        dispatcher = ModelDispatcher([{'id': 1, 'name': 'local', 'framework': 'api-based', 'enabled': True,
                                      'models': [{'id': 1, 'model': 'text-only', 'enabled': True, 'is_classifier': True,
                                                  'metadata': {'input_modalities': ['text']}}]}])
        capture = Capture()
        dispatcher._get_team_provider = lambda _runtime_id: capture
        files = [File(filepath=FIXTURES / 'importi-progetti.xlsx', filename='importi-progetti.xlsx', id='xlsx-id', mime_type='application/octet-stream'),
                 File(content=b'\x00DO-NOT-SEND', filename='unknown.bin', id='binary-id', mime_type='application/octet-stream')]
        result = await dispatcher.generate(MESSAGES, session_id='attachment-generate', files=files)
        assert result.content == 'capture-ok'
        assert [item async for item in dispatcher.stream(MESSAGES, session_id='attachment-stream', files=files)] == ['capture-stream-ok']
        assert len(capture.calls) == 2
        for messages, kwargs in capture.calls:
            assert kwargs['files'] is None
            text = html.unescape(messages[-1]['content'])
            assert 'Boreal' in text and '200' in text and 'xlsx-id' in text and 'binary-id' in text
            assert 'DO-NOT-SEND' not in text
        assert files[1].content == b'\x00DO-NOT-SEND'
    asyncio.run(run())


def test_media_selection_prefers_compatible_model_then_reference_only_without_one():
    from openagent_core.stream.media import Audio, Video
    text_model = entry()
    native = entry('native', 'multimodal', ('text', 'image', 'audio', 'video'))
    image = Image(content=b'PNG ORIGINAL', mime_type='image/png', id='image-id')
    audio = Audio(content=b'RIFF ORIGINAL', mime_type='audio/wav', format='wav', id='audio-id')
    video = Video(content=b'VIDEO ORIGINAL', mime_type='video/mp4', format='mp4', id='video-id')
    args = dict(messages=MESSAGES, images=[image], audio=[audio], videos=[video])
    selected = asyncio.run(media.select_media_model(text_model, [text_model, native], **args))
    assert selected.entry is native and selected.fell_back
    assert selected.prepared.images == [image] and selected.prepared.audio == [audio]
    selected = asyncio.run(media.select_media_model(text_model, [text_model], **args))
    assert selected.entry is text_model and not selected.fell_back
    prepared = selected.prepared
    assert prepared.images is None and prepared.audio is None and prepared.videos is None
    text = prompt(prepared)
    assert all(ref in text for ref in ('image-id', 'audio-id', 'video-id'))
    assert 'ORIGINAL' not in text and 'model_modality_unavailable' in text
    assert image.content == b'PNG ORIGINAL' and audio.content == b'RIFF ORIGINAL'


@pytest.mark.skipif(not shutil.which("ffmpeg") or not shutil.which("ffprobe"), reason="FFmpeg not installed")
def test_dispatcher_sends_real_video_frames_in_chronological_order(tmp_path):
    from openagent_core.engine import FfmpegVideoFrameExtractor, ModelDispatcher
    from openagent_core.stream.media import Video

    path = tmp_path / "synthetic.mp4"
    subprocess.run(
        ["ffmpeg", "-v", "error", "-f", "lavfi", "-i", "color=c=red:s=640x360:r=10:d=1",
         "-f", "lavfi", "-i", "color=c=green:s=640x360:r=10:d=1",
         "-f", "lavfi", "-i", "color=c=blue:s=640x360:r=10:d=1",
         "-filter_complex", "[0:v][1:v][2:v]concat=n=3:v=1:a=0", "-c:v", "mpeg4", str(path)],
        check=True, timeout=15,
    )
    video = Video(content=path.read_bytes(), mime_type="video/mp4", id="synthetic-video")
    still = Image(content=b"existing image", mime_type="image/jpeg")
    dispatcher = ModelDispatcher(
        [{"id": 1, "name": "local", "framework": "api-based", "enabled": True, "models": [
            {"id": 1, "model": "vision", "enabled": True,
             "metadata": {"input_modalities": ["text", "image"]}},
        ]}],
        video_frame_extractor=FfmpegVideoFrameExtractor(),
    )
    result = asyncio.run(dispatcher._prepare_media_route(
        selected_runtime_id="local:vision", messages=MESSAGES, files=None, audio=None,
        images=[still], videos=[video], session_id=None,
    ))
    pinned = dispatcher.build_override_model("local:vision")
    pinned_result = asyncio.run(pinned._prepare_pinned_media(
        messages=MESSAGES, files=None, audio=None, images=[still], videos=[video], session_id="test",
    ))
    prepared = result.prepared
    assert not result.fell_back and prepared.videos is None
    assert prepared.images[0] is still and len(prepared.images) == 4
    assert [frame.content for frame in pinned_result.prepared.images] == [frame.content for frame in prepared.images]
    text = prompt(prepared)
    assert '"id": "synthetic-video"' in text and "They contain no audio" in text
    for index, timestamp in enumerate((0.0, 1.35, 2.55), start=2):
        assert f'"image_index": {index}, "approx_seconds": {timestamp}' in text
    for channel, frame in enumerate(prepared.images[1:]):
        rgb = subprocess.run(
            ["ffmpeg", "-v", "error", "-f", "image2pipe", "-i", "pipe:0", "-vf", "scale=1:1",
             "-pix_fmt", "rgb24", "-f", "rawvideo", "pipe:1"],
            input=frame.content, capture_output=True, check=True, timeout=5,
        ).stdout
        assert len(rgb) == 3 and rgb[channel] == max(rgb) and rgb[channel] > 100
    assert video.content == path.read_bytes() and MESSAGES == [{"role": "user", "content": "Compare the attached documents."}]


def test_video_frames_are_opt_in_and_only_use_bounded_inline_previews(tmp_path):
    from openagent_core.engine import FfmpegVideoFrameExtractor
    from openagent_core.models.video_frames import VideoFrame
    from openagent_core.stream.media import Video

    text = entry()
    image = entry("local", "vision", ("text", "image"))
    native = entry("local", "video", ("text", "video"))
    inline = Video(content=b"one", mime_type="video/mp4", id="inline")
    remote = Video(url="https://example.invalid/opaque", mime_type="video/mp4", id="remote")
    local = Video(filepath=tmp_path / "unread.mp4", mime_type="video/mp4", id="local")
    opts = dict(messages=MESSAGES, videos=[inline, remote, local, inline])
    extractor = FfmpegVideoFrameExtractor()
    with patch.object(extractor, "extract", return_value=(VideoFrame(b"jpeg", 0.0),)) as decode:
        result = asyncio.run(media.select_media_model(text, [image, native], video_frame_extractor=extractor, **opts))
        assert result.entry is native and result.prepared.videos == opts["videos"]
        result = asyncio.run(media.select_media_model(text, [image], **opts))
        assert result.entry is text and result.prepared.images is None
        decode.assert_not_called()
        result = asyncio.run(media.select_media_model(text, [image], video_frame_extractor=extractor, **opts))
        decode.assert_awaited_once_with(b"one", "video/mp4")
        assert result.entry is image and result.fell_back
        references = prompt(result.prepared)
        assert "video_frames_unavailable" in references and "per_turn_video_preview_limit" in references
        assert '"id": "remote"' in references and "example.invalid" not in references
        assert '"id": "local"' in references and result.prepared.videos is None
    with patch.object(extractor, "_sample") as sample:
        assert asyncio.run(extractor.extract(b"x" * (8 * 1024 * 1024 + 1), "video/mp4")) == ()
        assert asyncio.run(extractor.extract(b"x", "video/webm")) == ()
        sample.assert_not_called()
    result = asyncio.run(media.select_media_model(
        text, [image], messages=MESSAGES, videos=[remote], video_frame_extractor=extractor,
    ))
    assert result.entry is text and result.prepared.images is None
    assert "video_frames_unavailable" in prompt(result.prepared)


def test_host_image_id_remains_available_to_authorized_host_reader(tmp_path):
    from openagent_core.core.agent import _build_runtime_media
    path = tmp_path / 'image.png'
    path.write_bytes(b'PNG ORIGINAL')
    images, _, _, _ = _build_runtime_media([{'file_id': 'host-image-id', 'path': str(path),
        'filename': 'image.png', 'mime_type': 'image/png', 'type': 'image'}])
    selected = asyncio.run(media.select_media_model(entry(), [entry()], messages=MESSAGES, images=images))
    assert 'host-image-id' in prompt(selected.prepared)
    assert selected.prepared.images is None and path.read_bytes() == b'PNG ORIGINAL'


def test_opaque_reference_escaping_and_size_limits():
    original = File(content=b'\x00', id='</attachment_reference>\n"host-id"', filepath='/' + 'x' * 5000,
                    filename='../../客户</attachment>\x00.bin', mime_type='application/octet-stream')
    result = prepare([original])
    raw = result.messages[-1]['content']
    assert raw.count('</attachment_reference>') == 1
    assert 'local_path_omitted' in raw and len(raw) < 2000
    assert '&lt;/attachment_reference&gt;' in raw
    assert original.id == '</attachment_reference>\n"host-id"'
