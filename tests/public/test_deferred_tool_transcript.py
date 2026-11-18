from copy import deepcopy
from openagent_core.session_transcript import defer_tool_message_content


def test_linked_tool_body_is_deferred_without_losing_ordered_media_or_mutating_storage_projection():
    message = {'role': 'tool', 'text': 'large result' * 10000,
        'tool_invocation_id': 'tool-one', 'tool_summary': {'id': 'tool-one', 'status': 'success'},
        'parts': [{'kind': 'text', 'text': 'large result' * 10000},
            {'kind': 'attachment', 'attachment': {'artifact_id': 'image'}},
            {'kind': 'ui_view', 'view_id': 'view', 'revision': 2}],
        'attachments': [{'artifact_id': 'image'}], 'model': 'provider:model'}
    original = deepcopy(message)
    result = defer_tool_message_content(message)
    assert result['text'] == '' and result['parts'] == original['parts'][1:]
    assert result['attachments'] == original['attachments']
    assert result['tool_summary'] == {'id': 'tool-one', 'status': 'success', 'details_deferred': True}
    assert message == original


def test_human_assistant_unlinked_and_mismatched_tool_messages_are_unchanged():
    for role, invocation, summary in [('user', 'one', {'id': 'one'}),
            ('assistant', 'one', {'id': 'one'}), ('tool', None, None),
            ('tool', 'one', {'id': 'other'})]:
        message = {'role': role, 'text': 'full text', 'tool_invocation_id': invocation,
                   'tool_summary': summary, 'parts': [{'kind': 'text', 'text': 'full text'}]}
        assert defer_tool_message_content(message) is message
