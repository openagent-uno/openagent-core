import assert from 'node:assert/strict';
import test from 'node:test';
import { classifyModelFailure, reconcileTurnCompletion } from '../dist/index.js';

test('billing failures have a different disposition from transient throttling', () => {
  assert.equal(classifyModelFailure(429, '{"error":{"code":"1113","message":"No resource package."}}'), 'quota_exhausted');
  assert.equal(classifyModelFailure(429, '{"error":{"type":"insufficient_quota"}}'), 'quota_exhausted');
  assert.equal(classifyModelFailure(402, ''), 'quota_exhausted');
  assert.equal(classifyModelFailure(429, '{"error":{"code":"rate_limit_exceeded"}}'), 'transient');
  assert.equal(classifyModelFailure(503, 'invalid tool_choice'), 'transient');
});

test('only an explicit tool-choice rejection is eligible for relaxing tool choice', () => {
  assert.equal(classifyModelFailure(400, '{"error":{"param":"tool_choice","code":"unsupported_value"}}'), 'unsupported_tool_choice');
  assert.equal(classifyModelFailure(422, 'tool_choice is not supported'), 'unsupported_tool_choice');
  assert.equal(classifyModelFailure(400, '{"error":{"message":"Invalid input field"}}'), 'rejected');
  assert.equal(classifyModelFailure(401, 'invalid tool_choice'), 'rejected');
});

test('a turn without final prose preserves confirmed and uncertain tool receipts', () => {
  const confirmed = {state: 'confirmed', value: 'record-a'};
  const uncertain = {state: 'uncertain', value: 'record-b'};
  const rejected = {state: 'rejected', value: 'record-c'};
  assert.deepEqual(reconcileTurnCompletion(' \n', [confirmed, rejected, uncertain]), {
    kind: 'effects', effects: [confirmed, uncertain],
  });
  assert.deepEqual(reconcileTurnCompletion(null, [rejected]), {kind: 'missing'});
  assert.deepEqual(reconcileTurnCompletion('Answer', [confirmed]), {kind: 'answer', text: 'Answer'});
});
