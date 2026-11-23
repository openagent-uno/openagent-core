import assert from 'node:assert/strict';
import {RunClient, RunHTTPError} from '@openagent/sdk';
const client = new RunClient(process.argv[2], () => ({Authorization:'Bearer test-alice'}));
const request = {run_id:'typescript',session_id:'session',idempotency_key:'typescript',input:'hello'};
await client.submit(request);
assert.equal((await client.wait(request.run_id, undefined, 10)).output, 'alice: hello');
await client.submit(request);
const events = await client.events(request.run_id);
assert(events.some(event => event.kind === 'run.success'));
assert.deepEqual(await client.events(request.run_id, events.at(-1).cursor), []);
const paged = [];
for await (const event of client.iterEvents(request.run_id, 0, 1)) paged.push(event);
assert.deepEqual(paged, events);
const firstPage = await client.eventsPage(request.run_id, 0, 1);
assert.deepEqual(firstPage, {events: events.slice(0, 1), cursor: events[0].cursor, has_more: true});
assert.deepEqual(await client.finalAnswer(request.run_id), {
  run_id:request.run_id, segment_run_id:request.run_id, final_answer:null,
});
await assert.rejects(() => client.submit({...request,input:'changed'}), error => error instanceof RunHTTPError && error.status === 409);
assert.deepEqual(await client.children(request.run_id), []);
console.log('TypeScript SDK authenticated protocol passed');
