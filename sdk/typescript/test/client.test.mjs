import assert from 'node:assert/strict';
import test from 'node:test';
import {createServer} from 'node:http';
import {RunClient, AcceptanceUncertain, RunAdmissionPending} from '../dist/index.js';

test('a lost acceptance does not resubmit a possible external effect', async () => {
  let submissions=0;
  const server=createServer((request,response) => {
    if(request.method==='POST') { submissions++; request.socket.destroy(); return; }
    response.setHeader('Content-Type','application/json');
    response.end(JSON.stringify({run_id:'run',session_id:'s',tenant_id:'t',status:'success',request_digest:'digest',output:'persisted',cancel_requested:false}));
  });
  await new Promise(resolve => server.listen(0,'127.0.0.1',resolve));
  try {
    const client=new RunClient(`http://127.0.0.1:${server.address().port}`,()=>({Authorization:'Bearer fixture'}));
    await assert.rejects(()=>client.submit({run_id:'run',session_id:'s',idempotency_key:'run',input:'execute'}),AcceptanceUncertain);
    assert.equal((await client.get('run')).output,'persisted');
    assert.equal(submissions,1);
  } finally { await new Promise(resolve=>server.close(resolve)); }
});

test('replay stops when cancelled or when the server repeats a cursor', async () => {
  const client = new RunClient('http://localhost', () => ({}));
  let pages = 0;
  client.eventsPage = async (_id, after) => {
    pages++;
    return {events: [{cursor: after + 1}], cursor: after + 1, has_more: true};
  };
  for await (const event of client.iterEvents('run')) {
    if (event.cursor === 12) break;
  }
  assert.equal(pages, 12);
  client.eventsPage = async () => ({events: [], cursor: 0, has_more: true});
  await assert.rejects(() => client.iterEvents('run').next(), /did not advance/);
});

test('a matching deferred-admission receipt exposes the retry interval; other 503s remain uncertain', async () => {
  let submissions = 0;
  const server = createServer(async (request, response) => {
    submissions++;
    let body = '';
    for await (const chunk of request) body += chunk;
    assert.equal(JSON.parse(body).admission_policy, 'defer-unready');
    response.writeHead(503, {'Content-Type': 'application/json', 'Retry-After': '7'});
    response.end(JSON.stringify({error: 'run_admission_pending', retry_after_seconds: submissions === 1 ? 7 : 8}));
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  try {
    const client = new RunClient(`http://127.0.0.1:${server.address().port}`, () => ({}));
    const input = {run_id: 'r', session_id: 's', idempotency_key: 'r', input: 'trial', admission_policy: 'defer-unready'};
    await assert.rejects(() => client.submit(input), error => error instanceof RunAdmissionPending && error.retryAfterSeconds === 7);
    await assert.rejects(() => client.submit(input), AcceptanceUncertain);
    assert.equal(submissions, 2);
  } finally {
    await new Promise(resolve => server.close(resolve));
  }
});
