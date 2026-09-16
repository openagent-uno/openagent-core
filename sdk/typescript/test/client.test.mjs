import assert from 'node:assert/strict';
import test from 'node:test';
import {createServer} from 'node:http';
import {RunClient, AcceptanceUncertain} from '../dist/index.js';

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
