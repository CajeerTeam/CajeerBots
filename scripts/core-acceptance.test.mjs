import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile, readdir } from 'node:fs/promises';
import { createServer } from 'node:http';
import { waitHealth } from './runtime-processes.mjs';
import { stableVersion } from './stable-evidence.mjs';

const root = new URL('../', import.meta.url);
test('release metadata agrees across all workspace packages, CLI, API and deployment', async () => {
  const read = path => readFile(new URL(path, root), 'utf8');
  const version = (await read('VERSION')).trim();
  assert.equal(version, stableVersion);
  assert.equal(JSON.parse(await read('package.json')).version, version);
  for (const group of ['apps','packages','examples']) {
    for (const dir of await readdir(new URL(group+'/',root), { withFileTypes:true })) {
      if (!dir.isDirectory()) continue;
      const path = `${group}/${dir.name}/package.json`;
      let raw;
      try { raw=await read(path); } catch (e) { if(e.code==='ENOENT') continue; throw e; }
      assert.equal(JSON.parse(raw).version,version,path);
    }
  }
  for (const path of ['apps/cli/src/main.ts','apps/api/src/modules/observability/system.controller.ts','docker-compose.prod.yml','docker-compose.yml','contracts/openapi/cajeerbots.openapi.yaml']) assert((await read(path)).includes(version),path);
});

test('readiness rejects HTTP 200 with unhealthy body and accepts real healthy response', async t => {
  let calls=0;
  const server=createServer((_req,res)=>{res.setHeader('content-type','application/json');res.end(JSON.stringify({ok:++calls>1}));});
  await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
  t.after(()=>new Promise(resolve=>server.close(resolve)));
  const service={name:'test',child:{exitCode:null,signalCode:null}};
  assert.deepEqual(await waitHealth(service,`http://127.0.0.1:${server.address().port}/health`,3000),{ok:true});
  assert.equal(calls,2);
  service.result={code:1,error:'spawn failed'};
  await assert.rejects(waitHealth(service,'http://127.0.0.1:1',3000),/exited before readiness/);
});
