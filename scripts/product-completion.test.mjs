import { test, after } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';
import { generateKeyPairSync, sign, createHash } from 'node:crypto';
import { createServer } from 'node:http';
const compiler = await import(process.env.TS_COMPILER_PATH ? pathToFileURL(process.env.TS_COMPILER_PATH).href : 'typescript');
const ts = compiler.default;
const dir = await mkdtemp(join(tmpdir(), 'cajeer-rc4-'));
after(() => rm(dir, { recursive: true, force: true }));
async function compile(source, name, replacements = {}) {
  let text = await readFile(new URL(`../${source}`, import.meta.url), 'utf8');
  for (const [a,b] of Object.entries(replacements)) text = text.replaceAll(a,b);
  const file = join(dir, name + '.mjs');
  await writeFile(file, ts.transpileModule(text, { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ESNext } }).outputText);
  return import(pathToFileURL(file).href);
}
const { validateBotForm } = await compile('packages/message-core/src/forms.ts','forms');
const { dispatchProductInteraction, validateConversationAnswer, parseAnswer } = await compile('apps/bot-runtime/src/runtime/product-interactions.ts','interactions',{'@cajeerbots/message-core':'./forms.mjs'});
const { verifyArtifactIntegrity } = await compile('packages/registry/src/integrity.ts','integrity');
const { resolveExecutable, resolveExtensionEntry } = await compile('packages/extension-runtime/src/executable.ts','executable');
const { CajeerClient } = await compile('packages/sdk/src/client.ts','sdk');
// Load the real runtime with its unrelated manifest parser stubbed for this storage-only test.
await compile('packages/extension-runtime/src/runtime-events.ts','runtime-events');
await writeFile(join(dir,'storage-test-db.mjs'), 'export const Prisma = { JsonNull: Symbol.for("Prisma.JsonNull") };');
await writeFile(join(dir,'storage-test-sdk.mjs'), 'export const extensionManifestSchema = { parse() { throw new Error("unexpected manifest parse"); } };');
const { DbExtensionRuntime } = await compile('packages/extension-runtime/src/runtime.ts','extension-runtime',{
  './executable.js':'./executable.mjs', './runtime-events.js':'./runtime-events.mjs', '@cajeerbots/extension-sdk':'./storage-test-sdk.mjs', '@cajeerbots/db':'./storage-test-db.mjs'
});
const account = { id:'bot1', communityId:'c1', provider:'telegram' };
const cmd = { id:'cmd', communityId:'c1', name:'form', handler:'form.start', metadata:{code:'survey'}, scopes:[], cooldownSeconds:0, isEnabled:true };
const form = { id:'f1', code:'survey', communityId:'c1', updatedAt:new Date('2026-01-01'), fields:[{name:'age',label:'Возраст',type:'number',required:true,min:18},{name:'agree',label:'Согласие',type:'boolean',required:true}], isActive:true };
function harness(){
 const states=new Map(), replay=new Map(), submissions=[], events=[];
 const k=(x)=>JSON.stringify([x.communityId,x.provider,x.userId,x.flow]);
 const tx={ $queryRaw:async()=>[1], idempotencyKey:{findUnique:async({where})=>replay.get(where.key),create:async({data})=>{replay.set(data.key,data);return data;}}, conversationState:{findUnique:async({where})=>states.get(k(where.communityId_provider_userId_flow)),upsert:async({create,update,where})=>{const key=k(where.communityId_provider_userId_flow);states.set(key,globalThis.structuredClone(states.has(key)?{...states.get(key),...update}:create));},deleteMany:async({where})=>states.delete(k(where))}, community:{findUnique:async()=>({language:'ru'})}, interactionCommand:{findFirst:async({where})=>where.id===cmd.id?cmd:null}, interactionCallbackAction:{findUnique:async({where})=>where.communityId_code.code==='menu'?{id:'cb',isActive:true,handler:'menu.open',scopes:[],payload:{code:'home'}}:null,findFirst:async()=>null}, interactionMenu:{findFirst:async()=>({title:'Home',buttons:[]})}, messageFormDefinition:{findFirst:async({where})=>where.communityId==='c1'?form:null}, messageFormSubmission:{create:async({data})=>submissions.push(data)}, conversationFlow:{findFirst:async()=>null}, outboxMessage:{create:async({data})=>events.push(data)}, user:{findUnique:async()=>null} };
 const db={$transaction:async(fn)=>fn(tx)};
 const run=(type,name,payload={},command=null,id=undefined,target='chat1',userId='u1')=>dispatchProductInteraction(db,account,{type,name,payload,userId,provider:account.provider},command,target,id);
 return {run,states,submissions,replay,tx};
}
test('typed form rejects invalid boolean and number',()=>{assert.equal(validateBotForm(form.fields,{age:' ',agree:'maybe'}).ok,false);assert.deepEqual(validateBotForm(form.fields,{age:'21',agree:'нет'}).normalized,{age:21,agree:false});});
test('form completes once, replays final response and retains typed data',async()=>{const h=harness();assert.match((await h.run('command','form',{},cmd,'a')).text,/Возраст/);assert.match((await h.run('message','message',{text:'15'},null,'b')).text,/Минимум/);assert.match((await h.run('message','message',{text:'20'},null,'c')).text,/Согласие/);assert.match((await h.run('message','message',{text:'да'},null,'d')).text,/принята/);await h.run('message','message',{text:'да'},null,'d');assert.equal(h.submissions.length,1);assert.deepEqual(h.submissions[0].normalized,{age:20,agree:true});});
test('conversation state isolated by user and chat, cancel clears state',async()=>{const h=harness();await h.run('command','form',{},cmd,'a');assert.equal(await h.run('message','message',{text:'20'},null,'b','other'),undefined);assert.equal(await h.run('message','message',{text:'20'},null,'c','chat1','u2'),undefined);await h.run('command','cancel',{},null,'d');assert.equal(h.states.size,0);});
test('registered callback opens menu; unknown callback cannot dispatch fallback',async()=>{const h=harness();assert.equal((await h.run('callback','menu')).text,'Home');assert.equal(await h.run('callback','debug'),null);});
test('revoked origin prevents resuming form',async()=>{const h=harness();await h.run('command','form',{},cmd);h.tx.interactionCommand.findFirst=async()=>null;assert.match((await h.run('message','message',{text:'20'})).text,/отозван/);assert.equal(h.states.size,0);});
test('conversation validation rejects invalid choices and boolean',()=>{assert.equal(validateConversationAnswer({id:'x',input:'boolean'},'maybe').ok,false);assert.equal(validateConversationAnswer({id:'x',input:'choice',choices:[{label:'A',value:'a'}]},'b').ok,false);assert.deepEqual(validateConversationAnswer({id:'x',input:'number'},'42'),{ok:true,value:42});});
test('Discord answer extracts actual slash option',()=>assert.equal(parseAnswer({provider:'discord',type:'slash_command',name:'answer',userId:'x',payload:{options:[{name:'value',value:'42'}]}}),'42'));
test('Ed25519 integrity rejects modified bytes and incorrect keys',()=>{const {privateKey,publicKey}=generateKeyPairSync('ed25519');const bytes=Buffer.from('bundle');const input={bytes,sha256:createHash('sha256').update(bytes).digest('hex'),signature:sign(null,bytes,privateKey).toString('base64'),signatureAlgorithm:'ed25519',publicKey:publicKey.export({type:'spki',format:'pem'})};assert.equal(verifyArtifactIntegrity(input).ok,true);assert.equal(verifyArtifactIntegrity({...input,bytes:Buffer.from('tampered')}).ok,false);assert.equal(verifyArtifactIntegrity({...input,publicKey:'not a key'}).ok,false);});
test('verified executable runs actual ESM and rejects corrupted code',async()=>{const source='export default { handleEvent: async () => 42 }';const artifactCodeSha256=createHash('sha256').update(source).digest('hex');const manifest={name:'example',entry:'./index.js'};const url=await resolveExecutable({artifactSource:source,artifactCodeSha256},manifest);assert.equal(await (await import(url)).default.handleEvent(),42);await assert.rejects(resolveExecutable({artifactSource:source+' ',artifactCodeSha256},manifest),/integrity/);assert.throws(()=>resolveExtensionEntry('/tmp',{name:'example',entry:'../../bad.js'}),/Unsafe/);});
test('SDK rollback makes authenticated API call with explicit discard policy',async()=>{const server=createServer(async(req,res)=>{let body='';for await(const c of req)body+=c;assert.equal(req.url,'/api/updates/plans/p1/rollback');assert.equal(req.headers['x-api-key'],'token');assert.deepEqual(JSON.parse(body),{discardPending:true});res.setHeader('content-type','application/json');res.end('{"status":"rolled_back"}');});await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));try{const c=new CajeerClient({baseUrl:`http://127.0.0.1:${server.address().port}`,apiKey:'token'});assert.equal((await c.rollbackUpdatePlan('p1',true)).status,'rolled_back');}finally{await new Promise(resolve=>server.close(resolve));}});

test('extension storage reads typed JSON without losing null or falsy values and scopes keys', async () => {
  const values = new Map();
  const id = (key) => JSON.stringify(key);
  const prisma = { extensionStorageItem: {
    findUnique: async ({where}) => values.get(id(where.extensionId_namespace_key)) ?? null,
    upsert: async ({where,create,update}) => {
      const key = id(where.extensionId_namespace_key);
      const row = values.has(key) ? {...values.get(key), ...update} : create;
      values.set(key, {...row, value: row.value === Symbol.for('Prisma.JsonNull') ? null : row.value});
    },
    deleteMany: async ({where}) => values.delete(id(where)),
  }};
  const runtime = new DbExtensionRuntime(prisma);
  const first = runtime.createContext('extension-a', {name:'a'}).storage;
  const second = runtime.createContext('extension-b', {name:'b'}).storage;
  assert.equal(await first.get('settings','missing'), undefined);
  for (const [key,value] of Object.entries({zero:0, disabled:false, empty:'', object:{enabled:true}, array:[1,2]})) {
    await first.set('settings',key,value);
    assert.deepEqual(await first.get('settings',key),value);
    assert.equal(await second.get('settings',key),undefined);
    assert.equal(await first.get('other',key),undefined);
  }
  // Prisma reads a stored JSON null as null; it is distinct from a missing row.
  await first.set('settings','nullable',null);
  assert.equal(await first.get('settings','nullable'),null);
  await first.set('settings','zero',42);
  assert.equal(await first.get('settings','zero'),42);
  await first.delete('settings','zero');
  assert.equal(await first.get('settings','zero'),undefined);
});

const { upsertInteractionCommand, upsertMessageButtonLayout, findMessageButtonLayout } = await compile('packages/db/src/provider-records.ts', 'provider-records');

function providerRecordHarness() {
  const locks = [];
  function model(compoundKey) {
    const rows = [];
    const matches = (row, where) => Object.entries(where).every(([key, value]) => row[key] === value);
    const api = {
      rows,
      findFirst: async ({ where }) => rows.find((row) => matches(row, where)) ?? null,
      create: async ({ data }) => {
        const row = { ...data, id: String(rows.length + 1) };
        rows.push(row);
        return row;
      },
      update: async ({ where, data }) => {
        const row = rows.find((item) => item.id === where.id);
        assert.ok(row);
        Object.assign(row, data);
        return row;
      },
      upsert: async ({ where, create, update }) => {
        const key = where[compoundKey];
        assert.notEqual(key.provider, null);
        assert.notEqual(key.provider, undefined);
        const existing = rows.find((row) => matches(row, key));
        return existing ? api.update({ where: { id: existing.id }, data: update }) : api.create({ data: create });
      },
    };
    return api;
  }
  return {
    locks,
    $queryRaw: async (_sql, key) => { locks.push(key); return [{ '?column?': 1 }]; },
    interactionCommand: model('communityId_provider_name'),
    messageButtonLayout: model('communityId_code_provider_locale'),
  };
}

test('generic command import and repeated seed update by ID without null compound selector', async () => {
  const tx = providerRecordHarness();
  const base = { communityId: 'c1', name: 'help', description: 'original' };
  const first = await upsertInteractionCommand(tx, base, {});
  const second = await upsertInteractionCommand(tx, { ...base, provider: null, description: 'changed' }, { description: 'changed' });
  assert.equal(second.id, first.id);
  assert.equal(second.description, 'changed');
  await upsertInteractionCommand(tx, base, {});
  assert.equal(second.description, 'changed');
  assert.equal(tx.interactionCommand.rows.length, 1);
  assert.equal(tx.locks[0], tx.locks[1]);
  await upsertInteractionCommand(tx, { ...base, provider: 'telegram' }, {});
  await upsertInteractionCommand(tx, { ...base, communityId: 'c2' }, {});
  assert.equal(tx.interactionCommand.rows.length, 3);
});

test('generic button layouts preserve provider, locale and community identity', async () => {
  const tx = providerRecordHarness();
  const base = { communityId: 'c1', code: 'home', locale: 'ru', rows: [['first']] };
  const first = await upsertMessageButtonLayout(tx, base, {});
  const updated = await upsertMessageButtonLayout(tx, { ...base, provider: null }, { rows: [['updated']] });
  assert.equal(updated.id, first.id);
  assert.deepEqual(updated.rows, [['updated']]);
  assert.equal(updated.provider, null);
  await upsertMessageButtonLayout(tx, { ...base, provider: 'vk' }, {});
  await upsertMessageButtonLayout(tx, { ...base, locale: 'en' }, {});
  await upsertMessageButtonLayout(tx, { ...base, communityId: 'c2' }, {});
  assert.equal(tx.messageButtonLayout.rows.length, 4);
  assert.equal(tx.locks[0], tx.locks[1]);
});

test('button lookup uses exact provider then generic fallback, never another provider or community', async () => {
  const tx = providerRecordHarness();
  const base = { communityId: 'c1', code: 'home', locale: 'ru', rows: [] };
  const vk = await upsertMessageButtonLayout(tx, { ...base, provider: 'vk' }, {});
  assert.equal(await findMessageButtonLayout(tx, 'c1', 'home', 'ru', 'telegram'), null);
  const generic = await upsertMessageButtonLayout(tx, base, {});
  assert.equal((await findMessageButtonLayout(tx, 'c1', 'home', 'en', 'telegram')).id, generic.id);
  assert.equal((await findMessageButtonLayout(tx, 'c1', 'home', 'ru', 'vk')).id, vk.id);
  assert.equal((await findMessageButtonLayout(tx, 'c1', 'home', 'ru')).id, generic.id);
  assert.equal(await findMessageButtonLayout(tx, 'c2', 'home', 'ru', 'vk'), null);
});
